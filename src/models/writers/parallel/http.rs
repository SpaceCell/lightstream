// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Parallel HTTP/2 table writer
//!
//! Fans one table sequence across several concurrent HTTP/2 request
//! streams on a single h2 client connection. Each stream runs its own
//! [`HttpTableWriter`](crate::models::writers::http::HttpTableWriter) driven by a dedicated task, so the streams upload
//! in parallel and aggregate throughput is the sum across them.
//!
//! Each stream carries an independent ordered sequence of batches;
//! ordering is preserved within a stream, not across the set. The
//! receiver merges the streams back into one table stream.

use std::io;

use http::{Method, Request, Uri};
use minarrow::{Field, Table, TableV};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::compression::Compression;
use crate::models::writers::http::HttpTableWriter;
use crate::traits::parallel_transport_writer::{ParallelTransportWriter, SEQ_ID_META_KEY};
use crate::traits::transport_writer::IPCTransportWriter;

/// Bounded depth per stream channel. Lets the producer pipeline a few
/// tables ahead of each stream task without unbounded buffering.
const STREAM_CHANNEL_DEPTH: usize = 8;

/// Async Arrow IPC writer that distributes tables across several
/// concurrent HTTP/2 request streams on one client connection.
///
/// Open with [`HttpParallelTableWriter::connect`], write tables with
/// [`write_table`](ParallelTransportWriter::write_table), then
/// [`finish`](ParallelTransportWriter::finish) to flush and close every
/// stream.
pub struct HttpParallelTableWriter {
    schema: Vec<Field>,
    senders: Vec<mpsc::Sender<(TableV, Option<u64>)>>,
    tasks: Vec<JoinHandle<io::Result<()>>>,
    next: usize,
    /// When set, each table is tagged with a monotonic sequence id on its
    /// record batch message so the receiver can recover global write order.
    ordered: bool,
}

impl HttpParallelTableWriter {
    /// Connect to an `http://` URL over plaintext h2c and open
    /// `stream_count` POST request streams to distribute tables across.
    /// Pass `None` for `compression` to write uncompressed batches.
    ///
    /// `dictionaries` carries `(dict_id, values)` pairs for categorical
    /// columns. Each pair is registered on every stream so any stream can
    /// encode the dictionary-backed columns.
    pub async fn connect(
        url: &str,
        stream_count: usize,
        schema: Vec<Field>,
        dictionaries: Vec<(i64, Vec<String>)>,
        compression: Option<Compression>,
    ) -> io::Result<Self> {
        assert!(stream_count >= 1, "stream_count must be at least 1");

        let uri: Uri = url
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let host = uri
            .host()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "uri missing host"))?
            .to_string();
        let port = uri.port_u16().unwrap_or(80);

        let tcp = TcpStream::connect((host.as_str(), port)).await?;
        let (mut send_request, conn) = h2::client::handshake(tcp)
            .await
            .map_err(io::Error::other)?;
        // One driver pumps I/O for every request stream on this connection.
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let mut senders = Vec::with_capacity(stream_count);
        let mut tasks = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            let req = Request::builder()
                .method(Method::POST)
                .uri(uri.clone())
                .body(())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            // The Arrow IPC body follows via send_data, so end_of_stream stays false.
            let (response, send_stream) = send_request
                .send_request(req, false)
                .map_err(io::Error::other)?;
            let mut writer = HttpTableWriter::from_send_stream(
                send_stream,
                response,
                schema.clone(),
                compression,
            )?;
            for (dict_id, values) in &dictionaries {
                writer.register_dictionary(*dict_id, values.clone());
            }
            let (tx, mut rx) = mpsc::channel::<(TableV, Option<u64>)>(STREAM_CHANNEL_DEPTH);
            let task = tokio::spawn(async move {
                while let Some((table, seq)) = rx.recv().await {
                    match seq {
                        Some(seq) => {
                            writer
                                .write_table_with_metadata(
                                    table,
                                    vec![(SEQ_ID_META_KEY.to_string(), seq.to_string())],
                                )
                                .await?
                        }
                        None => writer.write_table(table).await?,
                    }
                }
                writer.finish().await
            });
            senders.push(tx);
            tasks.push(task);
        }
        Ok(Self { schema, senders, tasks, next: 0, ordered: false })
    }

    /// As [`connect`](Self::connect), but tags each table with a monotonic
    /// sequence id carried on its record batch message envelope as Arrow
    /// custom_metadata (`ls.seq_id`). The receiver reads the id and sorts on
    /// it to recover the global write order across streams.
    pub async fn connect_ordered(
        url: &str,
        stream_count: usize,
        schema: Vec<Field>,
        dictionaries: Vec<(i64, Vec<String>)>,
        compression: Option<Compression>,
    ) -> io::Result<Self> {
        let mut writer =
            Self::connect(url, stream_count, schema, dictionaries, compression).await?;
        writer.ordered = true;
        Ok(writer)
    }
}

impl ParallelTransportWriter for HttpParallelTableWriter {
    fn schema(&self) -> &[Field] {
        &self.schema
    }

    fn stream_count(&self) -> usize {
        self.senders.len()
    }

    async fn write_table(&mut self, table: impl Into<TableV> + Send) -> io::Result<()> {
        let seq = if self.ordered { Some(self.next as u64) } else { None };
        let idx = self.next % self.senders.len();
        self.next = self.next.wrapping_add(1);
        self.senders[idx]
            .send((table.into(), seq))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "HTTP/2 stream task closed"))
    }

    async fn write_all_tables(&mut self, tables: Vec<Table>) -> io::Result<()> {
        for table in tables {
            self.write_table(table).await?;
        }
        Ok(())
    }

    async fn finish(mut self) -> io::Result<()> {
        // Drop the senders so each stream task's receive loop ends and the
        // task flushes and closes its request stream.
        self.senders.clear();
        let mut first_err: Option<io::Error> = None;
        for task in self.tasks.drain(..) {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(join_err) => {
                    if first_err.is_none() {
                        first_err = Some(io::Error::other(join_err));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
