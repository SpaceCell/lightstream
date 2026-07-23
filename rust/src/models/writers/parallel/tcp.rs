// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Parallel TCP table writer
//!
//! Fans one table sequence across several concurrent TCP connections to a
//! single endpoint. TCP has no in-band stream multiplexing, so each "stream"
//! is its own connection running its own [`TcpTableWriter`](crate::models::writers::tcp::TcpTableWriter) driven by a
//! dedicated task. The connections send in parallel and aggregate throughput
//! is the sum across them.
//!
//! Order is preserved within a connection. Global write order across the set
//! is recovered by the receiver under
//! [`SortBehaviour::Ordered`](crate::traits::parallel_transport_reader::SortBehaviour::Ordered),
//! which pulls the connections in this writer's round-robin rotation.

use std::io;
use std::net::SocketAddr;

use minarrow::{Field, Table, TableV};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::compression::Compression;
use crate::models::writers::tcp::TcpTableWriter;
use crate::traits::parallel_transport_writer::{ParallelTransportWriter, SEQ_ID_META_KEY};
use crate::traits::transport_writer::IPCTransportWriter;

/// Bounded depth per connection channel. Lets the producer pipeline a few
/// tables ahead of each connection task without unbounded buffering.
const STREAM_CHANNEL_DEPTH: usize = 8;

/// Async Arrow IPC writer that distributes tables across several concurrent
/// TCP connections to one endpoint.
///
/// Open with [`TcpParallelTableWriter::connect`], write tables with
/// [`write_table`](ParallelTransportWriter::write_table), then
/// [`finish`](ParallelTransportWriter::finish) to flush and close every
/// connection.
pub struct TcpParallelTableWriter {
    schema: Vec<Field>,
    senders: Vec<mpsc::Sender<(TableV, Option<u64>)>>,
    tasks: Vec<JoinHandle<io::Result<()>>>,
    next: usize,
    /// When set, each table is tagged with a monotonic sequence id on its
    /// record batch message so the receiver can recover global write order.
    ordered: bool,
}

impl TcpParallelTableWriter {
    /// Open `stream_count` TCP connections to `addr` and prepare to distribute
    /// tables across them. Pass `None` for `compression` to write uncompressed
    /// batches.
    ///
    /// `dictionaries` carries `(dict_id, values)` pairs for categorical
    /// columns. Each pair is registered on every connection so any connection
    /// can encode the dictionary-backed columns.
    ///
    /// Connections open in order, so connection `i` pairs with the `i`-th
    /// connection the receiver accepts.
    pub async fn connect(
        addr: SocketAddr,
        stream_count: usize,
        schema: Vec<Field>,
        dictionaries: Vec<(i64, Vec<String>)>,
        compression: Option<Compression>,
    ) -> io::Result<Self> {
        assert!(stream_count >= 1, "stream_count must be at least 1");
        let mut senders = Vec::with_capacity(stream_count);
        let mut tasks = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            let mut writer = TcpTableWriter::connect(addr, schema.clone(), compression).await?;
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
    /// custom_metadata (`ls.seq_id`). The receiver reads the id to recover the
    /// global write order across connections.
    pub async fn connect_ordered(
        addr: SocketAddr,
        stream_count: usize,
        schema: Vec<Field>,
        dictionaries: Vec<(i64, Vec<String>)>,
        compression: Option<Compression>,
    ) -> io::Result<Self> {
        let mut writer =
            Self::connect(addr, stream_count, schema, dictionaries, compression).await?;
        writer.ordered = true;
        Ok(writer)
    }
}

impl ParallelTransportWriter for TcpParallelTableWriter {
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
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "TCP connection task closed"))
    }

    async fn write_all_tables(&mut self, tables: Vec<Table>) -> io::Result<()> {
        for table in tables {
            self.write_table(table).await?;
        }
        Ok(())
    }

    async fn finish(mut self) -> io::Result<()> {
        // Drop the senders so each connection task's receive loop ends and the
        // task flushes and closes its connection.
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
