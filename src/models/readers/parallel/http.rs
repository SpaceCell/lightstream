//! # Parallel HTTP/2 table reader
//!
//! Accepts several concurrent HTTP/2 request streams on one server
//! connection and decodes them across cores, one task per stream, merging
//! the results through a channel into a single table stream. Each table is
//! paired with its sequence key - `Some` when the peer used an ordered
//! writer, `None` otherwise.
//!
//! The h2 server connection is the I/O driver for the in-flight request
//! bodies, so a background task keeps it polled while the accepted streams
//! decode. Tables arrive in the order the streams produce them; read with
//! [`SortBehaviour::Auto`] or sort on the keys to recover global write order.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use http::Response;
use minarrow::{Table, Vec64};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::enums::{BufferChunkSize, IPCMessageProtocol};
use crate::models::decoders::ipc::table_stream_decoder::TableStreamDecoder;
use crate::models::streams::http::{H2RecvRead, HttpByteStream};
use crate::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};
use crate::traits::parallel_transport_writer::SEQ_ID_META_KEY;

/// Per-stream flow-control window the server advertises on the handshake.
/// HTTP/2 upload throughput is bounded by the receiver's window, and h2's
/// 64 KiB default forces a WINDOW_UPDATE round-trip every 64 KiB. At 8 MiB
/// a multi-MiB Arrow batch ships without that stall, which on a
/// few-millisecond cross-host link would otherwise dominate the transfer.
const STREAM_WINDOW_BYTES: u32 = 8 * 1024 * 1024;

/// Bounded depth of the merge channel. Lets each stream task decode a few
/// tables ahead of the consumer without unbounded buffering.
const MERGE_CHANNEL_DEPTH: usize = 8;

/// Async Arrow IPC reader that decodes several concurrent HTTP/2 request
/// streams on one server connection in parallel and merges them into a
/// single table stream.
pub struct HttpParallelTableReader {
    rx: mpsc::Receiver<io::Result<(Table, Option<u64>)>>,
    tasks: Vec<JoinHandle<()>>,
    driver: JoinHandle<()>,
    stream_count: usize,
    sort: SortBehaviour,
}

impl HttpParallelTableReader {
    /// Accept `stream_count` request streams on an established h2 server
    /// `connection` and decode each on its own task. A headers-only 200 is
    /// returned on each stream so the client's response drain resolves while
    /// the request body keeps uploading. `sort` selects whether sequence keys
    /// are surfaced and whether `read_all_tables` returns them sorted.
    pub async fn accept<T>(
        mut connection: h2::server::Connection<T, Bytes>,
        stream_count: usize,
        sort: SortBehaviour,
    ) -> io::Result<Self>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        assert!(stream_count >= 1, "stream_count must be at least 1");
        let (tx, rx) = mpsc::channel(MERGE_CHANNEL_DEPTH);
        let mut tasks = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            let (request, mut respond) = connection
                .accept()
                .await
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "h2 connection closed before all streams were accepted",
                    )
                })?
                .map_err(io::Error::other)?;
            let response = Response::builder()
                .status(200)
                .body(())
                .map_err(io::Error::other)?;
            respond
                .send_response(response, true)
                .map_err(io::Error::other)?;
            let mut decoder = TableStreamDecoder::<Vec64<u8>>::new(
                HttpByteStream::new(H2RecvRead::new(request.into_body()), BufferChunkSize::Http),
                BufferChunkSize::Http.chunk_size(),
                IPCMessageProtocol::Stream,
                None,
            );
            let tx = tx.clone();
            let task = tokio::spawn(async move {
                loop {
                    match decoder.read_keyed().await {
                        Some(Ok((table, kv))) => {
                            let seq = if sort == SortBehaviour::None {
                                None
                            } else {
                                kv.and_then(|pairs| {
                                    pairs
                                        .into_iter()
                                        .find(|k| k.key == SEQ_ID_META_KEY)
                                        .and_then(|k| k.value.parse::<u64>().ok())
                                })
                            };
                            if tx.send(Ok((table, seq))).await.is_err() {
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            let _ = tx.send(Err(e)).await;
                            break;
                        }
                        None => break,
                    }
                }
            });
            tasks.push(task);
        }
        // Keep polling the connection so the accepted request bodies receive
        // their data frames. The loop ends when the peer closes; Drop aborts
        // it otherwise.
        let driver = tokio::spawn(async move { while connection.accept().await.is_some() {} });
        Ok(Self { rx, tasks, driver, stream_count, sort })
    }

    /// Run the h2 server handshake on an accepted TCP stream with
    /// upload-sized flow-control windows, then accept `stream_count` request
    /// streams. POST throughput is governed by the server's flow-control
    /// window, so this advertises [`STREAM_WINDOW_BYTES`] per stream and
    /// scales the connection window with `stream_count`.
    pub async fn from_tcp(
        tcp: TcpStream,
        stream_count: usize,
        sort: SortBehaviour,
    ) -> io::Result<Self> {
        let connection_window =
            (stream_count as u64 * STREAM_WINDOW_BYTES as u64).min(u32::MAX as u64) as u32;
        let connection = h2::server::Builder::new()
            .initial_window_size(STREAM_WINDOW_BYTES)
            .initial_connection_window_size(connection_window)
            .handshake::<_, Bytes>(tcp)
            .await
            .map_err(io::Error::other)?;
        Self::accept(connection, stream_count, sort).await
    }
}

impl Stream for HttpParallelTableReader {
    type Item = io::Result<(Table, Option<u64>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().rx.poll_recv(cx)
    }
}

impl ParallelTransportReader for HttpParallelTableReader {
    fn stream_count(&self) -> usize {
        self.stream_count
    }

    async fn read_all_tables(mut self) -> io::Result<Vec<(Table, Option<u64>)>> {
        let mut out = Vec::new();
        while let Some(item) = self.rx.recv().await {
            out.push(item?);
        }
        if self.sort == SortBehaviour::Auto {
            out.sort_by_key(|(_, seq)| (*seq).unwrap_or(u64::MAX));
        }
        Ok(out)
    }
}

impl Drop for HttpParallelTableReader {
    fn drop(&mut self) {
        self.driver.abort();
        for task in &self.tasks {
            task.abort();
        }
    }
}
