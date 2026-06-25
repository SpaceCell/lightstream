//! # Parallel QUIC table reader
//!
//! Accepts several concurrent QUIC streams on a single
//! [`quinn::Connection`] and decodes them across cores, one task per
//! stream, merging the results through a channel into a single table
//! stream. Each table is paired with its sequence key - `Some` when the
//! peer used an ordered writer, `None` otherwise.
//!
//! Tables arrive in the order the streams produce them. To recover the
//! global write order across streams, read with [`SortBehaviour::Auto`]
//! or sort on the keys yourself.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::{Table, Vec64};
use quinn::Connection;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::enums::{BufferChunkSize, IPCMessageProtocol};
use crate::models::decoders::ipc::table_stream_decoder::TableStreamDecoder;
use crate::models::streams::quic::QuicByteStream;
use crate::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};
use crate::traits::parallel_transport_writer::SEQ_ID_META_KEY;

/// Bounded depth of the merge channel. Lets each stream task decode a few
/// tables ahead of the consumer without unbounded buffering.
const MERGE_CHANNEL_DEPTH: usize = 8;

/// Async Arrow IPC reader that decodes several concurrent QUIC streams on
/// one connection in parallel and merges them into a single table stream.
pub struct QuicParallelTableReader {
    rx: mpsc::Receiver<io::Result<(Table, Option<u64>)>>,
    tasks: Vec<JoinHandle<()>>,
    stream_count: usize,
    sort: SortBehaviour,
}

impl QuicParallelTableReader {
    /// Accept `stream_count` unidirectional QUIC streams on `conn` and decode
    /// each on its own task. `sort` selects whether sequence keys are surfaced
    /// and whether [`read_all_tables`](ParallelTransportReader::read_all_tables)
    /// returns them sorted.
    pub async fn accept(
        conn: &Connection,
        stream_count: usize,
        sort: SortBehaviour,
    ) -> io::Result<Self> {
        assert!(stream_count >= 1, "stream_count must be at least 1");
        let (tx, rx) = mpsc::channel(MERGE_CHANNEL_DEPTH);
        let mut tasks = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            let recv = conn.accept_uni().await.map_err(io::Error::other)?;
            let mut decoder = TableStreamDecoder::<Vec64<u8>>::new(
                QuicByteStream::new(recv, BufferChunkSize::WebTransport),
                BufferChunkSize::WebTransport.chunk_size(),
                IPCMessageProtocol::Stream,
                None,
            );
            let tx = tx.clone();
            let task = tokio::spawn(async move {
                loop {
                    match decoder.read_keyed().await {
                        Some(Ok((table, kv))) => {
                            // The sequence key is read here from this task's
                            // own decoder output and forwarded with its table.
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
        Ok(Self { rx, tasks, stream_count, sort })
    }
}

impl Stream for QuicParallelTableReader {
    type Item = io::Result<(Table, Option<u64>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().rx.poll_recv(cx)
    }
}

impl ParallelTransportReader for QuicParallelTableReader {
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

impl Drop for QuicParallelTableReader {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
