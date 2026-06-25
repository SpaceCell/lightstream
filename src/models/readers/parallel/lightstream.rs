//! # Parallel Lightstream protocol reader
//!
//! Accepts several concurrent Lightstream protocol connections on a
//! [`TcpListener`] and decodes them across cores, one task per connection.
//! Each task feeds its own channel, and the reader merges the channels into a
//! single table stream.
//!
//! A single table type is registered on every connection at
//! [`accept`](LightstreamParallelReader::accept). Tables pair with a key of
//! `None`, since the protocol frame carries no per-table sequence. Under
//! [`SortBehaviour::None`] and [`SortBehaviour::RequestKeys`] tables surface in
//! the order the connections produce them. Under [`SortBehaviour::Ordered`] the
//! reader pulls the connections in the writer's round-robin rotation, so tables
//! surface in global write order. Connections are accepted in order, so the
//! `i`-th accepted connection pairs with the writer's `i`-th connection.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_util::StreamExt;
use minarrow::{Field, Table, Vec64};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::models::readers::lightstream::LightstreamReader;
use crate::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};

/// Bounded depth of each per-connection channel. Lets a connection task decode
/// a few tables ahead of the consumer without unbounded buffering, and applies
/// backpressure to a connection that runs ahead of the rotation.
const STREAM_CHANNEL_DEPTH: usize = 8;

type StreamItem = io::Result<(Table, Option<u64>)>;

/// Async Lightstream protocol reader that decodes several concurrent
/// connections in parallel and merges them into a single table stream.
pub struct LightstreamParallelReader {
    streams: Vec<mpsc::Receiver<StreamItem>>,
    tasks: Vec<JoinHandle<()>>,
    stream_count: usize,
    sort: SortBehaviour,
    /// Next connection to pull. Under `Ordered` this walks the writer's
    /// rotation; otherwise it rotates the starting point so no connection is
    /// starved.
    cursor: usize,
    /// Tracks which connections have closed, used by the arrival-order merge to
    /// end once every connection is drained.
    closed: Vec<bool>,
}

impl LightstreamParallelReader {
    /// Accept `stream_count` Lightstream protocol connections on `listener`,
    /// register `type_name` with `schema` on each, and decode each on its own
    /// task. `sort` selects whether tables are emitted in global write order.
    pub async fn accept(
        listener: &TcpListener,
        stream_count: usize,
        type_name: &str,
        schema: Vec<Field>,
        sort: SortBehaviour,
    ) -> io::Result<Self> {
        assert!(stream_count >= 1, "stream_count must be at least 1");
        let mut streams = Vec::with_capacity(stream_count);
        let mut tasks = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            let (socket, _peer) = listener.accept().await?;
            let (read_half, _write_half) = socket.into_split();
            let mut reader = LightstreamReader::<Vec64<u8>>::new(read_half);
            reader.register_table(type_name, schema.clone());
            let (tx, rx) = mpsc::channel(STREAM_CHANNEL_DEPTH);
            let task = tokio::spawn(async move {
                while let Some(item) = reader.next().await {
                    match item {
                        Ok(message) => {
                            // Only tables are part of the merged table stream;
                            // other message types are skipped.
                            if let Some(table) = message.into_table() {
                                if tx.send(Ok((table, None))).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            break;
                        }
                    }
                }
            });
            streams.push(rx);
            tasks.push(task);
        }
        Ok(Self {
            streams,
            tasks,
            stream_count,
            sort,
            cursor: 0,
            closed: vec![false; stream_count],
        })
    }
}

impl Stream for LightstreamParallelReader {
    type Item = StreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.sort == SortBehaviour::Ordered {
            // Pull the connections in the writer's rotation. The next table in
            // global order is always the head of connection
            // `cursor % stream_count`, so a single targeted recv yields it. A
            // closed target means that position will never arrive, ending the
            // merged stream.
            let idx = this.cursor % this.stream_count;
            return match this.streams[idx].poll_recv(cx) {
                Poll::Ready(Some(item)) => {
                    this.cursor += 1;
                    Poll::Ready(Some(item))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }

        // Arrival-order merge. Scan the connections from a rotating start so a
        // single busy connection cannot starve the others, returning the first
        // ready table and ending once every connection has closed.
        let n = this.stream_count;
        let mut any_pending = false;
        for offset in 0..n {
            let idx = (this.cursor + offset) % n;
            if this.closed[idx] {
                continue;
            }
            match this.streams[idx].poll_recv(cx) {
                Poll::Ready(Some(item)) => {
                    this.cursor = (idx + 1) % n;
                    return Poll::Ready(Some(item));
                }
                Poll::Ready(None) => this.closed[idx] = true,
                Poll::Pending => any_pending = true,
            }
        }
        if any_pending {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}

impl ParallelTransportReader for LightstreamParallelReader {
    fn stream_count(&self) -> usize {
        self.stream_count
    }

    async fn read_all_tables(mut self) -> io::Result<Vec<(Table, Option<u64>)>> {
        let mut out = Vec::new();
        while let Some(item) = self.next().await {
            out.push(item?);
        }
        Ok(out)
    }
}

impl Drop for LightstreamParallelReader {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
