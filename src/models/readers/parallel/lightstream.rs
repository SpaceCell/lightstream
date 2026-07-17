// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Parallel Lightstream protocol reader
//!
//! Accepts several concurrent Lightstream protocol connections on a
//! [`TcpListener`](tokio::net::TcpListener) and decodes them across cores, one task per connection.
//! Each task feeds its own channel, and the reader merges the channels into a
//! single frame stream of [`LightstreamMessage`](crate::models::frames::lightstream_message::LightstreamMessage) values, so protobuf messages
//! and Arrow tables share the wire.
//!
//! Message and table types are registered on every connection at
//! [`accept`](crate::models::readers::parallel::lightstream::LightstreamParallelReader::accept). Under [`SortBehaviour::None`](crate::traits::parallel_transport_reader::SortBehaviour::None)
//! and [`SortBehaviour::RequestKeys`](crate::traits::parallel_transport_reader::SortBehaviour::RequestKeys) frames surface in the order the
//! connections produce them. Under [`SortBehaviour::Ordered`](crate::traits::parallel_transport_reader::SortBehaviour::Ordered) the reader pulls
//! the connections in the writer's round-robin rotation, so frames surface in
//! global send order. Each connection announces its index before any frames,
//! so it is placed by that index rather than by accept order - the global order
//! holds regardless of the order the connections are established.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_util::StreamExt;
use minarrow::{Field, Vec64};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::models::frames::lightstream_message::LightstreamMessage;
use crate::models::decoders::limits::DecodeLimits;
use crate::models::readers::lightstream::LightstreamReader;
use crate::traits::parallel_transport_reader::SortBehaviour;

/// Bounded depth of each per-connection channel. Lets a connection task decode
/// a few frames ahead of the consumer without unbounded buffering, and applies
/// backpressure to a connection that runs ahead of the rotation.
const STREAM_CHANNEL_DEPTH: usize = 8;

type StreamItem = io::Result<LightstreamMessage>;

/// Async Lightstream protocol reader that decodes several concurrent
/// connections in parallel and merges them into a single frame stream.
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
    /// registering each name in `messages` and each `(name, schema)` in
    /// `tables` on every connection in that order, and decode each on its own
    /// task. `sort` selects whether frames are emitted in global send order.
    pub async fn accept(
        listener: &TcpListener,
        stream_count: usize,
        messages: &[&str],
        tables: &[(&str, Vec<Field>)],
        sort: SortBehaviour,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        assert!(stream_count >= 1, "stream_count must be at least 1");
        let mut slots: Vec<Option<mpsc::Receiver<StreamItem>>> =
            (0..stream_count).map(|_| None).collect();
        let mut tasks = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            let (socket, _peer) = listener.accept().await?;
            let (mut read_half, _write_half) = socket.into_split();
            // Read the connection's announced index, then slot it by that index
            // so the writer-to-reader connection mapping does not depend on the
            // order connections were accepted.
            let mut index_byte = [0u8; 1];
            read_half.read_exact(&mut index_byte).await?;
            let index = index_byte[0] as usize;
            if index >= stream_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("connection index {index} out of range for {stream_count} streams"),
                ));
            }
            if slots[index].is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("duplicate connection index {index}"),
                ));
            }
            let mut reader = LightstreamReader::<Vec64<u8>>::new(read_half, limits);
            for name in messages {
                reader.register_message(*name);
            }
            for (name, schema) in tables {
                reader.register_table(*name, schema.clone());
            }
            let (tx, rx) = mpsc::channel(STREAM_CHANNEL_DEPTH);
            let task = tokio::spawn(async move {
                while let Some(item) = reader.next().await {
                    match item {
                        Ok(message) => {
                            if tx.send(Ok(message)).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            break;
                        }
                    }
                }
            });
            slots[index] = Some(rx);
            tasks.push(task);
        }
        let streams: Vec<mpsc::Receiver<StreamItem>> =
            slots.into_iter().map(|slot| slot.expect("every connection index filled")).collect();
        Ok(Self {
            streams,
            tasks,
            stream_count,
            sort,
            cursor: 0,
            closed: vec![false; stream_count],
        })
    }

    /// Number of connections being merged.
    pub fn stream_count(&self) -> usize {
        self.stream_count
    }

    /// Read every connection to completion and return the merged frames.
    pub async fn read_all(mut self) -> io::Result<Vec<LightstreamMessage>> {
        let mut out = Vec::new();
        while let Some(item) = self.next().await {
            out.push(item?);
        }
        Ok(out)
    }
}

impl Stream for LightstreamParallelReader {
    type Item = StreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.sort == SortBehaviour::Ordered {
            // Pull the connections in the writer's rotation. The next frame in
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
        // ready frame and ending once every connection has closed.
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

impl Drop for LightstreamParallelReader {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
