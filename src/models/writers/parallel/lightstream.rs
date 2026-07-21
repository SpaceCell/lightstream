// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Parallel Lightstream protocol writer
//!
//! Fans one frame sequence across several concurrent Lightstream protocol
//! connections to a single endpoint. Each connection runs its own
//! [`LightstreamWriter`](crate::models::writers::lightstream::LightstreamWriter) driven by a dedicated task, so the connections send
//! in parallel and aggregate throughput is the sum across them.
//!
//! Message types and table types are registered on every connection at
//! [`connect`](crate::models::writers::parallel::lightstream::LightstreamParallelWriter::connect). Callers then send by type
//! name - [`send_table`](crate::models::writers::parallel::lightstream::LightstreamParallelWriter::send_table) for Arrow
//! tables, [`send_message`](crate::models::writers::parallel::lightstream::LightstreamParallelWriter::send_message) and
//! [`send_proto`](crate::models::writers::parallel::lightstream::LightstreamParallelWriter::send_proto) for messages - so
//! protobuf messages and Arrow tables share the wire. Frames route round-robin
//! in send order. Order within a connection is preserved; global order across
//! the set is recovered by the receiver under
//! [`SortBehaviour::Ordered`](crate::traits::parallel_transport_reader::SortBehaviour::Ordered),
//! which pulls the connections in this writer's round-robin rotation.
//!
//! Each connection announces its index before any frames, so the receiver
//! pairs it by index regardless of the order connections are accepted.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;

use minarrow::{Field, TableV, Vec64};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::models::frames::lightstream_message::LightstreamMessage;
use crate::models::writers::lightstream::LightstreamWriter;

/// Bounded depth per connection channel. Lets the producer pipeline a few
/// frames ahead of each connection task without unbounded buffering.
const STREAM_CHANNEL_DEPTH: usize = 8;

/// Async Lightstream protocol writer that distributes messages and tables
/// across several concurrent connections to one endpoint.
///
/// Open with [`LightstreamParallelWriter::connect`], send by type name with
/// [`send_table`](Self::send_table) / [`send_message`](Self::send_message)
/// (and [`send_proto`](Self::send_proto) under the `protobuf` feature), then
/// [`finish`](Self::finish) to flush and close every connection.
pub struct LightstreamParallelWriter {
    senders: Vec<mpsc::Sender<LightstreamMessage>>,
    tasks: Vec<JoinHandle<io::Result<()>>>,
    /// Type registry mapping a registered name to its wire tag.
    registry: HashMap<String, u8>,
    next: usize,
}

impl LightstreamParallelWriter {
    /// Open `stream_count` Lightstream protocol connections to `addr`,
    /// registering each name in `messages` and each `(name, schema)` in
    /// `tables` on every connection in that order. Each connection announces
    /// its index before any frames, so the receiver pairs it by index
    /// regardless of the order connections are accepted.
    pub async fn connect(
        addr: SocketAddr,
        stream_count: usize,
        messages: &[&str],
        tables: &[(&str, Vec<Field>)],
    ) -> io::Result<Self> {
        assert!(stream_count >= 1, "stream_count must be at least 1");
        assert!(stream_count <= 256, "stream_count must be at most 256");
        let mut senders = Vec::with_capacity(stream_count);
        let mut tasks = Vec::with_capacity(stream_count);
        let mut registry = HashMap::new();
        for index in 0..stream_count {
            let stream = TcpStream::connect(addr).await?;
            let (_read, mut write) = stream.into_split();
            // Announce this connection's index before any frames, so the
            // receiver places it by index rather than by accept order.
            write.write_all(&[index as u8]).await?;
            let mut writer = LightstreamWriter::<_, Vec64<u8>>::new(write);
            // Every connection registers the same types in the same order, so
            // the tags match. Record the name to tag mapping once.
            for name in messages {
                let tag = writer.register_message(*name);
                if index == 0 {
                    registry.insert(name.to_string(), tag);
                }
            }
            for (name, schema) in tables {
                let tag = writer.register_table(*name, schema.clone());
                if index == 0 {
                    registry.insert(name.to_string(), tag);
                }
            }
            let (tx, mut rx) = mpsc::channel::<LightstreamMessage>(STREAM_CHANNEL_DEPTH);
            let task = tokio::spawn(async move {
                while let Some(frame) = rx.recv().await {
                    writer.send_frame(&frame).await?;
                }
                writer.flush().await?;
                writer.shutdown().await
            });
            senders.push(tx);
            tasks.push(task);
        }
        Ok(Self { senders, tasks, registry, next: 0 })
    }

    /// Number of connections frames are distributed across.
    pub fn stream_count(&self) -> usize {
        self.senders.len()
    }

    /// Send an opaque message payload by registered type name.
    pub async fn send_message(&mut self, name: &str, payload: Vec<u8>) -> io::Result<()> {
        let tag = self.tag_for(name)?;
        self.route(LightstreamMessage::Message { tag, payload }).await
    }

    /// Send an Arrow table or table view by registered type name. A
    /// whole table sends as the full-width view of itself.
    pub async fn send_table(&mut self, name: &str, table: impl Into<TableV>) -> io::Result<()> {
        let tag = self.tag_for(name)?;
        self.route(LightstreamMessage::Table { tag, table: table.into() }).await
    }

    /// Send a protobuf message by registered type name. Encodes via prost and
    /// sends the bytes as an opaque message payload.
    #[cfg(feature = "protobuf")]
    pub async fn send_proto<M: prost::Message>(&mut self, name: &str, msg: &M) -> io::Result<()> {
        self.send_message(name, msg.encode_to_vec()).await
    }

    /// Resolve a registered type name to its wire tag.
    fn tag_for(&self, name: &str) -> io::Result<u8> {
        self.registry.get(name).copied().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("unknown type name '{name}'"))
        })
    }

    /// Route one frame to the next connection in the round-robin rotation.
    async fn route(&mut self, frame: LightstreamMessage) -> io::Result<()> {
        let idx = self.next % self.senders.len();
        self.next = self.next.wrapping_add(1);
        self.senders[idx].send(frame).await.map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "Lightstream protocol connection task closed")
        })
    }

    /// Flush and close every connection, returning the first error a connection
    /// task reported.
    pub async fn finish(mut self) -> io::Result<()> {
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
