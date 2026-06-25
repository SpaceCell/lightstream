//! # Parallel Lightstream protocol writer
//!
//! Fans one table sequence across several concurrent Lightstream protocol
//! connections to a single endpoint. Each connection runs its own
//! [`LightstreamWriter`] driven by a dedicated task, so the connections send
//! in parallel and aggregate throughput is the sum across them.
//!
//! A single table type is registered on every connection at
//! [`connect`](LightstreamParallelWriter::connect). Order is preserved within
//! a connection. Global write order across the set is recovered by the
//! receiver under
//! [`SortBehaviour::Ordered`](crate::traits::parallel_transport_reader::SortBehaviour::Ordered),
//! which pulls the connections in this writer's round-robin rotation.

use std::io;
use std::net::SocketAddr;

use minarrow::{Field, Table, Vec64};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::models::writers::lightstream::LightstreamWriter;
use crate::traits::parallel_transport_writer::ParallelTransportWriter;

/// Bounded depth per connection channel. Lets the producer pipeline a few
/// tables ahead of each connection task without unbounded buffering.
const STREAM_CHANNEL_DEPTH: usize = 8;

/// Async Lightstream protocol writer that distributes tables across several
/// concurrent connections to one endpoint.
///
/// Open with [`LightstreamParallelWriter::connect`], write tables with
/// [`write_table`](ParallelTransportWriter::write_table), then
/// [`finish`](ParallelTransportWriter::finish) to flush and close every
/// connection.
pub struct LightstreamParallelWriter {
    schema: Vec<Field>,
    senders: Vec<mpsc::Sender<Table>>,
    tasks: Vec<JoinHandle<io::Result<()>>>,
    next: usize,
}

impl LightstreamParallelWriter {
    /// Open `stream_count` Lightstream protocol connections to `addr` and
    /// register `type_name` with `schema` on each, ready to distribute tables
    /// across them.
    ///
    /// Connections open in order, so connection `i` pairs with the `i`-th
    /// connection the receiver accepts.
    pub async fn connect(
        addr: SocketAddr,
        stream_count: usize,
        type_name: &str,
        schema: Vec<Field>,
    ) -> io::Result<Self> {
        assert!(stream_count >= 1, "stream_count must be at least 1");
        let mut senders = Vec::with_capacity(stream_count);
        let mut tasks = Vec::with_capacity(stream_count);
        for _ in 0..stream_count {
            let stream = TcpStream::connect(addr).await?;
            let (_read, write) = stream.into_split();
            let mut writer = LightstreamWriter::<_, Vec64<u8>>::new(write);
            writer.register_table(type_name, schema.clone());
            let name = type_name.to_string();
            let (tx, mut rx) = mpsc::channel::<Table>(STREAM_CHANNEL_DEPTH);
            let task = tokio::spawn(async move {
                while let Some(table) = rx.recv().await {
                    writer.send_table(&name, &table).await?;
                }
                writer.flush().await?;
                writer.shutdown().await
            });
            senders.push(tx);
            tasks.push(task);
        }
        Ok(Self { schema, senders, tasks, next: 0 })
    }
}

impl ParallelTransportWriter for LightstreamParallelWriter {
    fn schema(&self) -> &[Field] {
        &self.schema
    }

    fn stream_count(&self) -> usize {
        self.senders.len()
    }

    async fn write_table(&mut self, table: Table) -> io::Result<()> {
        let idx = self.next % self.senders.len();
        self.next = self.next.wrapping_add(1);
        self.senders[idx].send(table).await.map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "Lightstream protocol connection task closed")
        })
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
