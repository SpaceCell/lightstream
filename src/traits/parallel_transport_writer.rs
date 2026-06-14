//! # Parallel transport writer trait
//!
//! Writing interface for transports that fan one table sequence across
//! several concurrent streams on a single connection i.e. QUIC and
//! HTTP/2, where the protocol multiplexes independent streams.
//!
//! A parallel writer routes tables across `stream_count` streams in
//! round-robin order. Each stream carries its own ordered sequence of
//! batches; ordering is preserved within a stream, not across streams.
//! Aggregate throughput is the sum across streams.

use std::future::Future;
use std::io;

use minarrow::{Field, Table};

/// Shared writing interface for transports that distribute tables over
/// several concurrent streams on one connection.
pub trait ParallelTransportWriter {
    /// Schema shared by every stream.
    fn schema(&self) -> &[Field];

    /// Number of concurrent streams the writer drives.
    fn stream_count(&self) -> usize;

    /// Route one table to the next stream in round-robin order.
    fn write_table(&mut self, table: Table) -> impl Future<Output = io::Result<()>> + Send;

    /// Route a batch of tables, distributing them across the streams.
    fn write_all_tables(
        &mut self,
        tables: Vec<Table>,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Flush and close every stream, awaiting completion. Returns the
    /// first stream error if any stream failed.
    fn finish(self) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Sized;
}
