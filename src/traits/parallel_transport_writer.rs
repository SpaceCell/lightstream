//! Writer interface for transports with multiple concurrent streams.
//!
//! Tables are distributed across the streams in round-robin order. Ordering is
//! preserved within each stream, but not between streams.

use std::future::Future;
use std::io;

use minarrow::{Field, Table};

/// Arrow `custom_metadata` key carrying the per-table sequence id that the
/// ordered parallel writers attach and the parallel readers surface. The
/// receiver sorts on it to recover global write order across streams.
pub(crate) const SEQ_ID_META_KEY: &str = "ls.seq_id";

/// Writes tables across multiple concurrent transport streams.
pub trait ParallelTransportWriter {
    /// Returns the schema used by all streams.
    fn schema(&self) -> &[Field];

    /// Returns the number of active streams.
    fn stream_count(&self) -> usize;

    /// Writes a table to the next stream in round-robin order.
    fn write_table(&mut self, table: Table) -> impl Future<Output = io::Result<()>> + Send;

    /// Writes all tables, distributing them across the available streams.
    fn write_all_tables(
        &mut self,
        tables: Vec<Table>,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Flushes and closes all streams.
    ///
    /// Returns the first error encountered while completing a stream.
    fn finish(self) -> impl Future<Output = io::Result<()>> + Send
    where
        Self: Sized;
}
