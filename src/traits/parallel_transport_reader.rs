//! Reader interface for transports with multiple concurrent streams.
//!
//! Tables from all streams are exposed through a single merged stream. Ordering
//! is preserved within each source stream, but not between streams.

use std::future::Future;
use std::io;

use futures_core::Stream;
use minarrow::Table;

/// Reads and merges tables from multiple concurrent transport streams.
pub trait ParallelTransportReader: Stream<Item = io::Result<Table>> + Sized {
    /// Returns the number of streams being merged.
    fn stream_count(&self) -> usize;

    /// Reads all streams to completion and returns the received tables.
    fn read_all_tables(self) -> impl Future<Output = io::Result<Vec<Table>>> + Send;
}
