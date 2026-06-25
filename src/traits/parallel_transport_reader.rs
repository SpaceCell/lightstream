//! Reader interface for transports with multiple concurrent streams.
//!
//! Tables from all streams are exposed through a single merged stream. Ordering
//! is preserved within each source stream, but not between streams.

use std::future::Future;
use std::io;

use futures_core::Stream;
use minarrow::Table;

/// Controls how a parallel reader surfaces and orders the per-table sequence
/// keys carried on each record batch by the ordered parallel writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortBehaviour {
    /// Do not surface sequence keys. Every item's key is `None`.
    None,
    /// Surface each table's sequence key. The caller sorts on the keys to
    /// recover global order across streams.
    RequestKeys,
    /// Surface keys and sort the collected result by sequence in
    /// `read_all_tables`. The streaming interface still yields in arrival
    /// order.
    Auto,
}

/// Reads and merges tables from multiple concurrent transport streams.
///
/// Each table is paired with its sequence key - `Some` when the peer used an
/// ordered writer, `None` otherwise.
pub trait ParallelTransportReader:
    Stream<Item = io::Result<(Table, Option<u64>)>> + Sized
{
    /// Returns the number of streams being merged.
    fn stream_count(&self) -> usize;

    /// Reads all streams to completion and returns the received tables paired
    /// with their sequence keys.
    fn read_all_tables(self) -> impl Future<Output = io::Result<Vec<(Table, Option<u64>)>>> + Send;
}
