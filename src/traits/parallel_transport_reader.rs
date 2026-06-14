//! # Parallel transport reader trait
//!
//! Reading interface for transports that accept several concurrent
//! streams on one connection and merge them into a single table stream.
//!
//! The merged reader yields tables from every stream as they arrive.
//! Ordering is preserved within a stream, not across streams - the same
//! contract a parallel writer produces.

use std::future::Future;
use std::io;

use futures_core::Stream;
use minarrow::Table;

/// Shared reading interface for transports that accept several
/// concurrent streams and merge them into one table stream.
///
/// Implementors also implement `Stream<Item = io::Result<Table>>`, the
/// merged view across every stream.
pub trait ParallelTransportReader: Stream<Item = io::Result<Table>> + Sized {
    /// Number of concurrent streams being merged.
    fn stream_count(&self) -> usize;

    /// Drain every stream to end and collect all tables.
    fn read_all_tables(self) -> impl Future<Output = io::Result<Vec<Table>>> + Send;
}
