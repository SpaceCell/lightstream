// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Writer interface for transports with multiple concurrent streams.
//!
//! Tables are distributed across the streams in round-robin order. Order within
//! a stream is preserved, and the receiver recovers the global order from that
//! rotation.

use std::future::Future;
use std::io;

use minarrow::{Field, Table, TableV};

/// Arrow `custom_metadata` key carrying the per-table sequence id that the
/// ordered parallel writers attach and the parallel readers surface. The
/// receiver sorts on it to recover global write order across streams.
#[cfg(any(feature = "tcp", feature = "quic", feature = "http"))]
pub(crate) const SEQ_ID_META_KEY: &str = "ls.seq_id";

/// Writes tables across multiple concurrent transport streams.
pub trait ParallelTransportWriter {
    /// Returns the schema used by all streams.
    fn schema(&self) -> &[Field];

    /// Returns the number of active streams.
    fn stream_count(&self) -> usize;

    /// Writes a table to the next stream in round-robin order.
    fn write_table(
        &mut self,
        table: impl Into<TableV> + Send,
    ) -> impl Future<Output = io::Result<()>> + Send;

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
