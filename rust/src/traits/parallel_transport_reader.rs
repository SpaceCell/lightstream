// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reader interface for transports with multiple concurrent streams.
//!
//! Tables from all streams are exposed through a single merged stream. Order
//! within a source stream is always preserved. Global write order across
//! streams is recovered with [`SortBehaviour::Ordered`](crate::traits::parallel_transport_reader::SortBehaviour::Ordered), which pulls the
//! streams in the writer's round-robin rotation.

use std::future::Future;
use std::io;

use futures_core::Stream;
use minarrow::Table;

/// Controls how a parallel reader surfaces and orders the per-table sequence
/// keys carried on each record batch by the ordered parallel writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortBehaviour {
    /// Do not surface sequence keys. Every item's key is `None` and tables
    /// arrive in the order the streams produce them, at no ordering cost.
    None,
    /// Surface each table's sequence key without reordering. Tables arrive in
    /// stream-production order. The caller sorts on the keys to recover global
    /// write order across streams.
    RequestKeys,
    /// Emit tables in global write order across streams. The reader pulls the
    /// streams in the writer's round-robin rotation, so each table surfaces in
    /// ascending sequence on the streaming interface and in
    /// [`read_all_tables`](ParallelTransportReader::read_all_tables). A lagging
    /// stream holds back the tables behind it, the bound on per-stream
    /// buffering inherent to any ordered merge.
    Ordered,
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
