//! # Transport reader trait
//!
//! Common interface for all transport-level table readers i.e. TCP, WebSocket,
//! UDS, QUIC, WebTransport, and stdio.
//!
//! Every transport reader wraps an inner [`TableReader`] and delegates the same
//! set of read methods. This trait captures that contract so new transports get
//! compile-time enforcement instead of copy-paste.
//!
//! [`TableReader`]: crate::models::readers::ipc::table_reader::TableReader

use std::future::Future;
use std::io;

use futures_core::Stream;
use minarrow::{Field, SuperTable, Table};

/// Shared reading interface for all transport-level Arrow IPC readers.
///
/// Implementors must also implement `Stream<Item = io::Result<Table>>`, which
/// is enforced by the supertrait bound.
pub trait TransportReader: Stream<Item = io::Result<Table>> + Sized {
    /// Read all tables from the stream until it closes.
    fn read_all_tables(self) -> impl Future<Output = io::Result<Vec<Table>>> + Send;

    /// Read up to `n` tables. If `n` is `None`, read until end of stream.
    fn read_tables(self, n: Option<usize>) -> impl Future<Output = io::Result<Vec<Table>>> + Send;

    /// Read batches and assemble into a `SuperTable`.
    ///
    /// If `n` is `None`, read until end of stream.
    fn read_to_super_table(
        self,
        name: Option<String>,
        n: Option<usize>,
    ) -> impl Future<Output = io::Result<SuperTable>> + Send;

    /// Read all batches and concatenate into a single `Table`.
    fn combine_to_table(
        self,
        name: Option<String>,
    ) -> impl Future<Output = io::Result<Table>> + Send;

    /// Return the decoded schema, if available after the first schema message.
    fn schema(&self) -> Option<&[Field]>;

    /// Read the next table from the stream, or `None` on end of stream.
    fn read_next(&mut self) -> impl Future<Output = io::Result<Option<Table>>> + Send;
}
