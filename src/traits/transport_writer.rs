//! # Transport writer trait
//!
//! Common interface for all transport-level table writers i.e. TCP, WebSocket,
//! UDS, QUIC, WebTransport, and stdio.
//!
//! Every transport writer wraps an inner [`TableSink`] and delegates the same
//! set of write methods. This trait captures that contract so new transports get
//! compile-time enforcement instead of copy-paste.
//!
//! [`TableSink`]: crate::models::sinks::table_sink::TableSink

use std::future::Future;
use std::io;

use minarrow::{Field, Table};

/// Shared writing interface for all transport-level Arrow IPC writers.
pub trait TransportWriter {
    /// Get the schema used for this writer.
    fn schema(&self) -> &[Field];

    /// Register a dictionary for categorical columns.
    fn register_dictionary(&mut self, dict_id: i64, values: Vec<String>);

    /// Write a single table and flush.
    fn write_table(&mut self, table: Table) -> impl Future<Output = io::Result<()>> + Send;

    /// Write all tables and close.
    fn write_all_tables(&mut self, tables: Vec<Table>) -> impl Future<Output = io::Result<()>> + Send;

    /// Finalise the stream. Must be called after writing all tables.
    fn finish(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}
