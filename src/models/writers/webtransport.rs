//! # WebTransport table writer
//!
//! High-level async writer that sends Arrow IPC encoded tables over a
//! WebTransport send stream.
//!
//! Wraps a [`TableSink`] over a [`wtransport::SendStream`], hiding the wiring
//! so callers get a one-liner API.
//!
//! Uses `Vec<u8>` (8-byte aligned) encoding, matching the alignment
//! expected by the standard Arrow IPC frame decoder on the read side.

use std::io;
use std::pin::Pin;

use futures_util::sink::SinkExt;
use minarrow::{Field, Table};

use crate::compression::Compression;
use crate::enums::IPCMessageProtocol;
use crate::models::sinks::table_sink::TableSink;
use crate::traits::transport_writer::TransportWriter;

/// Async Arrow IPC writer over a WebTransport send stream.
///
/// Wraps a WebTransport send stream and writes Arrow IPC stream protocol data
/// using the standard encoding pipeline.
///
/// Uses 8-byte aligned buffers to match the frame decoder on the
/// read side, which always uses 8-byte alignment for frame boundary
/// calculation.
pub struct WebTransportTableWriter {
    sink: TableSink<wtransport::SendStream>,
}

impl WebTransportTableWriter {
    /// Wrap a WebTransport send stream and prepare to write Arrow IPC tables.
    ///
    /// Uses `IPCMessageProtocol::Stream` — the unbounded protocol suited
    /// for network transport where the total number of batches is not
    /// known up front.
    pub fn new(send: wtransport::SendStream, schema: Vec<Field>) -> io::Result<Self> {
        let sink = TableSink::new(send, schema, IPCMessageProtocol::Stream)?;
        Ok(Self { sink })
    }

    /// Wrap a WebTransport send stream with optional compression.
    pub fn with_compression(
        send: wtransport::SendStream,
        schema: Vec<Field>,
        compression: Compression,
    ) -> io::Result<Self> {
        let sink =
            TableSink::with_compression(send, schema, IPCMessageProtocol::Stream, compression)?;
        Ok(Self { sink })
    }
}

impl TransportWriter for WebTransportTableWriter {
    /// Get the schema used for this writer.
    fn schema(&self) -> &[Field] {
        &self.sink.schema
    }

    /// Register a dictionary for categorical columns.
    fn register_dictionary(&mut self, dict_id: i64, values: Vec<String>) {
        self.sink.inner.register_dictionary(dict_id, values);
    }

    /// Write a single table and flush.
    async fn write_table(&mut self, table: Table) -> io::Result<()> {
        SinkExt::send(&mut self.sink, table).await?;
        SinkExt::flush(&mut self.sink).await?;
        Ok(())
    }

    /// Write all tables and close.
    async fn write_all_tables(&mut self, tables: Vec<Table>) -> io::Result<()> {
        let mut sink = Pin::new(&mut self.sink);
        for table in tables {
            SinkExt::send(&mut sink, table).await?;
        }
        SinkExt::close(&mut sink).await?;
        Ok(())
    }

    /// Finalise the stream. Must be called after writing all tables.
    async fn finish(&mut self) -> io::Result<()> {
        SinkExt::close(&mut self.sink).await
    }
}
