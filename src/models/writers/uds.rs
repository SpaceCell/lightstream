//! # Unix domain socket table writer
//!
//! High-level async writer that connects to a UDS endpoint and sends
//! Arrow IPC encoded tables over the wire.
//!
//! Wraps a [`TableSink`] over a UDS write half, hiding the wiring
//! so callers get a one-liner API.
//!
//! Uses `Vec<u8>` (8-byte aligned) encoding, matching the alignment
//! expected by the standard Arrow IPC frame decoder on the read side.

use std::io;
use std::path::Path;
use std::pin::Pin;

use futures_util::sink::SinkExt;
use minarrow::{Field, Table};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::UnixStream;

use crate::compression::Compression;
use crate::enums::IPCMessageProtocol;
use crate::models::sinks::table_sink::TableSink;

/// Async Arrow IPC writer over a Unix domain socket connection.
///
/// Connects to a local UDS endpoint and writes Arrow IPC stream
/// protocol data using the standard encoding pipeline.
///
/// Uses 8-byte aligned buffers to match the frame decoder on the
/// read side, which always uses 8-byte alignment for frame boundary
/// calculation.
pub struct UdsTableWriter {
    sink: TableSink<OwnedWriteHalf>,
}

impl UdsTableWriter {
    /// Connect to a UDS server and prepare to write Arrow IPC tables.
    ///
    /// Uses `IPCMessageProtocol::Stream` — the unbounded protocol suited
    /// for network transport where the total number of batches is not
    /// known up front.
    pub async fn connect(
        path: impl AsRef<Path>,
        schema: Vec<Field>,
    ) -> io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        let (_read, write) = stream.into_split();
        let sink = TableSink::new(write, schema, IPCMessageProtocol::Stream)?;
        Ok(Self { sink })
    }

    /// Connect with optional compression.
    pub async fn connect_with_compression(
        path: impl AsRef<Path>,
        schema: Vec<Field>,
        compression: Compression,
    ) -> io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        let (_read, write) = stream.into_split();
        let sink = TableSink::with_compression(
            write,
            schema,
            IPCMessageProtocol::Stream,
            compression,
        )?;
        Ok(Self { sink })
    }

    /// Wrap an existing UDS write half as a table writer.
    pub fn from_write_half(
        write_half: OwnedWriteHalf,
        schema: Vec<Field>,
    ) -> io::Result<Self> {
        let sink = TableSink::new(write_half, schema, IPCMessageProtocol::Stream)?;
        Ok(Self { sink })
    }

    /// Get the schema used for this writer.
    pub fn schema(&self) -> &[Field] {
        &self.sink.schema
    }

    /// Register a dictionary for categorical columns.
    pub fn register_dictionary(&mut self, dict_id: i64, values: Vec<String>) {
        self.sink.inner.register_dictionary(dict_id, values);
    }

    /// Write a single table and flush.
    pub async fn write_table(&mut self, table: Table) -> io::Result<()> {
        SinkExt::send(&mut self.sink, table).await?;
        SinkExt::flush(&mut self.sink).await?;
        Ok(())
    }

    /// Write all tables from an iterator and close.
    pub async fn write_all_tables<I>(&mut self, tables: I) -> io::Result<()>
    where
        I: IntoIterator<Item = Table>,
    {
        let mut sink = Pin::new(&mut self.sink);
        for table in tables {
            SinkExt::send(&mut sink, table).await?;
        }
        SinkExt::close(&mut sink).await?;
        Ok(())
    }

    /// Finalise the stream. Must be called after writing all tables.
    pub async fn finish(&mut self) -> io::Result<()> {
        SinkExt::close(&mut self.sink).await
    }
}
