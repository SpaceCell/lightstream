//! # Stdout table writer
//!
//! High-level async writer that writes Arrow IPC encoded tables to stdout.
//!
//! Wraps a [`TableSink`] over `tokio::io::Stdout`, hiding the wiring
//! so callers get a simple API for CLI tools.
//!
//! Uses `Vec<u8>` (8-byte aligned) encoding, matching the alignment
//! expected by the standard Arrow IPC frame decoder on the read side.
//!
//! ## CLI pipeline example
//!
//! ```bash
//! producer | my_tool | consumer
//! ```
//!
//! Where `my_tool` uses `StdinTableReader` to read tables and
//! `StdoutTableWriter` to write them.

use std::io;
use std::pin::Pin;

use futures_util::sink::SinkExt;
use minarrow::{Field, Table};
use tokio::io::Stdout;

use crate::compression::Compression;
use crate::enums::IPCMessageProtocol;
use crate::models::sinks::table_sink::TableSink;

/// Async Arrow IPC writer to stdout.
///
/// Writes Arrow IPC stream protocol data to stdout using the standard
/// encoding pipeline.
///
/// Uses 8-byte aligned buffers to match the frame decoder on the
/// read side, which always uses 8-byte alignment for frame boundary
/// calculation.
pub struct StdoutTableWriter {
    sink: TableSink<Stdout>,
}

impl StdoutTableWriter {
    /// Create a new stdout table writer with the given schema.
    ///
    /// Uses `IPCMessageProtocol::Stream` — the unbounded protocol suited
    /// for pipe-based transport where the total number of batches is not
    /// known up front.
    pub fn new(schema: Vec<Field>) -> io::Result<Self> {
        let stdout = tokio::io::stdout();
        let sink = TableSink::new(stdout, schema, IPCMessageProtocol::Stream)?;
        Ok(Self { sink })
    }

    /// Create a stdout table writer with optional compression.
    pub fn new_with_compression(
        schema: Vec<Field>,
        compression: Compression,
    ) -> io::Result<Self> {
        let stdout = tokio::io::stdout();
        let sink = TableSink::with_compression(
            stdout,
            schema,
            IPCMessageProtocol::Stream,
            compression,
        )?;
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
