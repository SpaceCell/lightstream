//! # TCP table writer
//!
//! High-level async writer that connects to a TCP endpoint and sends
//! Arrow IPC encoded tables over the wire.
//!
//! Wraps a [`TableSink`] over a TCP write half, hiding the wiring
//! so callers get a one-liner API.
//!
//! Uses `Vec64<u8>` for 64-byte SIMD aligned encoding.

use std::io;
use std::pin::Pin;

use futures_util::sink::SinkExt;
use minarrow::{Field, Table};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::compression::Compression;
use crate::enums::IPCMessageProtocol;
use crate::models::sinks::table_sink::TableSink64;
use crate::traits::transport_writer::IPCTransportWriter;

/// Async Arrow IPC writer over a TCP connection.
///
/// Connects to a remote TCP endpoint and writes Arrow IPC stream
/// protocol data using the standard encoding pipeline.
///
/// Uses 64-byte SIMD aligned buffers via Vec64.
pub struct TcpTableWriter {
    sink: TableSink64<OwnedWriteHalf>,
}

impl TcpTableWriter {
    /// Connect to a TCP server and prepare to write Arrow IPC tables.
    ///
    /// Uses `IPCMessageProtocol::Stream` - the unbounded protocol suited
    /// for network transport where the total number of batches is not
    /// known up front.
    pub async fn connect(addr: impl ToSocketAddrs, schema: Vec<Field>) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (_read, write) = stream.into_split();
        let sink = TableSink64::new(write, schema, IPCMessageProtocol::Stream)?;
        Ok(Self { sink })
    }

    /// Connect with optional compression.
    pub async fn connect_with_compression(
        addr: impl ToSocketAddrs,
        schema: Vec<Field>,
        compression: Compression,
    ) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (_read, write) = stream.into_split();
        let sink =
            TableSink64::new_with_compression(write, schema, IPCMessageProtocol::Stream, compression)?;
        Ok(Self { sink })
    }

    /// Wrap an existing TCP write half as a table writer.
    pub fn from_write_half(write_half: OwnedWriteHalf, schema: Vec<Field>) -> io::Result<Self> {
        let sink = TableSink64::new(write_half, schema, IPCMessageProtocol::Stream)?;
        Ok(Self { sink })
    }
}

impl IPCTransportWriter for TcpTableWriter {
    /// Get the schema used for this writer.
    fn schema(&self) -> &[Field] {
        &self.sink.schema
    }

    /// Register a dictionary for categorical columns.
    fn register_dictionary(&mut self, dict_id: i64, values: Vec<String>) {
        self.sink.codec.register_dictionary(dict_id, values);
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
