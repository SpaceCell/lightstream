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
use crate::models::streams::tcp::TcpWriteHalf;
use crate::traits::transport_writer::IPCTransportWriter;

/// Async Arrow IPC writer over a TCP connection.
///
/// Connects to a remote TCP endpoint and writes Arrow IPC stream
/// protocol data using the standard encoding pipeline. The underlying
/// transport may be plaintext or TLS-wrapped; both share the same wire
/// framing.
///
/// Uses 64-byte SIMD aligned buffers via Vec64.
pub struct TcpTableWriter {
    sink: TableSink64<TcpWriteHalf>,
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
        let sink = TableSink64::new(
            TcpWriteHalf::Plain(write),
            schema,
            IPCMessageProtocol::Stream,
        )?;
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
        let sink = TableSink64::new_with_compression(
            TcpWriteHalf::Plain(write),
            schema,
            IPCMessageProtocol::Stream,
            compression,
        )?;
        Ok(Self { sink })
    }

    /// Wrap an existing TCP write half as a table writer.
    pub fn from_write_half(write_half: OwnedWriteHalf, schema: Vec<Field>) -> io::Result<Self> {
        let sink = TableSink64::new(
            TcpWriteHalf::Plain(write_half),
            schema,
            IPCMessageProtocol::Stream,
        )?;
        Ok(Self { sink })
    }

    /// Connect to a TCP server, upgrade the channel to TLS via the supplied
    /// `rustls::ClientConfig`, and return a table writer over the encrypted
    /// channel. Pass `None` for `compression` to write uncompressed batches.
    ///
    /// No default root store is bundled - the caller supplies one through
    /// their `ClientConfig`.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        addr: impl ToSocketAddrs,
        server_name: rustls_pki_types::ServerName<'static>,
        config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
        schema: Vec<Field>,
        compression: Option<Compression>,
    ) -> io::Result<Self> {
        use crate::models::streams::tcp::TcpWriteHalf;
        let tcp = TcpStream::connect(addr).await?;
        let connector = tokio_rustls::TlsConnector::from(config);
        let tls = connector.connect(server_name, tcp).await?;
        let (_read_half, write_half) = tokio::io::split(tls);
        let half = TcpWriteHalf::Tls(Box::new(write_half));
        let sink = match compression {
            Some(c) => {
                TableSink64::new_with_compression(half, schema, IPCMessageProtocol::Stream, c)?
            }
            None => TableSink64::new(half, schema, IPCMessageProtocol::Stream)?,
        };
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
