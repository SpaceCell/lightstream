//! # TCP table reader
//!
//! High-level async reader that connects to a TCP endpoint streaming
//! Arrow IPC data and decodes it into MinArrow tables.
//!
//! Wraps [`TableReader`] over a [`TcpByteStream`], hiding the wiring
//! so callers get a one-liner API.
//!
//! ## Continuous streaming
//!
//! `TcpTableReader` implements `Stream<Item = io::Result<Table>>`, so it
//! can be used with `StreamExt` for infinite or long-lived streams:
//!
//! ```rust,no_run
//! use futures_util::StreamExt;
//! # async fn run() -> std::io::Result<()> {
//! # use lightstream::models::readers::tcp::TcpTableReader;
//! let mut reader = TcpTableReader::connect("127.0.0.1:9000").await?;
//! while let Some(result) = reader.next().await {
//!     let table = result?;
//!     // process each batch as it arrives
//! }
//! # Ok(()) }
//! ```

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::{Field, SuperTable, Table};
use tokio::net::ToSocketAddrs;

use crate::enums::{BufferChunkSize, IPCMessageProtocol};
use crate::models::readers::ipc::table_reader::TableReader;
use crate::models::streams::tcp::TcpByteStream;

/// Async Arrow IPC reader over a TCP connection.
///
/// Connects to a remote TCP endpoint, reads an Arrow IPC stream,
/// and decodes it into MinArrow tables using the standard pipeline.
///
/// Implements `Stream<Item = io::Result<Table>>` for continuous streaming.
pub struct TcpTableReader {
    inner: TableReader<TcpByteStream, Vec<u8>>,
}

impl TcpTableReader {
    /// Connect to a TCP server streaming Arrow IPC and return a table reader.
    ///
    /// Uses `IPCMessageProtocol::Stream` and a 64 KiB initial decode capacity.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let stream = TcpByteStream::connect(addr).await?;
        let inner = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
        Ok(Self { inner })
    }

    /// Connect with explicit chunk size and protocol control.
    pub async fn connect_with(
        addr: impl ToSocketAddrs,
        chunk_size: BufferChunkSize,
        protocol: IPCMessageProtocol,
    ) -> io::Result<Self> {
        let stream = TcpByteStream::connect(addr).await?;
        let inner = TableReader::new(stream, chunk_size.chunk_size(), protocol);
        Ok(Self { inner })
    }

    /// Wrap an existing `TcpByteStream` as a table reader.
    pub fn from_stream(stream: TcpByteStream, protocol: IPCMessageProtocol) -> Self {
        let inner = TableReader::new(stream, 64 * 1024, protocol);
        Self { inner }
    }

    /// Read all tables from the stream until it closes.
    pub async fn read_all_tables(self) -> io::Result<Vec<Table>> {
        self.inner.read_all_tables().await
    }

    /// Read up to `n` tables. If `n` is `None`, read until end of stream.
    pub async fn read_tables(self, n: Option<usize>) -> io::Result<Vec<Table>> {
        self.inner.read_tables(n).await
    }

    /// Read batches and assemble into a `SuperTable`.
    ///
    /// If `n` is `None`, read until end of stream.
    pub async fn read_to_super_table(
        self,
        name: Option<String>,
        n: Option<usize>,
    ) -> io::Result<SuperTable> {
        self.inner.read_to_super_table(name, n).await
    }

    /// Read all batches and concatenate into a single `Table`.
    pub async fn combine_to_table(self, name: Option<String>) -> io::Result<Table> {
        self.inner.combine_to_table(name).await
    }

    /// Return the decoded schema, if available after the first schema message.
    pub fn schema(&self) -> Option<&[Field]> {
        self.inner.schema()
    }

    /// Read the next table from the stream, or `None` on end of stream.
    pub async fn read_next(&mut self) -> io::Result<Option<Table>> {
        self.inner.read_next().await
    }
}

impl Stream for TcpTableReader {
    type Item = io::Result<Table>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        Pin::new(&mut me.inner).poll_next(cx)
    }
}
