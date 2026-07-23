// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # TCP table reader
//!
//! High-level async reader that connects to a TCP endpoint streaming
//! Arrow IPC data and decodes it into MinArrow tables.
//!
//! Wraps [`TableReader`](crate::models::readers::ipc::table::TableReader) over a [`TcpByteStream`](crate::models::streams::tcp::TcpByteStream), hiding the wiring
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
//! let mut reader = TcpTableReader::connect("127.0.0.1:9000", None).await?;
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
use minarrow::{Field, SuperTable, Table, Vec64};
use tokio::net::{TcpListener, ToSocketAddrs};

use crate::enums::{BufferChunkSize, IPCMessageProtocol};
use crate::models::decoders::limits::DecodeLimits;
use crate::models::readers::ipc::table::TableReader;
use crate::models::streams::tcp::TcpByteStream;
use crate::models::transports::tcp::TcpTransport;
use crate::traits::transport_reader::IPCTransportReader;

/// Async Arrow IPC reader over a TCP connection.
///
/// Connects to a remote TCP endpoint, reads an Arrow IPC stream,
/// and decodes it into MinArrow tables using the standard pipeline.
///
/// Implements `Stream<Item = io::Result<Table>>` for continuous streaming.
pub struct TcpTableReader {
    inner: TableReader<Vec64<u8>>,
}

impl TcpTableReader {
    /// Connect to a TCP server streaming Arrow IPC and return a table reader.
    ///
    /// Uses 8-byte alignment for compatibility with all Arrow producers.
    pub async fn connect(
        addr: impl ToSocketAddrs,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        let stream = TcpByteStream::connect(addr).await?;
        let inner = TableReader::<Vec64<u8>>::new(
            stream,
            BufferChunkSize::Http.chunk_size(),
            IPCMessageProtocol::Stream,
            limits,
        );
        Ok(Self { inner })
    }

    /// Connect with explicit chunk size and protocol control.
    pub async fn connect_with(
        addr: impl ToSocketAddrs,
        chunk_size: BufferChunkSize,
        protocol: IPCMessageProtocol,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        let stream = TcpByteStream::connect(addr).await?;
        let inner =
            TableReader::<Vec64<u8>>::new(stream, chunk_size.chunk_size(), protocol, limits);
        Ok(Self { inner })
    }

    /// Accept the next inbound connection and return a table reader over it.
    ///
    /// Serves the accepting peer role. The caller binds the listener,
    /// e.g. via `TcpTransport::bind`, and holds it across connections.
    /// Uses `BufferChunkSize::Http` (64 KiB) and protocol
    /// `IPCMessageProtocol::Stream`.
    pub async fn accept(
        listener: &TcpListener,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        let (read_half, _write_half) = TcpTransport::accept(listener).await?;
        let stream = TcpByteStream::from_read_half(read_half, BufferChunkSize::Http);
        Ok(Self::from_stream(stream, IPCMessageProtocol::Stream, limits))
    }

    /// Wrap an existing `TcpByteStream` as a table reader.
    pub fn from_stream(
        stream: TcpByteStream,
        protocol: IPCMessageProtocol,
        limits: Option<DecodeLimits>,
    ) -> Self {
        let inner = TableReader::<Vec64<u8>>::new(
            stream,
            BufferChunkSize::Http.chunk_size(),
            protocol,
            limits,
        );
        Self { inner }
    }

    /// Connect to a TCP server, upgrade the channel to TLS via the supplied
    /// `rustls::ClientConfig`, and return a table reader over the encrypted
    /// channel. Uses `BufferChunkSize::Http` (64 KiB) and protocol
    /// `IPCMessageProtocol::Stream` - TCP is unbounded by nature. Callers
    /// that need different chunk sizing should build a `TcpByteStream`
    /// directly and hand it to [`Self::from_stream`].
    ///
    /// No default root store is bundled - the caller supplies one through
    /// their `ClientConfig`.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        addr: impl tokio::net::ToSocketAddrs,
        server_name: rustls_pki_types::ServerName<'static>,
        config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        let stream =
            crate::models::streams::tcp::TcpByteStream::connect_tls(addr, server_name, config)
                .await?;
        let inner = TableReader::<Vec64<u8>>::new(
            stream,
            BufferChunkSize::Http.chunk_size(),
            IPCMessageProtocol::Stream,
            limits,
        );
        Ok(Self { inner })
    }
}

impl IPCTransportReader for TcpTableReader {
    /// Read all tables from the stream until it closes.
    async fn read_all_tables(self) -> io::Result<Vec<Table>> {
        self.inner.read_all_tables().await
    }

    /// Read up to `n` tables. If `n` is `None`, read until end of stream.
    async fn read_tables(self, n: Option<usize>) -> io::Result<Vec<Table>> {
        self.inner.read_tables(n).await
    }

    /// Read batches and assemble into a `SuperTable`.
    ///
    /// If `n` is `None`, read until end of stream.
    async fn read_to_super_table(
        self,
        name: Option<String>,
        n: Option<usize>,
    ) -> io::Result<SuperTable> {
        self.inner.read_to_super_table(name, n).await
    }

    /// Read all batches and concatenate into a single `Table`.
    async fn combine_to_table(self, name: Option<String>) -> io::Result<Table> {
        self.inner.combine_to_table(name).await
    }

    /// Return the decoded schema, if available after the first schema message.
    fn schema(&self) -> Option<&[Field]> {
        self.inner.schema()
    }

    /// Read the next table from the stream, or `None` on end of stream.
    async fn read_next(&mut self) -> io::Result<Option<Table>> {
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
