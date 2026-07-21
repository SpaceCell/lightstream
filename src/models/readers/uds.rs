// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Unix domain socket table reader
//!
//! High-level async reader that connects to a UDS endpoint streaming
//! Arrow IPC data and decodes it into MinArrow tables.
//!
//! Wraps [`TableReader`](crate::models::readers::ipc::table::TableReader) over a [`UdsByteStream`](crate::models::streams::uds::UdsByteStream), hiding the wiring
//! so callers get a one-liner API.
//!
//! ## Continuous streaming
//!
//! `UdsTableReader` implements `Stream<Item = io::Result<Table>>`, so it
//! can be used with `StreamExt` for infinite or long-lived streams:
//!
//! ```rust,no_run
//! use futures_util::StreamExt;
//! # async fn run() -> std::io::Result<()> {
//! # use lightstream::models::readers::uds::UdsTableReader;
//! let mut reader = UdsTableReader::connect("/tmp/my.sock", None).await?;
//! while let Some(result) = reader.next().await {
//!     let table = result?;
//!     // process each batch as it arrives
//! }
//! # Ok(()) }
//! ```

use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::{Field, SuperTable, Table, Vec64};
use tokio::net::UnixListener;

use crate::enums::{BufferChunkSize, IPCMessageProtocol};
use crate::models::decoders::limits::DecodeLimits;
use crate::models::readers::ipc::table::TableReader;
use crate::models::streams::uds::UdsByteStream;
use crate::models::transports::uds::UdsTransport;
use crate::traits::transport_reader::IPCTransportReader;

/// Async Arrow IPC reader over a Unix domain socket connection.
///
/// Connects to a local UDS endpoint, reads an Arrow IPC stream,
/// and decodes it into MinArrow tables using the standard pipeline.
///
/// Implements `Stream<Item = io::Result<Table>>` for continuous streaming.
pub struct UdsTableReader {
    inner: TableReader<Vec64<u8>>,
}

impl UdsTableReader {
    /// Connect to a UDS server streaming Arrow IPC and return a table reader.
    ///
    /// Uses 8-byte alignment for compatibility with all Arrow producers.
    pub async fn connect(
        path: impl AsRef<Path>,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        let stream = UdsByteStream::connect(path).await?;
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
        path: impl AsRef<Path>,
        chunk_size: BufferChunkSize,
        protocol: IPCMessageProtocol,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        let stream = UdsByteStream::connect(path).await?;
        let inner =
            TableReader::<Vec64<u8>>::new(stream, chunk_size.chunk_size(), protocol, limits);
        Ok(Self { inner })
    }

    /// Accept the next inbound connection and return a table reader over it.
    ///
    /// Serves the accepting peer role. The caller binds the listener,
    /// e.g. via `UdsTransport::bind`, and holds it across connections.
    /// Uses `BufferChunkSize::Http` (64 KiB) and protocol
    /// `IPCMessageProtocol::Stream`.
    pub async fn accept(
        listener: &UnixListener,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        let (read_half, _write_half) = UdsTransport::accept(listener).await?;
        let stream = UdsByteStream::from_read_half(read_half, BufferChunkSize::Http);
        Ok(Self::from_stream(stream, IPCMessageProtocol::Stream, limits))
    }

    /// Wrap an existing `UdsByteStream` as a table reader.
    pub fn from_stream(
        stream: UdsByteStream,
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
}

impl IPCTransportReader for UdsTableReader {
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

impl Stream for UdsTableReader {
    type Item = io::Result<Table>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        Pin::new(&mut me.inner).poll_next(cx)
    }
}
