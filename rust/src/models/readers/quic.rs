// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # QUIC table reader
//!
//! High-level async reader that wraps a QUIC receive stream and decodes
//! Arrow IPC data into MinArrow tables.
//!
//! Wraps [`TableReader`](crate::models::readers::ipc::table::TableReader) over a [`QuicByteStream`](crate::models::streams::quic::QuicByteStream), hiding the wiring
//! so callers get a one-liner API.
//!
//! ## Continuous streaming
//!
//! `QuicTableReader` implements `Stream<Item = io::Result<Table>>`, so it
//! can be used with `StreamExt` for infinite or long-lived streams:
//!
//! ```ignore
//! use futures_util::StreamExt;
//! use lightstream::models::readers::quic::QuicTableReader;
//!
//! let mut reader = QuicTableReader::from_recv(recv_stream, None);
//! while let Some(result) = reader.next().await {
//!     let table = result?;
//!     // process each batch as it arrives
//! }
//! ```

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::{Field, SuperTable, Table, Vec64};

use crate::enums::{BufferChunkSize, IPCMessageProtocol};
use crate::models::decoders::limits::DecodeLimits;
use crate::models::readers::ipc::table::TableReader;
use crate::models::streams::quic::QuicByteStream;
use crate::traits::transport_reader::IPCTransportReader;

/// Async Arrow IPC reader over a QUIC receive stream.
///
/// Wraps a QUIC receive stream, reads an Arrow IPC stream,
/// and decodes it into MinArrow tables using the standard pipeline.
///
/// Implements `Stream<Item = io::Result<Table>>` for continuous streaming.
pub struct QuicTableReader {
    inner: TableReader<Vec64<u8>>,
}

impl QuicTableReader {
    /// Wrap a QUIC receive stream as a table reader.
    ///
    /// Uses `IPCMessageProtocol::Stream` and a 64 KiB initial decode capacity.
    /// The default chunk size is `BufferChunkSize::WebTransport` (64 KiB).
    pub fn from_recv(recv: quinn::RecvStream, limits: Option<DecodeLimits>) -> Self {
        let stream = QuicByteStream::new(recv, BufferChunkSize::WebTransport);
        let inner = TableReader::<Vec64<u8>>::new(
            stream,
            BufferChunkSize::WebTransport.chunk_size(),
            IPCMessageProtocol::Stream,
            limits,
        );
        Self { inner }
    }

    /// Wrap a QUIC receive stream with explicit chunk size and protocol control.
    pub fn from_recv_with(
        recv: quinn::RecvStream,
        chunk_size: BufferChunkSize,
        protocol: IPCMessageProtocol,
        limits: Option<DecodeLimits>,
    ) -> Self {
        let stream = QuicByteStream::new(recv, chunk_size);
        let inner =
            TableReader::<Vec64<u8>>::new(stream, chunk_size.chunk_size(), protocol, limits);
        Self { inner }
    }

    /// Wrap an existing `QuicByteStream` as a table reader.
    pub fn from_stream(
        stream: QuicByteStream,
        protocol: IPCMessageProtocol,
        limits: Option<DecodeLimits>,
    ) -> Self {
        let inner = TableReader::<Vec64<u8>>::new(
            stream,
            BufferChunkSize::WebTransport.chunk_size(),
            protocol,
            limits,
        );
        Self { inner }
    }
}

impl IPCTransportReader for QuicTableReader {
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

impl Stream for QuicTableReader {
    type Item = io::Result<Table>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        Pin::new(&mut me.inner).poll_next(cx)
    }
}
