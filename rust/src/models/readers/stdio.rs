// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Stdin table reader
//!
//! High-level async reader that reads Arrow IPC data from stdin
//! and decodes it into MinArrow tables.
//!
//! Wraps [`TableReader`](crate::models::readers::ipc::table::TableReader) over a [`StdinByteStream`](crate::models::streams::stdio::StdinByteStream), hiding the wiring
//! so callers get a simple API for CLI tools.
//!
//! ## Continuous streaming
//!
//! `StdinTableReader` implements `Stream<Item = io::Result<Table>>`, so it
//! can be used with `StreamExt` for pipe-based streaming:
//!
//! ```rust,no_run
//! use futures_util::StreamExt;
//! # async fn run() -> std::io::Result<()> {
//! # use lightstream::models::readers::stdio::StdinTableReader;
//! let mut reader = StdinTableReader::new(None);
//! while let Some(result) = reader.next().await {
//!     let table = result?;
//!     // process each batch as it arrives from the pipe
//! }
//! # Ok(()) }
//! ```
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
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::{Field, SuperTable, Table, Vec64};

use crate::enums::{BufferChunkSize, IPCMessageProtocol};
use crate::models::decoders::limits::DecodeLimits;
use crate::models::readers::ipc::table::TableReader;
use crate::models::streams::stdio::{StdinByteStream, from_stdin, from_stdin_default};
use crate::traits::transport_reader::IPCTransportReader;

/// Async Arrow IPC reader over stdin.
///
/// Reads Arrow IPC stream data from stdin and decodes it into
/// MinArrow tables using the standard pipeline.
///
/// Implements `Stream<Item = io::Result<Table>>` for continuous streaming.
pub struct StdinTableReader {
    inner: TableReader<Vec64<u8>>,
}

impl StdinTableReader {
    /// Create a stdin table reader with default settings.
    ///
    /// Uses `IPCMessageProtocol::Stream` and a 64 KiB chunk size.
    pub fn new(limits: Option<DecodeLimits>) -> Self {
        let stream = from_stdin_default();
        let inner = TableReader::<Vec64<u8>>::new(
            stream,
            BufferChunkSize::InMemory.chunk_size(),
            IPCMessageProtocol::Stream,
            limits,
        );
        Self { inner }
    }

    /// Create a stdin table reader with explicit chunk size and protocol.
    pub fn new_with(
        chunk_size: BufferChunkSize,
        protocol: IPCMessageProtocol,
        limits: Option<DecodeLimits>,
    ) -> Self {
        let stream = from_stdin(chunk_size);
        let inner =
            TableReader::<Vec64<u8>>::new(stream, chunk_size.chunk_size(), protocol, limits);
        Self { inner }
    }

    /// Wrap an existing `StdinByteStream` as a table reader.
    pub fn from_stream(
        stream: StdinByteStream,
        protocol: IPCMessageProtocol,
        limits: Option<DecodeLimits>,
    ) -> Self {
        let inner = TableReader::<Vec64<u8>>::new(
            stream,
            BufferChunkSize::InMemory.chunk_size(),
            protocol,
            limits,
        );
        Self { inner }
    }
}

impl IPCTransportReader for StdinTableReader {
    /// Read all tables from stdin until EOF.
    async fn read_all_tables(self) -> io::Result<Vec<Table>> {
        self.inner.read_all_tables().await
    }

    /// Read up to `n` tables. If `n` is `None`, read until EOF.
    async fn read_tables(self, n: Option<usize>) -> io::Result<Vec<Table>> {
        self.inner.read_tables(n).await
    }

    /// Read batches and assemble into a `SuperTable`.
    ///
    /// If `n` is `None`, read until EOF.
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

    /// Read the next table from stdin, or `None` on EOF.
    async fn read_next(&mut self) -> io::Result<Option<Table>> {
        self.inner.read_next().await
    }
}

impl Default for StdinTableReader {
    fn default() -> Self {
        Self::new(None)
    }
}

impl Stream for StdinTableReader {
    type Item = io::Result<Table>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        Pin::new(&mut me.inner).poll_next(cx)
    }
}
