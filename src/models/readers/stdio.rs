//! # Stdin table reader
//!
//! High-level async reader that reads Arrow IPC data from stdin
//! and decodes it into MinArrow tables.
//!
//! Wraps [`TableReader`] over a [`StdinByteStream`], hiding the wiring
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
//! let mut reader = StdinTableReader::new();
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
use minarrow::{Field, SuperTable, Table};

use crate::enums::{BufferChunkSize, IPCMessageProtocol};
use crate::models::readers::ipc::table_reader::TableReader;
use crate::models::streams::stdio::StdinByteStream;

/// Async Arrow IPC reader over stdin.
///
/// Reads Arrow IPC stream data from stdin and decodes it into
/// MinArrow tables using the standard pipeline.
///
/// Implements `Stream<Item = io::Result<Table>>` for continuous streaming.
pub struct StdinTableReader {
    inner: TableReader<StdinByteStream, Vec<u8>>,
}

impl StdinTableReader {
    /// Create a stdin table reader with default settings.
    ///
    /// Uses `IPCMessageProtocol::Stream` and a 64 KiB chunk size.
    pub fn new() -> Self {
        let stream = StdinByteStream::default_size();
        let inner = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
        Self { inner }
    }

    /// Create a stdin table reader with explicit chunk size and protocol.
    pub fn new_with(chunk_size: BufferChunkSize, protocol: IPCMessageProtocol) -> Self {
        let stream = StdinByteStream::new(chunk_size);
        let inner = TableReader::new(stream, chunk_size.chunk_size(), protocol);
        Self { inner }
    }

    /// Wrap an existing `StdinByteStream` as a table reader.
    pub fn from_stream(stream: StdinByteStream, protocol: IPCMessageProtocol) -> Self {
        let inner = TableReader::new(stream, 64 * 1024, protocol);
        Self { inner }
    }

    /// Read all tables from stdin until EOF.
    pub async fn read_all_tables(self) -> io::Result<Vec<Table>> {
        self.inner.read_all_tables().await
    }

    /// Read up to `n` tables. If `n` is `None`, read until EOF.
    pub async fn read_tables(self, n: Option<usize>) -> io::Result<Vec<Table>> {
        self.inner.read_tables(n).await
    }

    /// Read batches and assemble into a `SuperTable`.
    ///
    /// If `n` is `None`, read until EOF.
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

    /// Read the next table from stdin, or `None` on EOF.
    pub async fn read_next(&mut self) -> io::Result<Option<Table>> {
        self.inner.read_next().await
    }
}

impl Default for StdinTableReader {
    fn default() -> Self {
        Self::new()
    }
}

impl Stream for StdinTableReader {
    type Item = io::Result<Table>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        Pin::new(&mut me.inner).poll_next(cx)
    }
}
