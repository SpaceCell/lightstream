//! # WebSocket table reader
//!
//! High-level async reader that connects to a WebSocket endpoint streaming
//! Arrow IPC data and decodes it into MinArrow tables.
//!
//! Wraps [`TableReader`] over a [`WebSocketByteStream`], hiding the wiring
//! so callers get a one-liner API.
//!
//! ## Continuous streaming
//!
//! `WebSocketTableReader` implements `Stream<Item = io::Result<Table>>`, so it
//! can be used with `StreamExt` for infinite or long-lived streams:
//!
//! ```rust,no_run
//! use futures_util::StreamExt;
//! # async fn run() -> std::io::Result<()> {
//! # use lightstream::models::readers::websocket::WebSocketTableReader;
//! let mut reader = WebSocketTableReader::connect("ws://127.0.0.1:9000").await?;
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
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::enums::IPCMessageProtocol;
use crate::models::readers::ipc::table_reader::TableReader;
use crate::models::streams::websocket::WebSocketByteStream;

/// The concrete stream type produced by splitting a client WebSocket connection.
type WsSplitStream =
    futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Async Arrow IPC reader over a WebSocket connection.
///
/// Connects to a remote WebSocket endpoint, reads binary messages containing
/// Arrow IPC data, and decodes them into MinArrow tables.
///
/// Implements `Stream<Item = io::Result<Table>>` for continuous streaming.
pub struct WebSocketTableReader {
    inner: TableReader<WebSocketByteStream<WsSplitStream>, Vec<u8>>,
}

impl WebSocketTableReader {
    /// Connect to a WebSocket server streaming Arrow IPC and return a table reader.
    ///
    /// Uses `IPCMessageProtocol::Stream` and a 64 KiB initial decode capacity.
    /// The write half of the WebSocket is dropped — use `from_stream` if you
    /// need bidirectional communication.
    pub async fn connect(url: &str) -> io::Result<Self> {
        let (ws_stream, _response) = connect_async(url)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
        let (_, read_half) = futures_util::StreamExt::split(ws_stream);
        let byte_stream = WebSocketByteStream::new(read_half);
        let inner = TableReader::new(byte_stream, 64 * 1024, IPCMessageProtocol::Stream);
        Ok(Self { inner })
    }

    /// Wrap an existing `WebSocketByteStream` as a table reader.
    pub fn from_stream(stream: WebSocketByteStream<WsSplitStream>) -> Self {
        let inner = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
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

impl Stream for WebSocketTableReader {
    type Item = io::Result<Table>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        Pin::new(&mut me.inner).poll_next(cx)
    }
}
