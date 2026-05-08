//! # WebSocket table reader
//!
//! High-level async reader that connects to a WebSocket endpoint streaming
//! Arrow IPC data and decodes it into MinArrow tables.
//!
//! Extracts the raw TCP stream after the tungstenite handshake and uses
//! [`WsRead`] for zero-copy WebSocket frame parsing on the data path.
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
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::{Field, SuperTable, Table, Vec64};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, connect_async};

use crate::enums::IPCMessageProtocol;
use crate::models::readers::ipc::table_reader::TableReader;
use crate::models::streams::websocket::{WsRead, WsWrite};
use crate::traits::transport_reader::IPCTransportReader;

/// Async Arrow IPC reader over a WebSocket connection.
///
/// Connects to a remote WebSocket endpoint, reads binary messages containing
/// Arrow IPC data, and decodes them into MinArrow tables.
///
/// Uses `WsRead` for zero-copy WebSocket frame parsing after the
/// tungstenite handshake completes.
///
/// Implements `Stream<Item = io::Result<Table>>` for continuous streaming.
pub struct WebSocketTableReader {
    inner: TableReader<Vec64<u8>>,
}

impl WebSocketTableReader {
    /// Connect to a WebSocket server streaming Arrow IPC and return a table reader.
    ///
    /// Uses `IPCMessageProtocol::Stream` and a 64 KiB initial decode capacity.
    /// The write half is dropped - use the Lightstream connection for
    /// bidirectional communication.
    pub async fn connect(url: &str) -> io::Result<Self> {
        let (ws_stream, _response) = connect_async(url)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
        let raw = ws_stream.into_inner();
        let (read_half, write_half) = tokio::io::split(raw);
        let (shared_writer, _ws_write) = WsWrite::new(write_half);
        let ws_read = WsRead::new(read_half, shared_writer);
        let inner = TableReader::<Vec64<u8>>::new(ws_read, 64 * 1024, IPCMessageProtocol::Stream);
        Ok(Self { inner })
    }

    /// Wrap a raw TCP stream (post-handshake) as a WebSocket table reader.
    ///
    /// Uses a sink writer for pong responses since the raw stream is not
    /// split. For full ping/pong support, use `connect` which splits the
    /// stream properly.
    pub fn from_raw_stream(
        stream: impl tokio::io::AsyncRead + Unpin + Send + 'static,
        protocol: IPCMessageProtocol,
    ) -> Self {
        let shared_writer = Arc::new(Mutex::new(tokio::io::sink()));
        let ws_read = WsRead::new(stream, shared_writer);
        let inner = TableReader::<Vec64<u8>>::new(ws_read, 64 * 1024, protocol);
        Self { inner }
    }
}

impl IPCTransportReader for WebSocketTableReader {
    async fn read_all_tables(self) -> io::Result<Vec<Table>> {
        self.inner.read_all_tables().await
    }

    async fn read_tables(self, n: Option<usize>) -> io::Result<Vec<Table>> {
        self.inner.read_tables(n).await
    }

    async fn read_to_super_table(
        self,
        name: Option<String>,
        n: Option<usize>,
    ) -> io::Result<SuperTable> {
        self.inner.read_to_super_table(name, n).await
    }

    async fn combine_to_table(self, name: Option<String>) -> io::Result<Table> {
        self.inner.combine_to_table(name).await
    }

    fn schema(&self) -> Option<&[Field]> {
        self.inner.schema()
    }

    async fn read_next(&mut self) -> io::Result<Option<Table>> {
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
