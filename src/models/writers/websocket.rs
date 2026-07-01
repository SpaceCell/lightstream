// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # WebSocket table writer
//!
//! High-level async writer that connects to a WebSocket endpoint and sends
//! Arrow IPC encoded tables as binary WebSocket messages.
//!
//! Extracts the raw TCP stream after the tungstenite handshake and uses
//! [`WsWrite`] for WebSocket binary frame encoding on the data path.
//!
//! Uses `Vec64<u8>` for 64-byte SIMD aligned encoding.
//!
//! ## Security
//!
//! The transport is whatever the URL scheme says: `ws://` is plaintext,
//! `wss://` runs the connection through tokio-tungstenite's bundled rustls
//! integration (webpki-roots verifier). Build with the `tls` feature so
//! that integration is compiled in.
//!
//! For pinned roots, a custom verifier, or client-auth keys, use
//! [`WebSocketTableWriter::connect_tls`] - it takes an
//! `Arc<rustls::ClientConfig>` directly and bypasses the bundled
//! verifier. The library does not enforce a transport policy; if a
//! deployment requires TLS, that is the caller's deployment decision.

use std::io;
use std::pin::Pin;

use futures_util::sink::SinkExt;
use minarrow::{Field, Table};
use tokio_tungstenite::connect_async;

use crate::compression::Compression;
use crate::enums::IPCMessageProtocol;
use crate::models::sinks::table_sink::TableSink64;
use crate::models::streams::websocket::WsWrite;
use crate::traits::transport_writer::IPCTransportWriter;

/// Concrete write-half type carried by [`WebSocketTableWriter`]. Boxing
/// the underlying [`tokio::io::AsyncWrite`] lets [`Self::connect`],
/// [`Self::connect_tls`] and [`Self::from_halves`] all return the same
/// struct type despite handing in different concrete write halves
/// (plaintext TCP, rustls TLS, or server-side accepted streams).
type WsAsyncWrite = Box<dyn tokio::io::AsyncWrite + Send + Sync + Unpin + 'static>;

/// Async Arrow IPC writer over a WebSocket connection.
///
/// Connects to a remote WebSocket endpoint and writes Arrow IPC stream
/// protocol data as binary WebSocket messages.
///
/// Uses `WsWrite` for WebSocket frame encoding after the tungstenite
/// handshake. Vec64 for 64-byte SIMD aligned encoding.
pub struct WebSocketTableWriter {
    sink: TableSink64<WsWrite<WsAsyncWrite>>,
}

impl WebSocketTableWriter {
    /// Connect to a WebSocket server and prepare to write Arrow IPC tables.
    ///
    /// Uses `IPCMessageProtocol::Stream` - the unbounded protocol suited
    /// for network transport where the total number of batches is not
    /// known up front.
    ///
    /// The read half is dropped - use the Lightstream connection for
    /// bidirectional communication.
    pub async fn connect(
        url: &str,
        schema: Vec<Field>,
        compression: Option<Compression>,
    ) -> io::Result<Self> {
        let ws_write = Self::plain_ws_write(url).await?;
        let sink = TableSink64::new(ws_write, schema, IPCMessageProtocol::Stream, compression)?;
        Ok(Self { sink })
    }

    async fn plain_ws_write(url: &str) -> io::Result<WsWrite<WsAsyncWrite>> {
        let (ws_stream, _response) = connect_async(url)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
        let raw = ws_stream.into_inner();
        let (_read_half, write_half) = tokio::io::split(raw);
        let boxed: WsAsyncWrite = Box::new(write_half);
        let (_shared, ws_write) = WsWrite::new(boxed);
        Ok(ws_write)
    }

    /// Connect to a `wss://` endpoint, performing the TLS handshake using
    /// the supplied `rustls::ClientConfig`. Pass `None` for `compression`
    /// to write uncompressed batches.
    ///
    /// `connect` already handles `wss://` via tokio-tungstenite's bundled
    /// webpki-roots verifier; this entry point is for callers that need a
    /// custom verifier, pinned roots, or client-auth keys.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        url: &str,
        config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
        schema: Vec<Field>,
        compression: Option<Compression>,
    ) -> io::Result<Self> {
        let ws_write = Self::tls_ws_write(url, config).await?;
        let sink = TableSink64::new(ws_write, schema, IPCMessageProtocol::Stream, compression)?;
        Ok(Self { sink })
    }

    #[cfg(feature = "tls")]
    async fn tls_ws_write(
        url: &str,
        config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    ) -> io::Result<WsWrite<WsAsyncWrite>> {
        use tokio_tungstenite::{Connector, connect_async_tls_with_config};
        let connector = Connector::Rustls(config);
        // tokio-tungstenite's positional args: tungstenite WebSocketConfig
        // override (None = library defaults) and a Nagle disable flag (false
        // = leave the socket's default).
        let ws_config: Option<tokio_tungstenite::tungstenite::protocol::WebSocketConfig> = None;
        let disable_nagle = false;
        let (ws_stream, _response) =
            connect_async_tls_with_config(url, ws_config, disable_nagle, Some(connector))
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
        let raw = ws_stream.into_inner();
        let (_read_half, write_half) = tokio::io::split(raw);
        let boxed: WsAsyncWrite = Box::new(write_half);
        let (_shared, ws_write) = WsWrite::new(boxed);
        Ok(ws_write)
    }

    /// Build a table writer from the raw post-handshake read and write
    /// halves of a WebSocket-upgraded TCP/TLS stream. The read half is
    /// retained inside a shared `WsWrite` so the writer can answer
    /// inbound ping frames with pongs on the same socket.
    ///
    /// Symmetric to
    /// [`WebSocketTableReader::from_halves`](crate::models::readers::websocket::WebSocketTableReader::from_halves).
    pub fn from_halves<R, W>(
        _read_half: R,
        write_half: W,
        schema: Vec<Field>,
        compression: Option<Compression>,
    ) -> io::Result<Self>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
        W: tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
    {
        let boxed: WsAsyncWrite = Box::new(write_half);
        let (_shared, ws_write) = WsWrite::new(boxed);
        let sink = TableSink64::new(ws_write, schema, IPCMessageProtocol::Stream, compression)?;
        Ok(Self { sink })
    }
}

impl IPCTransportWriter for WebSocketTableWriter {
    fn schema(&self) -> &[Field] {
        &self.sink.schema
    }

    fn register_dictionary(&mut self, dict_id: i64, values: Vec<String>) {
        self.sink.codec.register_dictionary(dict_id, values);
    }

    async fn write_table(&mut self, table: Table) -> io::Result<()> {
        SinkExt::send(&mut self.sink, table).await?;
        SinkExt::flush(&mut self.sink).await?;
        Ok(())
    }

    async fn write_all_tables(&mut self, tables: Vec<Table>) -> io::Result<()> {
        let mut sink = Pin::new(&mut self.sink);
        for table in tables {
            SinkExt::send(&mut sink, table).await?;
        }
        SinkExt::close(&mut sink).await?;
        Ok(())
    }

    async fn finish(&mut self) -> io::Result<()> {
        SinkExt::close(&mut self.sink).await
    }
}
