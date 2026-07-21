// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bidirectional Lightstream protocol connection.
//!
//! Wraps a [`LightstreamReader`](crate::models::readers::lightstream::LightstreamReader) and [`LightstreamWriter`](crate::models::writers::lightstream::LightstreamWriter) together with
//! transport-specific constructors for TCP, UDS, WebSocket, QUIC, stdio,
//! and WebTransport.
//!
//! Register named message and table types, then send and receive them over
//! a single connection. Tables use the Arrow IPC streaming protocol; messages
//! are opaque bytes, typed protobuf via [`send_proto`] with the `protobuf`
//! feature, or typed MessagePack via [`send_msgpack`] with the `msgpack`
//! feature.
//!
//! [`send_proto`]: LightstreamConnection::send_proto
//! [`send_msgpack`]: LightstreamConnection::send_msgpack

use std::io;

use minarrow::{Field, Vec64};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::models::frames::lightstream_message::LightstreamMessage;
use crate::models::decoders::limits::DecodeLimits;
use crate::models::readers::lightstream::LightstreamReader;
use crate::models::writers::lightstream::LightstreamWriter;
use crate::traits::stream_buffer::StreamBuffer;

/// Bidirectional Lightstream protocol connection.
///
/// Combines a reader and writer over the same type registry. Both sides
/// of the connection must register types in the same order.
///
/// Arrow tables are sent using the IPC streaming protocol - schema overhead
/// is paid once per table type. Messages are opaque bytes by default; enable
/// the `protobuf` feature for typed send/receive via prost.
pub struct LightstreamConnection<W, B = Vec64<u8>>
where
    W: AsyncWrite + Unpin + Send,
    B: StreamBuffer + Unpin,
{
    /// The protocol writer half.
    pub writer: LightstreamWriter<W, B>,
    /// The protocol reader half.
    pub reader: LightstreamReader<B>,
}

impl<W, B> LightstreamConnection<W, B>
where
    W: AsyncWrite + Unpin + Send,
    B: StreamBuffer + Unpin,
{
    /// Create a connection from an AsyncRead source and AsyncWrite destination.
    pub fn new(
        source: impl AsyncRead + Unpin + Send + 'static,
        writer_dest: W,
        limits: Option<DecodeLimits>,
    ) -> Self {
        Self {
            writer: LightstreamWriter::new(writer_dest),
            reader: LightstreamReader::new(source, limits),
        }
    }

    /// Register a message type on both halves. Returns the assigned type tag.
    pub fn register_message(&mut self, name: impl Into<String>) -> u8 {
        let name = name.into();
        let tag = self.writer.register_message(name.clone());
        let _ = self.reader.register_message(name);
        tag
    }

    /// Register a table type on both halves. Returns the assigned type tag.
    pub fn register_table(&mut self, name: impl Into<String>, schema: Vec<Field>) -> u8 {
        let name = name.into();
        let tag = self.writer.register_table(name.clone(), schema.clone());
        let _ = self.reader.register_table(name, schema);
        tag
    }

    /// Send an opaque message payload by type name.
    pub async fn send(&mut self, name: &str, payload: &[u8]) -> io::Result<()> {
        self.writer.send(name, payload).await
    }

    /// Send an Arrow table or table view by type name.
    pub async fn send_table(
        &mut self,
        name: &str,
        table: impl Into<minarrow::TableV>,
    ) -> io::Result<()> {
        self.writer.send_table(name, table).await
    }

    /// Read the next message from the connection.
    pub async fn recv(&mut self) -> Option<io::Result<LightstreamMessage>> {
        use futures_util::StreamExt;
        self.reader.next().await
    }

    /// Flush the writer.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }

    /// Shut down the writer.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.writer.shutdown().await
    }

    /// Send a protobuf message by type name.
    ///
    /// Encodes the message via `prost::Message::encode_to_vec` and sends it
    /// as an opaque payload.
    #[cfg(feature = "protobuf")]
    pub async fn send_proto<M: prost::Message>(&mut self, name: &str, msg: &M) -> io::Result<()> {
        self.writer.send_proto(name, msg).await
    }

    /// Send a MessagePack-encoded message by type name.
    ///
    /// Serialises the value via `rmp-serde` with `BytesMode::ForceAll` so
    /// that `Vec<u8>` and `&[u8]` fields are stored as binary, not as
    /// arrays of integers.
    #[cfg(feature = "msgpack")]
    pub async fn send_msgpack<M: serde::Serialize>(
        &mut self,
        name: &str,
        msg: &M,
    ) -> io::Result<()> {
        self.writer.send_msgpack(name, msg).await
    }
}

// ---------------------------------------------------------------------------
// Transport-specific constructors
// ---------------------------------------------------------------------------

#[cfg(feature = "tcp")]
mod tcp_impl {
    use super::*;
    use tokio::net::TcpStream;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

    /// Lightstream protocol connection over TCP.
    pub type TcpLightstreamConnection = LightstreamConnection<OwnedWriteHalf>;

    impl TcpLightstreamConnection {
        /// Create a connection from an established TCP stream.
        pub fn from_tcp(stream: TcpStream, limits: Option<DecodeLimits>) -> Self {
            let (read_half, write_half) = stream.into_split();
            Self::new(read_half, write_half, limits)
        }

        /// Create a connection from pre-split TCP halves.
        pub fn from_tcp_halves(
            read_half: OwnedReadHalf,
            write_half: OwnedWriteHalf,
            limits: Option<DecodeLimits>,
        ) -> Self {
            Self::new(read_half, write_half, limits)
        }
    }
}

#[cfg(feature = "tcp")]
pub use tcp_impl::TcpLightstreamConnection;

#[cfg(feature = "uds")]
mod uds_impl {
    use super::*;
    use tokio::net::UnixStream;
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

    /// Lightstream protocol connection over Unix domain sockets.
    pub type UdsLightstreamConnection = LightstreamConnection<OwnedWriteHalf>;

    impl UdsLightstreamConnection {
        /// Create a connection from an established Unix stream.
        pub fn from_uds(stream: UnixStream, limits: Option<DecodeLimits>) -> Self {
            let (read_half, write_half) = stream.into_split();
            Self::new(read_half, write_half, limits)
        }

        /// Create a connection from pre-split UDS halves.
        pub fn from_uds_halves(
            read_half: OwnedReadHalf,
            write_half: OwnedWriteHalf,
            limits: Option<DecodeLimits>,
        ) -> Self {
            Self::new(read_half, write_half, limits)
        }
    }
}

#[cfg(feature = "uds")]
pub use uds_impl::UdsLightstreamConnection;

#[cfg(feature = "websocket")]
mod websocket_impl {
    use super::*;
    use crate::models::streams::websocket::{WsRead, WsWrite};
    use tokio_tungstenite::WebSocketStream;

    /// Lightstream protocol connection over WebSocket.
    ///
    /// Extracts the raw TCP stream after the tungstenite handshake
    /// and uses WsRead/WsWrite for frame parsing on the data path.
    pub type WebSocketLightstreamConnection<T> =
        LightstreamConnection<WsWrite<tokio::io::WriteHalf<T>>>;

    impl<T> WebSocketLightstreamConnection<T>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        /// Create a server-side connection from a WebSocket stream.
        ///
        /// Extracts the raw TCP stream after the tungstenite handshake.
        /// Frames are written unmasked per RFC 6455's server role; the
        /// dialling side of the socket uses [`Self::from_websocket_client`].
        pub fn from_websocket(
            ws: WebSocketStream<T>,
            limits: Option<DecodeLimits>,
        ) -> Self {
            let raw = ws.into_inner();
            let (read_half, write_half) = tokio::io::split(raw);
            let (shared_writer, ws_write) = WsWrite::new(write_half);
            let ws_read = WsRead::new(read_half, shared_writer);
            Self::new(ws_read, ws_write, limits)
        }

        /// Create a client-side connection from a WebSocket stream.
        ///
        /// Extracts the raw TCP stream after the tungstenite handshake.
        /// Every outbound frame is masked with a fresh key per RFC 6455,
        /// which compliant servers require of clients.
        pub fn from_websocket_client(
            ws: WebSocketStream<T>,
            limits: Option<DecodeLimits>,
        ) -> Self {
            let raw = ws.into_inner();
            let (read_half, write_half) = tokio::io::split(raw);
            let (shared_writer, ws_write) = WsWrite::new_client(write_half);
            let ws_read = WsRead::new_client(read_half, shared_writer);
            Self::new(ws_read, ws_write, limits)
        }
    }
}

#[cfg(feature = "websocket")]
pub use websocket_impl::WebSocketLightstreamConnection;

#[cfg(feature = "quic")]
mod quic_impl {
    use super::*;

    /// Lightstream protocol connection over QUIC.
    pub type QuicLightstreamConnection = LightstreamConnection<quinn::SendStream>;

    impl QuicLightstreamConnection {
        /// Create a connection from QUIC send and receive streams.
        pub fn from_quic(
            recv: quinn::RecvStream,
            send: quinn::SendStream,
            limits: Option<DecodeLimits>,
        ) -> Self {
            Self::new(recv, send, limits)
        }
    }
}

#[cfg(feature = "quic")]
pub use quic_impl::QuicLightstreamConnection;

#[cfg(feature = "webtransport")]
mod webtransport_impl {
    use super::*;

    /// Lightstream protocol connection over WebTransport.
    pub type WebTransportLightstreamConnection = LightstreamConnection<wtransport::SendStream>;

    impl WebTransportLightstreamConnection {
        /// Create a connection from WebTransport send and receive streams.
        pub fn from_webtransport(
            recv: wtransport::RecvStream,
            send: wtransport::SendStream,
            limits: Option<DecodeLimits>,
        ) -> Self {
            Self::new(recv, send, limits)
        }
    }
}

#[cfg(feature = "webtransport")]
pub use webtransport_impl::WebTransportLightstreamConnection;

#[cfg(feature = "stdio")]
mod stdio_impl {
    use super::*;

    /// Lightstream protocol connection over stdin/stdout.
    pub type StdioLightstreamConnection = LightstreamConnection<tokio::io::Stdout>;

    impl StdioLightstreamConnection {
        /// Create a connection from stdin and stdout.
        pub fn from_stdio(limits: Option<DecodeLimits>) -> Self {
            Self::new(tokio::io::stdin(), tokio::io::stdout(), limits)
        }
    }
}

#[cfg(feature = "stdio")]
pub use stdio_impl::StdioLightstreamConnection;
