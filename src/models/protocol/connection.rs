//! Bidirectional Lightstream protocol connection.
//!
//! Wraps a [`LightstreamReader`] and [`LightstreamWriter`] together with
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

use futures_core::Stream;
use minarrow::Field;
use tokio::io::AsyncWrite;

use crate::models::frames::protocol_message::LightstreamMessage;
use crate::models::readers::lightstream::LightstreamReader;
use crate::models::writers::lightstream::LightstreamWriter;
use crate::traits::stream_buffer::StreamBuffer;

/// Bidirectional Lightstream protocol connection.
///
/// Combines a reader and writer over the same type registry. Both sides
/// of the connection must register types in the same order.
///
/// Arrow tables are sent using the IPC streaming protocol — schema overhead
/// is paid once per table type. Messages are opaque bytes by default; enable
/// the `protobuf` feature for typed send/receive via prost.
pub struct LightstreamConnection<S, W, B = Vec<u8>>
where
    S: Stream<Item = Result<B, io::Error>> + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
    B: StreamBuffer + Unpin,
{
    /// The protocol writer half.
    pub writer: LightstreamWriter<W, B>,
    /// The protocol reader half.
    pub reader: LightstreamReader<S, B>,
}

impl<S, W, B> LightstreamConnection<S, W, B>
where
    S: Stream<Item = Result<B, io::Error>> + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
    B: StreamBuffer + Unpin,
{
    /// Create a connection from pre-built reader and writer halves.
    pub fn from_parts(reader_stream: S, writer_dest: W) -> Self {
        Self {
            writer: LightstreamWriter::new(writer_dest),
            reader: LightstreamReader::new(reader_stream),
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

    /// Send an Arrow table by type name.
    pub async fn send_table(&mut self, name: &str, table: &minarrow::Table) -> io::Result<()> {
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
    pub async fn send_proto<M: prost::Message>(
        &mut self,
        name: &str,
        msg: &M,
    ) -> io::Result<()> {
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
    use crate::enums::BufferChunkSize;
    use crate::models::streams::tcp::TcpByteStream;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::TcpStream;

    /// Lightstream protocol connection over TCP.
    pub type TcpLightstreamConnection =
        LightstreamConnection<TcpByteStream, OwnedWriteHalf, Vec<u8>>;

    impl TcpLightstreamConnection {
        /// Create a connection from an established TCP stream.
        pub fn from_tcp(stream: TcpStream) -> Self {
            let (read_half, write_half) = stream.into_split();
            let byte_stream = TcpByteStream::from_read_half(read_half, BufferChunkSize::Http);
            Self::from_parts(byte_stream, write_half)
        }

        /// Create a connection from pre-split TCP halves.
        pub fn from_tcp_halves(read_half: OwnedReadHalf, write_half: OwnedWriteHalf) -> Self {
            let byte_stream = TcpByteStream::from_read_half(read_half, BufferChunkSize::Http);
            Self::from_parts(byte_stream, write_half)
        }
    }
}

#[cfg(feature = "tcp")]
pub use tcp_impl::TcpLightstreamConnection;

#[cfg(feature = "uds")]
mod uds_impl {
    use super::*;
    use crate::enums::BufferChunkSize;
    use crate::models::streams::uds::UdsByteStream;
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::UnixStream;

    /// Lightstream protocol connection over Unix domain sockets.
    pub type UdsLightstreamConnection =
        LightstreamConnection<UdsByteStream, OwnedWriteHalf, Vec<u8>>;

    impl UdsLightstreamConnection {
        /// Create a connection from an established Unix stream.
        pub fn from_uds(stream: UnixStream) -> Self {
            let (read_half, write_half) = stream.into_split();
            let byte_stream = UdsByteStream::from_read_half(read_half, BufferChunkSize::Http);
            Self::from_parts(byte_stream, write_half)
        }

        /// Create a connection from pre-split UDS halves.
        pub fn from_uds_halves(read_half: OwnedReadHalf, write_half: OwnedWriteHalf) -> Self {
            let byte_stream = UdsByteStream::from_read_half(read_half, BufferChunkSize::Http);
            Self::from_parts(byte_stream, write_half)
        }
    }
}

#[cfg(feature = "uds")]
pub use uds_impl::UdsLightstreamConnection;

#[cfg(feature = "websocket")]
mod websocket_impl {
    use super::*;
    use crate::models::streams::websocket::{WebSocketByteStream, WebSocketSinkAdapter};
    use futures_util::stream::{SplitSink, SplitStream};
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::WebSocketStream;

    /// Lightstream protocol connection over WebSocket.
    ///
    /// The type parameters reflect the split WebSocket halves.
    pub type WebSocketLightstreamConnection<T> = LightstreamConnection<
        WebSocketByteStream<SplitStream<WebSocketStream<T>>>,
        WebSocketSinkAdapter<SplitSink<WebSocketStream<T>, Message>>,
        Vec<u8>,
    >;

    impl<T> WebSocketLightstreamConnection<T>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        /// Create a connection from a WebSocket stream.
        pub fn from_websocket(ws: WebSocketStream<T>) -> Self {
            let (sink, stream) = ws.split();
            let byte_stream = WebSocketByteStream::new(stream);
            let write_adapter = WebSocketSinkAdapter::new(sink);
            Self::from_parts(byte_stream, write_adapter)
        }
    }
}

#[cfg(feature = "websocket")]
pub use websocket_impl::WebSocketLightstreamConnection;

#[cfg(feature = "quic")]
mod quic_impl {
    use super::*;
    use crate::models::streams::quic::QuicByteStream;
    use crate::enums::BufferChunkSize;

    /// Lightstream protocol connection over QUIC.
    pub type QuicLightstreamConnection =
        LightstreamConnection<QuicByteStream, quinn::SendStream, Vec<u8>>;

    impl QuicLightstreamConnection {
        /// Create a connection from QUIC send and receive streams.
        pub fn from_quic(recv: quinn::RecvStream, send: quinn::SendStream) -> Self {
            let byte_stream = QuicByteStream::new(recv, BufferChunkSize::WebTransport);
            Self::from_parts(byte_stream, send)
        }
    }
}

#[cfg(feature = "quic")]
pub use quic_impl::QuicLightstreamConnection;

#[cfg(feature = "webtransport")]
mod webtransport_impl {
    use super::*;
    use crate::models::streams::webtransport::WebTransportByteStream;
    use crate::enums::BufferChunkSize;

    /// Lightstream protocol connection over WebTransport.
    pub type WebTransportLightstreamConnection =
        LightstreamConnection<WebTransportByteStream, wtransport::SendStream, Vec<u8>>;

    impl WebTransportLightstreamConnection {
        /// Create a connection from WebTransport send and receive streams.
        pub fn from_webtransport(
            recv: wtransport::RecvStream,
            send: wtransport::SendStream,
        ) -> Self {
            let byte_stream = WebTransportByteStream::new(recv, BufferChunkSize::WebTransport);
            Self::from_parts(byte_stream, send)
        }
    }
}

#[cfg(feature = "webtransport")]
pub use webtransport_impl::WebTransportLightstreamConnection;

#[cfg(feature = "stdio")]
mod stdio_impl {
    use super::*;
    use crate::models::streams::stdio::{StdinByteStream, from_stdin_default};

    /// Lightstream protocol connection over stdin/stdout.
    pub type StdioLightstreamConnection =
        LightstreamConnection<StdinByteStream, tokio::io::Stdout, Vec<u8>>;

    impl StdioLightstreamConnection {
        /// Create a connection from stdin and stdout.
        pub fn from_stdio() -> Self {
            let byte_stream = from_stdin_default();
            let stdout = tokio::io::stdout();
            Self::from_parts(byte_stream, stdout)
        }
    }
}

#[cfg(feature = "stdio")]
pub use stdio_impl::StdioLightstreamConnection;
