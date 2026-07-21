// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Bidirectional io_uring-based Lightstream protocol connection.
//!
//! Generic over any [`UringStream`] implementor (UDS, TCP, etc.).
//! Uses tokio-uring's completion-based I/O directly - no ring thread,
//! no channels. Reads and writes happen on the calling async task via
//! the io_uring driver integrated into the tokio-uring runtime.
//!
//! Must be used from within a `tokio_uring::start()` runtime.

use std::io;

use minarrow::{Field, Vec64};
use tokio_uring::buf::BoundedBuf;

use crate::models::codecs::lightstream::LightstreamCodec;
use crate::models::decoders::limits::DecodeLimits;
use crate::models::frames::lightstream_message::{FRAME_HEADER_SIZE, LightstreamMessage};

use super::buf::UringBuf;
use super::stream::UringStream;

/// io_uring Lightstream connection over UDS.
pub type IoUringUdsConnection = IoUringConnection<tokio_uring::net::UnixStream>;

/// io_uring Lightstream connection over TCP.
pub type IoUringTcpConnection = IoUringConnection<tokio_uring::net::TcpStream>;

/// Read exactly `len` bytes from the stream into a Vec64 starting at `offset`.
async fn read_exact_vec64<S: UringStream>(
    stream: &S,
    mut buf: UringBuf,
    mut offset: usize,
    len: usize,
) -> io::Result<UringBuf> {
    let target = offset + len;
    while offset < target {
        let slice = buf.slice(offset..target);
        let (result, slice) = stream.read(slice).await;
        buf = slice.into_inner();
        let n = result?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed during read",
            ));
        }
        offset += n;
    }
    Ok(buf)
}

/// Read exactly `len` bytes from the stream into a `Vec<u8>` starting at `offset`.
async fn read_exact_vec<S: UringStream>(
    stream: &S,
    mut buf: Vec<u8>,
    mut offset: usize,
    len: usize,
) -> io::Result<Vec<u8>> {
    let target = offset + len;
    while offset < target {
        let slice = buf.slice(offset..target);
        let (result, slice) = stream.read(slice).await;
        buf = slice.into_inner();
        let n = result?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed during read",
            ));
        }
        offset += n;
    }
    Ok(buf)
}

/// Bidirectional Lightstream protocol connection using io_uring.
///
/// Generic over any [`UringStream`] transport. No ring thread, no
/// channels - reads and writes go directly through tokio-uring's
/// completion-based I/O on the calling task. Buffer recycling happens
/// naturally via the codec's SharedBuffer cache.
///
/// Must be used from within a `tokio_uring::start()` runtime.
pub struct IoUringConnection<S: UringStream> {
    stream: S,
    read_codec: LightstreamCodec<Vec64<u8>>,
    write_codec: LightstreamCodec<Vec64<u8>>,
    encode_buf: Vec64<u8>,
    /// Reusable header buffer
    header_buf: Vec<u8>,
    eof: bool,
    limits: DecodeLimits,
}

impl<S: UringStream> IoUringConnection<S> {
    /// Create a connection from any UringStream transport.
    pub fn new(stream: S, limits: Option<DecodeLimits>) -> Self {
        let limits = limits.unwrap_or_default();
        let mut header_buf = Vec::with_capacity(FRAME_HEADER_SIZE);
        header_buf.resize(FRAME_HEADER_SIZE, 0);
        Self {
            stream,
            read_codec: LightstreamCodec::new(Some(limits)),
            write_codec: LightstreamCodec::new(Some(limits)),
            encode_buf: Vec64::with_capacity(0),
            header_buf,
            eof: false,
            limits,
        }
    }

    /// Register a message type on both halves. Returns the assigned type tag.
    pub fn register_message(&mut self, name: impl Into<String>) -> u8 {
        let name = name.into();
        let tag = self.write_codec.register_message(name.clone());
        let _ = self.read_codec.register_message(name);
        tag
    }

    /// Register a table type on both halves. Returns the assigned type tag.
    pub fn register_table(&mut self, name: impl Into<String>, schema: Vec<Field>) -> u8 {
        let name = name.into();
        let tag = self
            .write_codec
            .register_table(name.clone(), schema.clone());
        let _ = self.read_codec.register_table(name, schema);
        tag
    }

    /// Send an opaque message payload by type name.
    pub async fn send(&mut self, name: &str, payload: &[u8]) -> io::Result<()> {
        let tag = self.write_codec.tag_by_name(name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown type name '{}'", name),
            )
        })?;
        let frame = self.write_codec.encode_message(tag, payload)?;
        self.stream.write_all(UringBuf(frame)).await.0?;
        Ok(())
    }

    /// Send an Arrow table by type name.
    pub async fn send_table(
        &mut self,
        name: &str,
        table: impl Into<minarrow::TableV>,
    ) -> io::Result<()> {
        let view: minarrow::TableV = table.into();
        let tag = self.write_codec.tag_by_name(name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown type name '{}'", name),
            )
        })?;

        self.write_codec
            .encode_table(tag, &view, &mut self.encode_buf)?;

        let wire_buf = std::mem::replace(&mut self.encode_buf, Vec64::with_capacity(0));
        let (result, UringBuf(returned)) = self.stream.write_all(UringBuf(wire_buf)).await;
        result?;

        self.encode_buf = returned;
        self.encode_buf.clear();
        Ok(())
    }

    /// Read the next message from the connection.
    pub async fn recv(&mut self) -> Option<io::Result<LightstreamMessage>> {
        if self.eof {
            return None;
        }

        // Read the 5-byte TLV header into the reusable header buffer
        let header_buf = std::mem::take(&mut self.header_buf);
        self.header_buf = match read_exact_vec(&self.stream, header_buf, 0, FRAME_HEADER_SIZE).await
        {
            Ok(buf) => buf,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.eof = true;
                return None;
            }
            Err(e) => {
                self.eof = true;
                return Some(Err(e));
            }
        };

        let tag = self.header_buf[0];
        let payload_len = u32::from_le_bytes(self.header_buf[1..5].try_into().unwrap()) as usize;

        // The declared length is wire data from the peer, so cap it
        // before any allocation.
        if let Err(e) = self
            .limits
            .check(payload_len, self.limits.max_frame_bytes, "TLV frame bytes")
        {
            self.eof = true;
            return Some(Err(e));
        }

        // Read payload directly into a Vec64 via UringBuf
        let payload_buf = UringBuf(Vec64::with_capacity(payload_len));

        if payload_len > 0 {
            let payload_buf =
                match read_exact_vec64(&self.stream, payload_buf, 0, payload_len).await {
                    Ok(buf) => buf,
                    Err(e) => {
                        self.eof = true;
                        return Some(Err(e));
                    }
                };
            Some(self.read_codec.decode_frame(tag, payload_buf.0))
        } else {
            Some(self.read_codec.decode_frame(tag, payload_buf.0))
        }
    }

    /// Flush the writer. No-op for io_uring.
    pub async fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Shut down the connection.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.stream.shutdown(std::net::Shutdown::Write)
    }

    /// Send a protobuf message by type name.
    #[cfg(feature = "protobuf")]
    pub async fn send_proto<M: prost::Message>(&mut self, name: &str, msg: &M) -> io::Result<()> {
        let bytes = msg.encode_to_vec();
        self.send(name, &bytes).await
    }

    /// Send a MessagePack-encoded message by type name.
    #[cfg(feature = "msgpack")]
    pub async fn send_msgpack<M: serde::Serialize>(
        &mut self,
        name: &str,
        msg: &M,
    ) -> io::Result<()> {
        let bytes = encode_msgpack(msg)?;
        self.send(name, &bytes).await
    }
}

// Transport-specific constructors

impl IoUringUdsConnection {
    /// Create a connection from a standard library `UnixStream`.
    pub fn from_unix_stream(
        stream: std::os::unix::net::UnixStream,
        limits: Option<DecodeLimits>,
    ) -> Self {
        Self::new(tokio_uring::net::UnixStream::from_std(stream), limits)
    }

    /// Create a connection from a tokio `UnixStream`.
    pub fn from_tokio_unix_stream(
        stream: tokio::net::UnixStream,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        Ok(Self::from_unix_stream(stream.into_std()?, limits))
    }

    /// Create a socketpair for inter-process I/O via io_uring.
    ///
    /// Returns the parent-side connection and a raw fd for the child
    /// process. The child fd should be passed to `Command::stdin` /
    /// `Command::stdout` (or both, since socketpairs are bidirectional).
    ///
    /// The parent side uses io_uring for completion-based I/O. The child
    /// can use any I/O model - sync, tokio, or io_uring.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (conn, child_fd) = IoUringUdsConnection::socketpair(None)?;
    /// let child = Command::new("my_worker")
    ///     .stdin(child_fd.try_clone()?)
    ///     .stdout(child_fd)
    ///     .spawn()?;
    /// conn.register_table("Data", schema);
    /// conn.send_table("Data", &table).await?;
    /// ```
    pub fn socketpair(
        limits: Option<DecodeLimits>,
    ) -> io::Result<(Self, std::os::unix::net::UnixStream)> {
        let (parent, child) = std::os::unix::net::UnixStream::pair()?;
        parent.set_nonblocking(true)?;
        let conn = Self::new(tokio_uring::net::UnixStream::from_std(parent), limits);
        Ok((conn, child))
    }
}

impl IoUringTcpConnection {
    /// Create a connection from a standard library `TcpStream`.
    pub fn from_tcp_stream(
        stream: std::net::TcpStream,
        limits: Option<DecodeLimits>,
    ) -> Self {
        Self::new(tokio_uring::net::TcpStream::from_std(stream), limits)
    }

    /// Create a connection from a tokio `TcpStream`.
    pub fn from_tokio_tcp_stream(
        stream: tokio::net::TcpStream,
        limits: Option<DecodeLimits>,
    ) -> io::Result<Self> {
        Ok(Self::from_tcp_stream(stream.into_std()?, limits))
    }
}

/// Encode a value to MessagePack bytes with efficient binary handling.
#[cfg(feature = "msgpack")]
pub(super) fn encode_msgpack<M: serde::Serialize>(msg: &M) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut serializer =
        rmp_serde::Serializer::new(&mut buf).with_bytes(rmp_serde::config::BytesMode::ForceAll);
    msg.serialize(&mut serializer)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(buf)
}
