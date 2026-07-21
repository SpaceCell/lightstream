// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Asynchronous TCP byte stream
//!
//! Wraps a TCP connection's read half as both [`AsyncRead`](tokio::io::AsyncRead) and [`Stream`](futures_core::Stream).
//!
//! ## AsyncRead
//! The direct decode path uses `AsyncRead` for zero-copy reads into the
//! decoder's managed buffers. This is the internal fast path.
//!
//! ## Stream
//! Yields [`SharedBuffer`](minarrow::structs::shared_buffer::SharedBuffer) windows from a [`StreamArena`](crate::models::streams::stream_arena::StreamArena) for zero-allocation
//! streaming. Each poll reads into the arena's spare capacity and yields an
//! immutable view of the filled region. In steady state, one arena allocation
//! is reused forever.

use std::io;
use std::pin::Pin;
#[cfg(feature = "tls")]
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::structs::shared_buffer::SharedBuffer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
#[cfg(feature = "tls")]
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;

use crate::enums::BufferChunkSize;
use crate::models::streams::stream_arena::StreamArena;
use crate::models::transports::tcp::TcpTransport;

/// Read half of a TCP byte stream.
///
/// Variants are dispatched at runtime so the surrounding reader stays a
/// single concrete type whether the connection is plaintext or TLS. The
/// TLS variant boxes the split-half because client and server TLS
/// streams have distinct concrete types in `tokio_rustls`; a trait
/// object keeps the public type uniform.
pub enum TcpReadHalf {
    Plain(OwnedReadHalf),
    #[cfg(feature = "tls")]
    Tls(Box<dyn AsyncRead + Send + Unpin + 'static>),
}

impl AsyncRead for TcpReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TcpReadHalf::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            TcpReadHalf::Tls(s) => Pin::new(&mut **s).poll_read(cx, buf),
        }
    }
}

/// Write half of a TCP byte stream. Same dispatch shape as [`TcpReadHalf`].
/// The Sync bound is required by `TableSink64`'s `Send + Sync` constraint;
/// the concrete TLS write halves from `tokio_rustls` satisfy it.
pub enum TcpWriteHalf {
    Plain(OwnedWriteHalf),
    #[cfg(feature = "tls")]
    Tls(Box<dyn AsyncWrite + Send + Sync + Unpin + 'static>),
}

impl AsyncWrite for TcpWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            TcpWriteHalf::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            TcpWriteHalf::Tls(s) => Pin::new(&mut **s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TcpWriteHalf::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            TcpWriteHalf::Tls(s) => Pin::new(&mut **s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            TcpWriteHalf::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            TcpWriteHalf::Tls(s) => Pin::new(&mut **s).poll_shutdown(cx),
        }
    }
}

/// A byte stream over a TCP connection.
///
/// Implements `AsyncRead` for the direct decode path and `Stream` for
/// zero-allocation SharedBuffer-based streaming. The underlying transport
/// may be plaintext or TLS-wrapped; both share the same wire framing.
pub struct TcpByteStream {
    reader: TcpReadHalf,
    eof: bool,
    chunk_size: usize,
    arena: StreamArena,
}

impl TcpByteStream {
    /// Connect to a TCP address over plaintext and return a byte stream.
    ///
    /// Splits the connection and reads from the read half.
    /// Uses `BufferChunkSize::Http` (64 KiB) as the default chunk size.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let (read_half, _write_half) = TcpTransport::connect(addr).await?;
        Ok(Self::from_read_half(read_half, BufferChunkSize::Http))
    }

    /// Wrap an existing TCP read half as a byte stream.
    ///
    /// Use this when you need to manage the split yourself,
    /// e.g. for bidirectional communication on the same socket.
    pub fn from_read_half(read_half: OwnedReadHalf, size: BufferChunkSize) -> Self {
        Self {
            reader: TcpReadHalf::Plain(read_half),
            eof: false,
            chunk_size: size.chunk_size(),
            arena: StreamArena::new(),
        }
    }

    /// Connect, upgrade the channel to TLS via the supplied
    /// `rustls::ClientConfig`, and return a byte stream over the encrypted
    /// channel. Uses `BufferChunkSize::Http` (64 KiB). Callers needing a
    /// different chunk size should hand an already-upgraded read half to
    /// [`Self::from_tls_read_half`].
    ///
    /// The caller controls verifier and root store through their
    /// `ClientConfig`; no default root store is bundled.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        addr: impl ToSocketAddrs,
        server_name: rustls_pki_types::ServerName<'static>,
        config: Arc<tokio_rustls::rustls::ClientConfig>,
    ) -> io::Result<Self> {
        let tcp = TcpStream::connect(addr).await?;
        let connector = tokio_rustls::TlsConnector::from(config);
        let tls = connector.connect(server_name, tcp).await?;
        let (read_half, _write_half) = tokio::io::split(tls);
        Ok(Self::from_tls_read_half(read_half, BufferChunkSize::Http))
    }

    /// Wrap an already-upgraded TLS read half as a byte stream.
    ///
    /// The server side typically obtains the upgraded stream by running
    /// `tokio_rustls::TlsAcceptor::accept(tcp).await`, then splits it with
    /// `tokio::io::split`. This entry point lets the caller hand the read
    /// half over without naming the split-half type at the call site.
    #[cfg(feature = "tls")]
    pub fn from_tls_read_half<R>(read_half: R, size: BufferChunkSize) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        Self {
            reader: TcpReadHalf::Tls(Box::new(read_half)),
            eof: false,
            chunk_size: size.chunk_size(),
            arena: StreamArena::new(),
        }
    }
}

impl Stream for TcpByteStream {
    type Item = Result<SharedBuffer, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();

        if me.eof {
            return Poll::Ready(None);
        }

        // Recycle or roll over if the arena is full
        if me.arena.remaining() < me.chunk_size {
            me.arena.recycle_or_reset();
        }

        // Read into the arena's spare capacity
        let chunk_start = me.arena.write_pos();
        let n = {
            let spare = me.arena.spare_uninit();
            let read_len = spare.len().min(me.chunk_size);
            let mut read_buf = ReadBuf::uninit(&mut spare[..read_len]);
            match Pin::new(&mut me.reader).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) => read_buf.filled().len(),
                Poll::Ready(Err(e)) => {
                    me.eof = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        };

        if n == 0 {
            me.eof = true;
            return Poll::Ready(None);
        }

        // SAFETY: ReadBuf reports exactly the bytes initialised by poll_read.
        unsafe { me.arena.advance(n) };
        let shared = me.arena.window(chunk_start, n);
        me.arena.align();
        Poll::Ready(Some(Ok(shared)))
    }
}

impl AsyncRead for TcpByteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        Pin::new(&mut me.reader).poll_read(cx, buf)
    }
}
