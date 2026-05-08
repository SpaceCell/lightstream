//! # Asynchronous TCP byte stream
//!
//! Wraps a TCP connection's read half as both [`AsyncRead`] and [`Stream`].
//!
//! ## AsyncRead
//! The direct decode path uses `AsyncRead` for zero-copy reads into the
//! decoder's managed buffers. This is the internal fast path.
//!
//! ## Stream
//! Yields [`SharedBuffer`] windows from a [`StreamArena`] for zero-allocation
//! streaming. Each poll reads into the arena's spare capacity and yields an
//! immutable view of the filled region. In steady state, one arena allocation
//! is reused forever.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::structs::shared_buffer::SharedBuffer;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::enums::BufferChunkSize;
use crate::models::streams::stream_arena::StreamArena;

/// A byte stream over a TCP connection.
///
/// Implements `AsyncRead` for the direct decode path and `Stream` for
/// zero-allocation SharedBuffer-based streaming.
pub struct TcpByteStream {
    reader: OwnedReadHalf,
    eof: bool,
    chunk_size: usize,
    arena: StreamArena,
}

impl TcpByteStream {
    /// Connect to a TCP address and return a byte stream.
    ///
    /// Splits the connection and reads from the read half.
    /// Uses `BufferChunkSize::Http` (64 KiB) as the default chunk size.
    pub async fn connect(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (read_half, _write_half) = stream.into_split();
        Ok(Self::from_read_half(read_half, BufferChunkSize::Http))
    }

    /// Wrap an existing TCP read half as a byte stream.
    ///
    /// Use this when you need to manage the split yourself,
    /// e.g. for bidirectional communication on the same socket.
    pub fn from_read_half(read_half: OwnedReadHalf, size: BufferChunkSize) -> Self {
        Self {
            reader: read_half,
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
            let spare = me.arena.spare_mut();
            let read_len = spare.len().min(me.chunk_size);
            let mut read_buf = ReadBuf::new(&mut spare[..read_len]);
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

        me.arena.advance(n);
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
