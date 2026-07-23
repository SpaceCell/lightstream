// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Asynchronous Unix domain socket byte stream
//!
//! Wraps a UDS connection's read half as both [`AsyncRead`](tokio::io::AsyncRead) and [`Stream`](futures_core::Stream).
//!
//! ## AsyncRead
//! The direct decode path uses `AsyncRead` for zero-copy reads into the
//! decoder's managed buffers. This is the internal fast path.
//!
//! ## Stream
//! Yields [`SharedBuffer`](minarrow::structs::shared_buffer::SharedBuffer) windows from a [`StreamArena`](crate::models::streams::stream_arena::StreamArena) for zero-allocation
//! streaming. Each poll reads into the arena's spare capacity and yields an
//! immutable view of the filled region.

use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::structs::shared_buffer::SharedBuffer;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::unix::OwnedReadHalf;

use crate::enums::BufferChunkSize;
use crate::models::streams::stream_arena::StreamArena;
use crate::models::transports::uds::UdsTransport;

/// A byte stream over a Unix domain socket connection.
///
/// Implements `AsyncRead` for the direct decode path and `Stream` for
/// zero-allocation SharedBuffer-based streaming.
pub struct UdsByteStream {
    reader: OwnedReadHalf,
    eof: bool,
    chunk_size: usize,
    arena: StreamArena,
}

impl UdsByteStream {
    /// Connect to a Unix domain socket and return a byte stream.
    ///
    /// Splits the connection and reads from the read half.
    /// Uses `BufferChunkSize::Http` (64 KiB) as the default chunk size.
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let (read_half, _write_half) = UdsTransport::connect(path).await?;
        Ok(Self::from_read_half(read_half, BufferChunkSize::Http))
    }

    /// Wrap an existing UDS read half as a byte stream.
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

impl Stream for UdsByteStream {
    type Item = Result<SharedBuffer, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();

        if me.eof {
            return Poll::Ready(None);
        }

        if me.arena.remaining() < me.chunk_size {
            me.arena.recycle_or_reset();
        }

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

impl AsyncRead for UdsByteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        Pin::new(&mut me.reader).poll_read(cx, buf)
    }
}
