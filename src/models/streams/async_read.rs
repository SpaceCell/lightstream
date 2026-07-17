// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Generic asynchronous byte stream adapter
//!
//! Wraps any [`AsyncRead`](tokio::io::AsyncRead) source as both [`AsyncRead`](tokio::io::AsyncRead) and [`Stream`](futures_core::Stream).
//!
//! ## AsyncRead
//! Passthrough to the inner source for the direct decode path.
//!
//! ## Stream
//! Yields [`SharedBuffer`](minarrow::structs::shared_buffer::SharedBuffer) windows from a [`StreamArena`](crate::models::streams::stream_arena::StreamArena) for zero-allocation
//! streaming. This is the generic building block behind transport-specific
//! byte streams - QUIC, WebTransport, and Stdin are type aliases over this.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::structs::shared_buffer::SharedBuffer;
use tokio::io::{AsyncRead, ReadBuf};

use crate::enums::BufferChunkSize;
use crate::models::streams::stream_arena::StreamArena;

/// A `Stream` that reads any `AsyncRead` source with zero-allocation
/// arena-backed SharedBuffer windows.
///
/// Also implements `AsyncRead` as a direct passthrough for the
/// internal decode path.
pub struct AsyncReadByteStream<R: AsyncRead + Unpin> {
    source: R,
    eof: bool,
    chunk_size: usize,
    arena: StreamArena,
}

impl<R: AsyncRead + Unpin> AsyncReadByteStream<R> {
    /// Wrap an async read source as a byte stream.
    pub fn new(source: R, size: BufferChunkSize) -> Self {
        Self {
            source,
            eof: false,
            chunk_size: size.chunk_size(),
            arena: StreamArena::new(),
        }
    }
}

impl<R: AsyncRead + Unpin> Stream for AsyncReadByteStream<R> {
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
            match Pin::new(&mut me.source).poll_read(cx, &mut read_buf) {
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

impl<R: AsyncRead + Unpin> AsyncRead for AsyncReadByteStream<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        Pin::new(&mut me.source).poll_read(cx, buf)
    }
}
