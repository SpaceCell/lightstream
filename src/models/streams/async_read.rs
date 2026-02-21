//! # Generic asynchronous byte stream adapter
//!
//! Wraps any [`AsyncRead`] source in a [`Stream`] that yields fixed-size
//! byte chunks.
//!
//! ## Overview
//! - Wraps any `AsyncRead` implementor using a buffered reader.
//! - Supports async backpressure via `poll_next`.
//! - Yields unaligned `Vec<u8>` chunks — alignment is deferred to the
//!   Arrow decoding layer where it matters.
//! - Chunk size controlled by [`BufferChunkSize`].
//!
//! ## Use cases
//! - Receive Arrow IPC streams from any async byte source without loading
//!   them fully into memory.
//! - Feed I/O from any transport directly into async Arrow decoding pipelines.
//! - Build custom transport adapters without reimplementing the buffered
//!   read-to-stream bridge.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::io::{AsyncRead, BufReader, ReadBuf};

use crate::enums::BufferChunkSize;

/// A `Stream` that reads any `AsyncRead` source in fixed-size byte chunks.
///
/// This is the generic building block behind all transport-specific byte
/// streams. Each transport module re-exports a type alias over this struct
/// e.g. `QuicByteStream`, `TcpByteStream` for discoverability.
///
/// ### Includes:
/// - `BufReader` based buffering over any `AsyncRead`
/// - Async back-pressure support via `poll_next`
/// - Control of chunk size via `BufferChunkSize`
///
/// ### Use cases:
/// - Receive Arrow IPC data from any async byte source
/// - Plug custom transports into Arrow decoding pipelines
pub struct AsyncReadByteStream<R: AsyncRead + Unpin> {
    /// Buffered reader over the source.
    reader: BufReader<R>,
    /// End-of-stream flag, prevents further reads after completion.
    eof: bool,
    /// Configured chunk size in bytes.
    chunk_size: usize,
}

impl<R: AsyncRead + Unpin> AsyncReadByteStream<R> {
    /// Wrap an async read source as a byte stream.
    pub fn new(source: R, size: BufferChunkSize) -> Self {
        let chunk_size = size.chunk_size();
        Self {
            reader: BufReader::with_capacity(chunk_size, source),
            eof: false,
            chunk_size,
        }
    }
}

impl<R: AsyncRead + Unpin> Stream for AsyncReadByteStream<R> {
    type Item = Result<Vec<u8>, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();

        if me.eof {
            return Poll::Ready(None);
        }

        let mut buf = vec![0u8; me.chunk_size];
        let mut read_buf = ReadBuf::new(&mut buf);

        match Pin::new(&mut me.reader).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let n = read_buf.filled().len();
                if n == 0 {
                    me.eof = true;
                    Poll::Ready(None)
                } else {
                    buf.truncate(n);
                    Poll::Ready(Some(Ok(buf)))
                }
            }
            Poll::Ready(Err(e)) => {
                me.eof = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for AsyncReadByteStream<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        Pin::new(&mut me.reader).poll_read(cx, buf)
    }
}
