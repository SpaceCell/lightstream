//! # Asynchronous stdin byte stream
//!
//! Wraps standard input in a [`Stream`] that yields fixed-size byte chunks.
//!
//! ## Overview
//! - Wraps `tokio::io::Stdin` with buffered reading.
//! - Supports async backpressure via `poll_next`.
//! - Yields unaligned `Vec<u8>` chunks — alignment is deferred to the
//!   Arrow decoding layer where it matters.
//! - Chunk size controlled by [`BufferChunkSize`].
//!
//! ## Use cases
//! - Receive Arrow IPC streams from Unix pipes without loading them fully into memory.
//! - Build CLI tools that compose with other Arrow-aware programs.
//! - Feed piped data directly into async Arrow decoding pipelines.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::io::{AsyncRead, BufReader, ReadBuf, Stdin};

use crate::enums::BufferChunkSize;

/// A `Stream` that reads from stdin in fixed-size byte chunks.
///
/// ### Includes:
/// - Tokio stdin + `BufReader` based
/// - Async back-pressure support via `poll_next`
/// - Control of chunk size via `BufferChunkSize`
///
/// ### Use cases:
/// - Receive Arrow IPC data from Unix pipes without loading the full stream into memory
/// - Build CLI tools that integrate with async Arrow decoding pipelines
pub struct StdinByteStream {
    /// Buffered reader over stdin.
    reader: BufReader<Stdin>,
    /// End-of-stream flag, prevents further reads after completion.
    eof: bool,
    /// Configured chunk size in bytes.
    chunk_size: usize,
}

impl StdinByteStream {
    /// Create a new stdin byte stream with the given chunk size.
    pub fn new(size: BufferChunkSize) -> Self {
        let chunk_size = size.chunk_size();
        Self {
            reader: BufReader::with_capacity(chunk_size, tokio::io::stdin()),
            eof: false,
            chunk_size,
        }
    }

    /// Create a stdin byte stream with the default chunk size for pipes.
    ///
    /// Uses `BufferChunkSize::Http` (64 KiB) which works well for most
    /// pipe-based streaming scenarios.
    pub fn default_size() -> Self {
        Self::new(BufferChunkSize::Http)
    }
}

impl Stream for StdinByteStream {
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

impl AsyncRead for StdinByteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        Pin::new(&mut me.reader).poll_read(cx, buf)
    }
}
