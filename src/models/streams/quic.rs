//! # Asynchronous QUIC byte stream
//!
//! Wraps a QUIC receive stream in a [`Stream`] that yields fixed-size
//! byte chunks.
//!
//! ## Overview
//! - Reads from a [`quinn::RecvStream`] using a buffered reader.
//! - Supports async backpressure via `poll_next`.
//! - Yields unaligned `Vec<u8>` chunks — alignment is deferred to the
//!   Arrow decoding layer where it matters.
//! - Chunk size controlled by [`BufferChunkSize`].
//!
//! ## Use cases
//! - Receive Arrow IPC streams over QUIC without loading them fully into memory.
//! - Feed QUIC I/O directly into async Arrow decoding pipelines.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::io::{AsyncRead, BufReader, ReadBuf};

use crate::enums::BufferChunkSize;

/// A `Stream` that reads a QUIC receive stream in fixed-size byte chunks.
///
/// ### Includes:
/// - Quinn QUIC + `BufReader` based
/// - Async back-pressure support via `poll_next`
/// - Control of chunk size via `BufferChunkSize`
///
/// ### Use cases:
/// - Receive Arrow IPC data over QUIC without loading the full stream into memory
/// - Integrate QUIC I/O into async Arrow decoding pipelines
pub struct QuicByteStream {
    /// Buffered reader over the QUIC receive stream.
    reader: BufReader<quinn::RecvStream>,
    /// End-of-stream flag, prevents further reads after completion.
    eof: bool,
    /// Configured chunk size in bytes.
    chunk_size: usize,
}

impl QuicByteStream {
    /// Wrap a QUIC receive stream as a byte stream.
    ///
    /// Use this when you have accepted a QUIC connection and opened
    /// or accepted a unidirectional/bidirectional stream.
    pub fn new(recv: quinn::RecvStream, size: BufferChunkSize) -> Self {
        let chunk_size = size.chunk_size();
        Self {
            reader: BufReader::with_capacity(chunk_size, recv),
            eof: false,
            chunk_size,
        }
    }
}

impl Stream for QuicByteStream {
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

impl AsyncRead for QuicByteStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        Pin::new(&mut me.reader).poll_read(cx, buf)
    }
}
