//! # Asynchronous Unix domain socket byte stream
//!
//! Wraps a UDS connection's read half in a [`Stream`] that yields
//! fixed-size byte chunks.
//!
//! ## Overview
//! - Splits a [`UnixStream`] and reads from the [`OwnedReadHalf`].
//! - Supports async backpressure via `poll_next`.
//! - Yields unaligned `Vec<u8>` chunks — alignment is deferred to the
//!   Arrow decoding layer where it matters.
//! - Chunk size controlled by [`BufferChunkSize`].
//!
//! ## Use cases
//! - Receive Arrow IPC streams over Unix domain sockets without loading them fully into memory.
//! - Feed local inter-process I/O directly into async Arrow decoding pipelines.

use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::io::{AsyncRead, BufReader, ReadBuf};
use tokio::net::unix::OwnedReadHalf;
use tokio::net::UnixStream;

use crate::enums::BufferChunkSize;

/// A `Stream` that reads a Unix domain socket connection in fixed-size byte chunks.
///
/// ### Includes:
/// - Tokio UDS + `BufReader` based
/// - Async back-pressure support via `poll_next`
/// - Control of chunk size via `BufferChunkSize`
///
/// ### Use cases:
/// - Receive Arrow IPC data over UDS without loading the full stream into memory
/// - Integrate local inter-process I/O into async Arrow decoding pipelines
pub struct UdsByteStream {
    /// Buffered reader over the UDS read half.
    reader: BufReader<OwnedReadHalf>,
    /// End-of-stream flag, prevents further reads after completion.
    eof: bool,
    /// Configured chunk size in bytes.
    chunk_size: usize,
}

impl UdsByteStream {
    /// Connect to a Unix domain socket and return a byte stream.
    ///
    /// Splits the connection and reads from the read half.
    /// Uses `BufferChunkSize::Http` (64 KiB) as the default chunk size.
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        let (read_half, _write_half) = stream.into_split();
        Ok(Self::from_read_half(read_half, BufferChunkSize::Http))
    }

    /// Wrap an existing UDS read half as a byte stream.
    ///
    /// Use this when you need to manage the split yourself,
    /// e.g. for bidirectional communication on the same socket.
    pub fn from_read_half(read_half: OwnedReadHalf, size: BufferChunkSize) -> Self {
        let chunk_size = size.chunk_size();
        Self {
            reader: BufReader::with_capacity(chunk_size, read_half),
            eof: false,
            chunk_size,
        }
    }
}

impl Stream for UdsByteStream {
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
