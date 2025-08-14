//! Async Arrow IPC table readers.
//!
//! High-level wrappers over a framed IPC decoder that work with any
//! `Stream<Item = Result<B, io::Error>> + AsyncRead`, yielding decoded `Table`s.
//! Provides `Vec<u8>` and 64-byte aligned `Vec64<u8>` aliases, implements
//! `Stream<Item = io::Result<Table>>`, and forwards `AsyncRead` to the inner source
//! for seamless interop in Tokio or other async runtimes.

use crate::enums::IPCMessageProtocol;
use crate::models::decoders::ipc::table_stream::GTableStreamDecoder;
use crate::traits::stream_buffer::StreamBuffer;
use futures_core::Stream;
use minarrow::{Field, Table, Vec64};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{self, AsyncRead, ReadBuf};

/// Async Arrow Table reader for common 8-byte aligned buffers (`Vec<u8>`)
pub type TableStreamReader<S> = GAsyncTablesReader<S, Vec<u8>>;

/// Async Arrow Table reader for SIMD 64-byte aligned buffers (`Vec64<u8>`)
pub type TableStreamReader64<S> = GAsyncTablesReader<S, Vec64<u8>>;

/// Generic, high-level async Arrow Table reader for Tokio and other async runtimes.
pub struct GAsyncTablesReader<S, B>
where
    S: Stream<Item = Result<B, io::Error>> + AsyncRead + Unpin + Send + Sync,
    B: StreamBuffer,
{
    streamer: GTableStreamDecoder<S, B>,
    finished: bool,
    error: Option<io::Error>,
}

impl<S, B> GAsyncTablesReader<S, B>
where
    S: Stream<Item = Result<B, io::Error>> + AsyncRead + Unpin + Send + Sync,
    B: StreamBuffer,
{
    /// Create a new Arrow table reader over the specified async byte stream
    pub fn new(source: S, initial_capacity: usize, protocol: IPCMessageProtocol) -> Self {
        Self {
            streamer: GTableStreamDecoder::new(source, initial_capacity, protocol),
            finished: false,
            error: None,
        }
    }

    /// Returns true if the reader has fully finished and no more tables are available
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Returns the Arrow IPC protocol used for this stream
    pub fn protocol(&self) -> IPCMessageProtocol {
        self.streamer.protocol
    }

    /// Returns the decoded schema, if it has been observed
    pub fn schema(&self) -> Option<&[Field]> {
        if !self.streamer.fields.is_empty() {
            Some(&self.streamer.fields)
        } else {
            None
        }
    }

    /// Returns a reference to the current dictionary set (may be empty)
    pub fn dictionaries(&self) -> &std::collections::HashMap<i64, Vec<String>> {
        &self.streamer.dicts
    }

    /// Returns the last error encountered (if any)
    pub fn last_error(&self) -> Option<&io::Error> {
        self.error.as_ref()
    }
}

impl<S, B> Stream for GAsyncTablesReader<S, B>
where
    S: Stream<Item = Result<B, io::Error>> + AsyncRead + Unpin + Send + Sync,
    B: Unpin + StreamBuffer,
{
    type Item = io::Result<Table>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.streamer).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.finished = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(table))) => Poll::Ready(Some(Ok(table))),
            Poll::Ready(Some(Err(e))) => {
                this.finished = true;
                this.error = Some(e);
                Poll::Ready(Some(Err(this.error.take().unwrap())))
            }
        }
    }
}

// Forward AsyncRead to the inner stream for chaining and interop.
impl<S, B> AsyncRead for GAsyncTablesReader<S, B>
where
    S: Stream<Item = Result<B, io::Error>> + AsyncRead + Unpin + Send + Sync,
    B: Unpin + StreamBuffer,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.streamer.inner.inner).poll_read(cx, buf)
    }
}
