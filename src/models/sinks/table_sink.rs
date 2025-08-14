//! # Asynchronous Arrow IPC table sink
//!
//! Wraps any `AsyncWrite` and streams [`minarrow::Table`] values as Arrow IPC frames.
//!
//! ## Overview:
//! - Supports Stream or File protocol
//! - Handles schema emission, optional compression, dictionary batches, record batches, and end-of-stream/footer generation.
//! - Supports both 8-byte (`Vec<u8>`) and 64-byte SIMD-aligned (`Vec64<u8>`) buffers via [`TableSink`] and [`TableSink64`] type aliases.
//! - Supports backpressure-friendly, chunked writes with partial-write handling in async runtimes (e.g. Tokio).

use crate::compression::Compression;
use crate::enums::IPCMessageProtocol;
use crate::models::encoders::ipc::table_stream::GTableStreamEncoder;
use crate::traits::stream_buffer::StreamBuffer;
use futures_core::Stream;
use minarrow::{Field, Table, Vec64};
use std::io;
use tokio::io::AsyncWrite;

use futures_sink::Sink;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Async Arrow Table Sink for (`Vec<u8>`).
///
/// Use this writer to stream Arrow [`Table`] values directly to any asynchronous byte sink,
/// automatically handling Arrow IPC framing, schema, dictionaries, and alignment.
/// It will write Arrow tables with 8-byte alignment.
///
/// When writing `Minarrow` objects, and high-performance/64-byte SIMD, use [`TableWriter64`].
pub type TableSink<W> = GTableSink<W, Vec<u8>>;

/// Async Arrow Table Sink for (`Vec64<u8>`).
///
/// Prefer this when you plan to use the SIMD feature for the `Lightning` crate kernels + library,
/// or otherwise want to make sure that the arrow stream /file you are writing to has
/// 64 byte aligned Arrow buffers.
pub type TableSink64<W> = GTableSink<W, Vec64<u8>>;

/// Generic asynchronous Arrow Table sink for Tokio and other async runtimes.
///
/// This wraps any compatible async byte sink (`W: AsyncWrite`) and handles
/// Arrow IPC protocol (framing, schema, dictionaries, record batches, footers).
///
/// It yields whole `Table` objects and is the recommended sink for *Minarrow* data.
/// Tables it yields are analogous with *Apache Arrow* *`Record Batches`* once
/// written into the wider ecosystem.
pub struct GTableSink<W, B>
where
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    B: StreamBuffer,
{
    pub(crate) schema: Vec<Field>,
    pub(crate) inner: GTableStreamEncoder<B>,
    pub(crate) destination: W,
    pub(crate) protocol: IPCMessageProtocol,
    pub(crate) schema_written: bool,
    pub(crate) finished: bool,
    pub(crate) frame_buf: Option<B>, // Current frame being written
    pub(crate) frame_pos: usize,     // How many bytes have been written so far
}

impl<W, B> GTableSink<W, B>
where
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    B: StreamBuffer + std::fmt::Debug + Unpin + 'static,
{
    /// Create a new generic Arrow Table writer.
    pub fn new(sink: W, schema: Vec<Field>, protocol: IPCMessageProtocol) -> io::Result<Self> {
        Ok(Self {
            inner: GTableStreamEncoder::new(schema.clone(), protocol),
            schema,
            destination: sink,
            protocol,
            schema_written: false,
            finished: false,
            frame_buf: None,
            frame_pos: 0,
        })
    }

    pub fn with_compression(
        sink: W,
        schema: Vec<Field>,
        protocol: IPCMessageProtocol,
        compression: Compression,
    ) -> io::Result<Self> {
        Ok(Self {
            inner: GTableStreamEncoder::with_compression(schema.clone(), protocol, compression),
            schema,
            destination: sink,
            protocol,
            schema_written: false,
            finished: false,
            frame_buf: None,
            frame_pos: 0,
        })
    }

    /// Expose a mutable reference to the inner sink.
    pub fn sink_mut(&mut self) -> &mut W {
        &mut self.destination
    }
}

impl<W, B> Sink<Table> for GTableSink<W, B>
where
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    B: StreamBuffer + std::fmt::Debug + Unpin + 'static,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, table: Table) -> Result<(), Self::Error> {
        if !self.schema_written {
            self.inner.write_schema_frame()?;
            self.schema_written = true;
        }
        self.inner.write_record_batch_frame(&table)?;
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // drain encoder into sink, honouring partial writes
        loop {
            // If no frame buffered, poll encoder for next one.
            if self.frame_buf.is_none() {
                match Pin::new(&mut self.inner).poll_next(cx) {
                    Poll::Ready(Some(Ok(frame))) => {
                        self.frame_pos = 0;
                        self.frame_buf = Some(frame);
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }

            // Attempt to write current frame.
            if let Some(buf) = self.frame_buf.take() {
                let remaining = &buf.as_ref()[self.frame_pos..];
                // Limit chunk size to avoid blocking the async runtime
                const MAX_WRITE_CHUNK: usize = 1024 * 1024; // 1MB chunks
                let chunk = if remaining.len() > MAX_WRITE_CHUNK {
                    &remaining[..MAX_WRITE_CHUNK]
                } else {
                    remaining
                };
                match Pin::new(&mut self.destination).poll_write(cx, chunk) {
                    Poll::Pending => {
                        self.frame_buf = Some(buf);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                    Poll::Ready(Ok(n)) => {
                        self.frame_pos += n;
                        if self.frame_pos < buf.as_ref().len() {
                            self.frame_buf = Some(buf);
                            // Explicitly wake the task to ensure it gets polled again
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        } else {
                            // frame fully written – loop for next
                            self.frame_pos = 0;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }
            } else {
                break; // nothing buffered
            }
        }

        // flush underlying AsyncWrite
        Pin::new(&mut self.destination).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Emit EOS / footer
        if !self.finished {
            self.inner.finish()?; // enqueue EOS or footer frame
            self.finished = true;
        }

        // Drain all remaining frames
        match self.as_mut().poll_flush(cx)? {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => { /* fall‑through */ }
        }

        // Shut down the underlying AsyncWrite
        Pin::new(&mut self.destination)
            .poll_shutdown(cx)
            .map_err(Into::into)
    }
}
