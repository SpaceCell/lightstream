// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Asynchronous Arrow IPC table sink
//!
//! Wraps any `AsyncWrite` and streams [`minarrow::Table`] values as Arrow IPC frames.
//!
//! ## Overview:
//! - Supports Stream or File protocol
//! - Handles schema emission, optional compression, dictionary batches, record batches, and end-of-stream/footer generation.
//! - Supports both 8-byte (`Vec<u8>`) and 64-byte SIMD-aligned (`Vec64<u8>`) buffers via [`TableSink`](crate::models::sinks::table_sink::TableSink) and [`TableSink64`](crate::models::sinks::table_sink::TableSink64) type aliases.
//! - Supports backpressure-friendly, chunked writes with partial-write handling in async runtimes (e.g. Tokio).

use crate::compression::Compression;
use crate::enums::IPCMessageProtocol;
use crate::models::codecs::ipc::ArrowIpcCodec;
use crate::models::writers::ipc::table_stream::TableStreamWriter;
use crate::traits::stream_buffer::StreamBuffer;
use minarrow::{Field, TableV, Vec64};
use std::io;
use tokio::io::AsyncWrite;

use futures_sink::Sink;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Async Arrow Table Sink for (`Vec<u8>`).
///
/// Use this writer to stream Arrow [`minarrow::Table`] values to any asynchronous byte sink,
/// automatically handling Arrow IPC framing, schema, dictionaries, and alignment.
/// It will write Arrow tables with 8-byte alignment.
///
/// When writing `Minarrow` objects, and high-performance/64-byte SIMD, use `TableWriter64`.
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
    B: StreamBuffer + Unpin + 'static,
{
    pub(crate) schema: Vec<Field>,
    pub(crate) codec: ArrowIpcCodec<B>,
    pub(crate) destination: W,
    pub(crate) protocol: IPCMessageProtocol,
    pub(crate) finished: bool,
    pub(crate) frame_buf: Option<B>, // Current frame being written
    pub(crate) frame_pos: usize,     // How many bytes have been written so far
    /// Pooled encode buffer reused across send_table calls.
    pub(crate) encode_buf: B,
    /// Frame-by-frame writer for File protocol with footer tracking
    pub(crate) file_writer: Option<TableStreamWriter<B>>,
}

impl<W, B> GTableSink<W, B>
where
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    B: StreamBuffer + std::fmt::Debug + Unpin + 'static,
{
    /// Create a new generic Arrow Table writer. Pass `None` for
    /// `compression` to write uncompressed batches; `Some(codec)` compresses
    /// every record-batch body.
    pub fn new(
        sink: W,
        schema: Vec<Field>,
        protocol: IPCMessageProtocol,
        compression: Option<Compression>,
    ) -> io::Result<Self> {
        let file_writer = if protocol == IPCMessageProtocol::File {
            Some(TableStreamWriter::new(schema.clone(), protocol, compression))
        } else {
            None
        };
        Ok(Self {
            codec: ArrowIpcCodec::new(schema.clone(), protocol, compression, None),
            schema,
            destination: sink,
            protocol,
            finished: false,
            frame_buf: None,
            frame_pos: 0,
            encode_buf: B::with_capacity(0),
            file_writer,
        })
    }

    /// Expose a mutable reference to the inner sink.
    pub fn sink_mut(&mut self) -> &mut W {
        &mut self.destination
    }

    /// Encode the table view into the pending frame buffer, attaching
    /// `custom_metadata` to its record batch message.
    pub(crate) fn encode_frame(
        &mut self,
        view: &TableV,
        custom_metadata: Option<&[(String, String)]>,
    ) -> io::Result<()> {
        if self.protocol == IPCMessageProtocol::Stream {
            // The Stream protocol encodes into a pooled buffer reused across
            // sends.
            let mut buf = std::mem::replace(&mut self.encode_buf, B::with_capacity(0));
            let len = buf.len();
            if len > 0 {
                buf.drain(0..len);
            }
            self.codec
                .encode_stream_batch(view, &mut buf, 0, custom_metadata)?;
            self.frame_buf = Some(buf);
            self.frame_pos = 0;
        } else if let Some(writer) = &mut self.file_writer {
            // The File protocol routes through the frame-by-frame writer that
            // tracks footer blocks.
            writer.write(view)?;
        }
        Ok(())
    }
}

impl<W, B> Sink<TableV> for GTableSink<W, B>
where
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    B: StreamBuffer + std::fmt::Debug + Unpin + 'static,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, view: TableV) -> Result<(), Self::Error> {
        self.get_mut().encode_frame(&view, None)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // drain encoder into sink, honouring partial writes
        loop {
            // If no frame buffered, pull next from the file writer's queue.
            if self.frame_buf.is_none() {
                if let Some(writer) = &mut self.file_writer {
                    if let Some(Ok(frame)) = writer.next_frame() {
                        self.frame_pos = 0;
                        self.frame_buf = Some(frame);
                    } else {
                        break;
                    }
                } else {
                    break;
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
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        } else {
                            // Frame fully written - reclaim the buffer for reuse
                            self.encode_buf = buf;
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
            if let Some(writer) = &mut self.file_writer {
                // File protocol: writer handles footer + EOS
                writer.finish()?;
            } else {
                // Stream protocol: write EOS into a buffer and queue it
                let mut eos_buf = B::with_capacity(8);
                self.codec.finish(&mut eos_buf)?;
                self.frame_buf = Some(eos_buf);
                self.frame_pos = 0;
            }
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
