//! # Asynchronous TLV sink
//!
//! Wraps any `AsyncWrite` and streams Type–Length–Value (TLV) frames produced
//! by [`TLVStreamWriter`].
//!
//! ## Overview:
//! - Encodes frames as `[type: u8][length: u32 LE][value: bytes]`.
//! - Works with both 8-byte (`Vec<u8>`) and 64-byte SIMD-aligned (`Vec64<u8>`) buffers
//!   via the generic `B: StreamBuffer`.
//! - Supports backpressure-friendly, chunked writes with partial-write handling in
//!   async runtimes (e.g. Tokio).
//! - Provides `write_frame` for manual queuing of frames and `finish` to signal
//!   end-of-stream.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_sink::Sink;
use tokio::io::AsyncWrite;

use crate::models::encoders::tlv::tlv_stream::TLVStreamWriter;
use crate::models::frames::tlv_frame::TLVFrame;
use crate::traits::stream_buffer::StreamBuffer;

/// Async TLV sink: wraps an AsyncWrite and emits TLV frames as they are written.
/// Use `TLVWriter<W, Vec<u8>>` or `TLVWriter<W, Vec64<u8>>` as needed.
pub struct TLVSink<W, B>
where
    W: AsyncWrite + Unpin + Send + 'static,
    B: StreamBuffer + Unpin + 'static,
{
    stream: TLVStreamWriter<B>,
    sink: W,
    finished: bool,
    frame_buf: Option<B>,
    frame_pos: usize,
}

impl<W, B> TLVSink<W, B>
where
    W: AsyncWrite + Unpin + Send + 'static,
    B: StreamBuffer + std::fmt::Debug + Unpin + 'static,
{
    /// Construct a new TLV writer over any async sink.
    pub fn new(sink: W) -> Self {
        Self {
            stream: TLVStreamWriter::new(),
            sink,
            finished: false,
            frame_buf: None,
            frame_pos: 0,
        }
    }

    /// Queue a new TLV frame for output.
    pub fn write_frame(&mut self, t: u8, value: &[u8]) -> io::Result<()> {
        self.stream.write_frame(t, value)
    }

    /// Finish the writer and flush remaining frames.
    pub fn finish(&mut self) {
        self.stream.finish();
        self.finished = true;
    }
}

impl<W, B> Sink<TLVFrame<'_>> for TLVSink<W, B>
where
    W: AsyncWrite + Unpin + Send + 'static,
    B: StreamBuffer + std::fmt::Debug + Unpin + 'static,
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, frame: TLVFrame) -> Result<(), Self::Error> {
        self.stream.write_frame(frame.t, frame.value)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            if self.frame_buf.is_none() {
                match Pin::new(&mut self.stream).poll_next(cx) {
                    Poll::Ready(Some(Ok(frame))) => {
                        self.frame_pos = 0;
                        self.frame_buf = Some(frame);
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }

            if let Some(buf) = self.frame_buf.take() {
                let chunk = &buf.as_ref()[self.frame_pos..];
                match Pin::new(&mut self.sink).poll_write(cx, chunk) {
                    Poll::Pending => {
                        self.frame_buf = Some(buf);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                    Poll::Ready(Ok(n)) => {
                        self.frame_pos += n;
                        if self.frame_pos < buf.as_ref().len() {
                            self.frame_buf = Some(buf);
                            return Poll::Pending;
                        } else {
                            self.frame_pos = 0;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                }
            } else {
                break;
            }
        }
        Pin::new(&mut self.sink).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if !self.finished {
            self.finish();
        }
        match self.as_mut().poll_flush(cx)? {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(()) => {}
        }
        Pin::new(&mut self.sink)
            .poll_shutdown(cx)
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::frames::tlv_frame::TLVFrame;
    use futures_util::{sink::SinkExt, task};
    use minarrow::Vec64;
    use tokio::io::{AsyncReadExt, DuplexStream, duplex};

    // Helper to read exactly n bytes from a DuplexStream.
    async fn read_exact_async(stream: &mut DuplexStream, mut n: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n);
        while n > 0 {
            let mut buf = vec![0u8; n];
            let got = stream.read(&mut buf).await.expect("read failed");
            assert!(got > 0, "stream closed early");
            out.extend_from_slice(&buf[..got]);
            n -= got;
        }
        out
    }

    #[tokio::test]
    async fn test_tlv_writer_send_and_finish() {
        let (client, mut server) = duplex(256);

        let mut writer = TLVSink::<_, Vec64<u8>>::new(client);

        // Write two frames (async Sink interface)
        let f1 = TLVFrame {
            t: 42,
            value: &[0xDE, 0xAD, 0xBE],
        };
        let f2 = TLVFrame {
            t: 1,
            value: &[0xFF],
        };
        SinkExt::send(&mut writer, f1).await.unwrap();
        SinkExt::send(&mut writer, f2).await.unwrap();

        // Explicitly close the writer (calls finish then shutdown)
        SinkExt::close(&mut writer).await.unwrap();

        // Read first frame: type 42, length 3, value [0xDE, 0xAD, 0xBE]
        let buf1 = read_exact_async(&mut server, 1 + 4 + 3).await;
        assert_eq!(buf1[0], 42);
        assert_eq!(&buf1[1..5], &(3u32.to_le_bytes()));
        assert_eq!(&buf1[5..8], &[0xDE, 0xAD, 0xBE]);

        // Read second frame: type 1, length 1, value [0xFF]
        let buf2 = read_exact_async(&mut server, 1 + 4 + 1).await;
        assert_eq!(buf2[0], 1);
        assert_eq!(&buf2[1..5], &(1u32.to_le_bytes()));
        assert_eq!(&buf2[5..6], &[0xFF]);

        // No more bytes should be sent after this (EOF)
        let mut tmp = [0u8; 1];
        let n = server.read(&mut tmp).await.unwrap();
        assert_eq!(n, 0); // EOF
    }

    #[tokio::test]
    async fn test_tlv_writer_flush_empty() {
        let (client, mut server) = duplex(64);
        let mut writer = TLVSink::<_, Vec64<u8>>::new(client);

        // Finish immediately (no frames)
        SinkExt::close(&mut writer).await.unwrap();

        // Nothing should be readable (EOF)
        let mut buf = [0u8; 1];
        let n = server.read(&mut buf).await.unwrap();
        assert_eq!(n, 0); // EOF
    }

    #[tokio::test]
    async fn test_tlv_writer_manual_write_frame() {
        let (client, mut server) = duplex(64);
        let mut writer = TLVSink::<_, Vec64<u8>>::new(client);

        // Use manual API, not Sink
        writer.write_frame(99, &[0x01, 0x02, 0x03]).unwrap();
        writer.finish();

        // Flush manually (poll_flush)
        let mut pin_writer = Pin::new(&mut writer);
        let waker = task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Poll flush to completion
        while let Poll::Pending = pin_writer.as_mut().poll_flush(&mut cx) {}

        // Data is available
        let buf = read_exact_async(&mut server, 1 + 4 + 3).await;
        assert_eq!(buf[0], 99);
        assert_eq!(&buf[1..5], &(3u32.to_le_bytes()));
        assert_eq!(&buf[5..8], &[0x01, 0x02, 0x03]);
    }
}
