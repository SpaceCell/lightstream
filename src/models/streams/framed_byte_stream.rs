//! # Generic async framed byte stream
//!
//! Adapts any chunked byte source into a stream of protocol frames using a
//! user-supplied [`FrameDecoder`].
//!
//! - Works with any `GenByteStream<B>` (e.g., network/file sources).
//! - Buffers partial input and yields complete frames as soon as available.
//! - Propagates protocol errors; detects truncated frames at EOF.
//! - Supports both standard and SIMD-aligned buffers via `StreamBuffer`.

use futures_core::Stream;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::enums::DecodeResult;
use crate::traits::byte_stream::GenByteStream;
use crate::traits::frame_decoder::FrameDecoder;
use crate::traits::stream_buffer::StreamBuffer;

/// Asynchronous framed byte stream adapter for binary protocols.
///
/// Converts a chunked byte source (`GenByteStream`) into a stream of
/// protocol-level frames using a user-supplied [`FrameDecoder`].
///
/// ## Overview
/// - Accepts any async source yielding `StreamBuffer` chunks (e.g. file, socket).
/// - Maintains an internal rolling buffer (`B`) for incremental decoding.
/// - Uses the decoder to extract frames as soon as enough bytes are available.
/// - On EOF, any remaining bytes in the buffer are treated as a truncated frame and error.
///
/// ## Behaviour
/// - Decoder output must follow [`DecodeResult`] semantics.
/// - Buffer is only drained after a successful frame decode.
/// - Suitable for framed protocols like Arrow IPC, Protobuf, custom TLV, etc.
///
/// ## Construction
/// Use [`FramedByteStream::new(stream, decoder, capacity)`] to create.
///
/// ## Errors
/// - Protocol errors or truncated buffers result in `io::Error`.
///
/// ## Examples
/// ```ignore
/// let stream = MyChunkedSource::new(...);
/// let decoder = MyFrameDecoder::new();
/// let framed = FramedByteStream::new(stream, decoder, 4096);
/// ```
pub struct FramedByteStream<S, D, B>
where
    S: GenByteStream<B>,
    D: FrameDecoder,
    B: StreamBuffer,
{
    /// The underlying byte source.
    pub(crate) inner: S,
    /// The stateful frame decoder.
    decoder: D,
    /// Rolling internal buffer holding unread/partial bytes.
    buf: B,
}

impl<S, D, B> FramedByteStream<S, D, B>
where
    S: GenByteStream<B>,
    D: FrameDecoder,
    B: StreamBuffer,
{
    /// Create a new framed byte stream with the specified decoder and buffer capacity.
    pub fn new(stream: S, decoder: D, initial_capacity: usize) -> Self {
        Self {
            inner: stream,
            decoder,
            buf: B::with_capacity(initial_capacity),
        }
    }
}

/// Implements `Stream` to yield decoded protocol frames from a chunked byte source.
impl<S, D, B> Stream for FramedByteStream<S, D, B>
where
    S: GenByteStream<B>,
    D: FrameDecoder + Unpin,
    B: StreamBuffer + Unpin,
{
    type Item = Result<D::Frame, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();

        loop {
            // Attempt to extract as many frames as possible from the buffer.
            match me.decoder.decode(me.buf.as_ref()) {
                Ok(DecodeResult::Frame { frame, consumed }) => {
                    // Remove consumed bytes from the buffer.
                    me.buf.drain(0..consumed);
                    return Poll::Ready(Some(Ok(frame)));
                }
                Ok(DecodeResult::NeedMore) => {
                    // Not enough bytes for a frame—read more data.
                }
                Err(e) => {
                    me.buf = B::default();
                    return Poll::Ready(Some(Err(e)));
                }
            }

            // Fetch the next chunk from the underlying byte stream.
            match Pin::new(&mut me.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => {
                    me.buf.extend_from_slice(chunk.as_ref());
                    continue; // Retry decoding with more data.
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // End-of-stream: if buffer still contains data, it's a protocol error.
                    if me.buf.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "truncated frame at end of stream",
                        ))));
                    }
                }
            }
        }
    }
}
