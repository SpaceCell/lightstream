//! Async Lightstream protocol reader.
//!
//! Reads TLV frames from an [`AsyncRead`] source, decoding each into a
//! [`LightstreamMessage`] via the codec's type registry.
//!
//! The reader accumulates the 5-byte TLV header, then reads the payload
//! into a Vec64 for zero-copy decode. Column data is mapped in place
//! via SharedBuffer slices.
//!
//! Table payloads are decoded using the Arrow IPC streaming protocol.
//! The codec maintains persistent schema and dictionary state per table
//! type, so the first table teaches the schema and subsequent tables
//! decode using that stored state.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::{Field, Vec64};
use tokio::io::{AsyncRead, ReadBuf};

use crate::models::codecs::lightstream::LightstreamCodec;
use crate::models::frames::lightstream_message::{FRAME_HEADER_SIZE, LightstreamMessage};
use crate::traits::stream_buffer::StreamBuffer;

const DEFAULT_CHUNK: usize = 64 * 1024;

/// Async reader for the Lightstream protocol.
///
/// Extracts TLV frames from an AsyncRead source and decodes them
/// into [`LightstreamMessage`] values using the codec's type registry.
///
/// Implements `Stream<Item = io::Result<LightstreamMessage>>`.
pub struct LightstreamReader<B: StreamBuffer = Vec64<u8>> {
    source: Box<dyn AsyncRead + Unpin + Send>,
    codec: LightstreamCodec<B>,
    /// TLV header accumulation (5 bytes: tag + u32 LE payload_len).
    header: [u8; FRAME_HEADER_SIZE],
    header_filled: usize,
    /// Per-frame payload buffer.
    payload: Vec64<u8>,
    payload_target: usize,
    tag: u8,
    chunk_size: usize,
    eof: bool,
}

impl<B: StreamBuffer + Unpin> LightstreamReader<B> {
    /// Create a new reader from any AsyncRead source.
    pub fn new(source: impl AsyncRead + Unpin + Send + 'static) -> Self {
        Self {
            source: Box::new(source),
            codec: LightstreamCodec::new(),
            header: [0u8; FRAME_HEADER_SIZE],
            header_filled: 0,
            payload: Vec64::with_capacity(0),
            payload_target: 0,
            tag: 0,
            chunk_size: DEFAULT_CHUNK,
            eof: false,
        }
    }

    /// Register a message type. Returns the assigned type tag.
    pub fn register_message(&mut self, name: impl Into<String>) -> u8 {
        self.codec.register_message(name)
    }

    /// Register a table type with the given schema. Returns the assigned type tag.
    pub fn register_table(&mut self, name: impl Into<String>, schema: Vec<Field>) -> u8 {
        self.codec.register_table(name, schema)
    }

    /// Borrow the codec for inspection.
    pub fn codec(&self) -> &LightstreamCodec<B> {
        &self.codec
    }
}

impl<B: StreamBuffer + Unpin> Stream for LightstreamReader<B> {
    type Item = io::Result<LightstreamMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Step 1: accumulate the 5-byte TLV header.
            if this.payload_target == 0 {
                if this.header_filled < FRAME_HEADER_SIZE {
                    if this.eof {
                        if this.header_filled == 0 {
                            return Poll::Ready(None);
                        }
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "stream ended with incomplete TLV header",
                        ))));
                    }

                    let remaining = &mut this.header[this.header_filled..];
                    let mut read_buf = ReadBuf::new(remaining);
                    match Pin::new(&mut *this.source).poll_read(cx, &mut read_buf) {
                        Poll::Ready(Ok(())) => {
                            let n = read_buf.filled().len();
                            if n == 0 {
                                this.eof = true;
                                continue;
                            }
                            this.header_filled += n;
                            continue;
                        }
                        Poll::Ready(Err(e)) => {
                            this.eof = true;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                // Header complete - parse tag and payload length.
                this.tag = this.header[0];
                let payload_len =
                    u32::from_le_bytes(this.header[1..5].try_into().unwrap()) as usize;
                this.payload_target = payload_len;

                // Prepare the payload buffer. Reuse the existing
                // allocation if it has enough capacity.
                this.payload.clear();
                if this.payload.capacity() < payload_len {
                    this.payload.reserve(payload_len - this.payload.capacity());
                }

                // Handle zero-length payloads
                if payload_len == 0 {
                    this.header_filled = 0;
                    this.payload_target = 0;
                    let frame_payload =
                        std::mem::replace(&mut this.payload, Vec64::with_capacity(0));
                    let msg = this.codec.decode_frame(this.tag, frame_payload)?;
                    return Poll::Ready(Some(Ok(msg)));
                }

                continue;
            }

            // Step 2: read payload bytes into the Vec64.
            let filled = this.payload.len();
            let remaining = this.payload_target - filled;

            if remaining > 0 {
                if this.eof {
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "stream ended with incomplete TLV payload",
                    ))));
                }

                let want = remaining
                    .max(this.chunk_size)
                    .min(this.payload.capacity() - filled);
                if want == 0 {
                    this.payload.reserve(this.chunk_size);
                }

                let spare = this.payload.spare_capacity_mut();
                let read_len = spare.len().min(remaining);
                let spare_slice = unsafe {
                    std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, read_len)
                };
                let mut read_buf = ReadBuf::new(spare_slice);

                match Pin::new(&mut *this.source).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            this.eof = true;
                            continue;
                        }
                        unsafe { this.payload.set_len(filled + n) };
                        continue;
                    }
                    Poll::Ready(Err(e)) => {
                        this.eof = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            // Payload complete - hand to the codec for zero-copy decode.
            let frame_payload =
                std::mem::replace(&mut this.payload, Vec64::with_capacity(0));
            this.header_filled = 0;
            this.payload_target = 0;
            let msg = this.codec.decode_frame(this.tag, frame_payload)?;
            return Poll::Ready(Some(Ok(msg)));
        }
    }
}
