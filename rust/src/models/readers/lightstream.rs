// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Async Lightstream protocol reader.
//!
//! Reads TLV frames from an [`AsyncRead`](tokio::io::AsyncRead) source, decoding each into a
//! [`LightstreamMessage`](crate::models::frames::lightstream_message::LightstreamMessage) via the codec's type registry.
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
use crate::models::decoders::limits::DecodeLimits;
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
    limits: DecodeLimits,
}

impl<B: StreamBuffer + Unpin> LightstreamReader<B> {
    /// Create a new reader from any AsyncRead source.
    pub fn new(
        source: impl AsyncRead + Unpin + Send + 'static,
        limits: Option<DecodeLimits>,
    ) -> Self {
        let limits = limits.unwrap_or_default();
        Self {
            source: Box::new(source),
            codec: LightstreamCodec::new(Some(limits)),
            header: [0u8; FRAME_HEADER_SIZE],
            header_filled: 0,
            payload: Vec64::with_capacity(0),
            payload_target: 0,
            tag: 0,
            chunk_size: DEFAULT_CHUNK,
            eof: false,
            limits,
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

                // Header complete - parse tag and payload length. The
                // declared length is wire data from the peer, so cap it
                // before any allocation.
                this.tag = this.header[0];
                let payload_len =
                    u32::from_le_bytes(this.header[1..5].try_into().unwrap()) as usize;
                if let Err(e) =
                    this.limits
                        .check(payload_len, this.limits.max_frame_bytes, "TLV frame bytes")
                {
                    this.eof = true;
                    return Poll::Ready(Some(Err(e)));
                }
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
                let mut read_buf = ReadBuf::uninit(&mut spare[..read_len]);

                match Pin::new(&mut *this.source).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            this.eof = true;
                            continue;
                        }
                        // SAFETY: `n` is the count tokio just initialised in
                        // the spare slice; `filled + n <= filled + read_len`
                        // which the spare-length bound above keeps within
                        // `this.payload.capacity()`.
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
            let frame_payload = std::mem::replace(&mut this.payload, Vec64::with_capacity(0));
            this.header_filled = 0;
            this.payload_target = 0;
            let msg = this.codec.decode_frame(this.tag, frame_payload)?;
            return Poll::Ready(Some(Ok(msg)));
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::*;

    /// A frame header declaring more bytes than `max_frame_bytes` is
    /// refused before any allocation happens.
    #[tokio::test]
    async fn oversized_frame_length_is_refused() {
        let mut header = vec![1u8];
        header.extend_from_slice(&u32::MAX.to_le_bytes());
        let mut reader: LightstreamReader = LightstreamReader::new(
            std::io::Cursor::new(header),
            Some(DecodeLimits {
                max_frame_bytes: 1024,
                ..DecodeLimits::default()
            }),
        );
        let err = reader.next().await.unwrap().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("TLV frame bytes"));
    }
}
