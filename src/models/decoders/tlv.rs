//! # TLV Frame Decoder
//!
//! Provides a [`FrameDecoder`] implementation for Type-Length-Value (TLV) encoded frames.
//!
//! ## Format
//! - **Type**: 1 byte (`u8`)
//! - **Length**: 4 bytes, little-endian (`u32`)
//! - **Value**: N bytes of payload, as specified by the length field
//!
//! Example frame layout: `[type][length][value...]`
//!
//! Produces [`TLVDecodedFrame`] instances for downstream consumers.

use std::convert::TryInto;
use std::io;

use crate::enums::DecodeResult;
use crate::models::frames::tlv_frame::TLVDecodedFrame;
use crate::traits::frame_decoder::FrameDecoder;
use crate::traits::stream_buffer::StreamBuffer;

/// Decoder for Type-Length-Value (TLV) frames.
///
/// Format:
/// - 1 byte: type field (`u8`)
/// - 4 bytes: little-endian length prefix (`u32`)
/// - N bytes: value/payload (`length` as specified)
///
/// Example: `[type][length][value...]`
pub struct TLVDecoder<B: StreamBuffer> {
    _phantom: std::marker::PhantomData<B>,
}

impl<B: StreamBuffer> TLVDecoder<B> {
    /// Create a new TLV decoder instance.
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<B: StreamBuffer> FrameDecoder for TLVDecoder<B> {
    type Frame = TLVDecodedFrame<B>;

    /// Attempt to decode a TLV frame from the buffer.
    ///
    /// Returns:
    /// - [`DecodeResult::Frame`] if a complete frame was found.
    /// - [`DecodeResult::NeedMore`] if additional bytes are required.
    /// - `Err` if the buffer is malformed.
    fn decode(&mut self, buf: &[u8]) -> io::Result<DecodeResult<Self::Frame>> {
        // At least 1 (type) + 4 (len)
        if buf.len() < 5 {
            return Ok(DecodeResult::NeedMore);
        }

        let t = buf[0];

        let len_bytes: [u8; 4] = buf[1..5].try_into().unwrap();
        let len = u32::from_le_bytes(len_bytes) as usize;

        if buf.len() < 5 + len {
            return Ok(DecodeResult::NeedMore);
        }

        let mut value = B::with_capacity(len);
        value.extend_from_slice(&buf[5..5 + len]);

        Ok(DecodeResult::Frame {
            frame: TLVDecodedFrame { t, value },
            consumed: 5 + len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::streams::framed_byte_stream::FramedByteStream;
    use futures_util::StreamExt;
    use minarrow::Vec64;

    #[tokio::test]
    async fn test_tlv_decoder() {
        let mut data = Vec::new();
        // Type 1, Length 3, Value [0xAA, 0xBB, 0xCC]
        data.push(1u8);
        data.extend_from_slice(&(3u32.to_le_bytes()));
        data.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        // Type 42, Length 5, Value [0xDE, 0xAD, 0xBE, 0xEF, 0x01]
        data.push(42u8);
        data.extend_from_slice(&(5u32.to_le_bytes()));
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01]);

        let chunks = vec![Ok(Vec64::from_slice(&data))];
        let decoder = TLVDecoder::<Vec64<u8>>::new();
        let mut stream = FramedByteStream::new(futures_util::stream::iter(chunks), decoder, 128);

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.t, 1);
        assert_eq!(first.value.as_slice(), &[0xAA, 0xBB, 0xCC]);

        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.t, 42);
        assert_eq!(second.value.as_slice(), &[0xDE, 0xAD, 0xBE, 0xEF, 0x01]);

        assert!(stream.next().await.is_none());
    }
}
