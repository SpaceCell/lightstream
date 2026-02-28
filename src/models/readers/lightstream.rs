//! Async Lightstream protocol reader.
//!
//! Wraps a [`FramedByteStream`] with a [`TLVDecoder`] and a
//! [`LightstreamCodec`], yielding decoded [`LightstreamMessage`] values.
//!
//! Table payloads are decoded using the Arrow IPC streaming protocol.
//! The codec maintains persistent schema and dictionary state per table
//! type, so the first table teaches the schema and subsequent tables
//! decode using that stored state.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use minarrow::Field;

use crate::models::decoders::tlv::TLVDecoder;
use crate::models::frames::protocol_message::LightstreamMessage;
use crate::models::protocol::codec::LightstreamCodec;
use crate::models::streams::framed_byte_stream::FramedByteStream;
use crate::traits::stream_buffer::StreamBuffer;

/// Async reader for the Lightstream protocol.
///
/// Extracts TLV frames from the underlying byte stream and decodes them
/// into [`LightstreamMessage`] values using the codec's type registry.
///
/// Implements `Stream<Item = io::Result<LightstreamMessage>>`.
pub struct LightstreamReader<S, B = Vec<u8>>
where
    S: Stream<Item = Result<B, io::Error>> + Unpin + Send,
    B: StreamBuffer,
{
    framed: FramedByteStream<S, TLVDecoder<B>, B>,
    codec: LightstreamCodec<B>,
}

impl<S, B> LightstreamReader<S, B>
where
    S: Stream<Item = Result<B, io::Error>> + Unpin + Send,
    B: StreamBuffer + Unpin,
{
    /// Create a new reader from a byte stream.
    pub fn new(stream: S) -> Self {
        Self {
            framed: FramedByteStream::new(stream, TLVDecoder::new(), 64 * 1024),
            codec: LightstreamCodec::new(),
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

impl<S, B> Stream for LightstreamReader<S, B>
where
    S: Stream<Item = Result<B, io::Error>> + Unpin + Send,
    B: StreamBuffer + Unpin,
{
    type Item = io::Result<LightstreamMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.framed).poll_next(cx) {
            Poll::Ready(Some(Ok(tlv_frame))) => {
                let msg = this.codec.decode_frame(tlv_frame.t, tlv_frame.value.as_ref())?;
                Poll::Ready(Some(Ok(msg)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
