//! # Arrow IPC Table Stream Decoder
//!
//! Asynchronous readers that consume framed Arrow IPC byte streams and yield `minarrow::Table`
//! values. Handles schema, dictionary batches, and record batches with protocol-aware state.
//!
//! - Supports both IPC protocols: file and stream ([`IPCMessageProtocol`]).
//! - Works with standard 8-byte buffers (`Vec<u8>`) and 64-byte SIMD-aligned buffers (`Vec64<u8>`).
//! - Public aliases: [`TableStreamDecoder`] (8-byte) and [`TableStreamDecoder64`] (64-byte).
//!
//! Internally wraps a [`FramedByteStream`] with an [`ArrowIPCFrameDecoder`] and manages batch
//! state, fields, and dictionaries. Implements `futures_core::Stream` and yields complete [`Table`]
//! values.

use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::arrow::message::org::apache::arrow::flatbuf as fb;
use crate::enums::{BatchState, IPCMessageProtocol};
use crate::models::decoders::ipc::parser::{
    handle_dictionary_batch, handle_record_batch, handle_schema_header,
};
use crate::models::decoders::ipc::protocol::ArrowIPCFrameDecoder;
use crate::models::streams::framed_byte_stream::FramedByteStream;
use crate::traits::stream_buffer::StreamBuffer;
use futures_core::Stream;
use minarrow::*;

// ------------------------- User-facing aliases ----------------------------------------//

/// Stream reader for Arrow Tables using standard 8-byte aligned buffers (`Vec<u8>`).
///
/// Wraps a stream of `Vec<u8>` chunks and yields fully decoded Arrow [`Table`] values
/// from Arrow IPC frames. This is the standard choice for reading from conventional IPC sources,
/// such as files, network sockets, or memory-mapped buffers produced by other Arrow implementations.
///
/// For high-performance cases where SIMD alignment is desired, prefer [`TableStreamReader64`] to
/// eliminate run-time reallocation, ensuring optimal buffer formatting at ingestion time. This is
/// guaranteed when using the `Minarrow` writers, or if it is known that the stream writer already
/// applies 64-byte alignment.
pub type TableStreamDecoder<S> = GTableStreamDecoder<S, Vec<u8>>;

/// Stream reader for Arrow Tables using 64-byte (SIMD) aligned buffers (`Vec64<u8>`).
///
/// Wraps a stream of `Vec64<u8>` chunks and yields fully decoded Arrow [`Table`] values
/// from Arrow IPC frames. Intended for high-performance sources (such as disk or network)
/// where 64-byte alignment enables efficient SIMD processing and avoids unnecessary run-time
/// allocations and hot-path checks.
///
/// This format is guaranteed if both the writer and the reader use `Minarrow`, or if you
/// control both endpoints and can ensure 64-byte alignment. Other Arrow implementations
/// commonly use 8-byte alignment. Both are permitted by the Arrow specification, but
/// 64-byte is recommended.
pub type TableStreamDecoder64<S> = GTableStreamDecoder<S, Vec64<u8>>;

// ------------------------- Structs ------------------------------------------------//

/// Generic stream reader for Arrow Tables from an asynchronous stream of IPC frames.
///
/// This struct wraps a stream of Arrow IPC-aligned byte buffers and yields fully
/// decoded Arrow [`Table`] values according to the Arrow IPC protocol.
///
/// This is the primary interface for streaming Arrow table ingestion from arbitrary
/// async byte sources. State, field, and dictionary management are automatic.
///
/// For most users, [`TableStreamReader`] and [`TableStreamReader64`] type aliases
/// are preferred for 8-byte and 64-byte buffer alignment, respectively.
pub struct GTableStreamDecoder<S, B>
where
    S: Stream<Item = Result<B, io::Error>> + Unpin + Send + Sync,
    B: StreamBuffer,
{
    pub(crate) inner: FramedByteStream<S, ArrowIPCFrameDecoder<B>, B>,
    state: BatchState,
    pub fields: Vec<Field>,
    pub dicts: HashMap<i64, Vec<String>>,
    pub protocol: IPCMessageProtocol,
}

impl<S, B> GTableStreamDecoder<S, B>
where
    S: Stream<Item = Result<B, io::Error>> + Unpin + Send + Sync,
    B: StreamBuffer,
{
    /// Create a new generic Arrow table stream reader over the provided stream and buffer type.
    ///
    /// Automatically handles Arrow IPC framing, schema, dictionaries, and record batch transitions.
    ///
    /// - `stream`: Source stream of Arrow IPC frames.
    /// - `initial_capacity`: Initial byte buffer allocation size.
    /// - `format`: Arrow IPC protocol variant (File or Stream).
    pub fn new(stream: S, initial_capacity: usize, protocol: IPCMessageProtocol) -> Self {
        Self {
            inner: FramedByteStream::new(
                stream,
                ArrowIPCFrameDecoder::new(protocol),
                initial_capacity,
            ),
            state: BatchState::NeedSchema,
            fields: Vec::new(),
            dicts: HashMap::new(),
            protocol,
        }
    }
}

impl<S, B> Stream for GTableStreamDecoder<S, B>
where
    S: Stream<Item = Result<B, io::Error>> + Unpin + Send + Sync,
    B: StreamBuffer + Unpin,
{
    type Item = io::Result<Table>;

    /// Yields the next fully decoded [`Table`] (record batch) from the Arrow IPC stream, or `None` on end of stream.
    ///
    /// Handles Arrow framing, schema, dictionary, and record batch transitions. Returns an error if
    /// protocol or message order is violated.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            let raw_frame = match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    if !matches!(this.state, BatchState::Done) {
                        // Early EOF before Arrow EOS/footer
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "Underlying stream ended before Arrow EOF (EOS marker or footer not seen)",
                        ))));
                    }
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(frame))) => frame,
            };

            // Handle EOS (empty message) for both protocols.
            if raw_frame.message.is_empty() && raw_frame.body.is_empty() {
                this.state = BatchState::Done;
                return Poll::Ready(None);
            }

            let af_msg = flatbuffers::root::<fb::Message>(&raw_frame.message.as_ref())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            match af_msg.header_type() {
                fb::MessageHeader::Schema if matches!(this.state, BatchState::NeedSchema) => {
                    this.fields = handle_schema_header(&af_msg)?;
                    this.state = BatchState::Ready;
                    continue;
                }

                fb::MessageHeader::DictionaryBatch if matches!(this.state, BatchState::Ready) => {
                    let db = af_msg.header_as_dictionary_batch().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing DictionaryBatch header")
                    })?;
                    handle_dictionary_batch(&db, &raw_frame.body.as_ref(), &mut this.dicts)?;
                    continue;
                }

                fb::MessageHeader::RecordBatch if matches!(this.state, BatchState::Ready) => {
                    let rec = af_msg.header_as_record_batch().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing RecordBatch header")
                    })?;
                    let table = handle_record_batch(
                        &rec,
                        &this.fields,
                        &this.dicts,
                        &raw_frame.body.as_ref(),
                    )?;
                    return Poll::Ready(Some(Ok(table)));
                }

                fb::MessageHeader::NONE if this.protocol == IPCMessageProtocol::File => {
                    this.state = BatchState::Done;
                    return Poll::Ready(None);
                }
                fb::MessageHeader::NONE => {
                    this.state = BatchState::Done;
                    return Poll::Ready(None);
                }
                _ => {
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected message order",
                    ))));
                }
            }
        }
    }
}
