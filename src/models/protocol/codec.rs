//! Unified type registry with encode and decode for the Lightstream protocol.
//!
//! [`LightstreamCodec`] merges the encoder and decoder into a single struct.
//! Since both sides of a connection must register types in the same order,
//! a unified codec eliminates dual-registration bugs.
//!
//! Messages are opaque `&[u8]` payloads. Tables leverage the real Arrow IPC
//! streaming protocol — the same one used by the rest of lightstream — so
//! schema and dictionary overhead is paid once per table type, not per table.
//!
//! ## How table encoding works
//!
//! Each registered table type gets a persistent `GTableStreamEncoder`. On
//! the first call to [`encode_table`], the encoder emits the full IPC stream
//! header: schema frame, dictionary frames for any categorical columns, and
//! the record batch. On subsequent calls, only the record batch is emitted.
//! The TLV frame wraps whatever IPC frames the encoder produces.
//!
//! On the decode side, the codec maintains persistent schema and dictionary
//! state per type. The first payload teaches the decoder the schema; after
//! that, batch-only payloads are decoded using the stored state.
//!
//! [`encode_table`]: LightstreamCodec::encode_table

use std::collections::HashMap;
use std::io;

use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::Field;

use crate::arrow::message::org::apache::arrow::flatbuf as fb;
use crate::enums::{DecodeResult, IPCMessageProtocol};
use crate::models::decoders::ipc::parser::{
    handle_dictionary_batch, handle_record_batch, handle_schema_header,
};
use crate::models::decoders::ipc::protocol::ArrowIPCFrameDecoder;
use crate::models::encoders::ipc::table_stream::GTableStreamEncoder;
use crate::models::frames::protocol_message::{FrameKind, LightstreamMessage, FRAME_HEADER_SIZE};
use crate::traits::frame_decoder::FrameDecoder;
use crate::traits::stream_buffer::StreamBuffer;

/// Internal metadata for a registered type.
struct TypeEntry<B: StreamBuffer> {
    name: String,
    kind: FrameKind,
    /// Persistent IPC streaming encoder for Table types. Schema and dict
    /// frames are emitted on the first table; subsequent tables emit only
    /// record batch frames.
    encoder: Option<GTableStreamEncoder<B>>,
    /// Persistent decode state for Table types: schema fields learned from
    /// the first payload's schema frame.
    decode_fields: Vec<Field>,
    /// Persistent decode state for Table types: dictionaries accumulated
    /// across payloads.
    decode_dicts: HashMap<i64, Vec<String>>,
}

/// Unified Lightstream protocol codec.
///
/// Maintains a type registry mapping sequential tags to either message or
/// table types. Both sides of a connection share the same registration
/// sequence, and this struct handles both encoding and decoding.
///
/// Table types use the Arrow IPC streaming protocol internally: the
/// encoder sends schema + dictionaries on the first table for each type,
/// then only record batches. The decoder accumulates schema and dictionary
/// state so it handles both full and batch-only payloads.
pub struct LightstreamCodec<B: StreamBuffer = Vec<u8>> {
    types: Vec<TypeEntry<B>>,
    name_index: HashMap<String, u8>,
}

impl<B: StreamBuffer + Unpin> LightstreamCodec<B> {
    /// Create a new empty codec.
    pub fn new() -> Self {
        Self {
            types: Vec::new(),
            name_index: HashMap::new(),
        }
    }

    /// Register a message type. Returns the assigned type tag.
    ///
    /// Messages are opaque byte payloads — encoding is the caller's
    /// responsibility.
    pub fn register_message(&mut self, name: impl Into<String>) -> u8 {
        let name = name.into();
        let tag = self.types.len() as u8;
        self.name_index.insert(name.clone(), tag);
        self.types.push(TypeEntry {
            name,
            kind: FrameKind::Message,
            encoder: None,
            decode_fields: Vec::new(),
            decode_dicts: HashMap::new(),
        });
        tag
    }

    /// Register a table type with the given Arrow schema. Returns the
    /// assigned type tag.
    pub fn register_table(&mut self, name: impl Into<String>, schema: Vec<Field>) -> u8 {
        let name = name.into();
        let tag = self.types.len() as u8;
        self.name_index.insert(name.clone(), tag);

        let mut encoder = GTableStreamEncoder::new(schema.clone(), IPCMessageProtocol::Stream);
        // Pre-register dictionaries for categorical columns
        for (i, field) in schema.iter().enumerate() {
            if let ArrowType::Dictionary(_) = field.dtype {
                encoder.register_dictionary(i as i64, Vec::new());
            }
        }

        self.types.push(TypeEntry {
            name,
            kind: FrameKind::Table,
            encoder: Some(encoder),
            decode_fields: Vec::new(),
            decode_dicts: HashMap::new(),
        });
        tag
    }

    /// Look up a type tag by name.
    pub fn tag_by_name(&self, name: &str) -> Option<u8> {
        self.name_index.get(name).copied()
    }

    /// Look up the name of a registered type by tag.
    pub fn name_by_tag(&self, tag: u8) -> Option<&str> {
        self.types.get(tag as usize).map(|e| e.name.as_str())
    }

    /// Look up the kind of a registered type by tag.
    pub fn kind_by_tag(&self, tag: u8) -> Option<FrameKind> {
        self.types.get(tag as usize).map(|e| e.kind)
    }

    /// Encode a message payload into a TLV frame.
    pub fn encode_message(&self, tag: u8, payload: &[u8]) -> io::Result<B> {
        let entry = self.types.get(tag as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown type tag {}", tag),
            )
        })?;
        if entry.kind != FrameKind::Message {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("type tag {} is not a message type", tag),
            ));
        }
        encode_frame::<B>(tag, payload)
    }

    /// Encode an Arrow table into a TLV frame.
    ///
    /// Uses the persistent IPC streaming encoder: the first call for a given
    /// type includes schema and dictionary frames; subsequent calls emit only
    /// the record batch.
    pub fn encode_table(&mut self, tag: u8, table: &minarrow::Table) -> io::Result<B> {
        let entry = self.types.get_mut(tag as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown type tag {}", tag),
            )
        })?;
        if entry.kind != FrameKind::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("type tag {} is not a table type", tag),
            ));
        }

        let encoder = entry.encoder.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "table encoder missing")
        })?;

        let ipc_payload = encode_table_to_ipc(encoder, table)?;
        encode_frame::<B>(tag, ipc_payload.as_ref())
    }

    /// Decode a TLV frame payload into a [`LightstreamMessage`].
    ///
    /// For table types, the decoder maintains persistent schema and dictionary
    /// state. The first payload is expected to contain a schema frame followed
    /// by optional dictionary frames and a record batch. Subsequent payloads
    /// may contain only a record batch, using the previously learned schema.
    pub fn decode_frame(&mut self, tag: u8, payload: &[u8]) -> io::Result<LightstreamMessage> {
        let entry = self.types.get_mut(tag as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown type tag {}", tag),
            )
        })?;

        match entry.kind {
            FrameKind::Message => Ok(LightstreamMessage::Message {
                tag,
                payload: payload.to_vec(),
            }),
            FrameKind::Table => {
                let table = decode_ipc_payload(
                    payload,
                    &mut entry.decode_fields,
                    &mut entry.decode_dicts,
                )?;
                Ok(LightstreamMessage::Table { tag, table })
            }
        }
    }
}

impl<B: StreamBuffer + Unpin> Default for LightstreamCodec<B> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Build a complete TLV frame: `[tag: u8][len: u32 LE][payload]`.
fn encode_frame<B: StreamBuffer>(tag: u8, payload: &[u8]) -> io::Result<B> {
    let total = FRAME_HEADER_SIZE + payload.len();
    let mut buf = B::with_capacity(total);
    buf.push(tag);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    Ok(buf)
}

/// Encode a table through the persistent IPC streaming encoder.
///
/// Handles dictionary registration for categorical columns, writes the
/// record batch frame, and drains all queued IPC frames into one buffer.
/// The encoder retains its state across calls, so schema and dictionary
/// frames are only emitted when needed.
fn encode_table_to_ipc<B: StreamBuffer + Unpin>(
    encoder: &mut GTableStreamEncoder<B>,
    table: &minarrow::Table,
) -> io::Result<B> {
    // Register dictionary values from categorical columns
    for (i, col) in table.cols.iter().enumerate() {
        if let ArrowType::Dictionary(_) = col.field.dtype {
            let uniques = extract_dict_values(&col.array);
            encoder.register_dictionary(i as i64, uniques);
        }
    }

    encoder.write_record_batch_frame(table)?;

    let mut total_len = 0;
    for frame in &encoder.out_frames {
        total_len += frame.as_ref().len();
    }

    let mut out = B::with_capacity(total_len);
    while let Some(frame) = encoder.out_frames.pop_front() {
        out.extend_from_slice(frame.as_ref());
    }

    Ok(out)
}

/// Extract dictionary values from a categorical column.
fn extract_dict_values(array: &minarrow::Array) -> Vec<String> {
    match array {
        minarrow::Array::TextArray(text) => match text {
            minarrow::TextArray::Categorical32(arr) => arr.unique_values.iter().cloned().collect(),
            #[cfg(feature = "extended_categorical")]
            minarrow::TextArray::Categorical8(arr) => arr.unique_values.iter().cloned().collect(),
            #[cfg(feature = "extended_categorical")]
            minarrow::TextArray::Categorical16(arr) => arr.unique_values.iter().cloned().collect(),
            #[cfg(feature = "extended_categorical")]
            minarrow::TextArray::Categorical64(arr) => arr.unique_values.iter().cloned().collect(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Decode an Arrow IPC payload using persistent schema and dictionary state.
///
/// Feeds the payload through `ArrowIPCFrameDecoder` to extract IPC frames,
/// then dispatches each frame:
/// - Schema → updates `fields`
/// - DictionaryBatch → updates `dicts`
/// - RecordBatch → decoded into a Table using current `fields` and `dicts`
///
/// If the payload contains no schema frame, the previously accumulated
/// `fields` and `dicts` are used for decoding.
fn decode_ipc_payload(
    payload: &[u8],
    fields: &mut Vec<Field>,
    dicts: &mut HashMap<i64, Vec<String>>,
) -> io::Result<minarrow::Table> {
    let mut decoder = ArrowIPCFrameDecoder::<Vec<u8>>::new(IPCMessageProtocol::Stream);
    let mut offset = 0;
    let mut result_table: Option<minarrow::Table> = None;

    while offset < payload.len() {
        match decoder.decode(&payload[offset..])? {
            DecodeResult::Frame { frame, consumed } => {
                offset += consumed;

                // Empty message + body = EOS marker — stop processing
                if frame.message.is_empty() && frame.body.is_empty() {
                    break;
                }

                let af_msg =
                    flatbuffers::root::<fb::Message>(frame.message.as_ref()).map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, e)
                    })?;

                match af_msg.header_type() {
                    fb::MessageHeader::Schema => {
                        *fields = handle_schema_header(&af_msg)?;
                    }
                    fb::MessageHeader::DictionaryBatch => {
                        let db = af_msg.header_as_dictionary_batch().ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "missing DictionaryBatch header",
                            )
                        })?;
                        handle_dictionary_batch(&db, frame.body.as_ref(), dicts)?;
                    }
                    fb::MessageHeader::RecordBatch => {
                        if fields.is_empty() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "received record batch before schema",
                            ));
                        }
                        let rec = af_msg.header_as_record_batch().ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "missing RecordBatch header",
                            )
                        })?;
                        result_table =
                            Some(handle_record_batch(&rec, fields, dicts, frame.body.as_ref())?);
                    }
                    fb::MessageHeader::NONE => {
                        // EOS
                        break;
                    }
                    other => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unexpected IPC message type: {:?}", other),
                        ));
                    }
                }
            }
            DecodeResult::NeedMore => {
                // Incomplete frame in a complete TLV payload — something is wrong
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated IPC frame within table payload",
                ));
            }
        }
    }

    result_table.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "table payload did not contain a record batch",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use minarrow::{
        Array, ArrowType, Buffer, Field, FieldArray, FloatArray, IntegerArray, NumericArray, Table,
        Vec64,
    };

    fn make_schema() -> Vec<Field> {
        vec![
            Field {
                name: "ids".into(),
                dtype: ArrowType::Int32,
                nullable: false,
                metadata: Default::default(),
            },
            Field {
                name: "values".into(),
                dtype: ArrowType::Float64,
                nullable: false,
                metadata: Default::default(),
            },
        ]
    }

    fn make_table() -> Table {
        Table {
            cols: vec![
                FieldArray::new(
                    Field {
                        name: "ids".into(),
                        dtype: ArrowType::Int32,
                        nullable: false,
                        metadata: Default::default(),
                    },
                    Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
                        data: Buffer::from(Vec64::from_slice(&[10i32, 20, 30])),
                        null_mask: None,
                    }))),
                ),
                FieldArray::new(
                    Field {
                        name: "values".into(),
                        dtype: ArrowType::Float64,
                        nullable: false,
                        metadata: Default::default(),
                    },
                    Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
                        data: Buffer::from(Vec64::from_slice(&[1.1, 2.2, 3.3])),
                        null_mask: None,
                    }))),
                ),
            ],
            n_rows: 3,
            name: "test".to_string(),
        }
    }

    #[test]
    fn test_register_message() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new();
        let tag = codec.register_message("Ping");
        assert_eq!(tag, 0);
        assert_eq!(codec.tag_by_name("Ping"), Some(0));
        assert_eq!(codec.name_by_tag(0), Some("Ping"));
        assert_eq!(codec.kind_by_tag(0), Some(FrameKind::Message));
    }

    #[test]
    fn test_register_table() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new();
        let tag = codec.register_table("Events", make_schema());
        assert_eq!(tag, 0);
        assert_eq!(codec.tag_by_name("Events"), Some(0));
        assert_eq!(codec.kind_by_tag(0), Some(FrameKind::Table));
    }

    #[test]
    fn test_register_multiple_types() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new();
        let msg_tag = codec.register_message("Ping");
        let tbl_tag = codec.register_table("Events", make_schema());
        assert_eq!(msg_tag, 0);
        assert_eq!(tbl_tag, 1);
        assert_eq!(codec.name_by_tag(0), Some("Ping"));
        assert_eq!(codec.name_by_tag(1), Some("Events"));
    }

    #[test]
    fn test_message_roundtrip() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new();
        let tag = codec.register_message("Ack");

        let payload = b"hello world";
        let frame = codec.encode_message(tag, payload).unwrap();

        // Verify wire format
        assert_eq!(frame[0], 0); // tag
        let len = u32::from_le_bytes(frame[1..5].try_into().unwrap()) as usize;
        assert_eq!(len, payload.len());
        assert_eq!(&frame[5..], payload);

        // Decode
        let msg = codec.decode_frame(tag, &frame[5..]).unwrap();
        assert!(msg.is_message());
        assert_eq!(msg.tag(), 0);
        assert_eq!(msg.payload().unwrap(), payload);
    }

    #[test]
    fn test_table_roundtrip() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new();
        let tag = codec.register_table("Events", make_schema());

        let table = make_table();
        let frame = codec.encode_table(tag, &table).unwrap();

        // Extract payload from TLV frame
        let payload = &frame[5..];
        let msg = codec.decode_frame(tag, payload).unwrap();
        assert!(msg.is_table());
        let decoded = msg.into_table().unwrap();
        assert_eq!(decoded.n_rows, 3);
        assert_eq!(decoded.cols.len(), 2);
    }

    #[test]
    fn test_table_multi_batch_roundtrip() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new();
        let tag = codec.register_table("Events", make_schema());

        let table = make_table();

        // First table: encoder emits schema + dict + record batch
        let frame1 = codec.encode_table(tag, &table).unwrap();
        let msg1 = codec.decode_frame(tag, &frame1[5..]).unwrap();
        let decoded1 = msg1.into_table().unwrap();
        assert_eq!(decoded1.n_rows, 3);
        assert_eq!(decoded1.cols.len(), 2);

        // Second table: encoder emits only record batch, decoder reuses schema
        let frame2 = codec.encode_table(tag, &table).unwrap();
        let msg2 = codec.decode_frame(tag, &frame2[5..]).unwrap();
        let decoded2 = msg2.into_table().unwrap();
        assert_eq!(decoded2.n_rows, 3);
        assert_eq!(decoded2.cols.len(), 2);

        // Third table: still works
        let frame3 = codec.encode_table(tag, &table).unwrap();
        let msg3 = codec.decode_frame(tag, &frame3[5..]).unwrap();
        let decoded3 = msg3.into_table().unwrap();
        assert_eq!(decoded3.n_rows, 3);
        assert_eq!(decoded3.cols.len(), 2);
    }

    #[test]
    fn test_unknown_tag_error() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new();
        assert!(codec.decode_frame(99, &[]).is_err());
    }

    #[test]
    fn test_type_mismatch_error() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new();
        codec.register_message("Msg");
        codec.register_table("Tbl", make_schema());

        // Try to encode a message with a table's tag
        assert!(codec.encode_message(1, &[]).is_err());
    }

    #[test]
    fn test_encode_frame() {
        let frame: Vec<u8> = encode_frame(3, b"hello").unwrap();
        assert_eq!(frame[0], 3);
        let payload_len = u32::from_le_bytes(frame[1..5].try_into().unwrap());
        assert_eq!(payload_len, 5);
        assert_eq!(&frame[5..], b"hello");
    }
}
