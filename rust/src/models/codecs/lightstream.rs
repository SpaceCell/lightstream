// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Unified type registry with encode and decode for the Lightstream protocol.
//!
//! [`LightstreamCodec`] merges the encoder and decoder into a single struct.
//! Since both sides of a connection must register types in the same order,
//! a unified codec eliminates dual-registration bugs.
//!
//! Messages are opaque `&[u8]` payloads. Tables leverage the real Arrow IPC
//! streaming protocol - the same one used by the rest of lightstream - so
//! schema and dictionary overhead is paid once per table type, not per table.
//!
//! ## How table encoding works
//!
//! Each registered table type gets a persistent [`ArrowIpcCodec`]. On
//! the first call to [`encode_table`], the codec emits the full IPC stream
//! header: schema frame, dictionary frames for any categorical columns, and
//! the record batch. On subsequent calls, only the record batch is emitted.
//! The TLV frame wraps whatever IPC frames the codec produces.
//!
//! On the decode side, the codec maintains persistent schema and dictionary
//! state per type. The first payload teaches the decoder the schema; after
//! that, batch-only payloads are decoded using the stored state.
//!
//! [`encode_table`]: LightstreamCodec::encode_table

use std::collections::HashMap;
use std::io;

use minarrow::structs::shared_buffer::SharedBuffer;
use minarrow::{Field, Vec64};

use super::ipc::ArrowIpcCodec;
use crate::models::decoders::limits::DecodeLimits;
use crate::models::frames::lightstream_message::{
    FRAME_HEADER_SIZE, FrameType, LightstreamMessage,
};
use crate::traits::stream_buffer::StreamBuffer;

/// Internal metadata for a registered type.
struct TypeEntry<B: StreamBuffer> {
    name: String,
    kind: FrameType,
    /// Arrow IPC codec for Table types. Handles encode, decode, schema
    /// tracking, dictionary state, and SharedBuffer recycling.
    ipc_codec: Option<ArrowIpcCodec<B>>,
}

/// Unified Lightstream protocol codec.
///
/// Maintains a type registry mapping sequential tags to either message or
/// table types. Both sides of a connection share the same registration
/// sequence, and this struct handles both encoding and decoding.
///
/// Table types use the Arrow IPC streaming protocol internally via
/// [`ArrowIpcCodec`]: the encoder sends schema + dictionaries on the first
/// table for each type, then only record batches. The decoder accumulates
/// schema and dictionary state so it handles both full and batch-only
/// payloads.
pub struct LightstreamCodec<B: StreamBuffer = Vec<u8>> {
    types: Vec<TypeEntry<B>>,
    name_index: HashMap<String, u8>,
    limits: DecodeLimits,
}

impl<B: StreamBuffer + Unpin> LightstreamCodec<B> {
    /// Create a new empty codec with the resource limits used for decoding.
    pub fn new(limits: Option<DecodeLimits>) -> Self {
        Self {
            types: Vec::new(),
            name_index: HashMap::new(),
            limits: limits.unwrap_or_default(),
        }
    }

    /// Register a message type. Returns the assigned type tag.
    ///
    /// Messages are opaque byte payloads - encoding is the caller's
    /// responsibility.
    pub fn register_message(&mut self, name: impl Into<String>) -> u8 {
        let name = name.into();
        let tag = self.types.len() as u8;
        self.name_index.insert(name.clone(), tag);
        self.types.push(TypeEntry {
            name,
            kind: FrameType::Message,
            ipc_codec: None,
        });
        tag
    }

    /// Register a table type with the given Arrow schema. Returns the
    /// assigned type tag.
    pub fn register_table(&mut self, name: impl Into<String>, schema: Vec<Field>) -> u8 {
        let name = name.into();
        let tag = self.types.len() as u8;
        self.name_index.insert(name.clone(), tag);

        self.types.push(TypeEntry {
            name,
            kind: FrameType::Table,
            ipc_codec: Some(ArrowIpcCodec::new(
                schema,
                crate::enums::IPCMessageProtocol::Stream,
                None,
                Some(self.limits),
            )),
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
    pub fn kind_by_tag(&self, tag: u8) -> Option<FrameType> {
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
        if entry.kind != FrameType::Message {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("type tag {} is not a message type", tag),
            ));
        }
        encode_frame::<B>(tag, payload)
    }

    /// Encode an Arrow table view into a caller-provided buffer as a TLV frame.
    ///
    /// Writes the complete TLV wire frame in one pass: column data is written
    /// from the view's arrays into `out`. Reuses the same buffer across calls.
    pub fn encode_table(
        &mut self,
        tag: u8,
        view: &minarrow::TableV,
        out: &mut B,
    ) -> io::Result<()> {
        let entry = self.types.get_mut(tag as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown type tag {}", tag),
            )
        })?;
        if entry.kind != FrameType::Table {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("type tag {} is not a table type", tag),
            ));
        }

        let codec = entry
            .ipc_codec
            .as_mut()
            .ok_or_else(|| io::Error::other("table codec missing"))?;

        // Clear buffer and write TLV header with placeholder length
        let len = out.len();
        if len > 0 {
            out.drain(0..len);
        }
        out.push(tag);
        out.extend_from_slice(&0u32.to_le_bytes());

        // Append IPC frames after the TLV header
        let ipc_len = codec.encode_stream_batch(view, out, 0, None)?;

        // Patch the TLV payload length
        let len_bytes = (ipc_len as u32).to_le_bytes();
        out.as_mut()[1..5].copy_from_slice(&len_bytes);

        Ok(())
    }

    /// Decode a TLV frame payload into a [`LightstreamMessage`].
    ///
    /// Takes ownership of the payload `Vec64<u8>`. Column data is referenced
    /// in place via SharedBuffer slices - no column bytes are copied.
    /// The SharedBuffer is cached for recycling: when the caller drops the
    /// previous table and the buffer becomes the sole owner, the Vec64 is
    /// reclaimed for the next batch.
    pub fn decode_frame(&mut self, tag: u8, payload: Vec64<u8>) -> io::Result<LightstreamMessage> {
        let entry = self.types.get_mut(tag as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown type tag {}", tag),
            )
        })?;

        match entry.kind {
            FrameType::Message => Ok(LightstreamMessage::Message {
                tag,
                payload: payload.as_ref().to_vec(),
            }),
            FrameType::Table => {
                let codec = entry
                    .ipc_codec
                    .as_mut()
                    .ok_or_else(|| io::Error::other("table codec missing"))?;
                let shared = SharedBuffer::from_vec64(payload);
                let table = codec.decode_payload(shared)?;
                Ok(LightstreamMessage::Table { tag, table: table.into() })
            }
        }
    }
}

impl<B: StreamBuffer + Unpin> Default for LightstreamCodec<B> {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Build a complete TLV frame: `[tag: u8][len: u32 LE][payload]`.
fn encode_frame<B: StreamBuffer>(tag: u8, payload: &[u8]) -> io::Result<B> {
    let total = FRAME_HEADER_SIZE + payload.len();
    let mut buf = B::with_capacity(total);
    buf.push(tag);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    Ok(buf)
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
        Table::new(
            "test".to_string(),
            Some(vec![
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
            ]),
        )
    }

    #[test]
    fn test_register_message() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new(None);
        let tag = codec.register_message("Ping");
        assert_eq!(tag, 0);
        assert_eq!(codec.tag_by_name("Ping"), Some(0));
        assert_eq!(codec.name_by_tag(0), Some("Ping"));
        assert_eq!(codec.kind_by_tag(0), Some(FrameType::Message));
    }

    #[test]
    fn test_register_table() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new(None);
        let tag = codec.register_table("Events", make_schema());
        assert_eq!(tag, 0);
        assert_eq!(codec.tag_by_name("Events"), Some(0));
        assert_eq!(codec.kind_by_tag(0), Some(FrameType::Table));
    }

    #[test]
    fn test_register_multiple_types() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new(None);
        let msg_tag = codec.register_message("Ping");
        let tbl_tag = codec.register_table("Events", make_schema());
        assert_eq!(msg_tag, 0);
        assert_eq!(tbl_tag, 1);
        assert_eq!(codec.name_by_tag(0), Some("Ping"));
        assert_eq!(codec.name_by_tag(1), Some("Events"));
    }

    /// Strip the TLV header from an encoded frame, returning the payload
    /// as an owned 64-byte aligned buffer for zero-copy decode.
    fn strip_tlv_header(frame: Vec64<u8>) -> Vec64<u8> {
        Vec64::from_slice(&frame[FRAME_HEADER_SIZE..])
    }

    #[test]
    fn test_message_roundtrip() {
        let mut codec = LightstreamCodec::<Vec64<u8>>::new(None);
        let tag = codec.register_message("Ack");

        let payload = b"hello world";
        let frame = codec.encode_message(tag, payload).unwrap();

        // Verify wire format
        assert_eq!(frame[0], 0); // tag
        let len = u32::from_le_bytes(frame[1..5].try_into().unwrap()) as usize;
        assert_eq!(len, payload.len());
        assert_eq!(&frame[5..], payload);

        let msg = codec.decode_frame(tag, strip_tlv_header(frame)).unwrap();
        assert!(msg.is_message());
        assert_eq!(msg.tag(), 0);
        assert_eq!(msg.payload().unwrap(), payload);
    }

    #[test]
    fn test_table_roundtrip() {
        let mut codec = LightstreamCodec::<Vec64<u8>>::new(None);
        let tag = codec.register_table("Events", make_schema());

        let table = make_table();
        let mut frame = Vec64::with_capacity(0);
        codec.encode_table(tag, &table.clone().into(), &mut frame).unwrap();

        let msg = codec.decode_frame(tag, strip_tlv_header(frame)).unwrap();
        assert!(msg.is_table());
        let decoded = msg.into_table().unwrap();
        assert_eq!(decoded.n_rows, 3);
        assert_eq!(decoded.cols.len(), 2);
    }

    #[test]
    fn test_table_multi_batch_roundtrip() {
        let mut codec = LightstreamCodec::<Vec64<u8>>::new(None);
        let tag = codec.register_table("Events", make_schema());

        let table = make_table();
        let mut frame = Vec64::with_capacity(0);

        // First table: encoder emits schema + dict + record batch
        codec.encode_table(tag, &table.clone().into(), &mut frame).unwrap();
        let msg1 = codec.decode_frame(tag, strip_tlv_header(frame)).unwrap();
        let decoded1 = msg1.into_table().unwrap();
        assert_eq!(decoded1.n_rows, 3);
        assert_eq!(decoded1.cols.len(), 2);

        // Second table: encoder emits only record batch, decoder reuses schema
        frame = Vec64::with_capacity(0);
        codec.encode_table(tag, &table.clone().into(), &mut frame).unwrap();
        let msg2 = codec.decode_frame(tag, strip_tlv_header(frame)).unwrap();
        let decoded2 = msg2.into_table().unwrap();
        assert_eq!(decoded2.n_rows, 3);
        assert_eq!(decoded2.cols.len(), 2);

        // Third table
        frame = Vec64::with_capacity(0);
        codec.encode_table(tag, &table.clone().into(), &mut frame).unwrap();
        let msg3 = codec.decode_frame(tag, strip_tlv_header(frame)).unwrap();
        let decoded3 = msg3.into_table().unwrap();
        assert_eq!(decoded3.n_rows, 3);
        assert_eq!(decoded3.cols.len(), 2);
    }

    #[test]
    fn test_unknown_tag_error() {
        let mut codec = LightstreamCodec::<Vec64<u8>>::new(None);
        assert!(
            codec
                .decode_frame(99, Vec64::<u8>::with_capacity(0))
                .is_err()
        );
    }

    #[test]
    fn test_type_mismatch_error() {
        let mut codec = LightstreamCodec::<Vec<u8>>::new(None);
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
