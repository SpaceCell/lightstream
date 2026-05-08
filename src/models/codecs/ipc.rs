//! Arrow IPC streaming codec with zero-copy encode and decode.
//!
//! Owns the persistent state needed for Arrow IPC streaming: the encoder
//! (which tracks schema/dictionary emission), decoded schema fields,
//! accumulated dictionaries, and the SharedBuffer cache for buffer
//! recycling across batches.
//!
//! The codec delegates all IPC frame dispatch to the decoder layer
//! (`decoders::ipc`). Callers feed it raw IPC frames or payloads and
//! receive decoded Tables back.

use std::collections::HashMap;
use std::io;

use minarrow::Field;
use minarrow::structs::shared_buffer::SharedBuffer;

use crate::compression::Compression;
use crate::enums::IPCMessageProtocol;
use crate::models::decoders::ipc::{decode_ipc_frame, decode_ipc_payload};
use crate::models::encoders::ipc::record_batch::encode_record_batch;
use crate::models::encoders::ipc::table_stream::TableStreamEncoder;
use crate::traits::stream_buffer::StreamBuffer;

// Re-export for callers that match on decode results
pub use crate::models::frames::ipc_message::IPCFrameResult;

/// Arrow IPC streaming codec with persistent encoder/decoder state.
///
/// Each instance handles one table type's schema. The encoder emits
/// schema and dictionary frames on the first batch, then only record
/// batches. The decoder accumulates schema and dictionary state from
/// the first payload and reuses it for subsequent batches.
///
/// SharedBuffer caching enables zero-allocation steady-state: when
/// the caller drops the previous Table before decoding the next batch,
/// the Vec64 backing is reclaimed.
pub struct ArrowIpcCodec<B: StreamBuffer> {
    /// Persistent IPC streaming encoder.
    pub(crate) encoder: TableStreamEncoder<B>,
    /// Decoded schema fields learned from the first payload.
    fields: Vec<Field>,
    /// Accumulated dictionaries across payloads.
    dicts: HashMap<i64, Vec<String>>,
    /// Cached SharedBuffer from the previous decode.
    shared_cache: Option<SharedBuffer>,
}

impl<B: StreamBuffer + Unpin> ArrowIpcCodec<B> {
    /// Create a new codec for the given schema, protocol, and compression.
    pub fn new(schema: Vec<Field>, protocol: IPCMessageProtocol, compression: Compression) -> Self {
        Self {
            encoder: TableStreamEncoder::new_with_compression(schema, protocol, compression),
            fields: Vec::new(),
            dicts: HashMap::new(),
            shared_cache: None,
        }
    }

    /// Encode a record batch into the output buffer.
    ///
    /// Writes IPC frames (schema + dicts on first call, then record
    /// batch only) directly into `out`. The `base_offset` parameter
    /// controls alignment relative to the buffer start - pass 0 for
    /// raw IPC, or the TLV header length for Lightstream framing.
    ///
    /// Returns the number of bytes appended.
    pub fn encode(
        &mut self,
        table: &minarrow::Table,
        out: &mut B,
        base_offset: usize,
    ) -> io::Result<usize> {
        encode_record_batch(&mut self.encoder, table, out, base_offset)
    }

    /// Decode a contiguous IPC payload containing schema + dicts + record batch.
    ///
    /// Used by the Lightstream protocol where the TLV frame contains
    /// the entire IPC payload in one buffer.
    pub fn decode_payload(&mut self, payload: SharedBuffer) -> io::Result<minarrow::Table> {
        let (table, shared) = decode_ipc_payload::<B>(
            payload,
            &mut self.fields,
            &mut self.dicts,
            self.shared_cache.take(),
        )?;
        self.shared_cache = Some(shared);
        Ok(table)
    }

    /// Decode a single IPC frame from a framed stream.
    ///
    /// Handles all message types: schema, dictionary, record batch, EOS.
    /// Column data is mapped as zero-copy SharedBuffer views for record
    /// batch frames.
    pub fn decode_frame(
        &mut self,
        message: &[u8],
        body: SharedBuffer,
        body_len: usize,
    ) -> io::Result<IPCFrameResult> {
        decode_ipc_frame(
            message,
            body,
            body_len,
            &mut self.fields,
            &mut self.dicts,
            &mut self.shared_cache,
        )
    }

    /// Access the decoded schema, if available.
    pub fn schema(&self) -> &[Field] {
        &self.fields
    }

    /// Access the accumulated dictionaries.
    pub fn dicts(&self) -> &HashMap<i64, Vec<String>> {
        &self.dicts
    }

    /// Access the protocol in use.
    pub fn protocol(&self) -> IPCMessageProtocol {
        self.encoder.protocol
    }

    /// Check whether schema has been received.
    pub fn has_schema(&self) -> bool {
        !self.fields.is_empty()
    }

    /// Register a dictionary for a categorical column.
    ///
    /// Must be called before the first encode for any column that uses
    /// dictionary encoding. The `id` is the column index.
    pub fn register_dictionary(&mut self, id: i64, values: Vec<String>) {
        self.encoder.register_dictionary(id, values);
    }

    /// Write the EOS marker into the output buffer, finalising the stream.
    ///
    /// For Stream protocol: writes the 8-byte EOS marker.
    /// For File protocol: delegates to the encoder's finish which
    /// handles footer + EOS + magic.
    ///
    /// Must be called after the last `encode()` call.
    pub fn finish(&mut self, out: &mut B) -> io::Result<()> {
        // EOS marker: continuation (0xFFFFFFFF) + zero metadata length
        out.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        Ok(())
    }
}
