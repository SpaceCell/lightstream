//! # Arrow IPC Table Stream Encoder
//!
//! Streaming Arrow IPC writer that serialises `Table` values into Arrow-compliant IPC frames.
//!
//! This module includes low-level, pull-based frame emission via [`GTableStreamEncoder`] with two
//! default type aliases for common buffer types:
//!
//! - [`TableStreamEncoder`] (uses `Vec<u8>` buffer)
//! - [`TableStreamEncoder64`] (uses SIMD-aligned `Vec64<u8>` buffer)
//!
//! It supports both Arrow IPC *Stream* and *File* protocols, optional compression, and dictionary
//! encoding for categorical columns. The encoder yields frames one-by-one via `poll_next`, so it
//! slots into futures, backpressure-aware and asynchronous situation (e.g, Tokio).
//!
//! For most use cases, prefer higher-level abstractions like `TableWriter`.  
//! Use this module when you require fine-grained control over IPC frame emission,  
//! need to integrate with custom IO layers, or are building your own low-level pipeline.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use futures_core::Stream;
use minarrow::{ArrowType, Field, Table, Vec64};

use crate::arrow::message::org::apache::arrow::flatbuf as fbm;
use crate::compression::{Compression, compress};
use crate::constants::DEFAULT_FRAME_ALLOCATION_SIZE;
use crate::enums::{IPCMessageProtocol, WriterState};
use crate::models::encoders::ipc::protocol::{IPCFrame, IPCFrameEncoder};
use crate::models::encoders::ipc::schema::build_flatbuf_recordbatch;
use crate::models::encoders::ipc::schema::{
    FooterBlockMeta, build_flatbuf_footer, build_flatbuf_schema, encode_flatbuf_dictionary,
};
use crate::traits::frame_encoder::FrameEncoder;
use crate::traits::stream_buffer::StreamBuffer;
use crate::utils::{align_to, as_bytes};
use minarrow::{Array, Bitmask, NumericArray, TextArray};

/// Low-level, pull-based streaming Arrow IPC writer producing encoded frames in a standard `Vec<u8>` buffer.
///
/// See [GenTableStreamWriter] for further details.
pub type TableStreamEncoder = GTableStreamEncoder<Vec<u8>>;

/// Low-level, pull-based streaming Arrow IPC writer producing frames in a SIMD-aligned `Vec64<u8>` buffer.
///
/// See [GenTableStreamWriter] for further details.
pub type TableStreamEncoder64 = GTableStreamEncoder<Vec64<u8>>;

/// Low-level, pull-based streaming Arrow IPC writer producing encoded frames.
///
/// This struct incrementally serialises Arrow `Table` data as IPC frames using any buffer
/// implementing `StreamBuffer`, supporting both Arrow *File* and *Stream* [IPC protocols](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc).
///
/// Typical usage: construct the writer, register any required dictionaries,
/// write tables in sequence, and finally call [`finish`] to emit the footer or EOS marker.
/// Each call to [`poll_next`] yields the next encoded IPC frame as a buffer for output.
///
/// For most applications, prefer the higher-level `TableWriter` abstraction, which adds
/// `async` support and yields complete record batches, simplifying integration in async pipelines.
///
/// Consider this low-level frame-based writer if you require synchronous operation, fine-grained control over IPC frame emission,
/// custom buffer integration, deterministic memory and backpressure handling, integration with your own bespoke IO
/// (e.g., Filesystem), or specialised workflows where async abstractions or high-level batching are not appropriate.
///
/// See also: [`TableReader`] and [`TableStreamReader`] for corresponding read-side utilities.
pub struct GTableStreamEncoder<B>
where
    B: StreamBuffer + 'static,
{
    /// Arrow IPC protocol (file or stream)
    pub protocol: IPCMessageProtocol,

    /// Current state of the writer (Fresh, SchemaDone, Closed, etc)
    pub state: WriterState,

    /// Compression codec for record batch bodies
    pub compression: Compression,

    /// Arrow schema for this stream (column definitions)
    pub schema: Vec<Field>,

    /// Set of dictionary IDs already written
    pub written_dict_ids: HashSet<i64>,

    /// Registered dictionaries for categorical columns (by column index)
    pub dictionaries: HashMap<i64, Vec<String>>,

    /// FlatBuffer builder instance for serialisation
    pub fbb: flatbuffers::FlatBufferBuilder<'static>,

    // ----- File format only -----
    /// Block metadata for all record batches (Arrow File protocol)
    blocks_record_batches: Vec<FooterBlockMeta>,

    /// Block metadata for all dictionary batches (Arrow File protocol)
    blocks_dictionaries: Vec<FooterBlockMeta>,

    /// Offsets for each written IPC frame (Arrow File protocol)
    pub frame_offsets: Vec<u64>,

    // ----- Streaming/buffering -----
    /// Queue of encoded IPC frames pending emission
    pub out_frames: VecDeque<B>,

    /// Total number of bytes written so far (Arrow File protocol)
    pub total_len_offset: u64,

    /// Total bytes written so far
    pub global_offset: usize,

    /// True if the writer is closed and finished emitting
    pub finished: bool,

    /// Stored task waker for waking up poll_next when new data is pushed
    pub waker: Option<Waker>,
}

impl<B> GTableStreamEncoder<B>
where
    B: StreamBuffer,
{
    /// Construct a new [`GenTableStreamWriter`] for the specified schema and protocol.
    ///
    /// # Arguments
    /// - `schema`: Vector of Arrow [`Field`]s describing the table structure.
    /// - `protocol`: Arrow IPC protocol (file or stream).
    ///
    /// # Returns
    /// New streaming writer in [`WriterState::Fresh`] state.
    pub fn new(schema: Vec<Field>, protocol: IPCMessageProtocol) -> Self {
        Self {
            protocol,
            state: WriterState::Fresh,
            compression: Compression::None,
            schema,
            written_dict_ids: HashSet::new(),
            dictionaries: HashMap::new(),
            fbb: flatbuffers::FlatBufferBuilder::with_capacity(4096),
            blocks_record_batches: Vec::new(),
            blocks_dictionaries: Vec::new(),
            frame_offsets: vec![],
            out_frames: VecDeque::new(),
            total_len_offset: 0,
            global_offset: 0,
            finished: false,
            waker: None,
        }
    }

    /// Construct a new [`GenTableStreamWriter`] with compression for the specified schema and protocol.
    ///
    /// # Arguments
    /// - `schema`: Vector of Arrow [`Field`]s describing the table structure.
    /// - `protocol`: Arrow IPC protocol (file or stream).
    /// - `compression`: Compression codec to apply to record batch bodies.
    ///
    /// # Returns
    /// New streaming writer in [`WriterState::Fresh`] state.
    pub fn with_compression(
        schema: Vec<Field>,
        protocol: IPCMessageProtocol,
        compression: Compression,
    ) -> Self {
        Self {
            protocol,
            state: WriterState::Fresh,
            compression,
            schema,
            written_dict_ids: HashSet::new(),
            dictionaries: HashMap::new(),
            fbb: flatbuffers::FlatBufferBuilder::with_capacity(4096),
            blocks_record_batches: Vec::new(),
            blocks_dictionaries: Vec::new(),
            frame_offsets: vec![],
            out_frames: VecDeque::new(),
            total_len_offset: 0,
            global_offset: 0,
            finished: false,
            waker: None,
        }
    }

    /// Register a dictionary for a categorical column.
    ///
    /// # Arguments
    /// - `id`: Dictionary ID (typically column index).
    /// - `uniques`: Vector of unique values for the dictionary.
    ///
    /// # Panics
    /// Will overwrite any existing dictionary for the given ID.
    pub fn register_dictionary(&mut self, id: i64, uniques: Vec<String>) {
        self.dictionaries.insert(id, uniques);
    }

    /// Serialises and emits the Arrow schema as the initial IPC frame.
    ///
    /// This method constructs the Arrow schema metadata as a FlatBuffers-encoded IPC message,
    /// then emits it as the first frame in the output stream. No column data or dictionaries are included.
    /// Updates internal state to reflect that the schema has been written.
    ///
    /// # Returns
    /// Returns `Ok(())` if the schema was successfully emitted, or an error if serialisation fails.
    ///
    /// # Errors
    /// Returns an error if schema serialisation or frame emission fails.
    pub fn write_schema_frame(&mut self) -> io::Result<()> {
        let meta = build_flatbuf_schema(&mut self.fbb, &self.schema)?;
        let body = B::with_capacity(DEFAULT_FRAME_ALLOCATION_SIZE);
        self.emit_frame(meta, body, fbm::MessageHeader::Schema);
        self.state = WriterState::SchemaDone;
        Ok(())
    }

    /// Write a single [`Table`] as an Arrow IPC record batch.
    ///
    /// - Writes schema frame if not already written.
    /// - Ensures dictionary frames are emitted if required.
    /// - Appends a record batch frame to the output stream.
    ///
    /// # Errors
    /// Returns error if table shape or types are inconsistent with schema, or writer is closed.
    pub fn write_record_batch_frame(&mut self, tbl: &Table) -> io::Result<()> {
        if self.state == WriterState::Closed {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "writer already finished",
            ));
        }
        if self.state == WriterState::Fresh {
            self.write_schema_frame()?;
        }
        if tbl.cols.len() != self.schema.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "table column count mismatch with writer schema",
            ));
        }
        // Collect dictionary ids first to avoid borrowing self.schema during mutation
        let dict_ids: Vec<i64> = self
            .schema
            .iter()
            .enumerate()
            .filter_map(|(col_idx, field)| {
                if let ArrowType::Dictionary(_) = field.dtype {
                    Some(col_idx as i64)
                } else {
                    None
                }
            })
            .collect();
        for dict_id in dict_ids {
            self.write_dictionary_frame_if_needed(dict_id)?;
        }
        // Encode the record batch as Arrow IPC frame
        let (meta, body) = self.encode_record_batch(tbl)?;
        self.emit_frame(meta, body, fbm::MessageHeader::RecordBatch);
        Ok(())
    }

    /// Writes a dictionary batch if not already written for this ID.
    ///
    /// # Arguments
    /// - `dict_id`: Dictionary ID (typically column index).
    ///
    /// # Errors
    /// Returns error if the dictionary was not registered.
    pub(crate) fn write_dictionary_frame_if_needed(&mut self, id: i64) -> io::Result<()> {
        if self.written_dict_ids.contains(&id) {
            return Ok(());
        }
        let uniques = self.dictionaries.get(&id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("dictionary id {id} not registered"),
            )
        })?;
        let (meta, body) = encode_flatbuf_dictionary(&mut self.fbb, id, uniques)?;
        self.emit_frame(meta, body, fbm::MessageHeader::DictionaryBatch);
        self.written_dict_ids.insert(id);
        Ok(())
    }

    /// Encodes and emits a single Arrow IPC frame (schema, record batch, or dictionary).
    ///
    /// This is used by:
    ///     1. `write_schema_frame` (the initial frame)
    ///     2. Any `dictionary` frames. For `Minarrow`, this is only used for `Categorical` types.
    ///     3. 1:M `write_record_batch_frame`
    ///     4. `finish` - the final frame.
    ///
    /// ### Behaviour
    /// - Serialises the given metadata (`meta`) and body (`body`) into an Arrow IPC frame using the
    ///   configured protocol (stream or file), via `ArrowIPCFrameEncoder::encode_frame`.
    /// - For file protocol, tracks the current byte offset, updates block metadata (for footer), and records frame offsets.
    /// - For stream protocol, frames are emitted immediately with no footer tracking.
    /// - The frame is appended to the outgoing buffer queue (`out_frames`) for consumption by the downstream sink or stream.
    /// - `is_first` is set if this is the first frame after the schema (required for Arrow File magic).
    /// - `is_last` is always false during normal operation; set true only at finish (to emit footer/EOS).
    ///
    /// ### Arguments
    /// - `meta`: Flatbuffer-encoded Arrow IPC message (schema, record batch, dictionary, etc).
    /// - `body`: Corresponding data buffer (columnar Arrow data).
    /// - `header_type`: Indicates the Arrow message type (e.g., RecordBatch, DictionaryBatch),
    ///   used for block metadata and footer generation (file format).
    ///
    /// ### Side Effects
    /// - Updates internal block offset tracking and block lists for Arrow File output.
    /// - Buffers the encoded frame for downstream emission.
    fn emit_frame(&mut self, meta: Vec<u8>, body: B, header_type: fbm::MessageHeader) {
        // Note: This logic overfits the `File` case which is not currently a problem, as there is
        // no "is_first" stream logicin ArrowIPCFrameEncoder::encode_frame.
        // However, it *will* create a bug if the Arrow stream protocol changes in future, and it needs 'is_first' handling.
        let is_first = self.protocol == IPCMessageProtocol::File && self.frame_offsets.is_empty();
        let is_last = false;

        let frame = IPCFrame {
            meta: &meta,
            body: body.as_ref(),
            protocol: self.protocol,
            is_first,
            is_last,
            footer_bytes: None,
        };

        let (encoded, ipc_frame_metadata) =
            IPCFrameEncoder::encode::<B>(&mut self.global_offset, &frame)
                .expect("IPC frame encoding failed");
        debug_assert!(encoded.len() == ipc_frame_metadata.frame_len());
        if self.protocol == IPCMessageProtocol::File {
            // For file track buffer, track offset and block metadata
            let block = FooterBlockMeta {
                offset: self.total_len_offset,
                // yes - includes the header
                metadata_len: ipc_frame_metadata.metadata_total_len() as u32
                    + ipc_frame_metadata.header_len as u32,
                body_len: ipc_frame_metadata.body_total_len() as u64,
            };

            match header_type {
                fbm::MessageHeader::DictionaryBatch => self.blocks_dictionaries.push(block),
                fbm::MessageHeader::RecordBatch => self.blocks_record_batches.push(block),
                _ => {}
            }
            self.frame_offsets.push(self.total_len_offset);
            self.total_len_offset += ipc_frame_metadata.frame_len() as u64;
        }
        self.out_frames.push_back(encoded);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    /// Finalise the Arrow IPC stream, writing any required footer and EOS marker.
    ///
    /// ### Encoder logic
    /// Via `ArrowIPCFrameEncoder::encode_frame`:
    /// - For `File` protocol, emits the Arrow footer and closing magic.
    /// - For `Stream` protocol, emits the end-of-stream marker.
    ///
    /// ### Errors
    /// Returns error if already finished.
    /// Finish the writer, emitting footer if required, and marking as closed.
    pub fn finish(&mut self) -> io::Result<()> {
        if self.state == WriterState::Closed {
            return Ok(());
        }
        match self.protocol {
            IPCMessageProtocol::File => {
                let is_first = self.frame_offsets.is_empty();
                println!("IS FIRST: {}", is_first);
                let is_last = true;
                let footer_bytes = build_flatbuf_footer(
                    &mut self.fbb,
                    &self.schema,
                    &self.blocks_dictionaries,
                    &self.blocks_record_batches,
                )?;
                let frame = IPCFrame {
                    meta: &[],
                    body: &[],
                    protocol: IPCMessageProtocol::File,
                    is_first,
                    is_last,
                    footer_bytes: Some(&footer_bytes),
                };
                let (footer_frame, _) =
                    IPCFrameEncoder::encode::<B>(&mut self.global_offset, &frame)?;
                self.out_frames.push_back(footer_frame);
            }
            IPCMessageProtocol::Stream => {
                // Only emit EOS marker if we actually wrote something (not in Fresh state)
                if self.state != WriterState::Fresh {
                    let frame = IPCFrame {
                        meta: &[],
                        body: &[],
                        protocol: IPCMessageProtocol::Stream,
                        is_first: false,
                        is_last: true,
                        footer_bytes: None,
                    };
                    let (eos_frame, _) =
                        IPCFrameEncoder::encode::<B>(&mut self.global_offset, &frame)?;
                    self.out_frames.push_back(eos_frame);
                }
            }
        }
        self.state = WriterState::Closed;
        self.finished = true;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        Ok(())
    }

    /// Estimate the total buffer size needed for a table's data to avoid expensive reallocations.
    fn estimate_body_size(&self, tbl: &Table) -> usize {
        let mut total_size = 0;

        for array in &tbl.cols {
            match &array.array {
                Array::NumericArray(num) => {
                    let data_size = match num {
                        NumericArray::Int32(_)
                        | NumericArray::UInt32(_)
                        | NumericArray::Float32(_) => tbl.n_rows * 4,
                        NumericArray::Int64(_)
                        | NumericArray::UInt64(_)
                        | NumericArray::Float64(_) => tbl.n_rows * 8,
                        #[cfg(feature = "extended_numeric_types")]
                        NumericArray::Int8(_) | NumericArray::UInt8(_) => tbl.n_rows,
                        #[cfg(feature = "extended_numeric_types")]
                        NumericArray::Int16(_) | NumericArray::UInt16(_) => tbl.n_rows * 2,
                        _ => tbl.n_rows * 8, // Default to 8 bytes per row
                    };
                    total_size += data_size + 64; // Data + padding
                }
                Array::BooleanArray(_) => {
                    total_size += (tbl.n_rows + 7) / 8 + 64; // Bitmask + padding
                }
                Array::TextArray(_) => {
                    // Rough estimate: assume average 20 chars per string
                    total_size += tbl.n_rows * 24 + 64; // Offsets (4 bytes) + avg data (20 bytes) + padding
                }
                _ => {
                    total_size += tbl.n_rows * 8 + 64; // Default estimate + padding
                }
            }
        }

        total_size
    }

    /// Serialises a [`Table`] as an Arrow IPC record batch, returning (metadata, body) buffers.
    ///
    /// Encodes all columns into Arrow-compliant buffers, constructs the FlatBuffers metadata,
    /// and returns the pair as `(meta, body)`.
    ///
    /// Errors if the table cannot be serialised.
    pub fn encode_record_batch(&mut self, tbl: &Table) -> io::Result<(Vec<u8>, B)> {
        let mut fb_field_nodes = Vec::with_capacity(tbl.cols.len());
        let mut fb_buffers = Vec::new();

        // Calculate estimated buffer size to avoid expensive reallocations
        let estimated_size = self.estimate_body_size(tbl);
        let mut body = B::with_capacity(estimated_size.max(DEFAULT_FRAME_ALLOCATION_SIZE));
        for (col_idx, array) in tbl.cols.iter().enumerate() {
            // here we check in non-release builds that the declared schema Field
            // matches the actual Field from the array, that we pull here to
            // conveniently bypass borrow-checker constraints
            debug_assert!(array.field == self.schema[col_idx].clone().into());
            let field = &array.field;
            self.encode_column(
                field,
                &array.array,
                tbl.n_rows,
                col_idx,
                &mut fb_field_nodes,
                &mut fb_buffers,
                &mut body,
            )?;
        }
        let meta = build_flatbuf_recordbatch(
            &mut self.fbb,
            tbl.n_rows,
            &fb_field_nodes,
            &fb_buffers,
            body.len(),
        )?;
        Ok((meta, body))
    }

    /// Encodes a single column from a high-level Arrow `Array` into Arrow IPC record batch buffers and flatbuffers metadata.
    ///
    /// This function dispatches based on the concrete array variant, selects the appropriate data and null bitmap slices,
    /// and delegates buffer encoding and metadata recording to `encode_field_buffers`. Categorical columns trigger dictionary
    /// batch handling if required. All offsets, null counts, and buffer lengths are recorded in accordance with the Arrow IPC standard.
    ///
    /// # Parameters
    /// - `field`: Schema field descriptor for the column.
    /// - `array`: The Arrow array to encode (concrete variant determines buffer structure).
    /// - `n_rows`: Number of logical rows in the column.
    /// - `col_idx`: Index of the column within the table schema, used for dictionary handling.
    /// - `fb_field_nodes`: Mutable vector to which field node metadata will be appended.
    /// - `fb_buffers`: Mutable vector to which buffer metadata (offsets/lengths) will be appended ("Flatbuffers" Arrow metadata payload).
    /// - `body`: Output buffer receiving all serialised column data (the Arrow "data" payload).
    /// - `writer`: Arrow IPC table stream encoder, required for dictionary batch serialisation.
    ///
    /// # Errors
    /// Returns an [`io::Error`] if the column type is unsupported, dictionary serialisation fails, or any buffer operation fails.
    #[inline(always)]
    fn encode_column(
        &mut self,
        field: &Field,
        array: &Array,
        n_rows: usize,
        col_idx: usize,
        fb_field_nodes: &mut Vec<fbm::FieldNode>,
        fb_buffers: &mut Vec<fbm::Buffer>,
        body: &mut B,
    ) -> io::Result<()> {
        // Important: at first glance it looks like the Arrow IPC data is getting written
        // into the Flatbuffer, due to the `push_buffer` return type. That's not what's happening.
        // The body is modified in place, and it returns `fbm::Buffer` because we snatch the required
        // offset and length metadata to attach to the `RecordBatch`, as per the standard.

        match array {
            Array::NumericArray(num) => {
                let (data_bytes, null_bitmap) = match num {
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::Int8(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::Int16(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    NumericArray::Int32(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    NumericArray::Int64(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::UInt8(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::UInt16(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    NumericArray::UInt32(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    NumericArray::UInt64(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    NumericArray::Float32(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    NumericArray::Float64(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "unsupported numeric subtype",
                        ));
                    }
                };
                self.encode_field_buffers(
                    field.nullable,
                    null_bitmap,
                    &[data_bytes],
                    n_rows,
                    col_idx,
                    false,
                    fb_field_nodes,
                    fb_buffers,
                    body,
                )
            }
            Array::BooleanArray(arr) => self.encode_field_buffers(
                field.nullable,
                arr.null_mask.as_ref(),
                &[arr.data.bits.as_slice()],
                n_rows,
                col_idx,
                false,
                fb_field_nodes,
                fb_buffers,
                body,
            ),
            Array::TextArray(TextArray::String32(arr)) => self.encode_field_buffers(
                field.nullable,
                arr.null_mask.as_ref(),
                &[as_bytes(arr.offsets.as_slice()), arr.data.as_slice()],
                n_rows,
                col_idx,
                false,
                fb_field_nodes,
                fb_buffers,
                body,
            ),
            #[cfg(feature = "large_string")]
            Array::TextArray(TextArray::String64(arr)) => self.encode_field_buffers(
                field.nullable,
                arr.null_mask.as_ref(),
                &[as_bytes(arr.offsets.as_slice()), arr.data.as_slice()],
                n_rows,
                col_idx,
                false,
                fb_field_nodes,
                fb_buffers,
                body,
            ),
            Array::TextArray(TextArray::Categorical32(arr)) => self.encode_field_buffers(
                field.nullable,
                arr.null_mask.as_ref(),
                &[as_bytes(arr.data.as_slice())],
                n_rows,
                col_idx,
                true,
                fb_field_nodes,
                fb_buffers,
                body,
            ),
            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical8(arr)) => self.encode_field_buffers(
                field.nullable,
                arr.null_mask.as_ref(),
                &[as_bytes(arr.data.as_slice())],
                n_rows,
                col_idx,
                true,
                fb_field_nodes,
                fb_buffers,
                body,
            ),
            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical16(arr)) => self.encode_field_buffers(
                field.nullable,
                arr.null_mask.as_ref(),
                &[as_bytes(arr.data.as_slice())],
                n_rows,
                col_idx,
                true,
                fb_field_nodes,
                fb_buffers,
                body,
            ),
            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical64(arr)) => self.encode_field_buffers(
                field.nullable,
                arr.null_mask.as_ref(),
                &[as_bytes(arr.data.as_slice())],
                n_rows,
                col_idx,
                true,
                fb_field_nodes,
                fb_buffers,
                body,
            ),
            #[cfg(feature = "datetime")]
            Array::TemporalArray(temp) => {
                let (data_bytes, null_bitmap) = match temp {
                    minarrow::TemporalArray::Datetime32(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    minarrow::TemporalArray::Datetime64(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    minarrow::TemporalArray::Null => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "null temporal array not supported",
                        ));
                    }
                };
                self.encode_field_buffers(
                    field.nullable,
                    null_bitmap,
                    &[data_bytes],
                    n_rows,
                    col_idx,
                    false,
                    fb_field_nodes,
                    fb_buffers,
                    body,
                )
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported column type in writer: {}", field.name),
            )),
        }
    }

    /// Encodes the buffers for a single column into the Arrow IPC record batch format.
    ///
    /// This function handles the following tasks:
    /// - Optionally writes a dictionary batch for the column if required (categorical types).
    /// - Serialises the null bitmap (if present) into the buffer body and updates buffer metadata.
    /// - Serialises all logical value buffers (data, offsets, indices) for the column into the buffer body.
    /// - Updates the FlatBuffer metadata vectors (`fb_field_nodes`, `fb_buffers`) with the correct offsets and lengths
    ///   for Arrow IPC compliance.
    ///
    /// # Parameters
    /// - `nullable`: Indicates whether the field is nullable (controls null bitmap handling).
    /// - `null_bitmap`: Optional reference to the null bitmap for this column, if it exists.
    /// - `data_slices`: One or more slices representing the value buffers for this column (e.g., data, offsets).
    /// - `n_rows`: Number of rows in the column (used for metadata).
    /// - `col_idx`: Column index, used for dictionary batch handling.
    /// - `write_dict`: If true, triggers dictionary batch serialisation for this column before buffer encoding.
    /// - `fb_field_nodes`: Mutable vector to which the field node metadata will be appended.
    /// - `fb_buffers`: Mutable vector to which flatbuffers metadata (offsets/lengths) will be appended.
    /// - `body`: Output buffer to which all serialised binary data (the arrow payload) will be written.
    /// - `writer`: Arrow IPC table stream encoder, required for dictionary handling.
    ///
    /// # Errors
    /// Returns an [`io::Error`] if dictionary writing fails or any downstream buffer operation fails.
    #[inline(always)]
    fn encode_field_buffers(
        &mut self,
        nullable: bool,
        null_bitmap: Option<&Bitmask>,
        data_slices: &[&[u8]],
        n_rows: usize,
        col_idx: usize,
        write_dict: bool,
        fb_field_nodes: &mut Vec<fbm::FieldNode>,
        fb_buffers: &mut Vec<fbm::Buffer>,
        body: &mut B,
    ) -> io::Result<()> {
        if write_dict {
            self.write_dictionary_frame_if_needed(col_idx as i64)?;
        }
        let (n_nulls, null_buf) = make_null_buffer(nullable, null_bitmap, body);
        fb_buffers.push(null_buf);
        for &slice in data_slices {
            let meta_buf = push_buffer_with_compression(body, slice, self.compression)?;
            fb_buffers.push(meta_buf);
        }
        fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, n_nulls as i64));
        Ok(())
    }
}

impl<B> Stream for GTableStreamEncoder<B>
where
    B: StreamBuffer + 'static + Unpin,
{
    type Item = io::Result<B>;

    /// Poll for the next output Arrow IPC frame as a chunked buffer.
    ///
    /// Yields one frame per call, in the order they were written.
    /// Returns `None` after `finish()` has been called and all frames have been yielded.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if let Some(frame) = this.out_frames.pop_front() {
            Poll::Ready(Some(Ok(frame)))
        } else if this.finished {
            Poll::Ready(None)
        } else {
            // Store waker for future push
            this.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Append a new Arrow IPC buffer region to the output body, applying required alignment as
/// set under the `StreamBuffer` trait configuration.
///
/// - Updates `body` in place.
/// - Returns Arrow buffer metadata describing the segment location and length (before padding).
///
/// # Arguments
/// - `body`: Output vector to append to.
/// - `bytes`: Raw data to write.
///
/// # Returns
/// Arrow buffer metadata (offset, length), for FlatBuffers metadata.
#[inline(always)]
fn push_buffer<B: StreamBuffer>(body: &mut B, bytes: &[u8]) -> fbm::Buffer {
    let offset = body.len();
    // Actual IPC data buffer modifies in-place
    // For very large slices, chunk the copy to avoid blocking the runtime too long
    const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
    if bytes.len() > CHUNK_SIZE {
        for chunk in bytes.chunks(CHUNK_SIZE) {
            body.extend_from_slice(chunk);
        }
    } else {
        body.extend_from_slice(bytes);
    }
    let len = bytes.len();

    // Here - we don't actually update the global offset, we just know,
    // globally, at what offset it needs to be for consistent alignment.
    // That global alignment is maintained in `append_message_frame`.
    // Hence, each body buffer just keeps the offsetted alignment.
    let pad = align_to::<B>(len);
    if pad != 0 {
        body.extend_from_slice(&[0u8; 64][..pad]);
    }
    // Metadata buffer (Flatbuffers) offsets are returned
    fbm::Buffer::new(offset as i64, len as i64)
}

/// Append a new Arrow IPC buffer region to the output body with optional compression.
///
/// When compression is enabled, the buffer is compressed according to Arrow IPC spec:
/// - 8-byte little-endian uncompressed length header
/// - Followed by compressed data
/// - Padding applied to the final (compressed) length
///
/// # Arguments
/// - `body`: Output vector to append to.
/// - `bytes`: Raw data to write.
/// - `compression`: Compression codec to apply.
///
/// # Returns
/// Arrow buffer metadata (offset, original uncompressed length), for FlatBuffers metadata.
#[inline(always)]
fn push_buffer_with_compression<B: StreamBuffer>(
    body: &mut B,
    bytes: &[u8],
    compression: Compression,
) -> Result<fbm::Buffer, io::Error> {
    let offset = body.len();
    let original_len = bytes.len();

    if compression == Compression::None {
        // No compression - use the standard path
        return Ok(push_buffer(body, bytes));
    }

    // Compress the buffer data
    let compressed = compress(bytes, compression)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Compression failed: {}", e)))?;

    // Write 8-byte uncompressed length header (little-endian)
    let uncompressed_len_bytes = (original_len as u64).to_le_bytes();
    body.extend_from_slice(&uncompressed_len_bytes);

    // Write compressed data in chunks to avoid blocking
    const CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
    if compressed.len() > CHUNK_SIZE {
        for chunk in compressed.chunks(CHUNK_SIZE) {
            body.extend_from_slice(chunk);
        }
    } else {
        body.extend_from_slice(&compressed);
    }

    // Calculate padding for the total written length (header + compressed data)
    let total_written = 8 + compressed.len();
    let pad = align_to::<B>(total_written);
    if pad != 0 {
        body.extend_from_slice(&[0u8; 64][..pad]);
    }

    // Return metadata with original uncompressed length for Arrow spec compliance
    Ok(fbm::Buffer::new(offset as i64, original_len as i64))
}

/// Construct an Arrow IPC null bitmap buffer and emit into the output body.
///
/// - If `nullable` is true and a null mask is present, emits the bitmask and returns the null count.
/// - Otherwise emits a zero-length buffer and returns null count zero.
///
/// # Arguments
/// - `nullable`: True if the column is nullable.
/// - `mask`: Optional reference to the column's null bitmask.
/// - `body`: Output buffer accumulating all body segments for this batch.
///
/// # Returns
/// Tuple: (`null_count`, `buffer` metadata for Arrow IPC)
#[inline(always)]
fn make_null_buffer<B: StreamBuffer>(
    nullable: bool,
    mask: Option<&Bitmask>,
    body: &mut B,
) -> (usize, fbm::Buffer) {
    if nullable {
        if let Some(mask) = mask {
            let n_nulls = mask.null_count();
            let buf = push_buffer(body, mask.bits.as_slice());
            (n_nulls, buf)
        } else {
            (0, fbm::Buffer::new(0, 0))
        }
    } else {
        (0, fbm::Buffer::new(0, 0))
    }
}

/// The below are 'logical' unit tests confirming:
/// - Unique dictionary IDs are respected.
/// - The macro populates buffers and field nodes as intended.
/// - Null handling works as expected for all supported index types.
///
/// For full roundtrip IO on the Stream reader and writer, see "../tests".
#[cfg(test)]
mod tests {
    use std::fs::File as StdFile;
    use std::io::{Read, Write};
    use std::sync::Arc;

    use minarrow::ffi::arrow_dtype::CategoricalIndexType;
    use minarrow::{
        Array, Bitmask, Buffer, CategoricalArray, Field, FieldArray, IntegerArray, Table,
        TextArray, Vec64,
    };
    use tempfile::NamedTempFile;

    use crate::constants::{ARROW_MAGIC_NUMBER, ARROW_MAGIC_NUMBER_PADDED};

    use super::*;

    fn make_bitmask(valid: &[bool]) -> Bitmask {
        let mut bits = vec![0u8; (valid.len() + 7) / 8];
        for (i, v) in valid.iter().enumerate() {
            if *v {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        Bitmask {
            bits: Buffer::from(Vec64::from_slice(&bits[..])),
            len: valid.len(),
        }
    }

    fn dict_strs() -> Vec<String> {
        vec![
            "apple".to_string(),
            "banana".to_string(),
            "pear".to_string(),
        ]
    }

    fn make_schema(idx_ty: CategoricalIndexType, nullable: bool) -> Vec<Field> {
        vec![Field {
            name: "col".to_string(),
            dtype: ArrowType::Dictionary(idx_ty),
            nullable,
            metadata: Default::default(),
        }]
    }

    fn make_table(arr: FieldArray, n_rows: usize) -> Table {
        Table {
            cols: vec![arr],
            n_rows,
            name: "tbl".to_string(),
        }
    }

    fn read_file_bytes(path: &std::path::Path) -> Vec<u8> {
        let mut f = StdFile::open(path).expect("file open");
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).expect("file read");
        buf
    }

    fn check_ipc_padding(buf: &[u8]) {
        // Checks: Flatbuffers message header (8 bytes) + meta + 64-byte padding + data
        // Only a minimal check here - you can parse the header and verify offset alignment
        // but at minimum ensure buffer length is a multiple of 8 and/or 64 as Arrow requires.
        assert_eq!(buf.len() % 8, 0, "Arrow IPC frame should be 8-byte aligned");
    }

    #[test]
    fn test_write_categorical_column_u32_to_file() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let mut writer = TableStreamEncoder::new(
            make_schema(CategoricalIndexType::UInt32, true),
            IPCMessageProtocol::Stream,
        );

        let arr = CategoricalArray {
            data: Buffer::from(Vec64::from_slice(&[1u32, 0, 2, 1])),
            unique_values: Vec64::from(dict_strs()),
            null_mask: Some(make_bitmask(&[true, false, true, true])),
        };

        writer.register_dictionary(0, dict_strs());

        let tbl = make_table(
            FieldArray::new(
                Field {
                    name: "col".to_string(),
                    dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
                    nullable: true,
                    metadata: Default::default(),
                },
                Array::TextArray(TextArray::Categorical32(Arc::new(arr))),
            ),
            4,
        );

        writer.write_record_batch_frame(&tbl).unwrap();
        writer.finish().unwrap();

        // Write to temp file
        let mut file = StdFile::create(&path).unwrap();
        for frame in writer.out_frames {
            use std::io::Write;
            file.write_all(&frame).unwrap();
        }
        file.flush().unwrap();
        drop(file); // Ensure file is closed before reading

        // Now read back
        let buf = read_file_bytes(&path);
        assert!(!buf.is_empty());
        check_ipc_padding(&buf);
    }

    // #[cfg(feature = "extended_categorical")]
    // #[test]
    // fn test_write_categorical_column_u8() {
    //     let temp = NamedTempFile::new().unwrap();
    //     let path = temp.path().to_path_buf();

    //     let mut writer = TableStreamEncoder::new(
    //         make_schema(CategoricalIndexType::UInt8, true),
    //         IPCMessageProtocol::Stream
    //     );

    //     let arr = CategoricalArray {
    //         data: Buffer::from(Vec64::from_slice(&[1u8, 0, 2, 1])),
    //         unique_values: Vec64::from(dict_strs()),
    //         null_mask: Some(make_bitmask(&[true, true, false, true]))
    //     };

    //     writer.register_dictionary(0, dict_strs());

    //     let tbl = make_table(
    //         FieldArray::new(
    //             Field {
    //                 name: "col".to_string(),
    //                 dtype: ArrowType::Dictionary(CategoricalIndexType::UInt8),
    //                 nullable: true,
    //                 metadata: Default::default()
    //             },
    //             Array::TextArray(TextArray::Categorical8(Arc::new(arr)))
    //         ),
    //         4
    //     );

    //     writer.write_table(&tbl).unwrap();
    //     writer.finish().unwrap();

    //     let mut buf = Vec::new();
    //     for frame in writer.out_frames {
    //         buf.extend_from_slice(&frame);
    //     }

    //     assert!(!buf.is_empty());
    //     check_ipc_padding(&buf);
    // }

    // #[cfg(feature = "extended_categorical")]
    // #[test]
    // fn test_write_categorical_column_u16() {
    //     let temp = NamedTempFile::new().unwrap();
    //     let path = temp.path().to_path_buf();

    //     let mut writer = TableStreamEncoder::new(
    //         make_schema(CategoricalIndexType::UInt16, false),
    //         IPCMessageProtocol::Stream
    //     );

    //     let arr = CategoricalArray {
    //         data: Buffer::from(Vec64::from_slice(&[2u16, 1, 0, 2])),
    //         unique_values: Vec64::from(dict_strs()),
    //         null_mask: None
    //     };

    //     writer.register_dictionary(0, dict_strs());

    //     let tbl = make_table(
    //         FieldArray::new(
    //             Field {
    //                 name: "col".to_string(),
    //                 dtype: ArrowType::Dictionary(CategoricalIndexType::UInt16),
    //                 nullable: false,
    //                 metadata: Default::default()
    //             },
    //             Array::TextArray(TextArray::Categorical16(Arc::new(arr)))
    //         ),
    //         4
    //     );

    //     writer.write_table(&tbl).unwrap();
    //     writer.finish().unwrap();

    //     let mut buf = Vec::new();
    //     for frame in writer.out_frames {
    //         buf.extend_from_slice(&frame);
    //     }

    //     assert!(!buf.is_empty());
    //     check_ipc_padding(&buf);
    // }

    // #[cfg(feature = "extended_categorical")]
    // #[test]
    // fn test_write_categorical_column_u64() {
    //     let temp = NamedTempFile::new().unwrap();
    //     let path = temp.path().to_path_buf();

    //     let mut writer = TableStreamEncoder::new(
    //         make_schema(CategoricalIndexType::UInt64, false),
    //         IPCMessageProtocol::Stream
    //     );

    //     let arr = CategoricalArray {
    //         data: Buffer::from(Vec64::from_slice(&[0u64, 2, 1, 0])),
    //         unique_values: Vec64::from(dict_strs()),
    //         null_mask: None
    //     };

    //     writer.register_dictionary(0, dict_strs());

    //     let tbl = make_table(
    //         FieldArray::new(
    //             Field {
    //                 name: "col".to_string(),
    //                 dtype: ArrowType::Dictionary(CategoricalIndexType::UInt64),
    //                 nullable: false,
    //                 metadata: Default::default()
    //             },
    //             Array::TextArray(TextArray::Categorical64(Arc::new(arr)))
    //         ),
    //         4
    //     );

    //     writer.write_table(&tbl).unwrap();
    //     writer.finish().unwrap();

    //     let mut buf = Vec::new();
    //     for frame in writer.out_frames {
    //         buf.extend_from_slice(&frame);
    //     }

    //     assert!(!buf.is_empty());
    //     check_ipc_padding(&buf);
    // }

    #[test]
    fn test_ipc_file_write_read_dict() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let mut writer = TableStreamEncoder::new(
            make_schema(CategoricalIndexType::UInt32, true),
            IPCMessageProtocol::File,
        );

        let arr = CategoricalArray {
            data: Buffer::from(Vec64::from_slice(&[0u32, 1, 1, 2])),
            unique_values: Vec64::from(dict_strs()),
            null_mask: Some(make_bitmask(&[true, false, true, true])),
        };

        writer.register_dictionary(0, dict_strs());

        let tbl = make_table(
            FieldArray::new(
                Field {
                    name: "col".to_string(),
                    dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
                    nullable: true,
                    metadata: Default::default(),
                },
                Array::TextArray(TextArray::Categorical32(Arc::new(arr))),
            ),
            4,
        );

        writer.write_record_batch_frame(&tbl).unwrap();
        writer.finish().unwrap();

        // Write to temp file
        let mut file = StdFile::create(&path).unwrap();
        for frame in writer.out_frames {
            use std::io::Write;
            file.write_all(&frame).unwrap();
        }
        file.flush().unwrap();
        drop(file);

        let buf = read_file_bytes(&path);
        println!("Written buffer:\n{:?}", buf);
        assert!(
            buf.starts_with(ARROW_MAGIC_NUMBER_PADDED),
            "file must start with Arrow magic"
        );
        assert!(
            buf.ends_with(ARROW_MAGIC_NUMBER),
            "file must end with Arrow magic"
        );
        println!("Written buffer len : {:?}", buf.len());
        // We add 2 for the end magic marker, and the 3rd 4-byte contination marker.
        // This is more of a sanity check than a starting alignment check.
        assert!(
            (buf.len() + 4 + 2) % 8 == 0,
            "Arrow IPC file must be a multiple of 8 bytes"
        );
    }

    #[test]
    fn test_ipc_file_write_read_std() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        // Create schema with a single Int32 (non-dictionary) column
        let schema = vec![Field {
            name: "col".to_string(),
            dtype: ArrowType::Int32,
            nullable: true,
            metadata: Default::default(),
        }];

        let mut writer = TableStreamEncoder64::new(schema.clone(), IPCMessageProtocol::File);

        // Create a simple Int32 array with null mask
        let data = vec![10i32, 20, 30, 40];
        let mask = make_bitmask(&[true, false, true, true]);
        let arr = NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(Vec64::from_slice(&data)),
            null_mask: Some(mask),
        }));

        let tbl = make_table(
            FieldArray::new(
                Field {
                    name: "col".to_string(),
                    dtype: ArrowType::Int32,
                    nullable: true,
                    metadata: Default::default(),
                },
                Array::NumericArray(arr),
            ),
            4,
        );

        writer.write_record_batch_frame(&tbl).unwrap();
        writer.finish().unwrap();

        // Write to temp file
        let mut file = StdFile::create(&path).unwrap();
        for frame in writer.out_frames {
            use std::io::Write;
            file.write_all(&frame).unwrap();
        }
        file.flush().unwrap();
        drop(file);

        let buf = read_file_bytes(&path);
        println!("Written buffer:\n{:?}", buf);
        assert!(
            buf.starts_with(ARROW_MAGIC_NUMBER_PADDED),
            "file must start with Arrow magic"
        );
        assert!(
            buf.ends_with(ARROW_MAGIC_NUMBER),
            "file must end with Arrow magic"
        );
    }
}
