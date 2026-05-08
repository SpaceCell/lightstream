//! Arrow IPC record batch encoding.
//!
//! Writes column data from a Table's arrays into an output buffer in one
//! pass, with no intermediate IPC body or frame allocations. The output
//! is a standard Arrow IPC record batch frame: continuation marker,
//! flatbuffer metadata, padding, and column data with alignment gaps.
//!
//! Used by both raw Arrow IPC writers and the Lightstream protocol codec.
//! The Lightstream codec wraps the output with a TLV header; raw Arrow
//! writers use it as-is.

use std::io;

use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{Array, Bitmask, NumericArray, TextArray};

use crate::arrow::message::org::apache::arrow::flatbuf as fbm;
use crate::compression::{Compression, compress};
use crate::enums::{IPCMessageProtocol, WriterState};
use crate::models::encoders::ipc::schema::build_flatbuf_recordbatch;
use crate::models::encoders::ipc::table_stream::TableStreamEncoder;
use crate::models::encoders::ipc::{IPCFrame, IPCFrameEncoder};
use crate::traits::frame_encoder::FrameEncoder;
use crate::traits::stream_buffer::StreamBuffer;
use crate::utils::{align_to, as_bytes, dict_values};

// ---------------------------------------------------------------------------
// Body layout types
// ---------------------------------------------------------------------------

/// A reference to column bytes plus alignment padding that follows.
pub(crate) struct WireRegion<'a> {
    pub(crate) data: &'a [u8],
    pub(crate) pad: usize,
}

/// Computed body layout for a record batch, collected without copying data.
pub(crate) struct BodyLayout<'a> {
    pub(crate) regions: Vec<WireRegion<'a>>,
    pub(crate) fb_field_nodes: Vec<fbm::FieldNode>,
    pub(crate) fb_buffers: Vec<fbm::Buffer>,
    pub(crate) body_size: usize,
}

// ---------------------------------------------------------------------------
// Body layout computation
// ---------------------------------------------------------------------------

/// Collect column data slice references and compute the exact IPC body
/// layout without copying any data.
///
/// Returns the regions to write, the flatbuffer metadata vectors, and
/// the exact body size. The caller builds the flatbuffer RecordBatch
/// message with the known body size and writes everything into a single
/// output buffer.
pub(crate) fn compute_body_layout<'a, B: StreamBuffer>(
    table: &'a minarrow::Table,
) -> io::Result<BodyLayout<'a>> {
    let n_rows = table.n_rows;
    let n_cols = table.cols.len();
    let mut regions: Vec<WireRegion<'a>> = Vec::with_capacity(n_cols * 3);
    let mut fb_field_nodes: Vec<fbm::FieldNode> = Vec::with_capacity(n_cols);
    let mut fb_buffers: Vec<fbm::Buffer> = Vec::with_capacity(n_cols * 2);
    let mut body_offset = 0usize;

    for col in &table.cols {
        let nullable = col.field.nullable;

        match &col.array {
            Array::NumericArray(num) => {
                let (data_bytes, null_mask): (&[u8], Option<&Bitmask>) = match num {
                    NumericArray::Int32(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    NumericArray::Int64(arr) => {
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
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::Int8(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::UInt8(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::Int16(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::UInt16(arr) => {
                        (as_bytes(arr.data.as_slice()), arr.null_mask.as_ref())
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "unsupported numeric subtype",
                        ));
                    }
                };
                push_null_region::<B>(
                    nullable,
                    null_mask,
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(data_bytes, &mut body_offset, &mut regions, &mut fb_buffers);
                fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, col.null_count as i64));
            }

            Array::BooleanArray(arr) => {
                push_null_region::<B>(
                    nullable,
                    arr.null_mask.as_ref(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(
                    arr.data.bits.as_slice(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, col.null_count as i64));
            }

            Array::TextArray(TextArray::String32(arr)) => {
                push_null_region::<B>(
                    nullable,
                    arr.null_mask.as_ref(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(
                    as_bytes(arr.offsets.as_slice()),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(
                    arr.data.as_slice(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, col.null_count as i64));
            }

            #[cfg(feature = "large_string")]
            Array::TextArray(TextArray::String64(arr)) => {
                push_null_region::<B>(
                    nullable,
                    arr.null_mask.as_ref(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(
                    as_bytes(arr.offsets.as_slice()),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(
                    arr.data.as_slice(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, col.null_count as i64));
            }

            #[cfg(any(not(feature = "default_categorical_8"), feature = "extended_categorical"))]
            Array::TextArray(TextArray::Categorical32(arr)) => {
                push_null_region::<B>(
                    nullable,
                    arr.null_mask.as_ref(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(
                    as_bytes(arr.data.as_slice()),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, col.null_count as i64));
            }

            #[cfg(feature = "default_categorical_8")]
            Array::TextArray(TextArray::Categorical8(arr)) => {
                push_null_region::<B>(
                    nullable,
                    arr.null_mask.as_ref(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(
                    as_bytes(arr.data.as_slice()),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, col.null_count as i64));
            }

            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical16(arr)) => {
                push_null_region::<B>(
                    nullable,
                    arr.null_mask.as_ref(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(
                    as_bytes(arr.data.as_slice()),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, col.null_count as i64));
            }

            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical64(arr)) => {
                push_null_region::<B>(
                    nullable,
                    arr.null_mask.as_ref(),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(
                    as_bytes(arr.data.as_slice()),
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, col.null_count as i64));
            }

            #[cfg(feature = "datetime")]
            Array::TemporalArray(temp) => {
                let (data_bytes, null_mask): (&[u8], Option<&Bitmask>) = match temp {
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
                push_null_region::<B>(
                    nullable,
                    null_mask,
                    &mut body_offset,
                    &mut regions,
                    &mut fb_buffers,
                );
                push_data_region::<B>(data_bytes, &mut body_offset, &mut regions, &mut fb_buffers);
                fb_field_nodes.push(fbm::FieldNode::new(n_rows as i64, col.null_count as i64));
            }

            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported column type: {}", col.field.name),
                ));
            }
        }
    }

    Ok(BodyLayout {
        regions,
        fb_field_nodes,
        fb_buffers,
        body_size: body_offset,
    })
}

// ---------------------------------------------------------------------------
// Record batch encoding
// ---------------------------------------------------------------------------

/// Encode a table as IPC frames, appending to the caller's buffer.
///
/// Handles schema and dictionary frames on the first batch via the encoder's
/// internal state. Appends the complete IPC frame sequence: prefix frames
/// then the record batch frame with column data.
///
/// `base_offset` is the byte position in `out` where IPC data starts,
/// used for alignment calculations so the body lands on a SIMD boundary
/// when the buffer's base address is aligned. Callers that prepend a wire
/// header pass `out.len()` before calling. Callers writing IPC-only pass 0.
///
/// Handles compression when enabled: each buffer is compressed individually
/// with a u64 LE uncompressed length prefix, per the Arrow IPC spec.
///
/// Returns the number of bytes appended to `out`.
pub(crate) fn encode_record_batch<B: StreamBuffer + Unpin>(
    encoder: &mut TableStreamEncoder<B>,
    table: &minarrow::Table,
    out: &mut B,
    base_offset: usize,
) -> io::Result<usize> {
    // Register dictionary values from categorical columns
    for (i, col) in table.cols.iter().enumerate() {
        if let ArrowType::Dictionary(_) = col.field.dtype {
            let uniques = dict_values(col).unwrap_or_default();
            encoder.register_dictionary(i as i64, uniques);
        }
    }

    // Encode schema + dictionary prefix frames on the first batch
    let mut prefix_frames: Vec<Vec<u8>> = Vec::new();
    if encoder.state == WriterState::Fresh {
        let schema_meta = encoder.encode_schema()?;
        // Emit schema as an IPC frame
        let frame = IPCFrame {
            meta: &schema_meta,
            body: &[],
            protocol: encoder.protocol,
            is_first: encoder.protocol == IPCMessageProtocol::File,
            is_last: false,
            footer_bytes: None,
        };
        let mut frame_offset = 0usize;
        let (encoded, _) = IPCFrameEncoder::encode::<B>(&mut frame_offset, &frame)?;
        prefix_frames.push(encoded.as_ref().to_vec());
    }
    let dict_ids = encoder.pending_dict_ids();
    for dict_id in dict_ids {
        if let Some((meta, body_vec)) = encoder.encode_dictionary(dict_id)? {
            let frame = IPCFrame {
                meta: &meta,
                body: &body_vec,
                protocol: encoder.protocol,
                is_first: false,
                is_last: false,
                footer_bytes: None,
            };
            let mut frame_offset = 0usize;
            let (encoded, _) = IPCFrameEncoder::encode::<B>(&mut frame_offset, &frame)?;
            prefix_frames.push(encoded.as_ref().to_vec());
        }
    }

    let prefix_size: usize = prefix_frames.iter().map(|f| f.len()).sum();

    // Compute the record batch body layout without copying column data
    let layout = compute_body_layout::<B>(table)?;

    // If compression is active, compress each buffer and recompute sizes.
    // Each compressed buffer gets a u64 LE uncompressed length prefix.
    let compressed: Option<Vec<Vec<u8>>> = if encoder.compression != Compression::None {
        let mut bufs = Vec::with_capacity(layout.regions.len());
        for region in &layout.regions {
            if region.data.is_empty() {
                bufs.push(Vec::new());
            } else {
                let c = compress(region.data, encoder.compression)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;
                let mut wire = Vec::with_capacity(8 + c.len());
                wire.extend_from_slice(&(region.data.len() as u64).to_le_bytes());
                wire.extend_from_slice(&c);
                bufs.push(wire);
            }
        }
        Some(bufs)
    } else {
        None
    };

    // Recompute buffer metadata if compressed
    let (body_size, fb_buffers) = if let Some(ref comp) = compressed {
        let mut bufs = Vec::with_capacity(comp.len());
        let mut offset = 0usize;
        for c in comp {
            let len = c.len();
            bufs.push(fbm::Buffer::new(offset as i64, len as i64));
            let pad = align_to::<B>(len);
            offset += len + pad;
        }
        (offset, bufs)
    } else {
        (layout.body_size, layout.fb_buffers)
    };

    let compression_type = encoder.compression.to_arrow_ipc_type()?;
    let meta = build_flatbuf_recordbatch(
        &mut encoder.fbb,
        table.n_rows,
        &layout.fb_field_nodes,
        &fb_buffers,
        body_size,
        compression_type,
    )?;

    // Compute IPC frame sizes with alignment
    let meta_end = base_offset + prefix_size + 4 + 4 + meta.len();
    let meta_pad = align_to::<B>(meta_end);
    let body_end = meta_end + meta_pad + body_size;
    let body_pad = align_to::<B>(body_end);

    let ipc_size = prefix_size + 4 + 4 + meta.len() + meta_pad + body_size + body_pad;
    out.reserve(ipc_size);

    // Write prefix frames (schema + dicts) into the output buffer
    for frame in &prefix_frames {
        out.extend_from_slice(frame);
    }

    // IPC continuation marker
    out.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());

    // Metadata size field (includes padding)
    out.extend_from_slice(&((meta.len() + meta_pad) as u32).to_le_bytes());

    // Metadata bytes
    out.extend_from_slice(&meta);

    // Metadata padding
    if meta_pad > 0 {
        out.extend_from_slice(&[0u8; 64][..meta_pad]);
    }

    // Column data - compressed or raw
    if let Some(ref comp) = compressed {
        for c in comp {
            out.extend_from_slice(c);
            let pad = align_to::<B>(c.len());
            if pad > 0 {
                out.extend_from_slice(&[0u8; 64][..pad]);
            }
        }
    } else {
        for region in &layout.regions {
            out.extend_from_slice(region.data);
            if region.pad > 0 {
                out.extend_from_slice(&[0u8; 64][..region.pad]);
            }
        }
    }

    // Body padding
    if body_pad > 0 {
        out.extend_from_slice(&[0u8; 64][..body_pad]);
    }

    Ok(ipc_size)
}


// ---------------------------------------------------------------------------
// Layout helpers
// ---------------------------------------------------------------------------

/// Record a null mask region into the body layout.
///
/// When the column is nullable and has a mask, pushes the mask bytes.
/// Otherwise records a zero-length buffer in the flatbuffer metadata.
fn push_null_region<'a, B: StreamBuffer>(
    nullable: bool,
    mask: Option<&'a Bitmask>,
    body_offset: &mut usize,
    regions: &mut Vec<WireRegion<'a>>,
    fb_buffers: &mut Vec<fbm::Buffer>,
) {
    if nullable {
        if let Some(m) = mask {
            let data = m.bits.as_slice();
            let pad = align_to::<B>(data.len());
            fb_buffers.push(fbm::Buffer::new(*body_offset as i64, data.len() as i64));
            regions.push(WireRegion { data, pad });
            *body_offset += data.len() + pad;
            return;
        }
    }
    fb_buffers.push(fbm::Buffer::new(0, 0));
}

/// Record a data region into the body layout.
fn push_data_region<'a, B: StreamBuffer>(
    data: &'a [u8],
    body_offset: &mut usize,
    regions: &mut Vec<WireRegion<'a>>,
    fb_buffers: &mut Vec<fbm::Buffer>,
) {
    let pad = align_to::<B>(data.len());
    fb_buffers.push(fbm::Buffer::new(*body_offset as i64, data.len() as i64));
    regions.push(WireRegion { data, pad });
    *body_offset += data.len() + pad;
}


