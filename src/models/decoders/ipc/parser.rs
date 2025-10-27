//! # IPC Parser
//!
//! Parses Arrow IPC `RecordBatch` messages into a `minarrow::Table`.
//!
//! ## Supported columns
//! - Fixed-width numerics: i32/i64, u32/u64, f32/f64
//!   - Optional: i8/u8, i16/u16 via `extended_numeric_types`
//! - Boolean (bit-packed)
//! - UTF-8 / LargeUTF-8 (32/64-bit offsets; `LargeString` behind `large_string`)
//! - Dictionary-encoded text (UInt32 by default; UInt8/UInt16/UInt64 behind `extended_categorical`)
//! - Date32/Date64 behind `datetime`
//!
//! ## Zero-copy behaviour
//! If an Arc-backed body is supplied and buffers are correctly aligned, columns are
//! created as shared views without buffer copies. Otherwise the decoder falls back to copying.
//!
//! ## Errors and limits
//! - Compressed `RecordBatch` bodies are not supported.
//! - Dictionary delta batches are rejected.
//! - All buffer regions are bounds-checked; malformed metadata yields `io::Error`.
//!
//! See the [Apache Arrow IPC specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).

//! RecordBatch -> Table decoder supporting Minarrow fixed‑width, Boolean,
//! UTF‑8, LargeUTF‑8, and Dictionary columns.

use std::io;
use std::sync::Arc;

use flatbuffers::Vector;
use minarrow::ffi::arrow_dtype::{ArrowType, CategoricalIndexType};
use minarrow::*;

use crate::arrow::message::org::apache::arrow::flatbuf as fb;
use crate::arrow::message::org::apache::arrow::flatbuf::{
    BodyCompression, Buffer, DictionaryBatch,
};
#[cfg(any(feature = "zstd", feature = "snappy"))]
use crate::compression::{Compression, decompress};
use crate::debug_println;
use crate::utils::SliceWrapper;
use crate::{AFMessage, AFMessageHeader};
use std::collections::HashMap;
use std::marker::PhantomData;

/// Used for parsing a RecordBatch into a Minarrow `Table`.
///
/// Can also be 'intercepted' and used in extreme performance-critical
/// situations to avoid async overhead.
pub struct RecordBatchParser;

impl RecordBatchParser {
    /// Parses a RecordBatch into a Minarrow `Table`.
    ///
    /// `arc_opt` may supply the backing buffer; if `None` we allocate a single
    /// `Arc<Vec64<u8>>` wrapping `arrow_buf` so that all aligned columns can
    /// reuse it without copies.
    pub fn parse_record_batch<'a>(
        message: &AFMessage<'a>,
        arrow_buf: &'a [u8],
        fields: &[Field],
        arc_opt: Option<Arc<[u8]>>,
    ) -> io::Result<Table> {
        if message.header_type() != AFMessageHeader::RecordBatch {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected RecordBatch header",
            ));
        }

        // TODO [3]: Compression handling
        if let Some(BodyCompression { .. }) = message
            .header_as_record_batch()
            .and_then(|rb| rb.compression())
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Compressed RecordBatch bodies are not yet supported",
            ));
        }

        let header = message.header_as_record_batch().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Missing RecordBatch payload")
        })?;

        let n_rows = header.length() as usize;
        let nodes = header
            .nodes()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing nodes"))?;
        let fbuf_meta = header
            .buffers()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing fbuf_meta"))?;

        if nodes.len() != fields.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Field count mismatch",
            ));
        }

        let mut cols = Vec::with_capacity(fields.len());
        let mut buffer_idx = 0;

        for (i, field) in fields.iter().enumerate() {
            let node = nodes.get(i);
            let field_len = node.length() as usize;
            let null_count = node.null_count() as usize;

            if field_len != n_rows {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Row count mismatch for {}", field.name),
                ));
            }

            let null_mask = Self::extract_null_mask(
                field,
                field_len,
                null_count,
                &fbuf_meta,
                &mut buffer_idx,
                arrow_buf,
            )?;

            let arr = match &field.dtype {
                // numeric primitives
                ArrowType::Int32 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<i32>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }
                ArrowType::Int64 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<i64>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }
                ArrowType::UInt32 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<u32>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::UInt32(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }
                ArrowType::UInt64 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<u64>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::UInt64(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }
                ArrowType::Float32 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<f32>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::Float32(Arc::new(FloatArray::new(
                        data, null_mask,
                    ))))
                }
                ArrowType::Float64 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<f64>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray::new(
                        data, null_mask,
                    ))))
                }
                #[cfg(feature = "extended_numeric_types")]
                ArrowType::Int8 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data = unsafe { Self::buffer_from_slice::<i8>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::Int8(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }
                #[cfg(feature = "extended_numeric_types")]
                ArrowType::Int16 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<i16>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::Int16(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }
                #[cfg(feature = "extended_numeric_types")]
                ArrowType::UInt8 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data = unsafe { Self::buffer_from_slice::<u8>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::UInt8(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }
                #[cfg(feature = "extended_numeric_types")]
                ArrowType::UInt16 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<u16>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::UInt16(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }

                // ---- boolean ---------------------------------------------------------------
                ArrowType::Boolean => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let bool_data = Bitmask::from_bytes(slice, field_len);
                    let bool_array = BooleanArray {
                        data: bool_data,
                        null_mask,
                        len: field_len,
                        _phantom: std::marker::PhantomData,
                    };
                    Array::BooleanArray(Arc::new(bool_array))
                }

                // ---- UTF‑8 -----------------------------------------------------------------
                ArrowType::String => {
                    let (data, offsets) = Self::parse_utf8_array::<u32>(
                        arrow_buf,
                        &fbuf_meta,
                        &mut buffer_idx,
                        field_len,
                        &field.name,
                        &arc_opt,
                    )?;
                    Array::TextArray(TextArray::String32(Arc::new(StringArray::new(
                        data,
                        null_mask.map(|mask| {
                            assert_eq!(
                                mask.len(),
                                field_len,
                                "String null_mask length must equal number of strings"
                            );
                            mask
                        }),
                        offsets,
                    ))))
                }
                #[cfg(feature = "large_string")]
                ArrowType::LargeString => {
                    // Check buffer size to determine if this should be parsed as String32 vs String64
                    let offsets_buf = fbuf_meta.get(buffer_idx);
                    let offsets_l = offsets_buf.length() as usize;
                    let expected_u32_size = (field_len + 1) * 4;
                    let expected_u64_size = (field_len + 1) * 8;

                    if offsets_l == expected_u32_size {
                        // Likely u32 offsets, parse as String32 instead
                        let (data, offsets) = Self::parse_utf8_array::<u32>(
                            arrow_buf,
                            &fbuf_meta,
                            &mut buffer_idx,
                            field_len,
                            &field.name,
                            &arc_opt,
                        )?;
                        let arr =
                            Array::TextArray(TextArray::String32(Arc::new(StringArray::new(
                                data,
                                null_mask.map(|mask| {
                                    assert_eq!(
                                        mask.len(),
                                        field_len,
                                        "String null_mask length must equal number of strings"
                                    );
                                    mask
                                }),
                                offsets,
                            ))));
                        // Create corrected field with String type instead of LargeString
                        let corrected_field = Field::new(
                            field.name.clone(),
                            ArrowType::String,
                            field.nullable,
                            Some(field.metadata.clone()),
                        );
                        cols.push(FieldArray::new(corrected_field, arr));
                        continue;
                    } else if offsets_l == expected_u64_size {
                        // u64 offsets, parse normally as String64
                        let (data, offsets) = Self::parse_utf8_array::<u64>(
                            arrow_buf,
                            &fbuf_meta,
                            &mut buffer_idx,
                            field_len,
                            &field.name,
                            &arc_opt,
                        )?;
                        Array::TextArray(TextArray::String64(Arc::new(StringArray::new(
                            data,
                            null_mask.map(|mask| {
                                assert_eq!(
                                    mask.len(),
                                    field_len,
                                    "String null_mask length must equal number of strings"
                                );
                                mask
                            }),
                            offsets,
                        ))))
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "Invalid offset buffer size for field {}: got {}, expected {} (u32) or {} (u64)",
                                field.name, offsets_l, expected_u32_size, expected_u64_size
                            ),
                        ));
                    }
                }
                #[cfg(feature = "datetime")]
                ArrowType::Date32 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<i32>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }
                #[cfg(feature = "large_string")]
                ArrowType::Date64 => {
                    let (slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data =
                        unsafe { Self::buffer_from_slice::<i64>(slice, field_len, &arc_opt) };
                    Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray::new(
                        data, null_mask,
                    ))))
                }
                // dictionary
                ArrowType::Dictionary(idx_ty) => {
                    // TODO[5]: isDelta check is done in the DictionaryBatch path,
                    // but we include a guard here in case a delta batch arrives first,
                    // as we do not yet support it.
                    if let Some(dict_batch) = message.header_as_dictionary_batch() {
                        if dict_batch.isDelta() {
                            return Err(io::Error::new(
                                io::ErrorKind::Unsupported,
                                "Unimplemented! (dictionary delta batches)",
                            ));
                        }
                    }

                    // indices
                    let (idx_slice, _) = Self::extract_buffer_slice(
                        &fbuf_meta,
                        &mut buffer_idx,
                        arrow_buf,
                        &field.name,
                    )?;
                    let data_buf: minarrow::Buffer<u32> =
                        unsafe { Self::buffer_from_slice::<u32>(idx_slice, field_len, &arc_opt) };

                    // unique offsets + bytes
                    let off_meta = fbuf_meta.get(buffer_idx);
                    buffer_idx += 1;
                    let val_meta = fbuf_meta.get(buffer_idx);
                    buffer_idx += 1;

                    let off_start = off_meta.offset() as usize;
                    let off_len = off_meta.length() as usize;
                    let off_slice = &arrow_buf[off_start..off_start + off_len];
                    let num_off = off_len / std::mem::size_of::<u32>();
                    let offsets_buf: minarrow::Buffer<u32> =
                        unsafe { Self::buffer_from_slice::<u32>(off_slice, num_off, &arc_opt) };

                    let val_start = val_meta.offset() as usize;
                    let val_len = val_meta.length() as usize;
                    let val_slice = &arrow_buf[val_start..val_start + val_len];
                    let bytes_buf: minarrow::Buffer<u8> =
                        unsafe { Self::buffer_from_slice::<u8>(val_slice, val_len, &arc_opt) };

                    // TODO[5]: u32 should be larger or dynamic to handle all variants
                    let offs: &[u32] = offsets_buf.as_ref();
                    let unique_n = offs.len().saturating_sub(1);
                    let mut unique_values = Vec64::<String>::with_capacity(unique_n);
                    for i in 0..unique_n {
                        let s =
                            std::str::from_utf8(&bytes_buf[offs[i] as usize..offs[i + 1] as usize])
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        unique_values.push(s.to_owned());
                    }

                    // choose variant by index type
                    match idx_ty {
                        CategoricalIndexType::UInt32 => {
                            Array::TextArray(TextArray::Categorical32(Arc::new(
                                CategoricalArray::<u32>::new(data_buf, unique_values, null_mask),
                            )))
                        }
                        #[cfg(feature = "extended_categorical")]
                        CategoricalIndexType::UInt8 => {
                            eprintln!(
                                "DEBUG parse_record_batch: Creating Categorical8, field_len={}, idx_slice.len()={}, unique_values.len()={}, null_mask={:?}",
                                field_len,
                                idx_slice.len(),
                                unique_values.len(),
                                null_mask.as_ref().map(|m| m.len())
                            );
                            let data8 = unsafe {
                                Self::buffer_from_slice::<u8>(idx_slice, field_len, &arc_opt)
                            };
                            Array::TextArray(TextArray::Categorical8(Arc::new(CategoricalArray::<
                                u8,
                            >::new(
                                data8,
                                unique_values,
                                null_mask,
                            ))))
                        }
                        #[cfg(feature = "extended_categorical")]
                        CategoricalIndexType::UInt16 => {
                            let data16 = unsafe {
                                Self::buffer_from_slice::<u16>(idx_slice, field_len, &arc_opt)
                            };
                            Array::TextArray(TextArray::Categorical16(Arc::new(
                                CategoricalArray::<u16>::new(data16, unique_values, null_mask),
                            )))
                        }
                        #[cfg(feature = "extended_categorical")]
                        CategoricalIndexType::UInt64 => {
                            let data64 = unsafe {
                                Self::buffer_from_slice::<u64>(idx_slice, field_len, &arc_opt)
                            };
                            Array::TextArray(TextArray::Categorical64(Arc::new(
                                CategoricalArray::<u64>::new(data64, unique_values, null_mask),
                            )))
                        }
                    }
                }

                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unsupported type: {:?}", other),
                    ));
                }
            };

            cols.push(FieldArray::new(field.clone(), arr));
        }

        Ok(Table {
            cols,
            n_rows,
            name: "RecordBatch".to_owned(),
        })
    }

    #[inline]
    pub fn extract_buffer_slice<'a>(
        fbuf_meta: &Vector<'a, Buffer>,
        buffer_idx: &mut usize,
        arrow_buf: &'a [u8],
        field_name: &str,
    ) -> io::Result<(&'a [u8], usize)> {
        let buf = fbuf_meta.get(*buffer_idx);
        *buffer_idx += 1;

        let offset = buf.offset() as usize;
        let length = buf.length() as usize;

        if offset + length > arrow_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Buffer out of bounds for {}", field_name),
            ));
        }

        Ok((&arrow_buf[offset..offset + length], offset))
    }

    /// Extract buffer slice with corrected offsets for decompressed data
    #[inline]
    pub fn extract_buffer_slice_with_offsets<'a>(
        fbuf_meta: &Vector<'a, Buffer>,
        buffer_idx: &mut usize,
        arrow_buf: &'a [u8],
        corrected_offsets: &[usize],
        field_name: &str,
    ) -> io::Result<(&'a [u8], usize)> {
        let buf = fbuf_meta.get(*buffer_idx);
        let current_buffer_idx = *buffer_idx;
        *buffer_idx += 1;

        if current_buffer_idx >= corrected_offsets.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Buffer index {} out of range for corrected offsets",
                    current_buffer_idx
                ),
            ));
        }

        let offset = corrected_offsets[current_buffer_idx];
        let length = buf.length() as usize;

        if offset + length > arrow_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Buffer out of bounds for {} (corrected offset)", field_name),
            ));
        }

        Ok((&arrow_buf[offset..offset + length], offset))
    }

    /// Turn a raw byte‑slice into a `Buffer<T>`.
    ///
    /// * If the bytes are properly aligned **and** we have an `Arc`,
    ///   we create a `Shared` view.
    /// * If aligned and no `Arc` is available, we copy into an
    ///   owned `Vec64<T>`.
    ///   in which case `decode_fixed_width_batch` supplies the `Arc`.
    /// * If unaligned we always copy.
    ///
    #[inline]
    pub unsafe fn buffer_from_slice<T: Copy>(
        slice: &[u8],
        len: usize,
        arc_bytes: &Option<Arc<[u8]>>,
    ) -> minarrow::Buffer<T> {
        let ptr = slice.as_ptr() as *const T;

        // Minimum alignment requirement
        let aligned_8 = (ptr as usize) % 8 == 0;
        let aligned_64 = (ptr as usize) % 64 == 0;

        debug_println!(
            "Creating buffer with:\nAligned 8: {:?}\nAligned 64: {:?}\narc_bytes is some: {:?}\n",
            aligned_8,
            aligned_64,
            arc_bytes.is_some()
        );

        println!("Off: {}", ptr as usize & 63);
        if ptr as usize & 63 == 0 {
            if let Some(arc) = arc_bytes {
                debug_println!("Aligned: Creating buffer with arc_bytes");
                // SAFETY: ptr must be within arc's allocation, correctly aligned and cover [ptr, ptr+len*T].
                // if it is not 64-byte aligned, the function will copy data to an owned buffer and flag it.
                unsafe { minarrow::Buffer::from_shared_raw(arc.clone(), ptr, len) }
            } else {
                debug_println!("Aligned: Allocating new buffer from slice");
                // No reusable Arc -> copy.
                let mut v = Vec64::with_capacity(len);
                unsafe { std::ptr::copy_nonoverlapping(ptr, v.as_mut_ptr(), len) };
                unsafe { v.set_len(len) };
                minarrow::Buffer::from(v)
            }
        } else {
            // Unaligned – must copy.
            debug_println!("Not aligned: Copying");
            let elem_size = std::mem::size_of::<T>();
            let mut v = Vec64::with_capacity(len);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    slice.as_ptr(),
                    v.as_mut_ptr() as *mut u8,
                    len * elem_size,
                )
            };
            unsafe { v.set_len(len) };
            minarrow::Buffer::from(v)
        }
    }

    // TODO[5]: Dictionary delta support

    /// Checks for the unsupported dictionary delta case.
    ///
    /// * If `isDelta == true` -> returns an **Unsupported** error.
    pub fn check_dictionary_delta(batch: &DictionaryBatch) -> io::Result<()> {
        if batch.isDelta() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unimplemented! (dictionary delta batches)",
            ));
        }
        Ok(())
    }

    /// Consume a validity‑bitmap buffer when present and build the proper
    /// `Bitmask`.  
    /// * For nullable fields we always return `Some(Bitmask)`  
    ///   – all‑true when the bitmap length is 0.  
    /// * For non‑nullable fields we skip an unexpected validity buffer
    ///   (length 0 or `ceil(n_rows/8)`) but return `None`.
    #[inline]
    pub fn extract_null_mask<'a>(
        field: &Field,
        field_len: usize,
        null_count: usize,
        fbuf_meta: &Vector<'a, Buffer>,
        buffer_idx: &mut usize,
        arrow_buf: &'a [u8],
    ) -> io::Result<Option<Bitmask>> {
        // handle non-nullable fields
        if !field.nullable {
            // Peek at the next buffer: it *may* be a redundant bitmap.
            let buf = fbuf_meta.get(*buffer_idx);
            let len_bytes = buf.length() as usize;
            let expected_validity_len = (field_len + 7) / 8; // ceil(n/8)

            if len_bytes == 0 || len_bytes == expected_validity_len {
                // It is a validity buffer – consume it, but ignore contents.
                *buffer_idx += 1;
            }
            return Ok(None);
        }

        // nullable: we must consume exactly one buffer
        let buf = fbuf_meta.get(*buffer_idx);
        *buffer_idx += 1;

        let offset = buf.offset() as usize;
        let len_bytes = buf.length() as usize;

        // If writer says `null_count == 0` **or** the bitmap is empty,
        // fabricate an all‑true mask.
        if null_count == 0 || len_bytes == 0 {
            return Ok(Some(Bitmask::new_set_all(field_len, true)));
        }

        // Real bitmap: bounds‑check then build.
        if offset + len_bytes > arrow_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Null buffer out of bounds for {}", field.name),
            ));
        }
        let bytes = &arrow_buf[offset..offset + len_bytes];
        Ok(Some(Bitmask::from_bytes(bytes, field_len)))
    }

    // Decodes an Arrow UTF8 or LargeUTF8 array from the IPC buffers.
    // This extracts both the concatenated string data (`Buffer<u8>`)
    // and the corresponding offset buffer (`Buffer<OffsetType>`).
    #[inline]
    pub fn parse_utf8_array<'a, OffsetType: Copy>(
        arrow_buf: &'a [u8],
        fbuf_meta: &Vector<'a, Buffer>,
        buffer_idx: &mut usize,
        field_len: usize,
        field_name: &str,
        arc_opt: &Option<Arc<[u8]>>,
    ) -> io::Result<(minarrow::Buffer<u8>, minarrow::Buffer<OffsetType>)> {
        let offsets_buf = fbuf_meta.get(*buffer_idx);
        let values_buf = fbuf_meta.get(*buffer_idx + 1);

        let offsets_o = offsets_buf.offset() as usize;
        let offsets_l = offsets_buf.length() as usize;
        let values_o = values_buf.offset() as usize;
        let values_l = values_buf.length() as usize;

        if offsets_o + offsets_l > arrow_buf.len() || values_o + values_l > arrow_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("String buffer out of bounds for {}", field_name),
            ));
        }

        println!("Creating buffer for {:?}", field_name);
        let data = unsafe {
            Self::buffer_from_slice::<u8>(
                &arrow_buf[values_o..values_o + values_l],
                values_l,
                arc_opt,
            )
        };

        println!("Creating offsets for {:?}", field_name);
        let offsets = unsafe {
            let off_slice = &arrow_buf[offsets_o..offsets_o + offsets_l];
            Self::buffer_from_slice::<OffsetType>(off_slice, field_len + 1, arc_opt)
        };

        *buffer_idx += 2;
        Ok((data, offsets))
    }
}

// ------------------------- Format Handlers ------------------------------------------------//

/// Parses and inserts a dictionary batch from Arrow IPC into the provided dictionary map.
///
/// Handles dictionary batches for categorical columns as per the Arrow IPC specification.
/// Validates offsets and buffer lengths. Returns error if out of bounds or malformed.
#[inline(always)]
pub(crate) fn handle_dictionary_batch(
    db: &fb::DictionaryBatch,
    body: &[u8],
    dicts: &mut HashMap<i64, Vec<String>>,
) -> io::Result<()> {
    if db.isDelta() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "delta dictionaries not supported",
        ));
    }
    let dict_id = db.id();
    let rec = db
        .data()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad dict batch"))?;
    let buffers = rec
        .buffers()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no buffers"))?;
    if buffers.len() < 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dictionary batch buffers < 3",
        ));
    }
    let off_meta = buffers.get(1);
    let data_meta = buffers.get(2);
    let off_off = off_meta.offset() as usize;
    let off_len = off_meta.length() as usize;
    let data_off = data_meta.offset() as usize;
    let data_len = data_meta.length() as usize;

    if off_off + off_len > body.len() || data_off + data_len > body.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "dictionary batch buffer out of bounds: off_off+off_len={}+{} or data_off+data_len={}+{} > body.len()={}",
                off_off,
                off_len,
                data_off,
                data_len,
                body.len()
            ),
        ));
    }

    let offs_slice = &body[off_off..off_off + off_len];
    let count = off_len / 4;
    if count < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dictionary batch offset count < 2",
        ));
    }
    let mut uniques = Vec::with_capacity(count - 1);
    for i in 0..(count - 1) {
        let start = u32::from_le_bytes(offs_slice[i * 4..i * 4 + 4].try_into().unwrap()) as usize;
        let end = u32::from_le_bytes(offs_slice[(i + 1) * 4..(i + 1) * 4 + 4].try_into().unwrap())
            as usize;
        if data_off + end > body.len() || data_off + start > body.len() || start > end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "dictionary batch string slice out of bounds",
            ));
        }
        let s = std::str::from_utf8(&body[data_off + start..data_off + end])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        uniques.push(s.to_owned());
    }

    dicts.insert(dict_id, uniques);
    Ok(())
}

/// Constructs an Arrow [`Table`] from a FlatBuffers record batch and supporting context.
///
/// Deserialises Arrow record batch and columns, reconstructs dictionary and buffer state.
#[inline(always)]
pub(crate) fn handle_record_batch(
    rec: &fb::RecordBatch,
    fields: &[Field],
    dicts: &HashMap<i64, Vec<String>>,
    body: &[u8],
) -> io::Result<Table> {
    let nodes = rec
        .nodes()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no nodes"))?;
    let buffers = rec
        .buffers()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no buffers"))?;
    let mut buffer_idx = 0;
    let mut cols = Vec::with_capacity(fields.len());
    let n_rows = nodes.get(0).length() as usize;

    #[allow(unused_mut)]
    let mut extract_buffer_slice =
        |buffer_idx: &mut usize, field_name: &str| -> io::Result<(&[u8], usize)> {
            RecordBatchParser::extract_buffer_slice(&buffers, buffer_idx, body, field_name)
        };

    for (col_idx, field) in fields.iter().enumerate() {
        let node = nodes.get(col_idx);
        let row_count = node.length() as usize;

        let null_mask = RecordBatchParser::extract_null_mask(
            field,
            row_count,
            node.null_count() as usize,
            &buffers,
            &mut buffer_idx,
            body,
        )?;

        match &field.dtype {
            #[cfg(feature = "extended_numeric_types")]
            ArrowType::Int8 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<i8>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::Int8,
                );
            }
            #[cfg(feature = "extended_numeric_types")]
            ArrowType::UInt8 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<u8>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::UInt8,
                );
            }
            #[cfg(feature = "extended_numeric_types")]
            ArrowType::Int16 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<i16>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::Int16,
                );
            }
            #[cfg(feature = "extended_numeric_types")]
            ArrowType::UInt16 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<u16>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::UInt16,
                );
            }
            ArrowType::Int32 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<i32>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::Int32,
                );
            }
            ArrowType::UInt32 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<u32>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::UInt32,
                );
            }
            ArrowType::Int64 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<i64>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::Int64,
                );
            }
            ArrowType::UInt64 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<u64>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::UInt64,
                );
            }
            ArrowType::Float32 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_float_col::<f32>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::Float32,
                );
            }
            ArrowType::Float64 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_float_col::<f64>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::Float64,
                );
            }
            ArrowType::Boolean => {
                let (data_slice, data_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(
                    &field.name,
                    col_idx,
                    data_offset,
                    data_slice.len(),
                    body.len(),
                )?;
                let arr = BooleanArray {
                    data: Bitmask {
                        bits: minarrow::Buffer::from(Vec64::from_slice(data_slice)),
                        len: n_rows,
                    },
                    len: n_rows,
                    null_mask,
                    _phantom: PhantomData,
                };
                cols.push(FieldArray::new(
                    field.clone(),
                    Array::BooleanArray(arr.into()),
                ));
            }
            ArrowType::String => {
                let (offs_slice, offs_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                let (data_slice, data_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_two_buffer_bounds(
                    &field.name,
                    col_idx,
                    offs_offset,
                    offs_slice.len(),
                    data_offset,
                    data_slice.len(),
                    body.len(),
                )?;
                #[cfg(not(feature = "large_string"))]
                let offs = cast_slice::<u32>(offs_slice);
                #[cfg(not(feature = "large_string"))]
                let arr = TextArray::String32(
                    StringArray::new(
                        minarrow::Buffer::from(Vec64::from_slice(data_slice)),
                        null_mask,
                        minarrow::Buffer::from(Vec64::from_slice(offs)),
                    )
                    .into(),
                );
                #[cfg(feature = "large_string")]
                let offs = cast_slice::<u64>(offs_slice);
                #[cfg(feature = "large_string")]
                let arr = TextArray::String64(
                    StringArray::new(
                        minarrow::Buffer::from(Vec64::from_slice(data_slice)),
                        null_mask,
                        minarrow::Buffer::from(Vec64::from_slice(offs)),
                    )
                    .into(),
                );
                cols.push(FieldArray::new(field.clone(), Array::TextArray(arr)));
            }
            #[cfg(feature = "large_string")]
            ArrowType::LargeString => {
                eprintln!(
                    "DEBUG: About to extract offset buffer for field '{}'",
                    field.name
                );
                let (offs_slice, offs_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                let (data_slice, data_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_two_buffer_bounds(
                    &field.name,
                    col_idx,
                    offs_offset,
                    offs_slice.len(),
                    data_offset,
                    data_slice.len(),
                    body.len(),
                )?;
                // Check if this is actually u32 offset data misidentified as LargeString
                // For string arrays, we have (n_elements + 1) offsets
                // If buffer size = (n_elements + 1) * 4, it's u32 offsets (String32)
                // If buffer size = (n_elements + 1) * 8, it's u64 offsets (String64)
                let expected_u32_size = (row_count + 1) * 4;
                let expected_u64_size = (row_count + 1) * 8;
                eprintln!(
                    "DEBUG: LargeString buffer size check for field '{}': got {}, expected {} (u32) or {} (u64), row_count={}",
                    field.name,
                    offs_slice.len(),
                    expected_u32_size,
                    expected_u64_size,
                    row_count
                );
                if offs_slice.len() == expected_u32_size {
                    // Likely u32 offsets, parse as String32 instead
                    let offs_u32 = cast_slice::<u32>(offs_slice);
                    let arr = TextArray::String32(
                        StringArray::new(
                            minarrow::Buffer::from(Vec64::from_slice(data_slice)),
                            null_mask,
                            minarrow::Buffer::from(Vec64::from_slice(offs_u32)),
                        )
                        .into(),
                    );
                    // Create corrected field with String type instead of LargeString
                    let corrected_field = Field::new(
                        field.name.clone(),
                        ArrowType::String,
                        field.nullable,
                        Some(field.metadata.clone()),
                    );
                    cols.push(FieldArray::new(corrected_field, Array::TextArray(arr)));
                } else if offs_slice.len() == expected_u64_size {
                    // Actual u64 offsets
                    let offs = cast_slice::<u64>(offs_slice);
                    let arr = TextArray::String64(
                        StringArray::new(
                            minarrow::Buffer::from(Vec64::from_slice(data_slice)),
                            null_mask,
                            minarrow::Buffer::from(Vec64::from_slice(offs)),
                        )
                        .into(),
                    );
                    cols.push(FieldArray::new(field.clone(), Array::TextArray(arr)));
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Invalid offset buffer size for field {}: got {}, expected {} (u32) or {} (u64)",
                            field.name,
                            offs_slice.len(),
                            expected_u32_size,
                            expected_u64_size
                        ),
                    ));
                }
            }
            #[cfg(feature = "datetime")]
            ArrowType::Date32 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<i32>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::Int32,
                );
            }
            #[cfg(feature = "datetime")]
            ArrowType::Date64 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col::<i64>(
                    &mut cols,
                    field,
                    data_slice,
                    null_mask.clone(),
                    NumericArray::Int64,
                );
            }
            ArrowType::Dictionary(idx_ty) => {
                let dict_key = col_idx as i64;
                let dict_values = match dicts.get(&dict_key) {
                    Some(d) => d,
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "Dictionary for column '{}' (col_idx={}) is missing before use",
                                field.name, col_idx
                            ),
                        ));
                    }
                };
                let (idx_slice, idx_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(
                    &field.name,
                    col_idx,
                    idx_offset,
                    idx_slice.len(),
                    body.len(),
                )?;
                match idx_ty {
                    CategoricalIndexType::UInt32 => {
                        eprintln!(
                            "DEBUG: Creating Categorical32 for field {}, idx_slice.len()={}, dict_values.len()={}, null_mask={:?}",
                            field.name,
                            idx_slice.len(),
                            dict_values.len(),
                            null_mask.as_ref().map(|m| m.len())
                        );
                        push_categorical_col::<u32>(
                            &mut cols,
                            field,
                            idx_slice,
                            dict_values,
                            null_mask.clone(),
                            TextArray::Categorical32,
                        );
                    }
                    #[cfg(feature = "extended_categorical")]
                    CategoricalIndexType::UInt8 => {
                        eprintln!(
                            "DEBUG: Creating Categorical8 for field {}, idx_slice.len()={}, dict_values.len()={}, null_mask={:?}",
                            field.name,
                            idx_slice.len(),
                            dict_values.len(),
                            null_mask.as_ref().map(|m| m.len())
                        );
                        push_categorical_col::<u8>(
                            &mut cols,
                            field,
                            idx_slice,
                            dict_values,
                            null_mask.clone(),
                            TextArray::Categorical8,
                        );
                    }
                    #[cfg(feature = "extended_categorical")]
                    CategoricalIndexType::UInt16 => {
                        eprintln!(
                            "DEBUG: Creating Categorical16 for field {}, idx_slice.len()={}, dict_values.len()={}, null_mask={:?}",
                            field.name,
                            idx_slice.len(),
                            dict_values.len(),
                            null_mask.as_ref().map(|m| m.len())
                        );
                        push_categorical_col::<u16>(
                            &mut cols,
                            field,
                            idx_slice,
                            dict_values,
                            null_mask.clone(),
                            TextArray::Categorical16,
                        );
                    }
                    #[cfg(feature = "extended_categorical")]
                    CategoricalIndexType::UInt64 => {
                        eprintln!(
                            "DEBUG: Creating Categorical64 for field {}, idx_slice.len()={}, dict_values.len()={}, null_mask={:?}",
                            field.name,
                            idx_slice.len(),
                            dict_values.len(),
                            null_mask.as_ref().map(|m| m.len())
                        );
                        push_categorical_col::<u64>(
                            &mut cols,
                            field,
                            idx_slice,
                            dict_values,
                            null_mask.clone(),
                            TextArray::Categorical64,
                        );
                    }
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unsupported field type {}", field.name),
                ));
            }
        }
    }

    Ok(Table {
        cols,
        n_rows,
        name: "RecordBatch".to_owned(),
    })
}

/// Variant of `handle_record_batch` that creates shared (Arc) buffers for zero-copy.
/// Used when reading from files where data is already in an Arc<[u8]>.
pub(crate) fn handle_record_batch_shared<M: ?Sized>(
    rec: &fb::RecordBatch,
    fields: &[Field],
    dicts: &HashMap<i64, Vec<String>>,
    arc_data: Arc<M>,
    body_offset: usize,
    body_len: usize,
) -> io::Result<Table>
where
    M: AsRef<[u8]> + Send + Sync + 'static,
{
    let nodes = rec
        .nodes()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no nodes"))?;
    let buffers = rec
        .buffers()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no buffers"))?;

    let mut buffer_idx = 0;
    let mut cols = Vec::with_capacity(fields.len());
    let n_rows = nodes.get(0).length() as usize;
    let full_data = arc_data.as_ref().as_ref();
    let body = &full_data[body_offset..body_offset + body_len];

    // Helper closure to extract buffer slices
    let extract_buffer_slice =
        |buffer_idx: &mut usize, field_name: &str| -> io::Result<(&[u8], usize)> {
            RecordBatchParser::extract_buffer_slice(&buffers, buffer_idx, body, field_name)
        };

    for (col_idx, field) in fields.iter().enumerate() {
        let node = nodes.get(col_idx);
        let row_count = node.length() as usize;

        let null_mask = RecordBatchParser::extract_null_mask(
            field,
            row_count,
            node.null_count() as usize,
            &buffers,
            &mut buffer_idx,
            body,
        )?;

        match &field.dtype {
            #[cfg(feature = "extended_numeric_types")]
            ArrowType::Int8 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<i8, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::Int8,
                    &arc_data,
                    body_offset,
                );
            }
            #[cfg(feature = "extended_numeric_types")]
            ArrowType::UInt8 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<u8, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::UInt8,
                    &arc_data,
                    body_offset,
                );
            }
            #[cfg(feature = "extended_numeric_types")]
            ArrowType::Int16 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<i16, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::Int16,
                    &arc_data,
                    body_offset,
                );
            }
            #[cfg(feature = "extended_numeric_types")]
            ArrowType::UInt16 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<u16, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::UInt16,
                    &arc_data,
                    body_offset,
                );
            }
            ArrowType::Int32 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<i32, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::Int32,
                    &arc_data,
                    body_offset,
                );
            }
            ArrowType::UInt32 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<u32, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::UInt32,
                    &arc_data,
                    body_offset,
                );
            }
            ArrowType::Int64 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<i64, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::Int64,
                    &arc_data,
                    body_offset,
                );
            }
            ArrowType::UInt64 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<u64, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::UInt64,
                    &arc_data,
                    body_offset,
                );
            }
            ArrowType::Float32 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_float_col_shared::<f32, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::Float32,
                    &arc_data,
                    body_offset,
                );
            }
            ArrowType::Float64 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_float_col_shared::<f64, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::Float64,
                    &arc_data,
                    body_offset,
                );
            }
            #[cfg(feature = "datetime")]
            ArrowType::Date32 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<i32, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::Int32,
                    &arc_data,
                    body_offset,
                );
            }
            #[cfg(feature = "datetime")]
            ArrowType::Date64 => {
                let (data_slice, data_off) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(&field.name, col_idx, data_off, data_slice.len(), body.len())?;
                push_numeric_col_shared::<i64, M>(
                    &mut cols,
                    field,
                    data_slice,
                    data_off,
                    null_mask.clone(),
                    NumericArray::Int64,
                    &arc_data,
                    body_offset,
                );
            }
            ArrowType::Boolean => {
                let (data_slice, data_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(
                    &field.name,
                    col_idx,
                    data_offset,
                    data_slice.len(),
                    body.len(),
                )?;

                // For boolean, we still need to copy as it's bit-packed
                let arr = BooleanArray {
                    data: Bitmask {
                        bits: minarrow::Buffer::from(Vec64::from_slice(data_slice)),
                        len: n_rows,
                    },
                    len: n_rows,
                    null_mask,
                    _phantom: PhantomData,
                };
                cols.push(FieldArray::new(
                    field.clone(),
                    Array::BooleanArray(arr.into()),
                ));
            }
            ArrowType::String => {
                let (offs_slice, offs_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                let (data_slice, data_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_two_buffer_bounds(
                    &field.name,
                    col_idx,
                    offs_offset,
                    offs_slice.len(),
                    data_offset,
                    data_slice.len(),
                    body.len(),
                )?;

                // Create shared buffers for string data using SharedBuffer
                use minarrow::structs::shared_buffer::SharedBuffer;

                let data_wrapper = SliceWrapper {
                    _owner: arc_data.clone(),
                    offset: body_offset + data_offset,
                    len: data_slice.len(),
                };
                let data_shared = SharedBuffer::from_owner(data_wrapper);
                let data_buf = minarrow::Buffer::from_shared(data_shared);

                let offs_wrapper = SliceWrapper {
                    _owner: arc_data.clone(),
                    offset: body_offset + offs_offset,
                    len: offs_slice.len(),
                };
                let offs_shared = SharedBuffer::from_owner(offs_wrapper);
                let offs_buf: minarrow::Buffer<u32> = minarrow::Buffer::from_shared(offs_shared);

                let arr =
                    TextArray::String32(StringArray::new(data_buf, null_mask, offs_buf).into());
                cols.push(FieldArray::new(field.clone(), Array::TextArray(arr)));
            }
            #[cfg(feature = "large_string")]
            ArrowType::LargeString => {
                eprintln!(
                    "DEBUG: About to extract offset buffer for field '{}'",
                    field.name
                );
                let (offs_slice, offs_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                let (data_slice, data_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_two_buffer_bounds(
                    &field.name,
                    col_idx,
                    offs_offset,
                    offs_slice.len(),
                    data_offset,
                    data_slice.len(),
                    body.len(),
                )?;

                // Create shared buffers for string data using SharedBuffer
                use minarrow::structs::shared_buffer::SharedBuffer;

                let data_wrapper = SliceWrapper {
                    _owner: arc_data.clone(),
                    offset: body_offset + data_offset,
                    len: data_slice.len(),
                };
                let data_shared = SharedBuffer::from_owner(data_wrapper);
                let data_buf = minarrow::Buffer::from_shared(data_shared);

                // Check if this is actually u32 offset data misidentified as LargeString
                // For string arrays, we have (n_elements + 1) offsets
                // If buffer size = (n_elements + 1) * 4, it's u32 offsets (String32)
                // If buffer size = (n_elements + 1) * 8, it's u64 offsets (String64)
                let expected_u32_size = (row_count + 1) * 4;
                let expected_u64_size = (row_count + 1) * 8;
                if offs_slice.len() == expected_u32_size {
                    // Likely u32 offsets, parse as String32 instead
                    let offs_wrapper = SliceWrapper {
                        _owner: arc_data.clone(),
                        offset: body_offset + offs_offset,
                        len: offs_slice.len(),
                    };
                    let offs_shared = SharedBuffer::from_owner(offs_wrapper);
                    let offs_buf: minarrow::Buffer<u32> =
                        minarrow::Buffer::from_shared(offs_shared);

                    let arr =
                        TextArray::String32(StringArray::new(data_buf, null_mask, offs_buf).into());
                    // Create corrected field with String type instead of LargeString
                    let corrected_field = Field::new(
                        field.name.clone(),
                        ArrowType::String,
                        field.nullable,
                        Some(field.metadata.clone()),
                    );
                    cols.push(FieldArray::new(corrected_field, Array::TextArray(arr)));
                } else if offs_slice.len() == expected_u64_size {
                    // Actual u64 offsets
                    let offs_wrapper = SliceWrapper {
                        _owner: arc_data.clone(),
                        offset: body_offset + offs_offset,
                        len: offs_slice.len(),
                    };
                    let offs_shared = SharedBuffer::from_owner(offs_wrapper);
                    let offs_buf: minarrow::Buffer<u64> =
                        minarrow::Buffer::from_shared(offs_shared);

                    let arr =
                        TextArray::String64(StringArray::new(data_buf, null_mask, offs_buf).into());
                    cols.push(FieldArray::new(field.clone(), Array::TextArray(arr)));
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Invalid offset buffer size for field {}: got {}, expected {} (u32) or {} (u64)",
                            field.name,
                            offs_slice.len(),
                            expected_u32_size,
                            expected_u64_size
                        ),
                    ));
                }
            }
            ArrowType::Dictionary(idx_ty) => {
                let dict_key = col_idx as i64;
                let dict_values = match dicts.get(&dict_key) {
                    Some(d) => d,
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "Dictionary for column '{}' (col_idx={}) is missing before use",
                                field.name, col_idx
                            ),
                        ));
                    }
                };
                let (idx_slice, idx_offset) = extract_buffer_slice(&mut buffer_idx, &field.name)?;
                check_buffer_bounds(
                    &field.name,
                    col_idx,
                    idx_offset,
                    idx_slice.len(),
                    body.len(),
                )?;

                match idx_ty {
                    CategoricalIndexType::UInt32 => {
                        eprintln!(
                            "DEBUG: Creating Categorical32 for field {}, idx_slice.len()={}, dict_values.len()={}, null_mask={:?}",
                            field.name,
                            idx_slice.len(),
                            dict_values.len(),
                            null_mask.as_ref().map(|m| m.len())
                        );
                        push_categorical_col::<u32>(
                            &mut cols,
                            field,
                            idx_slice,
                            dict_values,
                            null_mask.clone(),
                            TextArray::Categorical32,
                        );
                    }
                    #[cfg(feature = "extended_categorical")]
                    CategoricalIndexType::UInt8 => {
                        eprintln!(
                            "DEBUG: Creating Categorical8 for field {}, idx_slice.len()={}, dict_values.len()={}, null_mask={:?}",
                            field.name,
                            idx_slice.len(),
                            dict_values.len(),
                            null_mask.as_ref().map(|m| m.len())
                        );
                        push_categorical_col::<u8>(
                            &mut cols,
                            field,
                            idx_slice,
                            dict_values,
                            null_mask.clone(),
                            TextArray::Categorical8,
                        );
                    }
                    #[cfg(feature = "extended_categorical")]
                    CategoricalIndexType::UInt16 => {
                        eprintln!(
                            "DEBUG: Creating Categorical16 for field {}, idx_slice.len()={}, dict_values.len()={}, null_mask={:?}",
                            field.name,
                            idx_slice.len(),
                            dict_values.len(),
                            null_mask.as_ref().map(|m| m.len())
                        );
                        push_categorical_col::<u16>(
                            &mut cols,
                            field,
                            idx_slice,
                            dict_values,
                            null_mask.clone(),
                            TextArray::Categorical16,
                        );
                    }
                    #[cfg(feature = "extended_categorical")]
                    CategoricalIndexType::UInt64 => {
                        eprintln!(
                            "DEBUG: Creating Categorical64 for field {}, idx_slice.len()={}, dict_values.len()={}, null_mask={:?}",
                            field.name,
                            idx_slice.len(),
                            dict_values.len(),
                            null_mask.as_ref().map(|m| m.len())
                        );
                        push_categorical_col::<u64>(
                            &mut cols,
                            field,
                            idx_slice,
                            dict_values,
                            null_mask.clone(),
                            TextArray::Categorical64,
                        );
                    }
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unsupported type in shared handler: {:?}", field.dtype),
                ));
            }
        }
    }

    Ok(Table {
        cols,
        n_rows,
        name: "RecordBatch".to_owned(),
    })
}

/// Extracts all Arrow fields from a FlatBuffers Arrow schema message.
///
/// Converts FlatBuffers fields to native [`Field`] representations.
#[inline(always)]
pub fn handle_schema_header(af_msg: &fb::Message) -> io::Result<Vec<Field>> {
    // 1. Validate Flatbuffer version
    let version = af_msg.version();
    match version {
        fb::MetadataVersion::V1
        | fb::MetadataVersion::V2
        | fb::MetadataVersion::V3
        | fb::MetadataVersion::V4
        | fb::MetadataVersion::V5 => { /* Supported */ }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Arrow IPC: unsupported Flatbuffer metadata version {:?}",
                    version
                ),
            ));
        }
    }

    // 2. Parse and validate Schema header
    let schema = af_msg
        .header_as_schema()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "schema missing"))?;

    // 3. Enforce endianness = little endian
    let endianness = schema.endianness();
    if endianness != fb::Endianness::Little {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Arrow IPC: unsupported endianness {:?} - only Little Endian is supported",
                endianness
            ),
        ));
    }

    // 4. Extract fields
    let fb_fields = schema
        .fields()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no fields"))?;
    let mut fields = Vec::with_capacity(fb_fields.len());
    for i in 0..fb_fields.len() {
        fields.push(convert_flatbuffers_to_arrow_field(&fb_fields.get(i))?);
    }
    Ok(fields)
}

/// Converts a FlatBuffers Arrow Field definition to an Arrow [`Field`] struct.
///
/// Extracts name, nullability, user-defined metadata, and data type information.
#[inline]
pub fn convert_flatbuffers_to_arrow_field(fb_field: &fb::Field) -> io::Result<Field> {
    let name = fb_field
        .name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing field name"))?
        .to_string();

    let nullable = fb_field.nullable();
    let metadata = extract_metadata(fb_field.custom_metadata());
    let base_type = extract_base_type(fb_field)?;
    let dtype = extract_dtype(fb_field, base_type)?;

    Ok(Field {
        name,
        dtype,
        nullable,
        metadata,
    })
}

/// Extracts user-defined key-value metadata from a FlatBuffers Arrow Field.
///
/// Converts FlatBuffers custom metadata into a key-value [`BTreeMap`].
fn extract_metadata<'a>(
    meta_vec: Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<fb::KeyValue<'a>>>>,
) -> std::collections::BTreeMap<String, String> {
    let mut map = std::collections::BTreeMap::new();
    if let Some(vec) = meta_vec {
        for i in 0..vec.len() {
            let k = vec.get(i).key().unwrap_or("").to_string();
            let v = vec.get(i).value().unwrap_or("").to_string();
            map.insert(k, v);
        }
    }
    map
}

/// Determines the Arrow categorical index type from FlatBuffers dictionary index metadata.
///
/// Supports UInt32 by default, additional types under `extended_categorical`.
fn extract_categorical_index_type(
    index_type: Option<&fb::Int>,
) -> io::Result<CategoricalIndexType> {
    let idx_type = index_type.ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "missing dictionary index type")
    })?;
    match idx_type.bitWidth() {
        32 => Ok(CategoricalIndexType::UInt32),
        #[cfg(feature = "extended_categorical")]
        64 => Ok(CategoricalIndexType::UInt64),
        #[cfg(feature = "extended_categorical")]
        16 => Ok(CategoricalIndexType::UInt16),
        #[cfg(feature = "extended_categorical")]
        8 => Ok(CategoricalIndexType::UInt8),
        w => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported dict index width {w}"),
        )),
    }
}

/// Deduces the logical Arrow type from the FlatBuffers field variant.
///
/// Maps Arrow physical types, dictionaries, and text columns.
fn extract_base_type(fb_field: &fb::Field) -> io::Result<ArrowType> {
    match fb_field.type_type() {
        fb::Type::Int => {
            let i = fb_field
                .type__as_int()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing Int type"))?;
            match (i.bitWidth(), i.is_signed()) {
                #[cfg(feature = "extended_numeric_types")]
                (8, true) => Ok(ArrowType::Int8),
                #[cfg(feature = "extended_numeric_types")]
                (8, false) => Ok(ArrowType::UInt8),
                #[cfg(feature = "extended_numeric_types")]
                (16, true) => Ok(ArrowType::Int16),
                #[cfg(feature = "extended_numeric_types")]
                (16, false) => Ok(ArrowType::UInt16),
                (32, true) => Ok(ArrowType::Int32),
                (64, true) => Ok(ArrowType::Int64),
                (32, false) => Ok(ArrowType::UInt32),
                (64, false) => Ok(ArrowType::UInt64),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported int width",
                )),
            }
        }
        #[cfg(not(feature = "large_string"))]
        fb::Type::Utf8 => Ok(ArrowType::String),
        fb::Type::FloatingPoint => {
            let f = fb_field.type__as_floating_point().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing FloatingPoint type")
            })?;
            match f.precision() {
                fb::Precision::SINGLE => Ok(ArrowType::Float32),
                fb::Precision::DOUBLE => Ok(ArrowType::Float64),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported float precision",
                )),
            }
        }
        #[cfg(feature = "datetime")]
        fb::Type::Date => {
            let d = fb_field
                .type__as_date()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing Date type"))?;
            convert_date_unit_fb(d.unit())
        }
        fb::Type::Bool => Ok(ArrowType::Boolean),
        #[cfg(feature = "large_string")]
        fb::Type::Utf8 => Ok(ArrowType::LargeString),
        other => {
            if let Some(dict) = fb_field.dictionary() {
                let idx_ty = extract_categorical_index_type(dict.indexType().as_ref())?;
                Ok(ArrowType::Dictionary(idx_ty))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported fb type {other:?}"),
                ))
            }
        }
    }
}

/// Deduces the Arrow data type (possibly dictionary-wrapped) for an Arrow field.
///
/// Returns wrapped type if dictionary encoding present, otherwise raw type.
fn extract_dtype(fb_field: &fb::Field, base_type: ArrowType) -> io::Result<ArrowType> {
    if let Some(dict) = fb_field.dictionary() {
        let idx_ty = extract_categorical_index_type(dict.indexType().as_ref())?;
        Ok(ArrowType::Dictionary(idx_ty))
    } else {
        Ok(base_type)
    }
}

/// Casts a byte slice to a slice of T. Caller must guarantee correct alignment and length.
///
/// # Safety
/// Caller must ensure the data slice points to a valid byte array for type T.
#[inline(always)]
fn cast_slice<T: Copy>(data: &[u8]) -> &[T] {
    unsafe {
        std::slice::from_raw_parts(
            data.as_ptr() as *const T,
            data.len() / std::mem::size_of::<T>(),
        )
    }
}

/// Convert FlatBuffer DateUnit to ArrowType (shared helper to avoid duplication)
#[cfg(feature = "datetime")]
#[inline(always)]
fn convert_date_unit_fb(unit: fb::DateUnit) -> io::Result<ArrowType> {
    match unit {
        fb::DateUnit::DAY => Ok(ArrowType::Date32),
        fb::DateUnit::MILLISECOND => Ok(ArrowType::Date64),
        // Note: fb::DateUnit only has DAY and MILLISECOND variants in the Arrow spec
        // This _ pattern handles potential future additions or invalid values
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported Date unit {:?}", unit),
        )),
    }
}

/// Convert FlatBuffer DateUnit to ArrowType for file format (fbf namespace)
#[cfg(feature = "datetime")]
#[inline(always)]
fn convert_date_unit_fbf(
    unit: crate::arrow::file::org::apache::arrow::flatbuf::DateUnit,
) -> io::Result<ArrowType> {
    match unit {
        crate::arrow::file::org::apache::arrow::flatbuf::DateUnit::DAY => Ok(ArrowType::Date32),
        crate::arrow::file::org::apache::arrow::flatbuf::DateUnit::MILLISECOND => {
            Ok(ArrowType::Date64)
        }
        // Note: DateUnit only has DAY and MILLISECOND variants in the Arrow spec
        // This _ pattern handles potential future additions or invalid values
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported Date unit {:?}", unit),
        )),
    }
}

/// Appends a numeric column to the columns vector, constructing from raw bytes and optional null mask.
///
/// Supports i32, u32, i64, u64, etc.
#[inline(always)]
fn push_numeric_col<T>(
    cols: &mut Vec<FieldArray>,
    field: &Field,
    data_slice: &[u8],
    null_mask: Option<Bitmask>,
    make_array: fn(Arc<IntegerArray<T>>) -> NumericArray,
) where
    T: Copy,
{
    let values = cast_slice::<T>(data_slice);
    let arr = Arc::new(IntegerArray {
        data: minarrow::Buffer::from(Vec64::from_slice(values)),
        null_mask,
    });
    cols.push(FieldArray::new(
        field.clone(),
        Array::NumericArray(make_array(arr)),
    ));
}

/// Shared buffer version of push_numeric_col for zero-copy from Arc<M>
/// Creates truly zero-copy buffers when data is 64-byte aligned.
#[inline(always)]
fn push_numeric_col_shared<T, M: ?Sized>(
    cols: &mut Vec<FieldArray>,
    field: &Field,
    data_slice: &[u8],
    data_offset: usize,
    null_mask: Option<Bitmask>,
    make_array: fn(Arc<IntegerArray<T>>) -> NumericArray,
    arc_data: &Arc<M>,
    body_offset: usize,
) where
    T: Copy,
    M: AsRef<[u8]> + Send + Sync + 'static,
{
    use minarrow::structs::shared_buffer::SharedBuffer;

    let absolute_offset = body_offset + data_offset;
    let final_addr = arc_data.as_ref().as_ref().as_ptr() as usize + absolute_offset;

    debug_println!(
        "Numeric buffer {} - body_offset: {}, data_offset: {}, absolute_offset: {}, final_addr: 0x{:x}, aligned: {}",
        field.name,
        body_offset,
        data_offset,
        absolute_offset,
        final_addr,
        final_addr % 64 == 0
    );

    // Create a wrapper that references the slice we need
    let byte_len = data_slice.len();

    let wrapper = SliceWrapper {
        _owner: arc_data.clone(),
        offset: absolute_offset,
        len: byte_len,
    };

    let shared = SharedBuffer::from_owner(wrapper);
    debug_println!(
        "SharedBuffer pointer for {}: {:p}, aligned: {}",
        field.name,
        shared.as_slice().as_ptr(),
        shared.as_slice().as_ptr() as usize % 64 == 0
    );
    let data = minarrow::Buffer::from_shared(shared);

    let arr = Arc::new(IntegerArray { data, null_mask });
    cols.push(FieldArray::new(
        field.clone(),
        Array::NumericArray(make_array(arr)),
    ));
}

/// Appends a floating-point column to the columns vector, constructing from raw bytes and optional null mask.
///
/// Supports f32, f64, etc.
#[inline(always)]
fn push_float_col<T>(
    cols: &mut Vec<FieldArray>,
    field: &Field,
    data_slice: &[u8],
    null_mask: Option<Bitmask>,
    make_array: fn(Arc<FloatArray<T>>) -> NumericArray,
) where
    T: Copy,
{
    let values = cast_slice::<T>(data_slice);
    let arr = Arc::new(FloatArray {
        data: minarrow::Buffer::from(Vec64::from_slice(values)),
        null_mask,
    });
    cols.push(FieldArray::new(
        field.clone(),
        Array::NumericArray(make_array(arr)),
    ));
}

/// Shared buffer version of push_float_col for zero-copy from Arc<[u8]>
/// If the data is 64-byte aligned, creates a shared buffer. Otherwise clones.
#[inline(always)]
fn push_float_col_shared<T, M: ?Sized>(
    cols: &mut Vec<FieldArray>,
    field: &Field,
    data_slice: &[u8],
    data_offset: usize,
    null_mask: Option<Bitmask>,
    make_array: fn(Arc<FloatArray<T>>) -> NumericArray,
    arc_data: &Arc<M>,
    body_offset: usize,
) where
    T: Copy,
    M: AsRef<[u8]> + Send + Sync + 'static,
{
    use minarrow::structs::shared_buffer::SharedBuffer;

    let absolute_offset = body_offset + data_offset;
    let byte_len = data_slice.len();

    let wrapper = SliceWrapper {
        _owner: arc_data.clone(),
        offset: absolute_offset,
        len: byte_len,
    };

    let shared = SharedBuffer::from_owner(wrapper);
    let data = minarrow::Buffer::from_shared(shared);

    let arr = Arc::new(FloatArray { data, null_mask });
    cols.push(FieldArray::new(
        field.clone(),
        Array::NumericArray(make_array(arr)),
    ));
}

/// Appends a categorical (dictionary) column to the columns vector, constructing from indices and dictionary values.
///
/// Supports UInt32 by default; additional categorical index types if enabled.
#[inline(always)]
fn push_categorical_col<T: Copy + Integer>(
    cols: &mut Vec<FieldArray>,
    field: &Field,
    idx_slice: &[u8],
    dict_values: &[String],
    null_mask: Option<Bitmask>,
    variant: fn(Arc<CategoricalArray<T>>) -> TextArray,
) {
    let values = cast_slice::<T>(idx_slice);
    let arr = variant(Arc::new(CategoricalArray {
        data: minarrow::Buffer::from(Vec64::from_slice(values)),
        unique_values: Vec64::from(dict_values.to_vec()),
        null_mask,
    }));
    cols.push(FieldArray::new(field.clone(), Array::TextArray(arr)));
}

/// Checks that a buffer region lies within bounds; returns an error if not.
#[inline(always)]
fn check_buffer_bounds(
    field_name: &str,
    col_idx: usize,
    off: usize,
    len: usize,
    body_len: usize,
) -> io::Result<()> {
    if off + len > body_len {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("buffer out of bounds for {}/{}", field_name, col_idx),
        ))
    } else {
        Ok(())
    }
}

/// Checks that two buffer regions lie within bounds; returns an error if not.
#[inline(always)]
fn check_two_buffer_bounds(
    field_name: &str,
    col_idx: usize,
    off1: usize,
    len1: usize,
    off2: usize,
    len2: usize,
    body_len: usize,
) -> io::Result<()> {
    if off1 + len1 > body_len || off2 + len2 > body_len {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "buffer out of bounds for {} ({}), offsets {}+{} or {}+{} > {}",
                field_name, col_idx, off1, len1, off2, len2, body_len
            ),
        ))
    } else {
        Ok(())
    }
}

/// Converts a Flatbuffers `Field` to the `Minarrow` version
pub fn convert_fb_field_to_arrow(
    fbf_field: &crate::arrow::file::org::apache::arrow::flatbuf::Field,
) -> io::Result<Field> {
    use crate::arrow::file::org::apache::arrow::flatbuf as fbf;
    // name
    let name = fbf_field
        .name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing field name"))?
        .to_string();

    // nullable
    let nullable = fbf_field.nullable();

    // user metadata
    let metadata = {
        let mut map = std::collections::BTreeMap::<String, String>::new();
        if let Some(vec) = fbf_field.custom_metadata() {
            for i in 0..vec.len() {
                let kv = vec.get(i);
                map.insert(
                    kv.key().unwrap_or("").to_owned(),
                    kv.value().unwrap_or("").to_owned(),
                );
            }
        }
        map
    };

    // Check for dictionary encoding first, regardless of the underlying type
    let base_type = if let Some(dict) = fbf_field.dictionary() {
        use minarrow::ffi::arrow_dtype::CategoricalIndexType as Idx;
        let idx = dict
            .indexType()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing dict idx"))?;
        let idx_ty = match idx.bitWidth() {
            32 => Idx::UInt32,
            #[cfg(feature = "extended_categorical")]
            8 => Idx::UInt8,
            #[cfg(feature = "extended_categorical")]
            16 => Idx::UInt16,
            #[cfg(feature = "extended_categorical")]
            64 => Idx::UInt64,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad dict idx width",
                ));
            }
        };
        ArrowType::Dictionary(idx_ty)
    } else {
        // base dtype (non-dictionary)
        match fbf_field.type_type() {
            fbf::Type::Int => {
                let i = fbf_field.type__as_int().unwrap();
                match (i.bitWidth(), i.is_signed()) {
                    #[cfg(feature = "extended_numeric_types")]
                    (8, true) => ArrowType::Int8,
                    #[cfg(feature = "extended_numeric_types")]
                    (8, false) => ArrowType::UInt8,
                    #[cfg(feature = "extended_numeric_types")]
                    (16, true) => ArrowType::Int16,
                    #[cfg(feature = "extended_numeric_types")]
                    (16, false) => ArrowType::UInt16,
                    (32, true) => ArrowType::Int32,
                    (64, true) => ArrowType::Int64,
                    (32, false) => ArrowType::UInt32,
                    (64, false) => ArrowType::UInt64,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unsupported int width",
                        ));
                    }
                }
            }
            fbf::Type::FloatingPoint => {
                let f = fbf_field.type__as_floating_point().unwrap();
                match f.precision() {
                    fbf::Precision::SINGLE => ArrowType::Float32,
                    fbf::Precision::DOUBLE => ArrowType::Float64,
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "unsupported float prec",
                        ));
                    }
                }
            }
            #[cfg(not(feature = "large_string"))]
            fbf::Type::Utf8 => ArrowType::String,
            #[cfg(feature = "large_string")]
            fbf::Type::Utf8 => ArrowType::LargeString,
            fbf::Type::Bool => ArrowType::Boolean,
            #[cfg(feature = "datetime")]
            fbf::Type::Date => {
                let d = fbf_field.type__as_date().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing Date type")
                })?;
                convert_date_unit_fbf(d.unit())?
            }
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported fb type {other:?}"),
                ));
            }
        }
    };

    Ok(Field {
        name,
        dtype: base_type,
        nullable,
        metadata,
    })
}

/// Decompress a compressed record batch body buffer by buffer, creating a sequential layout
/// Each buffer is compressed individually with format: 8-byte uncompressed length + compressed data
/// Returns the decompressed body and the correct offset for each buffer in the decompressed layout
#[cfg(any(feature = "zstd", feature = "snappy"))]
pub(crate) fn decompress_sequential_body(
    buffers: &Vector<Buffer>,
    compressed_body: &[u8],
) -> io::Result<(Vec<u8>, Vec<usize>)> {
    let mut decompressed_body = Vec::new();
    let mut buffer_offsets = Vec::new(); // Offset of each buffer in the decompressed body
    let mut read_offset = 0; // Offset in the compressed body we're reading from

    eprintln!(
        "DEBUG: Decompressing {} buffers, compressed_body.len()={}",
        buffers.len(),
        compressed_body.len()
    );

    for i in 0..buffers.len() {
        let buf = buffers.get(i);
        let buf_length = buf.length() as usize; // This is the UNCOMPRESSED length

        eprintln!(
            "DEBUG: Buffer {}: uncompressed_length={}, read_offset={}",
            i, buf_length, read_offset
        );

        // Record the current offset in the decompressed body for this buffer
        buffer_offsets.push(decompressed_body.len());

        // Skip buffers with zero length (common for null masks)
        if buf_length == 0 {
            eprintln!("DEBUG: Skipping zero-length buffer {}", i);
            // Zero length buffers still take space in the decompressed output (nothing to decompress)
            continue;
        }

        // Check if this looks like a compressed buffer (starts with 8-byte uncompressed length)
        if read_offset + 8 > compressed_body.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Buffer {} header extends beyond body data", i),
            ));
        }

        let header_bytes = &compressed_body[read_offset..read_offset + 8];
        let uncompressed_len = u64::from_le_bytes(header_bytes.try_into().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid length header: {}", e),
            )
        })?) as usize;

        eprintln!(
            "DEBUG: Buffer {} header uncompressed_len={}",
            i, uncompressed_len
        );

        // Verify the uncompressed length matches the FlatBuffers metadata
        if uncompressed_len != buf_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Uncompressed length mismatch for buffer {}: header says {}, metadata says {}",
                    i, uncompressed_len, buf_length
                ),
            ));
        }

        let compressed_start = read_offset + 8;

        // For the compressed data size, we need to scan to find the right boundary
        // since compressed data can be variable size + padding
        let remaining_body = &compressed_body[compressed_start..];

        // Try different compression algorithms and find the one that works
        let compression_types = [
            #[cfg(feature = "snappy")]
            Compression::Snappy,
            #[cfg(feature = "zstd")]
            Compression::Zstd,
        ];

        let mut decompressed = None;
        let mut actual_compressed_size = 0;

        for &compression in &compression_types {
            // Try different compressed sizes - scan forward to find the right size
            for try_len in 1..=remaining_body.len() {
                let try_compressed = &remaining_body[..try_len];
                if let Ok(result) = decompress(try_compressed, compression) {
                    if result.len() == uncompressed_len {
                        eprintln!(
                            "DEBUG: Successfully decompressed buffer {} with {:?}, size {} -> {}",
                            i,
                            compression,
                            try_len,
                            result.len()
                        );
                        decompressed = Some(result);
                        actual_compressed_size = try_len;
                        break;
                    }
                }
            }

            if decompressed.is_some() {
                break;
            }
        }

        match decompressed {
            Some(data) => {
                decompressed_body.extend_from_slice(&data);
                // Move read offset past the header + actual compressed data + padding
                let total_size = 8 + actual_compressed_size;
                // Add padding to align to 64-byte boundary (Arrow IPC alignment)
                let aligned_size = ((total_size + 63) / 64) * 64;
                read_offset += aligned_size;
                eprintln!(
                    "DEBUG: Buffer {} processed, advancing read_offset by {} to {}",
                    i, aligned_size, read_offset
                );
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Could not decompress buffer {}", i),
                ));
            }
        }
    }

    eprintln!(
        "DEBUG: Decompressed body size: {} bytes",
        decompressed_body.len()
    );
    eprintln!("DEBUG: Buffer offsets count: {}", buffer_offsets.len());
    Ok((decompressed_body, buffer_offsets))
}

/// Heuristic to detect if a body contains compressed buffers
/// Looks for the compression header pattern: 8-byte uncompressed length + compressed data
#[cfg(any(feature = "zstd", feature = "snappy"))]
pub(crate) fn is_body_compressed(buffers: &Vector<Buffer>, body: &[u8]) -> bool {
    // Check a few buffers with non-zero length for compression headers
    for i in 0..buffers.len().min(3) {
        let buf = buffers.get(i);
        let buf_offset = buf.offset() as usize;
        let buf_length = buf.length() as usize;

        // Skip zero-length buffers
        if buf_length == 0 {
            continue;
        }

        // Check if we have enough bytes for the header
        if buf_offset + 8 <= body.len() {
            let header_bytes = &body[buf_offset..buf_offset + 8];
            if let Ok(header_bytes_arr) = header_bytes.try_into() {
                let uncompressed_len = u64::from_le_bytes(header_bytes_arr) as usize;

                // If the header's uncompressed length matches the FlatBuffers metadata length,
                // and it's a reasonable size, this is likely compressed
                if uncompressed_len == buf_length
                    && uncompressed_len > 0
                    && uncompressed_len < 100_000_000
                {
                    eprintln!(
                        "DEBUG: Heuristic found compression header at buffer {}: uncompressed_len={}",
                        i, uncompressed_len
                    );
                    return true;
                }
            }
        }
    }

    false
}
