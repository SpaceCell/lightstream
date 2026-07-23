// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Parquet table reader - *reads into `minarrow::Table`*
//!
//! ## Features
//! - Supports DataPageV2 plus the legacy V1 Parquet layout
//! - Decodes hybrid RLE/bit-packed definition levels and dictionary indices
//! - Handles `PLAIN` and `RLE_DICTIONARY` value encodings
//! - Optional feature-gated Snappy / Zstd compression
//! - Type maps to Arrow/Minarrow - {i32, i64, u32, u64, f32, f64, bool, utf8
//!   dictionary<u32/u64, and date32/date64 via `datetime` feature}
//! - No nested type support
//! - Works with any `Read + Seek`
//! - Reads into memory - no mmap zero-copy like IPC at the present time.
//!
//! ## Outputs
//! On success returns a fully materialised `Table`; otherwise yields an `IOError`
//! for malformed footers/headers, unsupported encodings, or truncated pages.

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::compression::{Compression, decompress};
use crate::constants::PARQUET_MAGIC;
use crate::error::IoError;
#[cfg(feature = "datetime")]
use crate::models::decoders::parquet::{decode_datetime32_plain, decode_datetime64_plain};
use crate::models::decoders::parquet::{
    decode_dictionary_indices_rle, decode_float32_plain, decode_float64_plain, decode_int32_plain,
    decode_int64_plain, decode_string_plain, decode_uint32_as_int32_plain,
    decode_uint64_as_int64_plain,
};
use crate::models::encoders::parquet::metadata::{
    ColumnChunkMeta, ColumnMetadata, DataPageHeader, DataPageHeaderV2, DictionaryPageHeader,
    FileMetaData, PageHeader, PageType, RowGroupMeta, SchemaElement, Statistics,
};
use crate::models::types::parquet::{
    ParquetEncoding, ParquetLogicalType, ParquetPhysicalType, parquet_to_arrow_type,
};
use minarrow::ffi::arrow_dtype::CategoricalIndexType;
use minarrow::{
    Array, ArrowType, Bitmask, BooleanArray, CategoricalArray, Field, FieldArray, FloatArray,
    IntegerArray, NumericArray, StringArray, Table, TextArray, Vec64, vec64,
};
#[cfg(feature = "datetime")]
use minarrow::{DatetimeArray, TemporalArray};

/// Read an entire in-memory Table from a Parquet v2 file.
///
// TODO: Serial read throughput on the chunked bench is ~390 MiB/s
// while the parallel path scales to ~1.2 GiB/s, so the serial decode
// is the bottleneck. Candidates worth profiling: the hybrid-RLE
// decode loop in `decode_hybrid`, per-page Read-based allocations in
// `read_data_page_v1`/`read_data_page_v2`, and the page-header
// thrift decode.
pub fn load_parquet_table<R: Read + Seek>(r: R) -> Result<Table, IoError> {
    read_parquet_impl(r, None)
}

/// Index type assumed for dictionary-encoded columns whose original
/// Arrow type was lost in the Parquet schema.
///
/// The writer maps every `ArrowType::Dictionary(_)` to physical Int32
/// with `NoneType` logical type, so the schema doesn't carry the index
/// width. Reads pick whichever width is the build's default categorical
/// type. Round-tripping a column written with `default_categorical_8`
/// disabled into a build that has it enabled (or vice versa) is not
/// supported.
#[inline]
fn default_categorical_index_type() -> CategoricalIndexType {
    #[cfg(feature = "default_categorical_8")]
    {
        CategoricalIndexType::UInt8
    }
    #[cfg(not(feature = "default_categorical_8"))]
    {
        CategoricalIndexType::UInt32
    }
}

/// Read only the named columns from a Parquet v2 file.
///
/// Column names must match the schema's `path_in_schema` entries. Returns
/// an error if any name is not found. The returned Table contains only the
/// projected columns, in schema order.
///
/// Because Parquet stores each column at a separate file offset, skipped
/// columns are never read from disk at all.
pub fn load_parquet_table_cols<R: Read + Seek>(
    r: R,
    columns: &[&str],
) -> Result<Table, IoError> {
    let projection: std::collections::HashSet<String> =
        columns.iter().map(|s| s.to_string()).collect();
    read_parquet_impl(r, Some(projection))
}

/// Shared implementation for full and projected Parquet reads.
/// When `projection` is `Some`, only columns whose names appear in the set
/// are read. Skipped columns never hit disk.
fn read_parquet_impl<R: Read + Seek>(
    mut r: R,
    projection: Option<std::collections::HashSet<String>>,
) -> Result<Table, IoError> {
    // read the 8-byte footer
    r.seek(SeekFrom::End(-8))?;
    let mut tail = [0u8; 8];
    r.read_exact(&mut tail)?;
    if &tail[4..] != PARQUET_MAGIC {
        return Err(IoError::Format("missing PAR1 footer".into()));
    }
    let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;

    // pull in the FileMetaData block
    r.seek(SeekFrom::End(-8 - footer_len as i64))?;
    let mut footer = vec![0u8; footer_len as usize];
    r.read_exact(&mut footer)?;
    let mut cur = std::io::Cursor::new(&footer);
    let meta = parse_file_metadata(&mut cur)?;

    // Validate projection names against schema before reading any data
    if let Some(ref proj) = projection {
        for name in proj {
            if !meta.schema.iter().any(|se| se.name == *name) {
                return Err(IoError::Format(format!(
                    "column '{}' not found in schema",
                    name
                )));
            }
        }
    }

    // map Parquet schema -> Arrow types. The schema list opens with a root
    // group element carrying no physical type; leaf column elements follow.
    // Map only the leaves, in schema order, so indices line up with the row
    // group's column chunks.
    let arrow_types: Vec<_> = meta
        .schema
        .iter()
        .filter_map(|se| se.type_.map(|ty| (ty, se.converted_type)))
        .map(|(ty, converted)| {
            parquet_to_arrow_type(ty, ParquetLogicalType::from_converted_type(converted))
        })
        .collect::<Result<_, _>>()?;

    // single row-group, flat schema only
    let rg = &meta.row_groups[0];
    let mut columns = Vec::with_capacity(rg.columns.len());

    for (col_idx, chunk) in rg.columns.iter().enumerate() {
        let cmeta = &chunk.meta_data;
        let col_name = &cmeta.path_in_schema[0];

        // Skip columns not in the projection
        if let Some(ref proj) = projection
            && !proj.contains(col_name)
        {
            continue;
        }

        // The Parquet schema has no slot to record that a column was
        // originally a Dictionary - the writer maps Dictionary(_) to a
        // physical Int32 with no logical type. Recover the Arrow shape
        // from the column metadata: a column carrying a dictionary page
        // is decoded as Dictionary using the build's default index
        // width (UInt8 when `default_categorical_8` is on, UInt32
        // otherwise). Round-trip across feature flags isn't supported.
        let schema_ty = &arrow_types[col_idx];
        let dictionary_ty = if cmeta.dictionary_page_offset.is_some() {
            Some(ArrowType::Dictionary(default_categorical_index_type()))
        } else {
            None
        };
        let ty: &ArrowType = dictionary_ty.as_ref().unwrap_or(schema_ty);

        // read the DICTIONARY_PAGE if present
        let dict = if let Some(dict_off) = cmeta.dictionary_page_offset {
            r.seek(SeekFrom::Start(dict_off as u64))?;
            let ph = parse_page_header(&mut r)?;
            if ph.type_ != PageType::DictionaryPage {
                return Err(IoError::Format("expected DICTIONARY_PAGE".into()));
            }
            let mut compr = vec![0u8; ph.compressed_page_size as usize];
            r.read_exact(&mut compr)?;
            match map_codec(cmeta.codec) {
                Some(c) => parse_dictionary_values(&decompress(&compr, c)?)?,
                None => parse_dictionary_values(&compr)?,
            }
        } else {
            Vec::new()
        };

        // walk all the DATA_PAGE_V2s in a row
        let total_vals = cmeta.num_values as usize;
        let mut def_levels = Vec::with_capacity(total_vals);
        let mut values_buf = Vec::new();
        let mut pages_read = 0;
        let mut page_encoding = ParquetEncoding::Plain;

        // seek once to the first data page
        r.seek(SeekFrom::Start(cmeta.data_page_offset as u64))?;

        while pages_read < total_vals {
            // parse the next page header
            let ph = parse_page_header(&mut r)?;
            let (page_defs, enc, page_vals) = match ph.type_ {
                PageType::DataPageV2 => read_data_page_v2(&mut r, &ph, cmeta)?,
                PageType::DataPage => read_data_page_v1(&mut r, &ph, cmeta)?,
                t => return Err(IoError::Format(format!("unsupported page type {:?}", t))),
            };
            if pages_read == 0 {
                page_encoding = enc;
            }

            // accumulate `page_defs.len()` logical rows
            let this_count = page_defs.len().min(total_vals - pages_read);
            def_levels.extend_from_slice(&page_defs[..this_count]);
            values_buf.extend_from_slice(&page_vals);
            pages_read += this_count;

            // cursor is at the start of the next page header
        }

        // decode the column array
        let array = decode_column(
            ty,
            page_encoding,
            &dict,
            &values_buf,
            total_vals,
            def_levels.clone(),
        )?;

        columns.push(FieldArray {
            field: Field {
                name: col_name.clone(),
                dtype: ty.clone(),
                nullable: chunk.meta_data.definition_level >= 1 || def_levels.iter().any(|&b| !b),
                metadata: Default::default(),
            }
            .into(),
            array,
            null_count: def_levels.iter().filter(|&&b| !b).count(),
        });
    }

    Ok(Table {
        cols: columns,
        n_rows: meta.num_rows as usize,
        name: String::new(),
        ..Default::default()
    })
}

/// DataPageV2 reader: read exactly `compressed_page_size` bytes, split into
/// rep / def, decompress the remainder, decode def‐levels, return the raw
/// values and the page encoding.
fn read_data_page_v2<R: Read>(
    r: &mut R,
    ph: &PageHeader,
    cmeta: &ColumnMetadata,
) -> Result<(Vec<bool>, ParquetEncoding, Vec<u8>), IoError> {
    let h = ph
        .data_page_header_v2
        .as_ref()
        .ok_or_else(|| IoError::Format("missing DataPageHeaderV2".into()))?;

    // 1) consume repetition‐levels bytes
    let mut rep = vec![0u8; h.repetition_levels_byte_length as usize];
    r.read_exact(&mut rep)?;

    // 2) consume definition‐levels bytes
    let mut def = vec![0u8; h.definition_levels_byte_length as usize];
    r.read_exact(&mut def)?;

    // 3) the remainder of this page is the `compressed_page_size - R - D`
    let body_len = (ph.compressed_page_size as usize)
        .checked_sub(rep.len() + def.len())
        .ok_or_else(|| IoError::Format("bad compressed_page_size".into()))?;
    let mut vs = vec![0u8; body_len];
    r.read_exact(&mut vs)?;

    // 4) decompress if needed
    let values_raw = match (h.is_compressed, map_codec(cmeta.codec)) {
        (true, Some(c)) => decompress(&vs, c)?,
        _ => vs,
    };

    // 5) decode definition‐levels (we ignore repetition entirely)
    let def_levels = if cmeta.definition_level == 0 && def.is_empty() {
        vec![true; h.num_rows as usize]
    } else {
        decode_hybrid(&def, 1, h.num_rows as usize)?
            .into_iter()
            .map(|v| v != 0)
            .collect()
    };

    Ok((def_levels, h.encoding, values_raw))
}

/// DataPageV1 reader
fn read_data_page_v1<R: Read>(
    r: &mut R,
    _ph: &PageHeader,
    cmeta: &ColumnMetadata,
) -> Result<(Vec<bool>, ParquetEncoding, Vec<u8>), IoError> {
    // read the 4-byte prefix of the def‐levels stream
    let def = read_len_prefixed(r)?;
    // read  the remaining compressed values for this page,
    // which in V1 is “to the end of this page” - as V1 only writes one page
    let mut vs = Vec::new();
    r.read_to_end(&mut vs)?;

    let num_vals = (cmeta.num_values as usize).max(def_levels_count(&def, 1));
    let def_levels = if cmeta.definition_level == 0 && def.is_empty() {
        vec![true; num_vals]
    } else {
        decode_hybrid(&def, 1, num_vals)?
            .into_iter()
            .map(|v| v != 0)
            .collect()
    };
    Ok((def_levels, ParquetEncoding::Plain, vs))
}

// Column-value decoder

fn decode_column(
    ty: &ArrowType,
    enc: ParquetEncoding,
    dict: &[Vec<u8>],
    buf: &[u8],
    len: usize,
    def_levels: Vec<bool>,
) -> Result<Array, IoError> {
    let mask = Some(Bitmask::from_bools(&def_levels));

    Ok(match ty {
        // numerics
        ArrowType::Int32 if enc == ParquetEncoding::Plain => {
            Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray::from_vec64(
                decode_int32_plain(buf)?,
                mask,
            ))))
        }
        ArrowType::UInt32 if enc == ParquetEncoding::Plain => {
            Array::NumericArray(NumericArray::UInt32(Arc::new(IntegerArray::from_vec64(
                decode_uint32_as_int32_plain(buf)?,
                mask,
            ))))
        }
        ArrowType::Int64 if enc == ParquetEncoding::Plain => {
            Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray::from_vec64(
                decode_int64_plain(buf)?,
                mask,
            ))))
        }
        ArrowType::UInt64 if enc == ParquetEncoding::Plain => {
            Array::NumericArray(NumericArray::UInt64(Arc::new(IntegerArray::from_vec64(
                decode_uint64_as_int64_plain(buf)?,
                mask,
            ))))
        }
        ArrowType::Float32 if enc == ParquetEncoding::Plain => {
            Array::NumericArray(NumericArray::Float32(Arc::new(FloatArray::from_vec64(
                decode_float32_plain(buf)?,
                mask,
            ))))
        }
        ArrowType::Float64 if enc == ParquetEncoding::Plain => {
            Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray::from_vec64(
                decode_float64_plain(buf)?,
                mask,
            ))))
        }

        // booleans
        ArrowType::Boolean if enc == ParquetEncoding::Plain => {
            Array::BooleanArray(Arc::new(BooleanArray::new(Bitmask::from_bytes(buf, len), mask)))
        }

        // strings
        ArrowType::String if enc == ParquetEncoding::Plain => {
            let (offsets, data) = decode_string_plain(buf, len)?;
            Array::TextArray(TextArray::String32(Arc::new(StringArray {
                offsets: offsets.into(),
                data: data.into(),
                null_mask: mask,
            })))
        }
        #[cfg(feature = "large_string")]
        ArrowType::LargeString if enc == ParquetEncoding::Plain => {
            use crate::models::decoders::parquet::decode_large_string_plain;

            let (offsets, data) = decode_large_string_plain(buf, len)?;
            Array::TextArray(TextArray::String64(Arc::new(StringArray {
                offsets: offsets.into(),
                data: data.into(),
                null_mask: mask,
            })))
        }

        // dictionary / categoricals
        ArrowType::Dictionary(key_ty) => {
            match (key_ty, enc) {
                // u32 keys
                #[cfg(any(
                    not(feature = "default_categorical_8"),
                    feature = "extended_categorical"
                ))]
                (CategoricalIndexType::UInt32, ParquetEncoding::RleDictionary) => {
                    let idx = decode_dictionary_indices_rle(buf, len)?;
                    build_cat32(idx, dict, mask)
                }
                #[cfg(any(
                    not(feature = "default_categorical_8"),
                    feature = "extended_categorical"
                ))]
                (CategoricalIndexType::UInt32, ParquetEncoding::Plain) => {
                    let idx = decode_uint32_as_int32_plain(buf)?;
                    build_cat32(idx, dict, mask)
                }

                // u8 keys (default when default_categorical_8 is enabled)
                #[cfg(feature = "default_categorical_8")]
                (CategoricalIndexType::UInt8, ParquetEncoding::RleDictionary) => {
                    let idx = decode_dictionary_indices_rle(buf, len)?;
                    build_cat8(idx, dict, mask)
                }
                #[cfg(feature = "default_categorical_8")]
                (CategoricalIndexType::UInt8, ParquetEncoding::Plain) => {
                    let idx = decode_uint32_as_int32_plain(buf)?;
                    build_cat8(idx, dict, mask)
                }

                // optional u64 keys
                #[cfg(all(feature = "extended_categorical", feature = "large_string"))]
                (CategoricalIndexType::UInt64, ParquetEncoding::RleDictionary) => {
                    let idx = decode_dictionary_indices_rle(buf, len)?;
                    let idx = idx.into_iter().map(|v| v as u64).collect();
                    build_cat64(idx, dict, mask)
                }
                #[cfg(all(feature = "extended_categorical", feature = "large_string"))]
                (CategoricalIndexType::UInt64, ParquetEncoding::Plain) => {
                    let idx = decode_uint64_as_int64_plain(buf)?;
                    build_cat64(idx, dict, mask)
                }

                _ => {
                    return Err(IoError::UnsupportedEncoding(format!(
                        "{:?} + {:?}",
                        key_ty, enc
                    )));
                }
            }
        }

        // temporal
        #[cfg(feature = "datetime")]
        ArrowType::Date32 if enc == ParquetEncoding::Plain => {
            Array::TemporalArray(TemporalArray::Datetime32(Arc::new(DatetimeArray {
                data: decode_datetime32_plain(buf)?.into(),
                null_mask: mask,
                time_unit: Default::default(),
            })))
        }
        #[cfg(feature = "datetime")]
        ArrowType::Date64 if enc == ParquetEncoding::Plain => {
            Array::TemporalArray(TemporalArray::Datetime64(Arc::new(DatetimeArray {
                data: decode_datetime64_plain(buf)?.into(),
                null_mask: mask,
                time_unit: Default::default(),
            })))
        }

        _ => {
            return Err(IoError::UnsupportedType(format!(
                "decode {:?} / {:?}",
                ty, enc
            )));
        }
    })
}

// categorical builders

#[cfg(any(
    not(feature = "default_categorical_8"),
    feature = "extended_categorical"
))]
fn build_cat32(idx: Vec64<u32>, dict_raw: &[Vec<u8>], mask: Option<Bitmask>) -> Array {
    let dict = dict_raw
        .iter()
        .map(|b| String::from_utf8(b.clone()).unwrap())
        .collect::<Vec64<_>>()
        .into();
    Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
        data: idx.into(),
        unique_values: dict,
        null_mask: mask,
    })))
}

#[cfg(feature = "default_categorical_8")]
fn build_cat8(idx: Vec64<u32>, dict_raw: &[Vec<u8>], mask: Option<Bitmask>) -> Array {
    let dict = dict_raw
        .iter()
        .map(|b| String::from_utf8(b.clone()).unwrap())
        .collect::<Vec64<_>>();
    let idx8: Vec64<u8> = idx.iter().map(|&v| v as u8).collect();
    Array::TextArray(TextArray::Categorical8(Arc::new(CategoricalArray {
        data: idx8.into(),
        unique_values: dict,
        null_mask: mask,
    })))
}

#[cfg(all(feature = "extended_categorical", feature = "large_string"))]
fn build_cat64(idx: Vec64<u64>, dict_raw: &[Vec<u8>], mask: Option<Bitmask>) -> Array {
    let dict = dict_raw
        .iter()
        .map(|b| String::from_utf8(b.clone()).unwrap())
        .collect::<Vec64<_>>()
        .into();
    Array::TextArray(TextArray::Categorical64(Arc::new(CategoricalArray {
        data: idx.into(),
        unique_values: dict,
        null_mask: mask,
    })))
}

// RLE/bit-packed Hybrid decoder

fn decode_hybrid(buf: &[u8], bit_width: u8, n: usize) -> Result<Vec64<u32>, IoError> {
    if bit_width == 0 {
        return Ok(vec64![0; n]);
    }
    let mut out = Vec64::with_capacity(n);
    let mut pos = 0usize;
    while out.len() < n {
        let (header, used) = read_uleb128(&buf[pos..])?;
        pos += used;
        if header & 1 == 0 {
            // RLE
            let run_len = (header >> 1) as usize;
            let bytes_per_value = bit_width.div_ceil(8) as usize;
            if pos + bytes_per_value > buf.len() {
                return Err(IoError::Format("truncated RLE run".into()));
            }
            let mut v_bytes = [0u8; 4];
            v_bytes[..bytes_per_value].copy_from_slice(&buf[pos..pos + bytes_per_value]);
            let v = u32::from_le_bytes(v_bytes);
            pos += bytes_per_value;
            let take = run_len.min(n - out.len());
            out.extend(std::iter::repeat_n(v, take));
        } else {
            // bit-packed
            let groups = (header >> 1) as usize; // 1 group = 8 values
            let total_values = groups * 8;
            let total_bits = total_values * (bit_width as usize);
            let total_bytes = total_bits.div_ceil(8);
            if pos + total_bytes > buf.len() {
                return Err(IoError::Format("truncated bit-packed run".into()));
            }
            let slice = &buf[pos..pos + total_bytes];
            // Unpack the run LSB-first with values contiguous inside each
            // 8-value group, per the Parquet hybrid RLE/bit-packing layout.
            let mut scratch = vec![0u32; total_values];
            for (idx, slot) in scratch.iter_mut().enumerate() {
                let bit_base = idx * bit_width as usize;
                for k in 0..bit_width as usize {
                    let bit = bit_base + k;
                    if (slice[bit / 8] >> (bit % 8)) & 1 != 0 {
                        *slot |= 1 << k;
                    }
                }
            }
            // push only as many as we actually need, dropping the padding
            let needed = n - out.len();
            out.extend(scratch.into_iter().take(needed));
            pos += total_bytes;
        }
    }
    Ok(out)
}

fn read_uleb128(buf: &[u8]) -> Result<(u64, usize), IoError> {
    let mut val = 0u64;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        val |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok((val, i + 1));
        }
        shift += 7;
        if shift > 63 {
            break;
        }
    }
    Err(IoError::Format("ULEB128 overflow/truncate".into()))
}

// utility for legacy V1 count heuristic
fn def_levels_count(buf: &[u8], bw: u8) -> usize {
    if buf.is_empty() {
        0
    } else if buf[0] & 1 == 0 {
        ((buf[0] as usize) >> 1).min(1 << bw)
    } else {
        0
    }
}

// Misc helpers

fn map_codec(id: i32) -> Option<Compression> {
    match id {
        0 => None,
        #[cfg(feature = "snappy")]
        1 => Some(Compression::Snappy),
        #[cfg(feature = "zstd")]
        6 => Some(Compression::Zstd), // spec: ZSTD = 6
        _ => None,
    }
}

fn read_len_prefixed<R: Read>(r: &mut R) -> Result<Vec<u8>, IoError> {
    let mut l4 = [0u8; 4];
    r.read_exact(&mut l4)?;
    let len = u32::from_le_bytes(l4) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn parse_dictionary_values(buf: &[u8]) -> Result<Vec<Vec<u8>>, IoError> {
    let mut c = std::io::Cursor::new(buf);
    let mut out = Vec::new();
    while (c.position() as usize) < buf.len() {
        let mut l4 = [0u8; 4];
        c.read_exact(&mut l4)?;
        let len = u32::from_le_bytes(l4) as usize;
        let mut s = vec![0u8; len];
        c.read_exact(&mut s)?;
        out.push(s);
    }
    Ok(out)
}

// Thrift Parsers

fn parse_file_metadata<R: Read>(r: &mut R) -> Result<FileMetaData, IoError> {
    let mut last = 0i16;
    let mut version = None;
    let mut schema = Vec::new();
    let mut num_rows = None;
    let mut row_groups = Vec::new();
    let mut kv_meta = None;
    let mut created_by = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r, &mut last)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => version = Some(thrift_read_i32(r)?),
            2 => {
                let (_elem_tpe, len) = thrift_read_list_begin(r)?;
                for _ in 0..len {
                    schema.push(parse_schema_element(r)?);
                }
            }
            3 => num_rows = Some(thrift_read_i64(r)?),
            4 => {
                let (_elem_tpe, len) = thrift_read_list_begin(r)?;
                for _ in 0..len {
                    row_groups.push(parse_row_group(r)?);
                }
            }
            5 => {
                // key_value_metadata is a list<KeyValue>, where each KeyValue
                // is a struct with 1=key and 2=value (value optional).
                let (_elem, len) = thrift_read_list_begin(r)?;
                let mut map = BTreeMap::new();
                for _ in 0..len {
                    let mut kv_last = 0i16;
                    let mut key = String::new();
                    let mut value = String::new();
                    loop {
                        let (t, fid) = thrift_read_field_begin(r, &mut kv_last)?;
                        if t == 0 {
                            break;
                        }
                        match fid {
                            1 => key = thrift_read_string(r)?,
                            2 => value = thrift_read_string(r)?,
                            _ => thrift_skip_field(r, t)?,
                        }
                    }
                    map.insert(key, value);
                }
                kv_meta = Some(map);
            }
            6 => created_by = Some(thrift_read_string(r)?),
            _ => thrift_skip_field(r, tpe)?,
        }
    }

    Ok(FileMetaData {
        version: version.ok_or_else(|| IoError::Format("Missing version".into()))?,
        schema,
        num_rows: num_rows.ok_or_else(|| IoError::Format("Missing num_rows".into()))?,
        row_groups,
        key_value_metadata: kv_meta,
        created_by,
    })
}

fn parse_schema_element<R: Read>(r: &mut R) -> Result<SchemaElement, IoError> {
    let mut last = 0i16;
    let mut name = None;
    let mut repetition_type = None;
    let mut type_ = None;
    let mut converted_type = None;
    let mut type_length = None;
    let mut precision = None;
    let mut scale = None;
    let mut field_id = None;
    let mut num_children = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r, &mut last)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => {
                type_ = Some(
                    ParquetPhysicalType::from_i32(thrift_read_i32(r)?)
                        .ok_or_else(|| IoError::Format("Invalid type_".into()))?,
                )
            }
            2 => type_length = Some(thrift_read_i32(r)?),
            3 => repetition_type = Some(thrift_read_i32(r)?),
            4 => name = Some(thrift_read_string(r)?),
            5 => num_children = Some(thrift_read_i32(r)?),
            6 => converted_type = Some(thrift_read_i32(r)?),
            7 => scale = Some(thrift_read_i32(r)?),
            8 => precision = Some(thrift_read_i32(r)?),
            9 => field_id = Some(thrift_read_i32(r)?),
            _ => thrift_skip_field(r, tpe)?,
        }
    }

    Ok(SchemaElement {
        name: name.ok_or_else(|| IoError::Format("SchemaElement missing name".into()))?,
        repetition_type: repetition_type.unwrap_or(0),
        type_,
        converted_type,
        type_length,
        precision,
        scale,
        field_id,
        num_children,
    })
}

fn parse_row_group<R: Read>(r: &mut R) -> Result<RowGroupMeta, IoError> {
    let mut last = 0i16;
    let mut columns = Vec::new();
    let mut total_byte_size = None;
    let mut num_rows = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r, &mut last)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => {
                let (_elem_tpe, len) = thrift_read_list_begin(r)?;
                for _ in 0..len {
                    columns.push(parse_column_chunk(r)?);
                }
            }
            2 => total_byte_size = Some(thrift_read_i64(r)?),
            3 => num_rows = Some(thrift_read_i64(r)?),
            _ => thrift_skip_field(r, tpe)?,
        }
    }
    Ok(RowGroupMeta {
        columns,
        total_byte_size: total_byte_size.unwrap_or(0),
        num_rows: num_rows.unwrap_or(0),
    })
}

fn parse_column_chunk<R: Read>(r: &mut R) -> Result<ColumnChunkMeta, IoError> {
    let mut last = 0i16;
    let mut file_offset = None;
    let mut meta_data = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r, &mut last)?;
        if tpe == 0 {
            break;
        }
        match id {
            2 => file_offset = Some(thrift_read_i64(r)?),
            3 => meta_data = Some(parse_column_meta_data(r)?),
            _ => thrift_skip_field(r, tpe)?,
        }
    }
    Ok(ColumnChunkMeta {
        file_offset: file_offset.unwrap_or(0),
        meta_data: meta_data.ok_or_else(|| IoError::Format("Missing ColumnMetaData".into()))?,
    })
}

fn parse_column_meta_data<R: Read>(r: &mut R) -> Result<ColumnMetadata, IoError> {
    let mut type_ = None;
    let mut encodings = Vec::new();
    let mut path_in_schema = Vec::new();
    let mut codec = None;
    let mut num_values = None;
    let mut total_uncompressed_size = None;
    let mut total_compressed_size = None;
    let mut data_page_offset = None;
    let mut dictionary_page_offset = None;
    let mut statistics = None;
    let mut last = 0i16;

    loop {
        let (tpe, id) = thrift_read_field_begin(r, &mut last)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => {
                let v = thrift_read_i32(r)?;
                type_ = Some(
                    ParquetPhysicalType::from_i32(v)
                        .ok_or_else(|| IoError::Format("Invalid physical type".into()))?,
                );
            }
            2 => {
                let (_elem_tpe, len) = thrift_read_list_begin(r)?;
                for _ in 0..len {
                    let v = thrift_read_i32(r)?;
                    encodings.push(
                        ParquetEncoding::from_i32(v)
                            .ok_or_else(|| IoError::Format("Invalid encoding".into()))?,
                    );
                }
            }
            3 => {
                let (_elem_tpe, len) = thrift_read_list_begin(r)?;
                for _ in 0..len {
                    path_in_schema.push(thrift_read_string(r)?);
                }
            }
            4 => codec = Some(thrift_read_i32(r)?),
            5 => num_values = Some(thrift_read_i64(r)?),
            6 => total_uncompressed_size = Some(thrift_read_i64(r)?),
            7 => total_compressed_size = Some(thrift_read_i64(r)?),
            9 => data_page_offset = Some(thrift_read_i64(r)?),
            11 => dictionary_page_offset = Some(thrift_read_i64(r)?),
            12 => statistics = Some(parse_statistics(r)?),
            _ => thrift_skip_field(r, tpe)?,
        }
    }

    Ok(ColumnMetadata {
        type_: type_.unwrap(),
        encodings,
        path_in_schema,
        codec: codec.unwrap_or(0),
        num_values: num_values.unwrap_or(0),
        total_uncompressed_size: total_uncompressed_size.unwrap_or(0),
        total_compressed_size: total_compressed_size.unwrap_or(0),
        data_page_offset: data_page_offset.unwrap_or(0),
        dictionary_page_offset,
        statistics,
        definition_level: 0,
    })
}

fn parse_statistics<R: Read>(r: &mut R) -> Result<Statistics, IoError> {
    let mut last = 0i16;
    let mut null_count = None;
    let mut distinct_count = None;
    let mut min = None;
    let mut max = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r, &mut last)?;
        if tpe == 0 {
            break;
        }
        // Statistics carries both the legacy signed min/max (fields 1/2) and
        // the modern unsigned-order max_value/min_value (fields 5/6). Prefer
        // whichever the writer emitted; the modern fields win when present.
        match id {
            1 => max = Some(thrift_read_bytes(r)?),
            2 => min = Some(thrift_read_bytes(r)?),
            3 => null_count = Some(thrift_read_i64(r)?),
            4 => distinct_count = Some(thrift_read_i64(r)?),
            5 => max = Some(thrift_read_bytes(r)?),
            6 => min = Some(thrift_read_bytes(r)?),
            _ => thrift_skip_field(r, tpe)?,
        }
    }
    Ok(Statistics {
        null_count,
        distinct_count,
        min,
        max,
    })
}

fn parse_page_header<R: Read + Seek>(r: &mut R) -> Result<PageHeader, IoError> {
    let mut last = 0i16;
    let mut ptype = None;
    let mut uncomp = None;
    let mut compr = None;
    let mut data_ph = None;
    let mut data_ph_v2 = None;
    let mut dict_ph = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r, &mut last)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => {
                ptype = Some(
                    PageType::from_i32(thrift_read_i32(r)?)
                        .ok_or_else(|| IoError::Format("Invalid PageType".into()))?,
                )
            }
            2 => uncomp = Some(thrift_read_i32(r)?),
            3 => compr = Some(thrift_read_i32(r)?),
            5 => {
                // DataPageHeader (v1), a nested compact struct at field id 5.
                data_ph = Some(parse_data_page_header(r)?);
            }
            7 => {
                // DictionaryPageHeader, a nested compact struct at field id 7.
                // Read its fields with a struct-local last-field-id starting
                // at zero, stopping on the field-stop byte and skipping any
                // unrecognised field.
                let mut inner = 0i16;
                let mut num_values = 0i32;
                let mut encoding = ParquetEncoding::Plain;
                let mut is_sorted = None;
                loop {
                    let (tpe2, id2) = thrift_read_field_begin(r, &mut inner)?;
                    if tpe2 == 0 {
                        break;
                    }
                    match id2 {
                        1 => num_values = thrift_read_i32(r)?,
                        2 => {
                            encoding =
                                ParquetEncoding::from_i32(thrift_read_i32(r)?).ok_or_else(|| {
                                    IoError::Format(
                                        "Invalid encoding in DictionaryPageHeader".into(),
                                    )
                                })?
                        }
                        3 => is_sorted = Some(tpe2 == TC_BOOL_TRUE),
                        _ => thrift_skip_field(r, tpe2)?,
                    }
                }
                dict_ph = Some(DictionaryPageHeader {
                    num_values,
                    encoding,
                    is_sorted,
                });
            }
            8 => {
                // DataPageHeaderV2, a nested compact struct at field id 8.
                let mut inner = 0i16;
                let mut num_rows = None;
                let mut num_nulls = None;
                let mut num_values = None;
                let mut encoding = None;
                let mut def_len = None;
                let mut rep_len = None;
                let mut is_compressed = None;
                let mut statistics = None;

                loop {
                    let (tpe2, id2) = thrift_read_field_begin(r, &mut inner)?;
                    if tpe2 == 0 {
                        break;
                    }
                    match id2 {
                        1 => num_values = Some(thrift_read_i32(r)?),
                        2 => num_nulls = Some(thrift_read_i32(r)?),
                        3 => num_rows = Some(thrift_read_i32(r)?),
                        4 => {
                            encoding =
                                Some(ParquetEncoding::from_i32(thrift_read_i32(r)?).ok_or_else(
                                    || {
                                        IoError::Format(
                                            "Invalid encoding in DataPageHeaderV2".into(),
                                        )
                                    },
                                )?)
                        }
                        5 => def_len = Some(thrift_read_i32(r)?),
                        6 => rep_len = Some(thrift_read_i32(r)?),
                        7 => is_compressed = Some(tpe2 == TC_BOOL_TRUE),
                        8 => statistics = Some(parse_statistics(r)?),
                        _ => thrift_skip_field(r, tpe2)?,
                    }
                }

                data_ph_v2 = Some(DataPageHeaderV2 {
                    num_rows: num_rows.unwrap_or(0),
                    num_nulls: num_nulls.unwrap_or(0),
                    num_values: num_values.unwrap_or(0),
                    encoding: encoding.ok_or_else(|| {
                        IoError::Format("Missing encoding in DataPageHeaderV2".into())
                    })?,
                    definition_levels_byte_length: def_len.unwrap_or(0),
                    repetition_levels_byte_length: rep_len.unwrap_or(0),
                    is_compressed: is_compressed.unwrap_or(false),
                    statistics,
                });
            }
            _ => thrift_skip_field(r, tpe)?,
        }
    }

    Ok(PageHeader {
        type_: ptype.ok_or_else(|| IoError::Format("Missing PageType".into()))?,
        uncompressed_page_size: uncomp.unwrap_or(0),
        compressed_page_size: compr.unwrap_or(0),
        data_page_header: data_ph,
        data_page_header_v2: data_ph_v2,
        dictionary_page_header: dict_ph,
    })
}

fn parse_data_page_header<R: Read>(r: &mut R) -> Result<DataPageHeader, IoError> {
    let mut last = 0i16;
    let mut num_values = None;
    let mut encoding = None;
    let mut dlev = None;
    let mut rlev = None;
    let mut stats = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r, &mut last)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => num_values = Some(thrift_read_i32(r)?),
            2 => encoding = Some(ParquetEncoding::from_i32(thrift_read_i32(r)?).unwrap()),
            3 => dlev = Some(ParquetEncoding::from_i32(thrift_read_i32(r)?).unwrap()),
            4 => rlev = Some(ParquetEncoding::from_i32(thrift_read_i32(r)?).unwrap()),
            5 => stats = Some(parse_statistics(r)?),
            _ => thrift_skip_field(r, tpe)?,
        }
    }
    Ok(DataPageHeader {
        num_values: num_values.unwrap_or(0),
        encoding: encoding.unwrap(),
        definition_level_encoding: dlev.unwrap(),
        repetition_level_encoding: rlev.unwrap(),
        statistics: stats,
    })
}

// Low-level TCompactProtocol readers.
//
// Compact type identifiers from the Thrift spec. Bool field values ride in
// the type nibble - TC_BOOL_TRUE and TC_BOOL_FALSE encode the value directly.

const TC_BOOL_TRUE: u8 = 1;
const TC_BOOL_FALSE: u8 = 2;
const TC_BYTE: u8 = 3;
const TC_I16: u8 = 4;
const TC_I32: u8 = 5;
const TC_I64: u8 = 6;
const TC_DOUBLE: u8 = 7;
const TC_BINARY: u8 = 8;
const TC_LIST: u8 = 9;
const TC_SET: u8 = 10;
const TC_MAP: u8 = 11;
const TC_STRUCT: u8 = 12;

/// Read an unsigned LEB128 varint.
fn thrift_read_varint<R: Read>(r: &mut R) -> Result<u64, IoError> {
    let mut val = 0u64;
    let mut shift = 0u32;
    loop {
        let mut b = [0u8; 1];
        r.read_exact(&mut b)?;
        val |= ((b[0] & 0x7f) as u64) << shift;
        if b[0] & 0x80 == 0 {
            return Ok(val);
        }
        shift += 7;
        if shift > 63 {
            return Err(IoError::Format("varint overflow".into()));
        }
    }
}

/// Decode a zigzag-encoded unsigned value back to a signed integer.
fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Read a compact field header, advancing the struct's running field id.
///
/// Returns the compact type nibble and the resolved field id. A short-form
/// header packs a 1..=15 delta into the high nibble; a zero delta signals the
/// long form with a zigzag varint field id following. The field-stop byte 0
/// returns `(0, 0)`.
fn thrift_read_field_begin<R: Read>(r: &mut R, last: &mut i16) -> Result<(u8, i16), IoError> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    let byte = b[0];
    if byte == 0 {
        return Ok((0, 0));
    }
    let ctype = byte & 0x0f;
    let delta = (byte >> 4) & 0x0f;
    let id = if delta == 0 {
        zigzag_decode(thrift_read_varint(r)?) as i16
    } else {
        *last + delta as i16
    };
    *last = id;
    Ok((ctype, id))
}

/// Read a zigzag varint i32 value.
fn thrift_read_i32<R: Read>(r: &mut R) -> Result<i32, IoError> {
    Ok(zigzag_decode(thrift_read_varint(r)?) as i32)
}

/// Read a zigzag varint i64 value.
fn thrift_read_i64<R: Read>(r: &mut R) -> Result<i64, IoError> {
    Ok(zigzag_decode(thrift_read_varint(r)?))
}

/// Read a compact string - varint length followed by UTF-8 bytes.
fn thrift_read_string<R: Read>(r: &mut R) -> Result<String, IoError> {
    let buf = thrift_read_bytes(r)?;
    String::from_utf8(buf).map_err(|e| IoError::Format(format!("UTF8 error: {}", e)))
}

/// Read a compact binary value - varint length followed by the raw bytes.
fn thrift_read_bytes<R: Read>(r: &mut R) -> Result<Vec<u8>, IoError> {
    let len = thrift_read_varint(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

/// Read a compact list header, returning the element compact type and count.
///
/// One byte packs the size in the high nibble and the element type in the low
/// nibble. A size nibble of 0xF signals the long form with a varint count.
fn thrift_read_list_begin<R: Read>(r: &mut R) -> Result<(u8, usize), IoError> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    let elem_tpe = b[0] & 0x0f;
    let size_nibble = (b[0] >> 4) & 0x0f;
    let len = if size_nibble == 0x0f {
        thrift_read_varint(r)? as usize
    } else {
        size_nibble as usize
    };
    Ok((elem_tpe, len))
}

/// Read a compact map header, returning the key type, value type, and count.
///
/// The count is a leading varint. An empty map stops there with no key/value
/// type byte; otherwise a single byte packs the key type in the high nibble
/// and the value type in the low nibble.
fn thrift_read_map_begin<R: Read>(r: &mut R) -> Result<(u8, u8, usize), IoError> {
    let len = thrift_read_varint(r)? as usize;
    if len == 0 {
        return Ok((0, 0, 0));
    }
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    let kt = (b[0] >> 4) & 0x0f;
    let vt = b[0] & 0x0f;
    Ok((kt, vt, len))
}

/// Skip a field value given its compact type. Bool field values ride in the
/// type nibble and carry no payload, so bool fields consume nothing here.
fn thrift_skip_field<R: Read>(r: &mut R, ctype: u8) -> Result<(), IoError> {
    match ctype {
        TC_BOOL_TRUE | TC_BOOL_FALSE => Ok(()),
        _ => thrift_skip_value(r, ctype),
    }
}

/// Skip a single value of the given compact type, recursing through nested
/// structs, lists, sets, and maps. Bool values encountered as container
/// elements consume one byte, unlike bool struct fields.
fn thrift_skip_value<R: Read>(r: &mut R, ctype: u8) -> Result<(), IoError> {
    match ctype {
        TC_BOOL_TRUE | TC_BOOL_FALSE | TC_BYTE => {
            let mut b = [0u8; 1];
            r.read_exact(&mut b)?;
        }
        TC_I16 | TC_I32 | TC_I64 => {
            thrift_read_varint(r)?;
        }
        TC_DOUBLE => {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
        }
        TC_BINARY => {
            let _ = thrift_read_bytes(r)?;
        }
        TC_LIST | TC_SET => {
            let (et, len) = thrift_read_list_begin(r)?;
            for _ in 0..len {
                thrift_skip_value(r, et)?;
            }
        }
        TC_MAP => {
            let (kt, vt, len) = thrift_read_map_begin(r)?;
            for _ in 0..len {
                thrift_skip_value(r, kt)?;
                thrift_skip_value(r, vt)?;
            }
        }
        TC_STRUCT => {
            let mut inner = 0i16;
            loop {
                let (ft, _) = thrift_read_field_begin(r, &mut inner)?;
                if ft == 0 {
                    break;
                }
                thrift_skip_field(r, ft)?;
            }
        }
        _ => {
            return Err(IoError::Format(format!(
                "Cannot skip unknown thrift type {}",
                ctype
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::models::encoders::parquet::data::encode_dictionary_indices_rle;

    use super::*;

    /// Build a Vec<u8> string dictionary from &strs.
    fn dict(strings: &[&str]) -> Vec<Vec<u8>> {
        strings.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    #[test]
    fn hybrid_rle_run() {
        // pattern: 6× value 3, bit-width = 2
        // header = run_len << 1 (=12)  => 0x0c
        // encoded value = 3 = 0b11  => two bytes (little-endian)
        let buf = [0x0c, 0x03, 0x00];
        let out = super::decode_hybrid(&buf, 2, 6).unwrap();
        assert_eq!(out.as_slice(), &[3, 3, 3, 3, 3, 3]);
    }

    #[test]
    fn hybrid_bitpacked_single_group() {
        // eight values: [1,0,1,0,1,0,1,0]  (bit-width 1)
        // header = (groups=1)<<1 | 1   => 3
        // packed byte = 0b01010101 = 0x55
        let buf = [0x03, 0x55];
        let out = super::decode_hybrid(&buf, 1, 8).unwrap();
        assert_eq!(out.as_slice(), &[1, 0, 1, 0, 1, 0, 1, 0]);
    }

    #[test]
    fn hybrid_mixed_runs() {
        let expect = vec64![7, 7, 7, 1, 2, 3, 4, 5, 7, 7, 7, 7];
        let mut tmp = Vec::new();
        encode_dictionary_indices_rle(&expect, &mut tmp).unwrap();
        let bit_width = tmp[0];
        let buf = &tmp[1..]; // hybrid wants stream after bitWidth
        let out = super::decode_hybrid(buf, bit_width, expect.len()).unwrap();
        assert_eq!(out.as_slice(), expect.as_slice());
    }
    #[cfg(not(feature = "default_categorical_8"))]
    #[test]
    fn decode_column_categorical_rle_dictionary() {
        let dict_raw = dict(&["foo", "bar"]);
        let idx: Vec<u32> = vec![0, 1, 1, 0];
        let mut encoded = Vec::new();
        encode_dictionary_indices_rle(&idx, &mut encoded).unwrap();

        let def_levels = vec![true; idx.len()];

        let array = super::decode_column(
            &ArrowType::Dictionary(CategoricalIndexType::UInt32),
            ParquetEncoding::RleDictionary,
            &dict_raw,
            &encoded,
            idx.len(),
            def_levels,
        )
        .expect("decode_column failed");

        match array {
            Array::TextArray(TextArray::Categorical32(cat)) => {
                assert_eq!(cat.data.as_slice(), idx.as_slice());
                let uniq: Vec<_> = cat.unique_values.iter().collect();
                assert_eq!(uniq, vec!["foo", "bar"]);
            }
            _ => panic!("unexpected array variant {:?}", array),
        }
    }

    #[cfg(feature = "default_categorical_8")]
    #[test]
    fn decode_column_categorical_rle_dictionary() {
        let dict_raw = dict(&["foo", "bar"]);
        let idx: Vec<u32> = vec![0, 1, 1, 0];
        let mut encoded = Vec::new();
        encode_dictionary_indices_rle(&idx, &mut encoded).unwrap();

        let def_levels = vec![true; idx.len()];

        let array = super::decode_column(
            &ArrowType::Dictionary(CategoricalIndexType::UInt8),
            ParquetEncoding::RleDictionary,
            &dict_raw,
            &encoded,
            idx.len(),
            def_levels,
        )
        .expect("decode_column failed");

        match array {
            Array::TextArray(TextArray::Categorical8(cat)) => {
                assert_eq!(cat.data.as_slice(), &[0u8, 1, 1, 0]);
                let uniq: Vec<_> = cat.unique_values.iter().collect();
                assert_eq!(uniq, vec!["foo", "bar"]);
            }
            _ => panic!("unexpected array variant {:?}", array),
        }
    }

    #[test]
    fn decode_column_plain_int32() {
        let values = [10i32, 20, -5];
        // Encode the values as PLAIN (little-endian bytes)
        let mut buf = Vec::new();
        for v in &values {
            buf.extend_from_slice(&v.to_le_bytes());
        }

        let def_levels = vec![true; values.len()];
        let array = decode_column(
            &ArrowType::Int32,
            ParquetEncoding::Plain,
            &[],
            &buf,
            values.len(),
            def_levels.clone(),
        )
        .unwrap();

        match array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                assert_eq!(arr.data.as_slice(), &values);
                assert!(arr.null_mask.as_ref().unwrap().all_true());
            }
            _ => panic!("unexpected array {:?}", array),
        }
    }

    #[test]
    fn decode_column_boolean_plain() {
        let bits = [true, false, true, true, false, false];
        let data_mask = Bitmask::from_bools(&bits);
        let def_levels = bits.to_vec(); // no nulls
        let array = decode_column(
            &ArrowType::Boolean,
            ParquetEncoding::Plain,
            &[],
            data_mask.as_slice(),
            bits.len(),
            def_levels,
        )
        .unwrap();

        match array {
            Array::BooleanArray(arr) => {
                let out: Vec<bool> = (0..bits.len()).map(|i| arr.data.get(i)).collect();
                assert_eq!(out.as_slice(), &bits);
            }
            _ => panic!("unexpected array {:?}", array),
        }
    }
}
