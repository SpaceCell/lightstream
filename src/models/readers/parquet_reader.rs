//! # Parquet table reader - *reads into `minarrow::Table`*
//!
//! ## Overview
//! - Supports DataPageV2 and legacy V1 Parquet layouts
//! - Decodes hybrid RLE/bit-packed definition levels and dictionary indices
//! - Handles `PLAIN` and `RLE_DICTIONARY` value encodings
//! - Optional feature-gated Snappy / Zstd compression
//! - Type mapping to Arrow/Minarrow - {i32, i64, u32, u64, f32, f64, bool, utf8
//!   dictionary<u32/u64 (feature-flagged)>, and date32/date64 via `datetime` feature}
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

/// Read an entire in‐memory Table from a Parquet v2 file.
pub fn read_parquet_table<R: Read + Seek>(mut r: R) -> Result<Table, IoError> {
    // read the 8‐byte footer
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

    // map Parquet schema -> Arrow types
    let arrow_types: Vec<_> = meta
        .schema
        .iter()
        .map(|se| {
            parquet_to_arrow_type(
                se.type_.unwrap(),
                ParquetLogicalType::from_converted_type(se.converted_type),
            )
        })
        .collect::<Result<_, _>>()?;

    // single row‐group, flat schema only
    let rg = &meta.row_groups[0];
    let mut columns = Vec::with_capacity(rg.columns.len());

    for (col_idx, chunk) in rg.columns.iter().enumerate() {
        let cmeta = &chunk.meta_data;
        let ty = &arrow_types[col_idx];

        // read the DICTIONARY_PAGE if present
        let dict = if let Some(dict_off) = cmeta.dictionary_page_offset {
            r.seek(SeekFrom::Start(dict_off as u64))?;
            let ph = parse_page_header(&mut r)?;
            if ph.type_ != PageType::DictionaryPage {
                return Err(IoError::Format("expected DICTIONARY_PAGE".into()));
            }
            let mut compr = vec![0u8; ph.compressed_page_size as usize];
            r.read_exact(&mut compr)?;
            let raw = decompress(&compr, map_codec(cmeta.codec))?;
            parse_dictionary_values(&raw)?
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
                name: chunk.meta_data.path_in_schema[0].clone(),
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
    let values_raw = if h.is_compressed {
        decompress(&vs, map_codec(cmeta.codec))?
    } else {
        vs
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
            Array::BooleanArray(Arc::new(BooleanArray {
                data: Bitmask::from_bytes(buf, len),
                null_mask: mask,
                len,
                _phantom: Default::default(),
            }))
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
                (CategoricalIndexType::UInt32, ParquetEncoding::RleDictionary) => {
                    let idx = decode_dictionary_indices_rle(buf, len)?;
                    build_cat32(idx, dict, mask)
                }
                (CategoricalIndexType::UInt32, ParquetEncoding::Plain) => {
                    let idx = decode_uint32_as_int32_plain(buf)?;
                    build_cat32(idx, dict, mask)
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
            let bytes_per_value = ((bit_width + 7) / 8) as usize;
            if pos + bytes_per_value > buf.len() {
                return Err(IoError::Format("truncated RLE run".into()));
            }
            let mut v_bytes = [0u8; 4];
            v_bytes[..bytes_per_value].copy_from_slice(&buf[pos..pos + bytes_per_value]);
            let v = u32::from_le_bytes(v_bytes);
            pos += bytes_per_value;
            let take = run_len.min(n - out.len());
            out.extend(std::iter::repeat(v).take(take));
        } else {
            // bit-packed
            let groups = (header >> 1) as usize; // 1 group = 8 values
            let total_values = groups * 8;
            let total_bits = (total_values as usize) * (bit_width as usize);
            let total_bytes = (total_bits + 7) / 8;
            if pos + total_bytes > buf.len() {
                return Err(IoError::Format("truncated bit-packed run".into()));
            }
            let slice = &buf[pos..pos + total_bytes];
            // decode all bit-packed values in one go
            let mut scratch = vec![0u32; total_values];
            for bit in 0..bit_width {
                for g in 0..groups {
                    // for bit 'bit', the byte for group 'g' lives at offset bit*groups + g
                    let byte = slice[bit as usize * groups + g];
                    for j in 0..8 {
                        if (byte >> j) & 1 != 0 {
                            scratch[g * 8 + j] |= 1 << bit;
                        }
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

fn map_codec(id: i32) -> Compression {
    match id {
        0 => Compression::None,
        #[cfg(feature = "snappy")]
        1 => Compression::Snappy,
        #[cfg(feature = "zstd")]
        6 => Compression::Zstd, // spec: ZSTD = 6
        _ => Compression::None,
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
    thrift_read_struct_begin(r)?;
    let mut version = None;
    let mut schema = Vec::new();
    let mut num_rows = None;
    let mut row_groups = Vec::new();
    let mut kv_meta = None;
    let mut created_by = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r)?;
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
                let (_kt, _vt, len) = thrift_read_map_begin(r)?;
                let mut map = BTreeMap::new();
                for _ in 0..len {
                    let k = thrift_read_string(r)?;
                    let v = thrift_read_string(r)?;
                    map.insert(k, v);
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
    thrift_read_struct_begin(r)?;
    let mut name = None;
    let mut repetition_type = None;
    let mut type_ = None;
    let mut converted_type = None;
    let mut type_length = None;
    let mut precision = None;
    let mut scale = None;
    let mut field_id = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => name = Some(thrift_read_string(r)?),
            2 => {
                type_ = Some(
                    ParquetPhysicalType::from_i32(thrift_read_i32(r)?)
                        .ok_or_else(|| IoError::Format("Invalid type_".into()))?,
                )
            }
            3 => repetition_type = Some(thrift_read_i32(r)?),
            6 => converted_type = Some(thrift_read_i32(r)?),
            7 => type_length = Some(thrift_read_i32(r)?),
            9 => precision = Some(thrift_read_i32(r)?),
            10 => scale = Some(thrift_read_i32(r)?),
            15 => field_id = Some(thrift_read_i32(r)?),
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
    })
}

fn parse_row_group<R: Read>(r: &mut R) -> Result<RowGroupMeta, IoError> {
    thrift_read_struct_begin(r)?;
    let mut columns = Vec::new();
    let mut total_byte_size = None;
    let mut num_rows = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r)?;
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
    thrift_read_struct_begin(r)?;
    let mut file_offset = None;
    let mut meta_data = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => file_offset = Some(thrift_read_i64(r)?),
            2 => {
                thrift_read_struct_begin(r)?;
                meta_data = Some(parse_column_meta_data(r)?);
            }
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

    loop {
        let (tpe, id) = thrift_read_field_begin(r)?;
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
            8 => data_page_offset = Some(thrift_read_i64(r)?),
            9 => dictionary_page_offset = Some(thrift_read_i64(r)?),
            10 => {
                thrift_read_struct_begin(r)?;
                statistics = Some(parse_statistics(r)?);
            }
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
    thrift_read_struct_begin(r)?;
    let mut null_count = None;
    let mut distinct_count = None;
    let mut min = None;
    let mut max = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => null_count = Some(thrift_read_i64(r)?),
            2 => distinct_count = Some(thrift_read_i64(r)?),
            3 => min = Some(thrift_read_bytes(r)?),
            4 => max = Some(thrift_read_bytes(r)?),
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
    thrift_read_struct_begin(r)?;
    let mut ptype = None;
    let mut uncomp = None;
    let mut compr = None;
    let mut data_ph = None;
    let mut data_ph_v2 = None;
    let mut dict_ph = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r)?;
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
            4 => {
                thrift_read_struct_begin(r)?;
                data_ph = Some(parse_data_page_header(r)?);
            }
            5 => {
                thrift_read_struct_begin(r)?;
                let num_values = thrift_read_i32(r)?;
                let _enc_id = thrift_read_i32(r)?;
                let mut is_sorted = None;
                let peek = thrift_peek_field(r)?;
                if let Some((2, 3)) = peek {
                    thrift_read_field_begin(r)?;
                    is_sorted = Some(thrift_read_bool(r)?);
                }
                dict_ph = Some(DictionaryPageHeader {
                    num_values,
                    encoding: ParquetEncoding::Plain,
                    is_sorted,
                });
            }
            7 => {
                // Parse DataPageHeaderV2
                thrift_read_struct_begin(r)?;
                let mut num_rows = None;
                let mut num_nulls = None;
                let mut num_values = None;
                let mut encoding = None;
                let mut def_len = None;
                let mut rep_len = None;
                let mut is_compressed = None;
                let mut statistics = None;

                loop {
                    let (tpe2, id2) = thrift_read_field_begin(r)?;
                    if tpe2 == 0 {
                        break;
                    }
                    match id2 {
                        1 => num_rows = Some(thrift_read_i32(r)?),
                        2 => num_nulls = Some(thrift_read_i32(r)?),
                        3 => num_values = Some(thrift_read_i32(r)?),
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
                        7 => is_compressed = Some(thrift_read_bool(r)?),
                        8 => {
                            thrift_read_struct_begin(r)?;
                            statistics = Some(parse_statistics(r)?);
                        }
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

/// Peeks the next Thrift field type and id, restoring the stream position.
fn thrift_peek_field<R: Read + Seek>(r: &mut R) -> Result<Option<(u8, i16)>, IoError> {
    let pos = r.stream_position()?;
    let result = thrift_read_field_begin(r);
    r.seek(SeekFrom::Start(pos))?;
    match result {
        Ok((0, _)) => Ok(None), // STOP
        Ok(x) => Ok(Some(x)),
        Err(e) => Err(e),
    }
}

fn parse_data_page_header<R: Read>(r: &mut R) -> Result<DataPageHeader, IoError> {
    thrift_read_struct_begin(r)?;
    let mut num_values = None;
    let mut encoding = None;
    let mut dlev = None;
    let mut rlev = None;
    let mut stats = None;

    loop {
        let (tpe, id) = thrift_read_field_begin(r)?;
        if tpe == 0 {
            break;
        }
        match id {
            1 => num_values = Some(thrift_read_i32(r)?),
            2 => encoding = Some(ParquetEncoding::from_i32(thrift_read_i32(r)?).unwrap()),
            3 => dlev = Some(ParquetEncoding::from_i32(thrift_read_i32(r)?).unwrap()),
            4 => rlev = Some(ParquetEncoding::from_i32(thrift_read_i32(r)?).unwrap()),
            5 => {
                thrift_read_struct_begin(r)?;
                stats = Some(parse_statistics(r)?);
            }
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

// Low-level Thrift readers

fn thrift_read_struct_begin<R: Read>(_r: &mut R) -> Result<(), IoError> {
    Ok(()) // marker
}

fn thrift_read_field_begin<R: Read>(r: &mut R) -> Result<(u8, i16), IoError> {
    let mut t = [0u8; 1];
    r.read_exact(&mut t)?;
    let tpe = t[0];
    if tpe == 0 {
        return Ok((0, 0));
    }
    let mut idb = [0u8; 2];
    r.read_exact(&mut idb)?;
    let id = i16::from_le_bytes(idb);
    Ok((tpe, id))
}

fn thrift_read_i32<R: Read>(r: &mut R) -> Result<i32, IoError> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(i32::from_le_bytes(b))
}

fn thrift_read_i64<R: Read>(r: &mut R) -> Result<i64, IoError> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(i64::from_le_bytes(b))
}

fn thrift_read_bool<R: Read>(r: &mut R) -> Result<bool, IoError> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0] != 0)
}

fn thrift_read_string<R: Read>(r: &mut R) -> Result<String, IoError> {
    let mut lb = [0u8; 4];
    r.read_exact(&mut lb)?;
    let len = i32::from_le_bytes(lb) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf).map_err(|e| IoError::Format(format!("UTF8 error: {}", e)))?)
}

fn thrift_read_bytes<R: Read>(r: &mut R) -> Result<Vec<u8>, IoError> {
    let mut lb = [0u8; 4];
    r.read_exact(&mut lb)?;
    let len = i32::from_le_bytes(lb) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn thrift_read_list_begin<R: Read>(r: &mut R) -> Result<(u8, usize), IoError> {
    let mut t = [0u8; 1];
    r.read_exact(&mut t)?;
    let elem_tpe = t[0];
    let mut lb = [0u8; 4];
    r.read_exact(&mut lb)?;
    let len = i32::from_le_bytes(lb) as usize;
    Ok((elem_tpe, len))
}

fn thrift_read_map_begin<R: Read>(r: &mut R) -> Result<(u8, u8, usize), IoError> {
    let mut kt = [0u8; 1];
    let mut vt = [0u8; 1];
    r.read_exact(&mut kt)?;
    r.read_exact(&mut vt)?;
    let mut lb = [0u8; 4];
    r.read_exact(&mut lb)?;
    let len = i32::from_le_bytes(lb) as usize;
    Ok((kt[0], vt[0], len))
}

fn thrift_skip_field<R: Read>(r: &mut R, tpe: u8) -> Result<(), IoError> {
    match tpe {
        2 => {
            let mut b = [0u8; 1];
            r.read_exact(&mut b)?;
        } // BOOL
        8 => {
            let mut b = [0u8; 4];
            r.read_exact(&mut b)?;
        } // I32
        10 => {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
        } // I64
        11 => {
            let _ = thrift_read_bytes(r)?;
        } // STRING
        12 => loop {
            let (ft, _) = thrift_read_field_begin(r)?;
            if ft == 0 {
                break;
            }
            thrift_skip_field(r, ft)?;
        }, // STRUCT
        15 => {
            let (et, len) = thrift_read_list_begin(r)?;
            for _ in 0..len {
                thrift_skip_field(r, et)?;
            }
        } // LIST
        13 => {
            let (_, _, len) = thrift_read_map_begin(r)?;
            for _ in 0..len {
                // skip key
                thrift_skip_field(r, 11)?; /*string*/
                thrift_skip_field(r, 11)?;
            }
        }
        _ => {
            return Err(IoError::Format(format!(
                "Cannot skip unknown thrift type {}",
                tpe
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
        // transposed byte = 0b01010101 = 0x55
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
    #[test]
    fn decode_column_categorical_rle_dictionary() {
        let dict_raw = dict(&["foo", "bar"]);
        // logical indices
        let idx: Vec<u32> = vec![0, 1, 1, 0];
        // encode indices with RLE_DICTIONARY
        let mut encoded = Vec::new();
        encode_dictionary_indices_rle(&idx, &mut encoded).unwrap();

        // all values present
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

        // Down-cast and verify
        match array {
            Array::TextArray(TextArray::Categorical32(cat)) => {
                // indices
                assert_eq!(cat.data.as_slice(), idx.as_slice());
                // dictionary values
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
