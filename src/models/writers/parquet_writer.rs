//! # Parquet file writer
//!
//! Parquet v2 writer for `minarrow::Table`.
//!
//! ## Overview
//! - Writes a single-row-group Parquet file with page-chunked columns
//! - Emits dictionary pages for categorical columns and data pages in fixed-size chunks
//! - Computes per-page null counts (only) into parquet statistics; tracks page/block offsets in metadata
//! - Supports `Compression::{None, Snappy, Zstd}` when enabled via features.
//! - Supported Encodings:
//!     * `PLAIN` for primitives and strings
//!     * `RLE_DICTIONARY` for categoricals
//!     * Uses RLE/bit-packing to encode definition and repetition levels
//!
//! ## Intended use
//! - Produce fast, straightforward, zero-dependency interoperable Parquet files directly from in-memory `Table` data
//! - Pipe/stream or persist to disk using any `Write + Seek` sink
//! - Avoid paying ecosystem compile-time penalty when simple read/writes are needed
//!
//! ## Limitations
//! - Focused subset of the spec (no nested types, decimals, maps/lists, etc.)
//! - One row group per file (sufficient for many batch/export workflows)
//!
//! ## Alternatives
//! - From `Minarrow`, go `.to_arrow()` and get immediate access to the official writer.
//!
//! ## Example
//! ```no_run
//! use std::fs::File;
//! use minarrow::Table;
//! use lightstream::compression::Compression;
//! use lightstream::models::writers::parquet_writer::write_parquet_table;
//!
//! # let table = Table::default();
//! let mut file = File::create("data.parquet").unwrap();
//! write_parquet_table(&table, &mut file, Compression::Zstd).unwrap();
//! ```

use std::io::{Seek, Write};

#[cfg(feature = "datetime")]
use minarrow::TemporalArray;
use minarrow::{Array, NumericArray, Table, TextArray};

use crate::compression::{Compression, compress};
use crate::error::IoError;
#[cfg(feature = "large_string")]
use crate::models::encoders::parquet::data::encode_large_string_plain;
use crate::models::encoders::parquet::data::{
    encode_bool_bitpacked, encode_float32_plain, encode_float64_plain, encode_int32_plain,
    encode_int64_plain, encode_string_plain, encode_uint32_as_int32_plain,
    encode_uint64_as_int64_plain,
};
use crate::models::encoders::parquet::metadata::{
    ColumnChunkMeta, ColumnMetadata, DataPageHeaderV2, DictionaryPageHeader, FileMetaData,
    PageHeader, PageType, RowGroupMeta, SchemaElement, Statistics,
};
use crate::models::types::parquet::ParquetLogicalType::{self};
use crate::models::types::parquet::{ParquetEncoding, arrow_type_to_parquet};
use crate::constants::PARQUET_MAGIC;

// Chunk size for page splitting
pub const PAGE_CHUNK_SIZE: usize = 32_768;

/// Write the in-memory [`Table`] to `out` in *Parquet v2* format,
/// supporting chunked/multi-page columns, per spec.
///
/// # Support
/// We have essentials Parquet support at this time.
/// TLDR: **One can write, and read Parquet from `Minarrow`, but reading
/// external files with more niche encodings may not work**.
///
/// **Implemented**:
/// - Multiple data pages per column are emitted in fixed-size chunks.
/// - Each dictionary page offset and first data page offset are stored in
///   ColumnMetadata.
/// - Offset is updated after every write, including page headers and page bodies.
/// - All page-level statistics are computed for that page's chunk only.
/// - `Zstd` and `Snappy` compression options
/// `Plain` encoding for all types, and `RLE encoding` for *categorical* types.
///
/// **Not Implemented**
/// - Other parquet encodings are not.
/// - *PR's are welcome!*
///
/// # Alternatives
/// - When using Minarrow, one can use `.to_arrow()` or `.to_polars() to
/// bridge over FFI to `arrow-rs`, `polars_arrow`, to immediately access the
/// full reader/writer ecosystem, but at the penalty of long compile times.
pub fn write_parquet_table<W: Write + Seek>(
    table: &Table,
    mut out: W,
    compression: Compression,
) -> Result<(), IoError> {
    // file-header magic
    out.write_all(PARQUET_MAGIC)?;

    // schema
    let schema: Vec<SchemaElement> = table
        .cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let (physical, logical) = arrow_type_to_parquet(&c.field.dtype).unwrap();
            SchemaElement {
                name: c.field.name.clone(),
                repetition_type: if c.field.nullable { 1 } else { 0 }, // OPTIONAL / REQUIRED
                type_: Some(physical),
                converted_type: logical_to_converted(&logical),
                type_length: None,
                precision: None,
                scale: None,
                field_id: Some(i as i32),
            }
        })
        .collect();

    // accumulators
    let mut row_groups = Vec::new();
    let mut columns_meta = Vec::new();
    let mut offset = PARQUET_MAGIC.len() as i64; // running file offset

    let n_rows = table.n_rows;
    let n_rows_i64 = n_rows as i64;

    // Column loop (multi-page support)
    for col in &table.cols {
        let mut dictionary_page_offset = None;
        let mut encodings = vec![ParquetEncoding::Plain];

        // Dictionary support
        if is_dictionary(&col.array) {
            let dict_offset_before = out.stream_position()?;
            match &col.array {
                Array::TextArray(TextArray::Categorical32(a)) => {
                    write_dictionary_page(
                        &mut out,
                        &mut offset,
                        a.unique_values.iter().map(|s| s.as_bytes()),
                        compression,
                    )?;
                }
                _ => {}
            }
            dictionary_page_offset = Some(dict_offset_before as i64);
            encodings.insert(0, ParquetEncoding::RleDictionary);
            offset = out.stream_position()? as i64;
        }

        // Data page chunking
        let n = col.len();
        let mut start = 0;
        let mut col_num_values = 0i64;
        let mut total_uncompressed_size = 0i64;
        let mut total_compressed_size = 0i64;

        let mut recorded_data_page_offset: Option<i64> = None;

        while start < n {
            let end = usize::min(start + PAGE_CHUNK_SIZE, n);
            let len = end - start;

            // encode the raw values for this slice
            let mut values_raw = Vec::new();
            match &col.array {
                Array::NumericArray(n) => match n {
                    NumericArray::Int32(a) => {
                        encode_int32_plain(&a.data[start..end], &mut values_raw)
                    }
                    NumericArray::UInt32(a) => {
                        encode_uint32_as_int32_plain(&a.data[start..end], &mut values_raw)
                    }
                    NumericArray::Int64(a) => {
                        encode_int64_plain(&a.data[start..end], &mut values_raw)
                    }
                    NumericArray::UInt64(a) => {
                        encode_uint64_as_int64_plain(&a.data[start..end], &mut values_raw)
                    }
                    NumericArray::Float32(a) => {
                        encode_float32_plain(&a.data[start..end], &mut values_raw)
                    }
                    NumericArray::Float64(a) => {
                        encode_float64_plain(&a.data[start..end], &mut values_raw)
                    }
                    _ => return Err(IoError::UnsupportedType("numeric".into())),
                },
                Array::BooleanArray(a) => {
                    encode_bool_bitpacked(
                        &a.data.slice_clone(start, end - start),
                        a.null_mask
                            .as_ref()
                            .map(|m| m.slice_clone(start, end - start))
                            .as_ref(),
                        len,
                        &mut values_raw,
                    );
                }
                Array::TextArray(TextArray::String32(a)) => encode_string_plain(
                    &a.offsets[start..=end],
                    &a.data,
                    a.null_mask
                        .as_ref()
                        .map(|m| m.slice_clone(start, end - start))
                        .as_ref(),
                    len,
                    &mut values_raw,
                )?,
                #[cfg(feature = "large_string")]
                Array::TextArray(TextArray::String64(a)) => encode_large_string_plain(
                    &a.offsets[start..=end],
                    &a.data,
                    a.null_mask
                        .as_ref()
                        .map(|m| m.slice_clone(start, end - start))
                        .as_ref(),
                    len,
                    &mut values_raw,
                )?,
                #[cfg(feature = "datetime")]
                Array::TemporalArray(TemporalArray::Datetime32(a)) => {
                    use crate::models::encoders::parquet::data::encode_datetime32_plain;

                    encode_datetime32_plain(&a.data[start..end], &mut values_raw)
                }
                #[cfg(feature = "datetime")]
                Array::TemporalArray(TemporalArray::Datetime64(a)) => {
                    use crate::models::encoders::parquet::data::encode_datetime64_plain;

                    encode_datetime64_plain(&a.data[start..end], &mut values_raw)
                }
                Array::TextArray(TextArray::Categorical32(a)) => {
                    encode_dictionary_indices_rle(&a.data[start..end], &mut values_raw)?
                }
                #[cfg(all(feature = "extended_categorical", feature = "large_string"))]
                Array::TextArray(TextArray::Categorical64(a)) => {
                    let idx: Vec<u32> = a.data[start..end]
                        .iter()
                        .map(|&v| u32::try_from(v))
                        .collect::<Result<_, _>>()
                        .map_err(|_| {
                            IoError::Format(
                                "Categorical64 dictionary > 4 294 967 295 entries".into(),
                            )
                        })?;
                    encode_dictionary_indices_rle(&idx, &mut values_raw)?;
                }
                _ => return Err(IoError::UnsupportedType(format!("array {:?}", col.array))),
            }

            // rep / def levels for this chunk
            let def_levels = col.array.null_mask().map_or_else(
                || vec![true; len],
                |mask| (start..end).map(|i| mask.get(i)).collect(),
            );
            let def_buf = encode_levels_rle(&def_levels);
            let rep_buf = encode_levels_rle(&vec![false; len]);

            let rep_len = rep_buf.len();
            let def_len = def_buf.len();

            // compress values
            let values_compressed = compress(&values_raw, compression)?;
            let compressed_page_size = rep_len + def_len + values_compressed.len();
            let uncompressed_page_size = rep_len + def_len + values_raw.len();

            // assemble page body
            let mut page_body = Vec::with_capacity(compressed_page_size);
            page_body.extend_from_slice(&rep_buf);
            page_body.extend_from_slice(&def_buf);
            page_body.extend_from_slice(&values_compressed);

            // per-page statistics (stub)
            let stats = Statistics {
                null_count: Some(def_levels.iter().filter(|&&v| !v).count() as i64),
                distinct_count: None,
                min: None,
                max: None,
            };

            // page header
            let header_offset = out.stream_position()? as i64;

            let mut header_buf = Vec::new();
            PageHeader {
                type_: PageType::DataPageV2,
                uncompressed_page_size: uncompressed_page_size as i32,
                compressed_page_size: compressed_page_size as i32,
                data_page_header: None,
                dictionary_page_header: None,
                data_page_header_v2: Some(DataPageHeaderV2 {
                    num_rows: len as i32,
                    num_nulls: def_levels.iter().filter(|&&v| !v).count() as i32,
                    num_values: (len - def_levels.iter().filter(|&&v| !v).count()) as i32,
                    encoding: if is_dictionary(&col.array) {
                        ParquetEncoding::RleDictionary
                    } else {
                        ParquetEncoding::Plain
                    },
                    definition_levels_byte_length: def_buf.len() as i32,
                    repetition_levels_byte_length: rep_buf.len() as i32,
                    is_compressed: compression != Compression::None,
                    statistics: Some(stats.clone()),
                }),
            }
            .write(&mut header_buf)?;
            out.write_all(&header_buf)?;
            out.write_all(&page_body)?;

            offset = out.stream_position()? as i64;

            if recorded_data_page_offset.is_none() {
                recorded_data_page_offset = Some(header_offset);
            }

            col_num_values += len as i64;
            total_uncompressed_size += uncompressed_page_size as i64;
            total_compressed_size += compressed_page_size as i64;
            start = end;
        }

        // column-chunk metadata
        let first_data = recorded_data_page_offset.expect("at least one data page must be emitted");
        let (phys, _) = arrow_type_to_parquet(&col.field.dtype)?;
        columns_meta.push(ColumnChunkMeta {
            file_offset: first_data,
            meta_data: ColumnMetadata {
                type_: phys,
                encodings: encodings.clone(),
                path_in_schema: vec![col.field.name.clone()],
                codec: compression as i32,
                num_values: col_num_values,
                total_uncompressed_size,
                total_compressed_size,
                data_page_offset: first_data,
                dictionary_page_offset,
                statistics: None,
                definition_level: if col.field.nullable { 1 } else { 0 },
            },
        });
    }

    // Row-group + footer
    let total_byte_size = offset - (PARQUET_MAGIC.len() as i64);
    row_groups.push(RowGroupMeta {
        columns: columns_meta,
        total_byte_size,
        num_rows: n_rows_i64,
    });

    let footer_start = out.stream_position()?;
    FileMetaData {
        version: 2,
        schema,
        num_rows: n_rows_i64,
        row_groups,
        key_value_metadata: None,
        created_by: Some("parquet_writer-v2".into()),
    }
    .write(&mut out)?;

    let footer_end = out.stream_position()?;
    println!(
        "DIAG: footer_start={}, footer_end={}, footer_len={}",
        footer_start,
        footer_end,
        footer_end - footer_start
    );
    // let footer_len = (footer_end - footer_start) as u32;
    // out.write_all(&footer_len.to_le_bytes())?;
    // out.write_all(PARQUET_MAGIC)?;
    // DIAGNOSTIC
    let file_end = out.stream_position()?;
    println!("DIAG: file_end after all writes: {}", file_end);
    Ok(())
}

// Helpers

/// Add a dictionary page and return its offset.
fn write_dictionary_page<'a, W, I>(
    out: &mut W,
    offset: &mut i64,
    values: I,
    compression: Compression,
) -> Result<(), IoError>
where
    W: Write + Seek,
    I: IntoIterator<Item = &'a [u8]>,
{
    // 1) Serialise dictionary entries (length‐prefixed)
    let mut raw = Vec::new();
    let mut entry_count = 0i32;
    for v in values {
        let len = v.len() as u32;
        raw.extend_from_slice(&len.to_le_bytes());
        raw.extend_from_slice(v);
        entry_count += 1;
    }

    // 2) Compress the dictionary payload
    let compressed = compress(&raw, compression)?;

    // 3) Write the header
    let mut header_buf = Vec::new();
    PageHeader {
        type_: PageType::DictionaryPage,
        uncompressed_page_size: raw.len() as i32,
        compressed_page_size: compressed.len() as i32,
        data_page_header: None,
        dictionary_page_header: Some(DictionaryPageHeader {
            num_values: entry_count,
            encoding: ParquetEncoding::Plain,
            is_sorted: None,
        }),
        data_page_header_v2: None,
    }
    .write(&mut header_buf)?;
    out.write_all(&header_buf)?;
    *offset = out.stream_position()? as i64;

    // 4) Write the compressed payload
    out.write_all(&compressed)?;
    *offset = out.stream_position()? as i64;

    Ok(())
}

/// true if the array is a (categorical) dictionary array.
#[cfg(feature = "extended_categorical")]
fn is_dictionary(arr: &Array) -> bool {
    matches!(
        arr,
        Array::TextArray(TextArray::Categorical32(_) | TextArray::Categorical64(_))
    )
}

/// true if the array is a (categorical) dictionary array.
#[cfg(not(feature = "extended_categorical"))]
fn is_dictionary(arr: &Array) -> bool {
    matches!(arr, Array::TextArray(TextArray::Categorical32(_)))
}

/// Encode levels (rep or def) with RLE/BitPacked hybrid, bit-width = 1.
///
/// We choose a **single RLE run** when all values are identical; otherwise
/// we fall back to emitting *one* bit-packed run (multiples of 8, padded).
pub fn encode_levels_rle(levels: &[bool]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);

    // Check for single-value run
    if levels.iter().all(|&b| b == levels[0]) {
        // RLE header = run_len << 1
        let header = (levels.len() as u64) << 1; // LSB = 0 → RLE
        write_uleb128(header, &mut out);
        out.push(levels[0] as u8); // 1-byte value (bit-width = 1)
        return out;
    }

    // Bit-packed path
    let padded_len = ((levels.len() + 7) / 8) * 8;
    let groups = padded_len / 8;
    let header = ((groups as u64) << 1) | 1; // LSB = 1 → bit-packed
    write_uleb128(header, &mut out);

    for g in 0..groups {
        let mut byte = 0u8;
        for bit in 0..8 {
            let idx = g * 8 + bit;
            if idx < levels.len() && levels[idx] {
                byte |= 1 << bit;
            }
        }
        out.push(byte);
    }
    out
}

/// write unsigned LEB128
fn write_uleb128(mut v: u64, o: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            o.push(b);
            break;
        }
        o.push(b | 0x80);
    }
}

/// Convert logical-type enum to legacy ConvertedType id for compatibility.
fn logical_to_converted(log: &ParquetLogicalType) -> Option<i32> {
    Some(match log {
        ParquetLogicalType::Utf8 => 1,
        #[cfg(feature = "datetime")]
        ParquetLogicalType::Date32 => 4,
        #[cfg(feature = "datetime")]
        ParquetLogicalType::Date64 => 5,
        #[cfg(feature = "datetime")]
        ParquetLogicalType::TimestampMillis => 9,
        #[cfg(feature = "datetime")]
        ParquetLogicalType::TimestampMicros => 10,
        #[cfg(feature = "datetime")]
        ParquetLogicalType::TimeMillis => 6,
        #[cfg(feature = "datetime")]
        ParquetLogicalType::TimeMicros => 7,
        ParquetLogicalType::IntType {
            bit_width: 8,
            is_signed: true,
        } => 11,
        ParquetLogicalType::IntType {
            bit_width: 16,
            is_signed: true,
        } => 12,
        ParquetLogicalType::IntType {
            bit_width: 32,
            is_signed: true,
        } => 13,
        ParquetLogicalType::IntType {
            bit_width: 64,
            is_signed: true,
        } => 14,
        ParquetLogicalType::IntType {
            bit_width: 8,
            is_signed: false,
        } => 15,
        ParquetLogicalType::IntType {
            bit_width: 16,
            is_signed: false,
        } => 16,
        ParquetLogicalType::IntType {
            bit_width: 32,
            is_signed: false,
        } => 17,
        ParquetLogicalType::IntType {
            bit_width: 64,
            is_signed: false,
        } => 18,
        _ => return None,
    })
}

/// Write dictionary indices for an RLE_DICTIONARY data-page.
///
/// *   No 4-byte length prefix (dictionary index streams never have one).
/// *   First byte is the bit-width (1-32).
/// *   Then a sequence of RLE or bit-packed runs (hybrid format).
///
/// The encoder performs RLE optimisation:  
/// every maximal run of a single value - of length ≥ 8, per spec -
/// is emitted as an RLE run; all other regions are emitted as the
/// shortest-possible bit-packed runs (multiples of 8 values, zero-padded).
///
/// On error (index wider than 32 bits) returns `IoError::Format`.
pub fn encode_dictionary_indices_rle(indices: &[u32], out: &mut Vec<u8>) -> Result<(), IoError> {
    if indices.is_empty() {
        out.push(0);
        return Ok(());
    }

    // bit-width
    let bit_width = (32 - indices.iter().max().unwrap().leading_zeros()).max(1) as u8;
    if bit_width > 32 {
        return Err(IoError::Format(
            "Dictionary index >32-bit not supported".into(),
        ));
    }
    out.push(bit_width);

    // helpers
    #[inline]
    fn write_uleb128(mut v: u64, o: &mut Vec<u8>) {
        loop {
            let b = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                o.push(b);
                break;
            }
            o.push(b | 0x80);
        }
    }

    let bytes_per_value = ((bit_width + 7) / 8) as usize;
    let n = indices.len();
    let mut i = 0;

    // main loop
    while i < n {
        // Detect maximal run of identical value starting at i
        let v = indices[i];
        let mut rle_len = 1;
        while i + rle_len < n && indices[i + rle_len] == v {
            rle_len += 1;
        }

        // Emit RLE run if it meets the spec threshold (≥8)
        if rle_len >= 8 {
            let header = (rle_len as u64) << 1; // LSB 0
            write_uleb128(header, out); // run-header
            for b in 0..bytes_per_value {
                // repeated value
                out.push((v >> (b * 8)) as u8);
            }
            i += rle_len;
            continue;
        }

        // Otherwise gather a bit-packed segment up to:
        //   – next RLE-eligible run, or
        //   – end of data,
        // but encode at least 8 values (spec) and a multiple of 8.
        let bp_start = i;
        let mut bp_len = 0usize;
        while i + bp_len < n {
            // stop if the upcoming region is an RLE-able run
            if bp_len >= 8 {
                let mut look = 1;
                while i + bp_len + look < n && indices[i + bp_len + look] == indices[i + bp_len] {
                    look += 1;
                    if look >= 8 {
                        break;
                    }
                }
                if look >= 8 {
                    break; // next RLE run begins here
                }
            }
            bp_len += 1;
        }
        let emit_len = ((bp_len + 7) / 8) * 8; // pad to /8
        let groups = emit_len / 8;
        let header = ((groups as u64) << 1) | 1; // LSB 1
        write_uleb128(header, out);

        // build a scratch buffer, and if there is another RLE run immediately
        // after these bp_len values, fill the padding with that next value
        let mut scratch = vec![0u32; emit_len];
        for j in 0..bp_len {
            scratch[j] = indices[bp_start + j];
        }

        // if the next run is RLE (i + bp_len < n && future run_len >= 8),
        // use its value to fill the padding; else fall back to zero
        if bp_len < emit_len && bp_start + bp_len < n {
            // peek the next value in the stream
            let pad_val = indices[bp_start + bp_len];
            // only use it if there really is an RLE run coming
            let mut look = 1;
            while bp_start + bp_len + look < n && indices[bp_start + bp_len + look] == pad_val {
                look += 1;
                if look >= 8 {
                    break;
                }
            }
            if look >= 8 {
                for j in bp_len..emit_len {
                    scratch[j] = pad_val;
                }
            }
        }
        for bit in 0..bit_width {
            for g in 0..groups {
                let mut byte = 0u8;
                for j in 0..8 {
                    let idx = g * 8 + j;
                    if ((scratch[idx] >> bit) & 1) != 0 {
                        byte |= 1 << j;
                    }
                }
                out.push(byte);
            }
        }
        i += bp_len;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // encode_levels_rle

    use minarrow::Vec64;

    use crate::models::decoders::parquet::decode_dictionary_indices_rle;

    #[test]
    fn levels_all_true_single_rle_run() {
        let levels = vec![true; 10]; // len = 10
        let buf = super::encode_levels_rle(&levels);
        // RLE header = len << 1 (=20)  => 0x14, value-byte = 0x01
        assert_eq!(buf, &[0x14, 0x01]);
    }

    #[test]
    fn levels_all_false_single_rle_run() {
        let levels = vec![false; 5]; // len = 5
        let buf = super::encode_levels_rle(&levels);
        // header = 10 (0x0A), value-byte = 0x00
        assert_eq!(buf, &[0x0A, 0x00]);
    }

    #[test]
    fn levels_mixed_bitpacked_exact_group() {
        // 8 levels: 1 0 1 0 1 0 1 0  -> byte 0b01010101 = 0x55
        let levels = [true, false, true, false, true, false, true, false];
        let buf = super::encode_levels_rle(&levels);
        assert_eq!(buf, &[0x03, 0x55]); // header 0x03 (= groups=1, bit-packed), data
    }

    #[test]
    fn levels_mixed_bitpacked_with_padding() {
        // 3 levels, padded to 8 in bit-packed encoder
        let levels = [true, false, false];
        // Expected: header 0x03 (1 group), byte = 0b00000001 = 0x01
        let buf = super::encode_levels_rle(&levels);
        assert_eq!(buf, &[0x03, 0x01]);
    }

    // decoder -> encoder roundtrip for dict rle

    fn roundtrip_dict_indices(indices: &[u32]) {
        let mut encoded = Vec::new();
        super::encode_dictionary_indices_rle(indices, &mut encoded).unwrap();
        let decoded = decode_dictionary_indices_rle(&encoded, indices.len()).unwrap();
        assert_eq!(decoded.as_slice(), indices);
    }

    #[test]
    fn dict_indices_all_equal_rle() {
        let idx = vec![7u32; 24]; // pure RLE run (≥8)
        roundtrip_dict_indices(&idx);
    }

    #[test]
    fn dict_indices_mixed_small() {
        let idx = vec![0, 1, 1, 2, 3, 3, 3, 4];
        roundtrip_dict_indices(&idx);
    }

    #[test]
    fn dict_indices_long_mixed_runs() {
        let mut idx = Vec64::new();
        // 12×5  (RLE), 10 ascending values (bit-packed), 16×2 (RLE)
        idx.extend(std::iter::repeat(5u32).take(12));
        idx.extend(0u32..10);
        idx.extend(std::iter::repeat(2u32).take(16));
        roundtrip_dict_indices(&idx);
    }
}
