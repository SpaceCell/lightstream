// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # CSV Decoder for Minarrow Tables
//!
//! - Accepts a CSV byte slice or any [`BufRead`](std::io::BufRead).
//! - Infers schema or uses a provided schema (optional).
//! - Supports: `Int32`, `Int64`, `UInt32`, `UInt64`, `Float32`, `Float64`, `Boolean`, `String32`, `Categorical32`, `Categorical8`.
//! - Custom delimiter, nulls, quoting, and dictionary mapping for categoricals.
//! - Produces a single [`Table`](minarrow::Table) via [`decode_csv`](crate::models::decoders::csv::decode_csv), or multiple batches via repeated calls to [`decode_csv_batch`](crate::models::decoders::csv::decode_csv_batch).
//!
//! ## Fast path
//!
//! `decode_csv` reads the entire input in one `read_to_end` call,
//! then pre-scans the byte buffer with `memchr3` (quote-aware) to
//! build an index of field positions. Columns are built directly
//! from byte slices into the buffer, so the only per-cell allocation
//! comes from quoted cells that contain embedded `""` escapes.
//! Schema inference samples the first 1024 data rows.
//!
//! Notes:
//! - Input is treated as UTF-8; invalid byte sequences are lossily decoded via `String::from_utf8_lossy`.
//! - See [`CsvDecodeOptions`](crate::models::decoders::csv::CsvDecodeOptions) for configurable delimiter, quoting, header handling, and schema control.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Cursor};
use std::sync::Arc;

use minarrow::ffi::arrow_dtype::CategoricalIndexType;
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, Field, FieldArray, FloatArray, IntegerArray, NumericArray,
    Table, TextArray, Vec64, vec64,
};

/// Options for CSV decoding.
#[derive(Debug, Clone)]
pub struct CsvDecodeOptions {
    /// Delimiter (e.g., b',' for CSV, b'\t' for TSV).
    pub delimiter: u8,
    /// String(s) that should be parsed as nulls.
    pub nulls: Vec<&'static str>,
    /// Quote character to use (default: '"').
    pub quote: u8,
    /// Whether to use the first row as a header.
    pub has_header: bool,
    /// Optional schema. If None, schema is inferred.
    pub schema: Option<Vec<Field>>,
    /// If true, all columns are loaded as String32.
    pub all_as_text: bool,
    /// For categoricals: columns that should be parsed as categorical.
    pub categorical_cols: HashSet<String>,
}

impl Default for CsvDecodeOptions {
    fn default() -> Self {
        CsvDecodeOptions {
            delimiter: b',',
            nulls: vec!["", "NA", "null", "NULL"],
            quote: b'"',
            has_header: true,
            schema: None,
            all_as_text: false,
            categorical_cols: HashSet::new(),
        }
    }
}

/// Number of data rows sampled for schema type inference.
const INFER_SAMPLE: usize = 1024;

/// Attempt to read *up to* `batch_size` *data* rows (plus one header row if `has_header`
/// is still true) from `reader`, and decode them into a single `Table`.  Returns
/// `Ok(None)` if there are no more rows to read.
pub fn decode_csv_batch<R: BufRead>(
    reader: &mut R,
    options: &CsvDecodeOptions,
    batch_size: usize,
) -> io::Result<Option<Table>> {
    let opts = options.clone();
    let need_header = opts.has_header;
    let mut buf = Vec::new();
    let mut chunk = Vec::new();
    let mut saw_any = false;
    let mut lines_to_read = batch_size;
    if need_header {
        // we need to read one extra line for the header
        lines_to_read += 1;
    }

    for _ in 0..lines_to_read {
        buf.clear();
        let n = reader.read_until(b'\n', &mut buf)?;
        if n == 0 {
            break;
        }
        // strip "\r\n" or "\n"
        if buf.ends_with(b"\r\n") {
            buf.truncate(buf.len() - 2);
        } else if buf.ends_with(b"\n") {
            buf.truncate(buf.len() - 1);
        }
        // skip leading blank lines
        if buf.is_empty() && !saw_any {
            continue;
        }
        saw_any = true;
        chunk.extend_from_slice(&buf);
        chunk.push(b'\n');
    }

    if !saw_any {
        // nothing read at all -> EOF
        return Ok(None);
    }

    // Now decode exactly that chunk
    let table = decode_csv(Cursor::new(chunk), &opts)?;
    Ok(Some(table))
}

/// Decodes CSV from a BufRead into a Minarrow Table.
/// Schema is inferred unless provided.
/// Errors propagate if CSV is malformed or parsing fails.
///
/// # Arguments
/// - `reader`: Any `BufRead` (e.g., `&[u8]`, `File`).
/// - `options`: CSV decode options.
///
/// # Returns
/// - On success, a Minarrow Table.
pub fn decode_csv<R: BufRead>(mut reader: R, options: &CsvDecodeOptions) -> io::Result<Table> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    decode_csv_bytes(&buf, options)
}

/// Decode an in-memory CSV byte slice into a Table.
///
/// Reads the entire byte buffer once to build a structural index of
/// field positions (quote-aware) and then builds columns directly
/// from byte slices into the buffer. Suitable for callers that
/// already have the CSV in memory (e.g. from `mmap` or
/// `read_to_end`).
pub fn decode_csv_bytes(buf: &[u8], options: &CsvDecodeOptions) -> io::Result<Table> {
    let CsvDecodeOptions {
        delimiter,
        ref nulls,
        quote,
        has_header,
        ref schema,
        all_as_text,
        ref categorical_cols,
    } = *options;

    // Skip any leading blank lines so the header detection lines up with
    // the first row that actually has data.
    let mut head = 0usize;
    while head < buf.len() && (buf[head] == b'\n' || buf[head] == b'\r') {
        head += 1;
    }
    let buf = &buf[head..];

    if buf.is_empty() {
        return Ok(Table {
            name: "csv".to_string(),
            cols: Vec::new(),
            n_rows: 0,
            ..Default::default()
        });
    }

    // Pass 1: quote-aware structural scan.
    //
    // `field_starts[i]` is the byte offset of the first byte of the i-th
    // field. The byte immediately before `field_starts[i+1]` is the
    // delimiter or newline that terminated field i (so field i's bytes
    // are `buf[field_starts[i] .. field_starts[i+1] - 1]`). The last
    // field is sentineled by an entry at `buf.len() + 1` so the same
    // `end - 1` math gives `buf.len()` for it.
    //
    // `row_end_fields[r]` is the index into `field_starts` of the field
    // that ended row r (the field whose terminator was a newline rather
    // than a delimiter).
    let n = buf.len();
    let mut field_starts: Vec<u32> = Vec::with_capacity(n / 16 + 16);
    field_starts.push(0);
    let mut row_end_fields: Vec<u32> = Vec::with_capacity(n / 64 + 16);

    let mut pos: usize = 0;
    while pos < n {
        let off = match memchr::memchr3(delimiter, b'\n', quote, &buf[pos..]) {
            Some(o) => o,
            None => break,
        };
        let abs = pos + off;
        let b = buf[abs];

        if b == quote {
            // Skip past the matching close quote, honouring `""` as an
            // embedded escape.
            pos = abs + 1;
            loop {
                let qoff = match memchr::memchr(quote, &buf[pos..]) {
                    Some(o) => o,
                    None => {
                        // Unterminated quote: stop scanning. Subsequent
                        // row-width validation will surface the error.
                        pos = n;
                        break;
                    }
                };
                let qabs = pos + qoff;
                if qabs + 1 < n && buf[qabs + 1] == quote {
                    pos = qabs + 2;
                } else {
                    pos = qabs + 1;
                    break;
                }
            }
        } else {
            // delimiter or newline: this field is now closed.
            let just_closed = field_starts.len() - 1;
            field_starts.push((abs + 1) as u32);
            if b == b'\n' {
                row_end_fields.push(just_closed as u32);
            }
            pos = abs + 1;
        }
    }

    // Trailing data without a closing newline still forms a row.
    let trailing_data = field_starts
        .last()
        .copied()
        .map(|s| (s as usize) < n)
        .unwrap_or(false);
    if trailing_data {
        let just_closed = field_starts.len() - 1;
        // Sentinel so cell end == `buf.len()` for the last field.
        field_starts.push((n + 1) as u32);
        row_end_fields.push(just_closed as u32);
    }

    let n_total_rows = row_end_fields.len();
    if n_total_rows == 0 {
        return Ok(Table {
            name: "csv".to_string(),
            cols: Vec::new(),
            n_rows: 0,
            ..Default::default()
        });
    }

    // Number of columns = number of fields in the first row.
    let n_cols = (row_end_fields[0] + 1) as usize;

    // Validate row width consistency.
    for r in 1..n_total_rows {
        let prev = row_end_fields[r - 1] as usize;
        let curr = row_end_fields[r] as usize;
        if curr - prev != n_cols {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "inconsistent row length",
            ));
        }
    }

    // Resolve the byte slice for a given flat field index, stripping a
    // trailing `\r` (so `\r\n` line endings parse cleanly).
    let field_bytes = |idx: usize| -> &[u8] {
        let s = field_starts[idx] as usize;
        let e_raw = field_starts[idx + 1] as usize;
        let e = e_raw.saturating_sub(1).min(n);
        let raw = &buf[s..e];
        if raw.last() == Some(&b'\r') {
            &raw[..raw.len() - 1]
        } else {
            raw
        }
    };

    // After row-width validation we know every row has exactly
    // `n_cols` fields, so the flat field index for cell (row, col)
    // collapses to a direct multiply. This is much hotter than the
    // row_end_fields lookup we used during scanning.
    let cell_field_idx = |row: usize, col: usize| -> usize { row * n_cols + col };

    // Header row: take column names from row 0, then everything else is data.
    let (data_row_offset, col_names): (usize, Vec<String>) = if has_header {
        let mut names = Vec::with_capacity(n_cols);
        for c in 0..n_cols {
            let f = field_bytes(cell_field_idx(0, c));
            let unq = unquote(f, quote);
            names.push(String::from_utf8_lossy(&unq).into_owned());
        }
        (1, names)
    } else {
        (0, (0..n_cols).map(|i| format!("col{}", i + 1)).collect())
    };

    let n_rows = n_total_rows - data_row_offset;

    // Schema inference (sample-based) when not provided. Operates on
    // byte slices directly - no per-cell String allocation, no UTF-8
    // round trips.
    let schema: Vec<Field> = if let Some(s) = schema.clone() {
        s
    } else if all_as_text {
        col_names
            .iter()
            .map(|name| Field {
                name: name.clone(),
                dtype: ArrowType::String,
                nullable: true,
                metadata: Default::default(),
            })
            .collect()
    } else {
        let sample = n_rows.min(INFER_SAMPLE);
        let mut types: Vec<ArrowType> = vec![ArrowType::String; n_cols];
        for col in 0..n_cols {
            let is_cat_col = categorical_cols.contains(&col_names[col]);
            let mut is_bool = true;
            let mut is_i32 = true;
            let mut is_i64 = true;
            let mut is_u32 = true;
            let mut is_u64 = true;
            let mut is_f32 = true;
            let mut is_f64 = true;

            for row in 0..sample {
                let raw = field_bytes(cell_field_idx(row + data_row_offset, col));
                let unq = unquote(raw, quote);
                let vb = trim_ascii(&unq);
                if is_null_bytes(vb, nulls) {
                    continue;
                }
                if is_bool && !is_bool_token(vb) {
                    is_bool = false;
                }
                if is_i32 && super::int_ascii::parse_ascii_int::<i32>(vb).is_none() {
                    is_i32 = false;
                }
                if is_i64 && super::int_ascii::parse_ascii_int::<i64>(vb).is_none() {
                    is_i64 = false;
                }
                if is_u32 && super::int_ascii::parse_ascii_int::<u32>(vb).is_none() {
                    is_u32 = false;
                }
                if is_u64 && super::int_ascii::parse_ascii_int::<u64>(vb).is_none() {
                    is_u64 = false;
                }
                if is_f32 && fast_float2::parse::<f32, _>(vb).is_err() {
                    is_f32 = false;
                }
                if is_f64 && fast_float2::parse::<f64, _>(vb).is_err() {
                    is_f64 = false;
                }
            }

            types[col] = if is_bool {
                ArrowType::Boolean
            } else if is_i32 {
                ArrowType::Int32
            } else if is_i64 {
                ArrowType::Int64
            } else if is_u32 {
                ArrowType::UInt32
            } else if is_u64 {
                ArrowType::UInt64
            } else if is_f64 {
                ArrowType::Float64
            } else if is_f32 {
                ArrowType::Float32
            } else if is_cat_col {
                #[cfg(not(feature = "default_categorical_8"))]
                {
                    ArrowType::Dictionary(CategoricalIndexType::UInt32)
                }
                #[cfg(feature = "default_categorical_8")]
                {
                    ArrowType::Dictionary(CategoricalIndexType::UInt8)
                }
            } else {
                ArrowType::String
            };
        }

        col_names
            .iter()
            .enumerate()
            .map(|(i, name)| Field {
                name: name.clone(),
                dtype: types[i].clone(),
                nullable: true,
                metadata: Default::default(),
            })
            .collect()
    };

    // Build each column in a single pass: extract cell bytes, unquote,
    // trim, null-check, and parse straight into the output buffer. No
    // Vec<Cow<[u8]>> intermediate, no double-walk over the data.
    let mut cols: Vec<FieldArray> = Vec::with_capacity(n_cols);
    for (col_idx, field) in schema.iter().enumerate() {
        let mut null_bools = vec![true; n_rows];
        let mut null_count = 0usize;

        let array = match &field.dtype {
            ArrowType::Int32 => build_int_col_inline::<i32>(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
            ArrowType::Int64 => build_int_col_inline::<i64>(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
            ArrowType::UInt32 => build_int_col_inline::<u32>(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
            ArrowType::UInt64 => build_int_col_inline::<u64>(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
            ArrowType::Float32 => build_float_col_inline::<f32>(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
            ArrowType::Float64 => build_float_col_inline::<f64>(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
            ArrowType::Boolean => build_bool_col_inline(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
            ArrowType::String => build_string_col_inline(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
            ArrowType::Dictionary(_) => build_categorical_col_inline(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
            _ => build_string_col_inline(
                buf,
                &field_starts,
                n_cols,
                data_row_offset,
                col_idx,
                n_rows,
                quote,
                nulls,
                &mut null_bools,
                &mut null_count,
            )?,
        };

        cols.push(FieldArray {
            field: Arc::new(field.clone()),
            array,
            null_count,
        });
    }

    Ok(Table {
        name: "csv".to_string(),
        cols,
        n_rows,
        ..Default::default()
    })
}

/// Resolve the byte slice for a given flat field index. Strips a
/// trailing `\r` so `\r\n` line endings parse cleanly.
#[inline]
fn cell_raw<'a>(buf: &'a [u8], field_starts: &[u32], field_idx: usize) -> &'a [u8] {
    let s = field_starts[field_idx] as usize;
    let e_raw = field_starts[field_idx + 1] as usize;
    let e = e_raw.saturating_sub(1).min(buf.len());
    let raw = &buf[s..e];
    if raw.last() == Some(&b'\r') {
        &raw[..raw.len() - 1]
    } else {
        raw
    }
}

/// Test whether the (trimmed) byte slice matches any configured null
/// token, including the implicit empty-string null. ASCII-case
/// insensitive to mirror the original `str` comparison.
#[inline]
fn is_null_bytes(bytes: &[u8], nulls: &[&'static str]) -> bool {
    if bytes.is_empty() {
        return true;
    }
    for n in nulls {
        let nb = n.as_bytes();
        if nb.len() == bytes.len()
            && nb
                .iter()
                .zip(bytes.iter())
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
        {
            return true;
        }
    }
    false
}

/// Recognise the standard CSV boolean tokens.
#[inline]
fn is_bool_token(bytes: &[u8]) -> bool {
    matches!(
        bytes,
        b"true"
            | b"True"
            | b"TRUE"
            | b"false"
            | b"False"
            | b"FALSE"
            | b"1"
            | b"0"
            | b"t"
            | b"T"
            | b"f"
            | b"F"
    )
}

/// Recognise the standard CSV boolean true tokens. False if not a
/// boolean token at all; callers should have null-checked first.
#[inline]
fn parse_bool(bytes: &[u8]) -> bool {
    matches!(bytes, b"true" | b"True" | b"TRUE" | b"1" | b"t" | b"T")
}

/// Strip outer quote characters and unescape doubled-`""` sequences.
/// The common case (no outer quote) returns a borrow; only quoted
/// cells containing an embedded `""` allocate.
#[inline]
fn unquote<'a>(bytes: &'a [u8], quote: u8) -> Cow<'a, [u8]> {
    if bytes.len() >= 2 && bytes[0] == quote && bytes[bytes.len() - 1] == quote {
        let inner = &bytes[1..bytes.len() - 1];
        if memchr::memchr(quote, inner).is_some() {
            let mut out = Vec::with_capacity(inner.len());
            let mut i = 0;
            while i < inner.len() {
                if inner[i] == quote && i + 1 < inner.len() && inner[i + 1] == quote {
                    out.push(quote);
                    i += 2;
                } else {
                    out.push(inner[i]);
                    i += 1;
                }
            }
            Cow::Owned(out)
        } else {
            Cow::Borrowed(inner)
        }
    } else {
        Cow::Borrowed(bytes)
    }
}

/// Trim leading/trailing ASCII whitespace without allocating.
#[inline]
fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|p| p + 1)
        .unwrap_or(0);
    if start >= end {
        &[]
    } else {
        &bytes[start..end]
    }
}

// ------- Column builders -------

fn mask_to_bitmask(mask: &[bool]) -> Bitmask {
    Bitmask::from_bools(mask)
}

/// Per-row inline integer column builder: extract cell bytes,
/// unquote, trim, null-check, parse straight into the output buffer.
/// No `Vec<Cow<[u8]>>` intermediate.
#[allow(clippy::too_many_arguments)]
fn build_int_col_inline<T>(
    buf: &[u8],
    field_starts: &[u32],
    n_cols: usize,
    data_row_offset: usize,
    col_idx: usize,
    n_rows: usize,
    quote: u8,
    nulls: &[&'static str],
    null_bools: &mut [bool],
    null_count: &mut usize,
) -> io::Result<Array>
where
    T: super::int_ascii::ParseAsciiInt + Copy + Default + 'static,
{
    let mut out: Vec64<T> = vec64![T::default(); n_rows];
    for r in 0..n_rows {
        let raw = cell_raw(buf, field_starts, (r + data_row_offset) * n_cols + col_idx);
        let unq = unquote(raw, quote);
        let vb = trim_ascii(&unq);
        if is_null_bytes(vb, nulls) {
            null_bools[r] = false;
            *null_count += 1;
            continue;
        }
        out[r] = super::int_ascii::parse_ascii_int::<T>(vb)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "failed to parse integer"))?;
    }
    pack_numeric(out, null_bools)
}

/// Per-row inline float column builder. Uses fast-float2's
/// Eisel-Lemire fast path, the same algorithm Rust std uses since
/// 1.55.
#[allow(clippy::too_many_arguments)]
fn build_float_col_inline<T>(
    buf: &[u8],
    field_starts: &[u32],
    n_cols: usize,
    data_row_offset: usize,
    col_idx: usize,
    n_rows: usize,
    quote: u8,
    nulls: &[&'static str],
    null_bools: &mut [bool],
    null_count: &mut usize,
) -> io::Result<Array>
where
    T: fast_float2::FastFloat + Copy + Default + 'static,
{
    let mut out: Vec64<T> = vec64![T::default(); n_rows];
    for r in 0..n_rows {
        let raw = cell_raw(buf, field_starts, (r + data_row_offset) * n_cols + col_idx);
        let unq = unquote(raw, quote);
        let vb = trim_ascii(&unq);
        if is_null_bytes(vb, nulls) {
            null_bools[r] = false;
            *null_count += 1;
            continue;
        }
        out[r] = fast_float2::parse::<T, _>(vb)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "failed to parse float"))?;
    }
    pack_numeric(out, null_bools)
}

fn pack_numeric<T: 'static>(out: Vec64<T>, null_mask: &[bool]) -> io::Result<Array> {
    let mask = Some(mask_to_bitmask(null_mask));
    // SAFETY note for the six `transmute::<Vec64<T>, Vec64<U>>` calls
    // below: each branch is gated by `TypeId::of::<T>() == TypeId::of::<U>()`,
    // so the source and target Vec64s have the same `T = U` element type,
    // size, alignment, and validity invariants. The transmute is an identity
    // copy that the compiler cannot prove statically because T is generic.
    let arr = if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
        Array::NumericArray(NumericArray::Int32(
            IntegerArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<i32>>(out) }),
                null_mask: mask,
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
        Array::NumericArray(NumericArray::Int64(
            IntegerArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<i64>>(out) }),
                null_mask: mask,
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
        Array::NumericArray(NumericArray::UInt32(
            IntegerArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<u32>>(out) }),
                null_mask: mask,
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
        Array::NumericArray(NumericArray::UInt64(
            IntegerArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<u64>>(out) }),
                null_mask: mask,
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f32>() {
        Array::NumericArray(NumericArray::Float32(
            FloatArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<f32>>(out) }),
                null_mask: mask,
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f64>() {
        Array::NumericArray(NumericArray::Float64(
            FloatArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<f64>>(out) }),
                null_mask: mask,
            }
            .into(),
        ))
    } else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported numeric type",
        ));
    };
    Ok(arr)
}

#[allow(clippy::too_many_arguments)]
fn build_bool_col_inline(
    buf: &[u8],
    field_starts: &[u32],
    n_cols: usize,
    data_row_offset: usize,
    col_idx: usize,
    n_rows: usize,
    quote: u8,
    nulls: &[&'static str],
    null_bools: &mut [bool],
    null_count: &mut usize,
) -> io::Result<Array> {
    let mut out: Vec64<bool> = vec64![false; n_rows];
    for r in 0..n_rows {
        let raw = cell_raw(buf, field_starts, (r + data_row_offset) * n_cols + col_idx);
        let unq = unquote(raw, quote);
        let vb = trim_ascii(&unq);
        if is_null_bytes(vb, nulls) {
            null_bools[r] = false;
            *null_count += 1;
            continue;
        }
        out[r] = parse_bool(vb);
    }
    Ok(Array::BooleanArray(
        minarrow::BooleanArray::new(Bitmask::from_bools(&out), Some(mask_to_bitmask(null_bools)))
            .into(),
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_string_col_inline(
    buf: &[u8],
    field_starts: &[u32],
    n_cols: usize,
    data_row_offset: usize,
    col_idx: usize,
    n_rows: usize,
    quote: u8,
    nulls: &[&'static str],
    null_bools: &mut [bool],
    null_count: &mut usize,
) -> io::Result<Array> {
    let mut offsets: Vec64<u32> = vec64![0u32; n_rows + 1];
    // 8 bytes/row is a conservative starting guess; Vec64 doubles
    // from here if needed.
    let mut data: Vec64<u8> = Vec64::with_capacity(n_rows * 8);
    let mut pos: u32 = 0;
    for r in 0..n_rows {
        let raw = cell_raw(buf, field_starts, (r + data_row_offset) * n_cols + col_idx);
        let unq = unquote(raw, quote);
        // Strings keep leading/trailing whitespace; only trim for the
        // null check.
        let trimmed = trim_ascii(&unq);
        if is_null_bytes(trimmed, nulls) {
            null_bools[r] = false;
            *null_count += 1;
        } else {
            data.extend_from_slice(&unq);
            pos += unq.len() as u32;
        }
        offsets[r + 1] = pos;
    }
    Ok(Array::TextArray(TextArray::String32(
        minarrow::StringArray {
            offsets: Buffer::from(offsets),
            data: Buffer::from(data),
            null_mask: Some(mask_to_bitmask(null_bools)),
        }
        .into(),
    )))
}

#[cfg(not(feature = "default_categorical_8"))]
#[allow(clippy::too_many_arguments)]
fn build_categorical_col_inline(
    buf: &[u8],
    field_starts: &[u32],
    n_cols: usize,
    data_row_offset: usize,
    col_idx: usize,
    n_rows: usize,
    quote: u8,
    nulls: &[&'static str],
    null_bools: &mut [bool],
    null_count: &mut usize,
) -> io::Result<Array> {
    let mut uniques: Vec<String> = Vec::new();
    let mut dict: HashMap<String, u32> = HashMap::new();
    let mut codes: Vec64<u32> = vec64![0u32; n_rows];

    for r in 0..n_rows {
        let raw = cell_raw(buf, field_starts, (r + data_row_offset) * n_cols + col_idx);
        let unq = unquote(raw, quote);
        let trimmed = trim_ascii(&unq);
        if is_null_bytes(trimmed, nulls) {
            null_bools[r] = false;
            *null_count += 1;
            continue;
        }
        let s = String::from_utf8_lossy(&unq).into_owned();
        let code = if let Some(&idx) = dict.get(&s) {
            idx
        } else {
            let idx = uniques.len() as u32;
            dict.insert(s.clone(), idx);
            uniques.push(s);
            idx
        };
        codes[r] = code;
    }
    Ok(Array::TextArray(TextArray::Categorical32(
        minarrow::CategoricalArray {
            data: Buffer::from(codes),
            unique_values: uniques.into(),
            null_mask: Some(mask_to_bitmask(null_bools)),
        }
        .into(),
    )))
}

#[cfg(feature = "default_categorical_8")]
#[allow(clippy::too_many_arguments)]
fn build_categorical_col_inline(
    buf: &[u8],
    field_starts: &[u32],
    n_cols: usize,
    data_row_offset: usize,
    col_idx: usize,
    n_rows: usize,
    quote: u8,
    nulls: &[&'static str],
    null_bools: &mut [bool],
    null_count: &mut usize,
) -> io::Result<Array> {
    let mut uniques: Vec<String> = Vec::new();
    let mut dict: HashMap<String, u8> = HashMap::new();
    let mut codes: Vec64<u8> = vec64![0u8; n_rows];

    for r in 0..n_rows {
        let raw = cell_raw(buf, field_starts, (r + data_row_offset) * n_cols + col_idx);
        let unq = unquote(raw, quote);
        let trimmed = trim_ascii(&unq);
        if is_null_bytes(trimmed, nulls) {
            null_bools[r] = false;
            *null_count += 1;
            continue;
        }
        let s = String::from_utf8_lossy(&unq).into_owned();
        let code = if let Some(&idx) = dict.get(&s) {
            idx
        } else {
            let idx = uniques.len() as u8;
            dict.insert(s.clone(), idx);
            uniques.push(s);
            idx
        };
        codes[r] = code;
    }
    Ok(Array::TextArray(TextArray::Categorical8(
        minarrow::CategoricalArray {
            data: Buffer::from(codes),
            unique_values: uniques.into(),
            null_mask: Some(mask_to_bitmask(null_bools)),
        }
        .into(),
    )))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_decode_basic_csv() {
        let csv = b"ints,strings,bools\n1,hello,true\n2,,false\n3,world,1\n4,rust,0\n";
        let opts = CsvDecodeOptions::default();
        let table = decode_csv(Cursor::new(&csv[..]), &opts).unwrap();

        assert_eq!(table.n_rows, 4);
        assert_eq!(table.cols.len(), 3);
        assert_eq!(table.cols[0].field.name, "ints");
        assert_eq!(table.cols[1].field.name, "strings");

        // Int column: 1..4
        match &table.cols[0].array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                let vals: Vec64<_> = arr.data.as_ref().iter().copied().collect();
                assert_eq!(vals, vec64![1, 2, 3, 4]);
            }
            _ => panic!("wrong type"),
        }

        // Bool column
        match &table.cols[2].array {
            Array::BooleanArray(arr) => {
                let actual: Vec<bool> = (0..arr.data.len()).map(|i| arr.data.get(i)).collect();
                assert_eq!(actual, vec![true, false, true, false]);
            }
            _ => panic!("wrong type"),
        }

        // Nulls - strings column has values ["hello", "", "world", "rust"] where "" is null
        // So 3 valid values, 1 null -> count_ones() should be 3
        match &table.cols[1].array {
            Array::TextArray(TextArray::String32(arr)) => {
                assert_eq!(arr.null_mask.as_ref().unwrap().count_ones(), 3); // 3 valid, 1 null
                assert_eq!(table.cols[1].null_count, 1); // Verify null count is correct
            }
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn test_decode_csv_custom_delim_and_quotes() {
        let csv = b"i|s|b\n1|\"h|ello\"|T\n2||f\n";
        let mut opts = CsvDecodeOptions::default();
        opts.delimiter = b'|';
        let table = decode_csv(Cursor::new(&csv[..]), &opts).unwrap();
        assert_eq!(table.n_rows, 2);
        match &table.cols[1].array {
            Array::TextArray(TextArray::String32(arr)) => {
                let s = std::str::from_utf8(&arr.data.as_ref()[..]).unwrap();
                assert!(s.contains("h|ello"));
            }
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn test_decode_csv_batch_basic() {
        use std::io::Cursor;
        // simple 3-row CSV with header
        let csv = b"col1,col2\n10,A\n20,B\n30,C\n";
        let mut reader = Cursor::new(&csv[..]);
        let mut opts = CsvDecodeOptions::default();

        // first batch_size = 2 -> should return rows 10,A and 20,B
        let batch1 = decode_csv_batch(&mut reader, &opts, 2)
            .unwrap()
            .expect("first batch should be Some");
        assert_eq!(batch1.n_rows, 2);
        // header should be correctly carried through
        assert_eq!(batch1.cols[0].field.name, "col1");
        assert_eq!(batch1.cols[1].field.name, "col2");
        // check values
        match &batch1.cols[0].array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                let v: Vec<i32> = arr.data.as_ref().iter().copied().collect();
                assert_eq!(v, vec![10, 20]);
            }
            _ => panic!("wrong type for col1"),
        }
        match &batch1.cols[1].array {
            Array::TextArray(TextArray::String32(arr)) => {
                let s = std::str::from_utf8(&arr.data.as_ref()[..]).unwrap();
                assert!(s.starts_with("AB")); // "A" + "B"
            }
            _ => panic!("wrong type for col2"),
        }

        // turn off header for next batch so we don't try to re-consume it
        opts.has_header = false;
        let batch2 = decode_csv_batch(&mut reader, &opts, 2)
            .unwrap()
            .expect("second batch should be Some");
        // only one row remains
        assert_eq!(batch2.n_rows, 1);
        match &batch2.cols[0].array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                assert_eq!(arr.data.as_ref()[0], 30);
            }
            _ => panic!(),
        }

        // third call ->  no more rows -> None
        let batch3 = decode_csv_batch(&mut reader, &opts, 2).unwrap();
        assert!(batch3.is_none());
    }

    #[test]
    fn decode_escaped_quotes() {
        let csv = b"id,msg\n1,\"She said \"\"hi\"\" yesterday\"\n";
        let table = decode_csv(std::io::Cursor::new(csv.as_ref()), &Default::default()).unwrap();
        match &table.cols[1].array {
            Array::TextArray(TextArray::String32(arr)) => {
                let text = std::str::from_utf8(&arr.data.as_ref()[..]).unwrap();
                assert_eq!(text, "She said \"hi\" yesterday");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn decode_embedded_newline() {
        let csv = b"id,comment\n1,\"line1\nline2\"\n";
        // default parser should keep newline inside
        let tbl = decode_csv(std::io::Cursor::new(csv.as_ref()), &Default::default()).unwrap();
        match &tbl.cols[1].array {
            Array::TextArray(TextArray::String32(arr)) => {
                let text = std::str::from_utf8(&arr.data.as_ref()[..]).unwrap();
                assert_eq!(text, "line1\nline2");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn decode_with_explicit_schema() {
        use minarrow::{ArrowType, Field};
        let csv = b"a,b\n001,1.23\n";
        let schema = vec![
            Field::new("a", ArrowType::String, false, None),
            Field::new("b", ArrowType::Float64, false, None),
        ];
        let opts = CsvDecodeOptions {
            schema: Some(schema.clone()),
            ..Default::default()
        };
        let tbl = decode_csv(std::io::Cursor::new(csv.as_ref()), &opts).unwrap();
        assert_eq!(tbl.cols[0].field.dtype, ArrowType::String); // honoured
    }

    #[test]
    fn decode_no_header() {
        let csv = b"10,20\n30,40\n";
        let opts = CsvDecodeOptions {
            has_header: false,
            ..Default::default()
        };
        let t = decode_csv(std::io::Cursor::new(csv.as_ref()), &opts).unwrap();
        assert_eq!(t.cols[0].field.name, "col1");
        assert_eq!(t.n_rows, 2);
    }

    #[test]
    fn decode_no_trailing_newline() {
        let csv = b"a,b\n1,2\n3,4";
        let t = decode_csv(std::io::Cursor::new(csv.as_ref()), &Default::default()).unwrap();
        assert_eq!(t.n_rows, 2);
        match &t.cols[0].array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                assert_eq!(arr.data.as_ref(), &[1, 3]);
            }
            _ => panic!("wrong type"),
        }
    }

    #[test]
    fn decode_crlf_line_endings() {
        let csv = b"a,b\r\n1,2\r\n3,4\r\n";
        let t = decode_csv(std::io::Cursor::new(csv.as_ref()), &Default::default()).unwrap();
        assert_eq!(t.n_rows, 2);
        match &t.cols[0].array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                assert_eq!(arr.data.as_ref(), &[1, 3]);
            }
            _ => panic!("wrong type"),
        }
    }
}
