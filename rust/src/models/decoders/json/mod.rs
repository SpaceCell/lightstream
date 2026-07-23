// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # JSON Decoder for Minarrow Tables
//!
//! Streams JSON into [`minarrow::Table`] from either:
//! - **Array-of-objects**: `[{"col_a": 1, "col_b": "x"}, ...]`
//! - **NDJSON** (newline-delimited): `{"col_a": 1, "col_b": "x"}\n...`
//!
//! Backed by `simd-json` via the [`simd`](crate::models::decoders::json::simd) module. Cells dispatch
//! directly into pre-allocated [`builder::ColumnBuilder`](crate::models::decoders::json::builder::ColumnBuilder) buffers - no
//! intermediate `Value` tree or per-cell allocations except when copying
//! string bytes into the column's data buffer.
//!
//! ## Schema
//! Schema is **required** - pass it via [`JsonDecodeOptions::schema`](crate::models::decoders::json::JsonDecodeOptions::schema). Schema
//! inference from sampled rows is a planned follow-up.
//!
//! ## Type mismatch handling
//! See [`builder::TypeMismatchPolicy`](crate::models::decoders::json::builder::TypeMismatchPolicy).

pub mod row_decoder;
pub mod value;
pub mod builder;
pub mod push;
pub mod simd;

use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead};
use std::sync::Arc;

use minarrow::{Consolidate, Field, Table};

use crate::models::decoders::json::row_decoder::JsonRowDecoder;
use crate::models::decoders::json::builder::{ColumnBuilder, TypeMismatchPolicy};
use crate::models::decoders::json::simd::TapeDecoder;

/// Options for JSON decoding.
#[derive(Debug, Clone)]
pub struct JsonDecodeOptions {
    /// Required schema for the resulting Table. JSON is parsed strictly into
    /// this layout - type inference is not performed.
    // TODO: optional schema inference via row sampling - follow-up phase.
    pub schema: Option<Vec<Field>>,
    /// Names that should be decoded as a dictionary/categorical column.
    /// Currently informational - the schema's [`ArrowType::Dictionary`](minarrow::ArrowType::Dictionary)
    /// already drives that branch.
    pub categorical_cols: HashSet<String>,
    /// Maximum bytes accumulated before forcing a parse. Lower = lower
    /// latency, less memory; higher = better throughput. Affects NDJSON
    /// streaming only; whole-array decode reads the entire input.
    pub max_chunk_bytes: usize,
    /// How to handle a JSON value whose type does not match the schema.
    pub on_type_mismatch: TypeMismatchPolicy,
    /// Initial capacity to reserve for each string column's data
    /// buffer, expressed as bytes-per-row. Tune up for columns with
    /// long string values to avoid reallocation; the default of 16
    /// suits short identifiers and labels.
    pub string_bytes_per_row: usize,
}

impl Default for JsonDecodeOptions {
    fn default() -> Self {
        JsonDecodeOptions {
            schema: None,
            categorical_cols: HashSet::new(),
            max_chunk_bytes: 1 << 20, // 1 MiB
            on_type_mismatch: TypeMismatchPolicy::Error,
            string_bytes_per_row: 16,
        }
    }
}

/// Decode a JSON array-of-objects from an in-memory byte slice.
///
/// simd-json mutates the input buffer in place during string unescape,
/// so the slice is `&mut [u8]`. Pass this directly when the bytes are
/// already in memory (e.g. from a memory-mapped file, a previously read
/// buffer, or a test fixture) to avoid the read-into-Vec round-trip
/// that [`decode_json`] does for `BufRead` sources.
///
/// The buffer's contents are modified by simd-json as a side effect; do
/// not parse the same buffer twice without re-populating it.
///
/// Inputs large enough to amortise the cost are split into N
/// object-boundary-aligned ranges by a single pre-pass, then parsed in
/// parallel via [`std::thread::scope`]; the per-thread Tables are
/// concatenated into one.
pub fn decode_json_slice(input: &mut [u8], options: &JsonDecodeOptions) -> io::Result<Table> {
    let schema = options
        .schema
        .as_ref()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "schema is required"))?;

    let n_threads = choose_array_thread_count(input.len());
    if n_threads <= 1 {
        return decode_array_buffer(input, schema, options);
    }

    let ranges = find_array_split_ranges(input, n_threads);
    if ranges.len() <= 1 {
        return decode_array_buffer(input, schema, options);
    }

    let tables = std::thread::scope(|s| -> io::Result<Vec<Table>> {
        let handles: Vec<_> = ranges
            .iter()
            .map(|&(start, end)| {
                let bytes = &input[start..end];
                s.spawn(move || -> io::Result<Table> {
                    // simd-json's tape parser needs a complete JSON
                    // document, so wrap the per-thread object range as
                    // `[obj_a,...,obj_b]`. The two-byte wrapper is the
                    // only allocation imposed by the split.
                    let mut buf = Vec::with_capacity(bytes.len() + 2);
                    buf.push(b'[');
                    buf.extend_from_slice(bytes);
                    buf.push(b']');
                    decode_array_buffer(&mut buf, schema, options)
                })
            })
            .collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            match h.join() {
                Ok(Ok(tbl)) => out.push(tbl),
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(io::Error::other(
                        "JSON array parser thread panicked",
                    ));
                }
            }
        }
        Ok(out)
    })?;

    Ok(tables.consolidate())
}

/// Parse a complete `[...]` JSON array document into one Table.
/// Allocates its own simd-json decoder, builders, and field map.
fn decode_array_buffer(
    input: &mut [u8],
    schema: &[Field],
    options: &JsonDecodeOptions,
) -> io::Result<Table> {
    let mut decoder = TapeDecoder::new();
    let mut builders = make_builders(
        schema,
        estimate_rows_array(input),
        options.string_bytes_per_row,
    )?;
    let field_map = make_field_map(schema);
    decoder.decode_rows(input, &mut builders, &field_map, options.on_type_mismatch)?;
    Ok(finish_table(schema, builders))
}

/// Pick the number of parallel parser threads for a JSON array.
fn choose_array_thread_count(input_len: usize) -> usize {
    // Array splitting requires a sequential pre-pass to find object
    // boundaries, so the size threshold is higher than NDJSON's.
    const MIN_FOR_PARALLEL: usize = 1 << 21; // 2 MiB
    if input_len < MIN_FOR_PARALLEL {
        return 1;
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(8)
}

/// Walk a `[obj_0,...,obj_{n-1}]` byte buffer and return up to
/// `n_threads` byte ranges, each covering a contiguous sequence of
/// complete top-level objects with no leading or trailing separator.
/// Returns an empty Vec when the input does not parse as an outer
/// array, or fewer than `n_threads` ranges when the array is too small
/// to split that many ways.
///
/// State machine tracks string and escape boundaries so braces inside
/// JSON string literals are ignored.
fn find_array_split_ranges(input: &[u8], n_threads: usize) -> Vec<(usize, usize)> {
    // Skip leading whitespace, expect `[`.
    let mut i = 0;
    while i < input.len() && input[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= input.len() || input[i] != b'[' {
        return Vec::new();
    }
    i += 1;

    let mut depth: i32 = 1;
    let mut in_string = false;
    let mut escape = false;
    let mut chunk_start: Option<usize> = None;
    let mut chunks: Vec<(usize, usize)> = Vec::with_capacity(n_threads);
    let mut threads_done = 0usize;
    let mut next_target = input.len() / n_threads;

    while i < input.len() && depth > 0 {
        let b = input[i];
        if escape {
            escape = false;
            i += 1;
            continue;
        }
        if in_string {
            match b {
                b'\\' => escape = true,
                b'"' => in_string = false,
                _ => {}
            }
            i += 1;
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' => {
                if depth == 1 && chunk_start.is_none() {
                    chunk_start = Some(i);
                }
                depth += 1;
            }
            b'[' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 1
                    && threads_done + 1 < n_threads
                    && i >= next_target
                    && chunk_start.is_some()
                {
                    let start = chunk_start.take().unwrap();
                    chunks.push((start, i + 1));
                    threads_done += 1;
                    next_target = input.len() * (threads_done + 1) / n_threads;
                }
            }
            b']' => depth -= 1,
            _ => {}
        }
        i += 1;
    }

    // Tail: whatever is left between the last split and the closing `]`.
    if let Some(start) = chunk_start {
        // `i` sits just after the byte that dropped `depth` to 0 - the
        // outer `]`. Walk back to the end of the last `}`.
        let mut end = i.saturating_sub(1);
        while end > start && input[end] != b'}' {
            end -= 1;
        }
        if input[end] == b'}' {
            chunks.push((start, end + 1));
        }
    }

    chunks
}

/// Decode a JSON array-of-objects from any `BufRead`.
///
/// Drains the reader into a `Vec<u8>` and forwards to
/// [`decode_json_slice`]. Prefer the slice form when the bytes are
/// already in memory.
///
/// `Vec<u8>` rather than `Vec64<u8>` for the read buffer: simd-json
/// memcpies the input into its own internal `AlignedBuf` (with required
/// trailing padding) on every parse regardless of the caller's
/// alignment, so 64-byte aligning the read buffer would not save any
/// copy on the parser side.
pub fn decode_json<R: BufRead>(mut reader: R, options: &JsonDecodeOptions) -> io::Result<Table> {
    let mut buf: Vec<u8> = Vec::new();
    reader.read_to_end(&mut buf)?;
    decode_json_slice(&mut buf, options)
}

/// Decode all NDJSON records from an in-memory byte slice.
///
/// Splits the input on newlines via memchr's SIMD byte search. For inputs
/// large enough to amortise the cost, the byte range is divided at
/// newline boundaries into N sub-ranges that each run on their own
/// thread via [`std::thread::scope`]; the resulting per-thread Tables
/// are then concatenated into one. Each thread reuses a
/// `max_chunk_bytes`-sized buffer so its simd-json passes stay
/// cache-resident.
///
/// For sources that arrive incrementally over a reader (and may exceed
/// available memory), use [`decode_ndjson`].
pub fn decode_ndjson_slice(input: &[u8], options: &JsonDecodeOptions) -> io::Result<Table> {
    let schema = options
        .schema
        .as_ref()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "schema is required"))?;

    let n_threads = choose_ndjson_thread_count(input.len(), options.max_chunk_bytes);
    if n_threads <= 1 {
        return decode_ndjson_range(input, schema, options);
    }

    let boundaries = split_ndjson_at_newlines(input, n_threads);
    let chunks: Vec<&[u8]> = boundaries
        .windows(2)
        .map(|w| &input[w[0]..w[1]])
        .filter(|c| !c.is_empty())
        .collect();

    if chunks.len() <= 1 {
        return decode_ndjson_range(input, schema, options);
    }

    let tables = std::thread::scope(|s| -> io::Result<Vec<Table>> {
        let handles: Vec<_> = chunks
            .iter()
            .map(|chunk| s.spawn(move || decode_ndjson_range(chunk, schema, options)))
            .collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            match h.join() {
                Ok(Ok(tbl)) => out.push(tbl),
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(io::Error::other(
                        "NDJSON parser thread panicked",
                    ));
                }
            }
        }
        Ok(out)
    })?;

    Ok(tables.consolidate())
}

/// Parse a single newline-aligned byte range into one Table. Caller
/// guarantees the range starts at a record boundary and ends after a
/// newline (or at end-of-input). Allocates its own simd-json decoder,
/// builders, and chunk buffer; safe to invoke from a worker thread.
fn decode_ndjson_range(
    input: &[u8],
    schema: &[Field],
    options: &JsonDecodeOptions,
) -> io::Result<Table> {
    let mut decoder = TapeDecoder::new();
    let mut builders = make_builders(schema, 0, options.string_bytes_per_row)?;
    let field_map = make_field_map(schema);

    let mut chunk: Vec<u8> = Vec::with_capacity(options.max_chunk_bytes);
    let mut line_count: usize = 0;
    let mut pos = 0;
    while pos < input.len() {
        let line_end = memchr::memchr(b'\n', &input[pos..])
            .map(|i| pos + i + 1)
            .unwrap_or(input.len());
        append_ndjson_line(&input[pos..line_end], &mut chunk, &mut line_count);
        pos = line_end;
        if chunk.len() >= options.max_chunk_bytes {
            flush_chunk(
                &mut chunk,
                &mut line_count,
                &mut decoder,
                &mut builders,
                &field_map,
                options.on_type_mismatch,
            )?;
        }
    }
    flush_chunk(
        &mut chunk,
        &mut line_count,
        &mut decoder,
        &mut builders,
        &field_map,
        options.on_type_mismatch,
    )?;

    Ok(finish_table(schema, builders))
}

/// Pick the number of parallel parser threads for an NDJSON payload.
///
/// Targets enough work per thread that simd-json's per-call setup is
/// amortised: each worker needs several `max_chunk_bytes` passes for
/// the parse loop to dominate its own allocation. Capped at the
/// detected core count.
fn choose_ndjson_thread_count(input_len: usize, max_chunk_bytes: usize) -> usize {
    // Below this size, parallel split-and-merge costs more than the
    // single-threaded parse saves. 4x the chunk budget gives every
    // worker at least a few passes' worth of bytes.
    let min_for_parallel = max_chunk_bytes.saturating_mul(4);
    if input_len < min_for_parallel {
        return 1;
    }
    let by_size = input_len / max_chunk_bytes.max(1);
    let by_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    by_size.min(by_cores).max(1)
}

/// Compute `n + 1` byte offsets that partition `input` into `n`
/// newline-aligned ranges. The first offset is 0, the last is
/// `input.len()`. Interior boundaries are snapped forward to the byte
/// after the next `\n`, so each range starts at a record boundary and
/// no record straddles two threads.
fn split_ndjson_at_newlines(input: &[u8], n: usize) -> Vec<usize> {
    let mut boundaries = Vec::with_capacity(n + 1);
    boundaries.push(0);
    let mut last = 0usize;
    for i in 1..n {
        let approx = (input.len() * i / n).max(last);
        let snap = memchr::memchr(b'\n', &input[approx..])
            .map(|j| approx + j + 1)
            .unwrap_or(input.len());
        boundaries.push(snap);
        last = snap;
    }
    boundaries.push(input.len());
    boundaries
}

/// Decode all NDJSON records from a `BufRead` into a single [`Table`].
///
/// Chunks lines up to `max_chunk_bytes` before each simd-json parse so
/// the input does not have to fit in memory. For in-memory inputs use
/// [`decode_ndjson_slice`] which avoids the per-line `read_until`
/// overhead.
pub fn decode_ndjson<R: BufRead>(mut reader: R, options: &JsonDecodeOptions) -> io::Result<Table> {
    let schema = options
        .schema
        .as_ref()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "schema is required"))?;

    let mut decoder = TapeDecoder::new();
    let mut builders = make_builders(schema, 0, options.string_bytes_per_row)?;
    let field_map = make_field_map(schema);

    let mut chunk: Vec<u8> = Vec::with_capacity(options.max_chunk_bytes);
    let mut line_count: usize = 0;
    let mut line: Vec<u8> = Vec::with_capacity(4096);

    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if !append_ndjson_line(&line, &mut chunk, &mut line_count) {
            continue;
        }
        if chunk.len() >= options.max_chunk_bytes {
            flush_chunk(
                &mut chunk,
                &mut line_count,
                &mut decoder,
                &mut builders,
                &field_map,
                options.on_type_mismatch,
            )?;
        }
    }
    flush_chunk(
        &mut chunk,
        &mut line_count,
        &mut decoder,
        &mut builders,
        &field_map,
        options.on_type_mismatch,
    )?;

    Ok(finish_table(schema, builders))
}

/// Read up to `batch_size` NDJSON records (or `max_chunk_bytes` worth, whichever
/// trips first) and decode them into a [`Table`]. Returns `Ok(None)` at end-of-stream.
pub fn decode_ndjson_batch<R: BufRead>(
    reader: &mut R,
    options: &JsonDecodeOptions,
    batch_size: usize,
) -> io::Result<Option<Table>> {
    let schema = options
        .schema
        .as_ref()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "schema is required"))?;

    let mut decoder = TapeDecoder::new();
    let mut chunk: Vec<u8> = Vec::with_capacity(options.max_chunk_bytes);
    let mut line_count: usize = 0;
    let mut line: Vec<u8> = Vec::with_capacity(4096);

    while line_count < batch_size && chunk.len() < options.max_chunk_bytes {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        append_ndjson_line(&line, &mut chunk, &mut line_count);
    }
    if line_count == 0 {
        return Ok(None);
    }

    let mut builders = make_builders(schema, line_count, options.string_bytes_per_row)?;
    let field_map = make_field_map(schema);
    flush_chunk(
        &mut chunk,
        &mut line_count,
        &mut decoder,
        &mut builders,
        &field_map,
        options.on_type_mismatch,
    )?;
    Ok(Some(finish_table(schema, builders)))
}

/// Trim a raw NDJSON line and append it to `chunk` with a leading `,` separator
/// when needed. Wraps the chunk in `[...]` lazily on first append. Returns
/// false for blank/whitespace-only lines.
pub(crate) fn append_ndjson_line(line: &[u8], chunk: &mut Vec<u8>, line_count: &mut usize) -> bool {
    let mut start = 0usize;
    let mut end = line.len();
    while end > start && matches!(line[end - 1], b'\n' | b'\r') {
        end -= 1;
    }
    while start < end && line[start].is_ascii_whitespace() {
        start += 1;
    }
    if start == end {
        return false;
    }
    if chunk.is_empty() {
        chunk.push(b'[');
    } else {
        chunk.push(b',');
    }
    chunk.extend_from_slice(&line[start..end]);
    *line_count += 1;
    true
}

/// Close the chunk array and feed it to the decoder. Resets `chunk` and
/// `line_count` for the next pass.
fn flush_chunk(
    chunk: &mut Vec<u8>,
    line_count: &mut usize,
    decoder: &mut TapeDecoder,
    builders: &mut [ColumnBuilder],
    field_map: &HashMap<&str, usize>,
    policy: TypeMismatchPolicy,
) -> io::Result<()> {
    if *line_count == 0 {
        chunk.clear();
        return Ok(());
    }
    chunk.push(b']');
    decoder.decode_rows(chunk.as_mut_slice(), builders, field_map, policy)?;
    chunk.clear();
    *line_count = 0;
    Ok(())
}

/// Map field names to column indices for O(1) key dispatch.
pub(crate) fn make_field_map(schema: &[Field]) -> HashMap<&str, usize> {
    schema
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name.as_str(), i))
        .collect()
}

/// Build one [`ColumnBuilder`] per schema field, reserving capacity for
/// `n_rows`. When `n_rows` is unknown, pass 0 - the builders grow on demand.
pub(crate) fn make_builders(
    schema: &[Field],
    n_rows: usize,
    string_bytes_per_row: usize,
) -> io::Result<Vec<ColumnBuilder>> {
    let mut out = Vec::with_capacity(schema.len());
    for field in schema {
        out.push(ColumnBuilder::for_field(
            field,
            n_rows,
            string_bytes_per_row,
        )?);
    }
    Ok(out)
}

/// Wrap finished builders in a [`Table`] tagged with the schema's fields.
pub(crate) fn finish_table(schema: &[Field], builders: Vec<ColumnBuilder>) -> Table {
    let cols = schema
        .iter()
        .zip(builders)
        .map(|(field, builder)| builder.finish(Arc::new(field.clone())))
        .collect();
    Table::new("json".to_string(), Some(cols))
}

/// Row-count estimate for a JSON array-of-objects, used to reserve
/// builder capacity. Counts top-level `{` characters by tracking brace
/// depth, ignoring braces that appear inside string literals or behind
/// a `\` escape. Treats this as an estimate only - misformed input
/// produces a misleading count but never panics or overcounts wildly.
fn estimate_rows_array(buf: &[u8]) -> usize {
    let mut depth = 0i32;
    let mut count = 0usize;
    let mut in_string = false;
    let mut escape = false;
    for &b in buf {
        if escape {
            // Skip whatever this escape consumes - we only care about
            // structural braces, and the next byte cannot be one.
            escape = false;
            continue;
        }
        if in_string {
            match b {
                b'\\' => escape = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' => {
                if depth == 0 {
                    count += 1;
                }
                depth += 1;
            }
            b'}' => depth -= 1,
            _ => {}
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use minarrow::{ArrowType, Field};

    fn schema() -> Vec<Field> {
        vec![
            Field::new("i", ArrowType::Int32, false, None),
            Field::new("s", ArrowType::String, true, None),
            Field::new("b", ArrowType::Boolean, false, None),
        ]
    }

    #[test]
    fn decode_array_basic() {
        let json = br#"[{"i":1,"s":"hello","b":true},{"i":2,"s":"world","b":false}]"#;
        let opts = JsonDecodeOptions {
            schema: Some(schema()),
            ..Default::default()
        };
        let tbl = decode_json(io::Cursor::new(&json[..]), &opts).unwrap();
        assert_eq!(tbl.n_rows, 2);
        assert_eq!(tbl.cols.len(), 3);
    }

    #[test]
    fn decode_ndjson_basic() {
        let json = b"{\"i\":1,\"s\":\"hello\",\"b\":true}\n{\"i\":2,\"s\":\"world\",\"b\":false}\n";
        let opts = JsonDecodeOptions {
            schema: Some(schema()),
            ..Default::default()
        };
        let tbl = decode_ndjson(io::Cursor::new(&json[..]), &opts).unwrap();
        assert_eq!(tbl.n_rows, 2);
    }

    #[test]
    fn missing_schema_errors() {
        let json = br#"[{"i":1}]"#;
        let opts = JsonDecodeOptions::default();
        let err = decode_json(io::Cursor::new(&json[..]), &opts).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn split_array_basic() {
        let json = br#"[{"i":1},{"i":2},{"i":3},{"i":4}]"#;
        let ranges = find_array_split_ranges(json, 2);
        // Each range must reparse to a valid `[ ... ]` array.
        assert!(ranges.len() >= 2);
        let mut total_rows = 0;
        for (start, end) in &ranges {
            let slice = &json[*start..*end];
            let mut wrapped = Vec::with_capacity(slice.len() + 2);
            wrapped.push(b'[');
            wrapped.extend_from_slice(slice);
            wrapped.push(b']');
            let v = simd_json::to_owned_value(&mut wrapped).unwrap();
            use simd_json::prelude::ValueAsArray;
            total_rows += v.as_array().unwrap().len();
        }
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn split_array_ignores_braces_inside_strings() {
        // The string value contains `{` and `,` that must not confuse
        // the depth tracker.
        let json = br#"[{"s":"a,{b}c"},{"s":"x"},{"s":"y"}]"#;
        let ranges = find_array_split_ranges(json, 3);
        let mut total_rows = 0;
        for (start, end) in &ranges {
            let slice = &json[*start..*end];
            let mut wrapped = Vec::with_capacity(slice.len() + 2);
            wrapped.push(b'[');
            wrapped.extend_from_slice(slice);
            wrapped.push(b']');
            let v = simd_json::to_owned_value(&mut wrapped).unwrap();
            use simd_json::prelude::ValueAsArray;
            total_rows += v.as_array().unwrap().len();
        }
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn split_array_handles_escapes() {
        // Backslash-quote inside the string must not close the string
        // early; backslash-brace must not increment depth.
        let json = br#"[{"s":"a\"b"},{"s":"c"}]"#;
        let ranges = find_array_split_ranges(json, 2);
        let mut total_rows = 0;
        for (start, end) in &ranges {
            let slice = &json[*start..*end];
            let mut wrapped = Vec::with_capacity(slice.len() + 2);
            wrapped.push(b'[');
            wrapped.extend_from_slice(slice);
            wrapped.push(b']');
            let v = simd_json::to_owned_value(&mut wrapped).unwrap();
            use simd_json::prelude::ValueAsArray;
            total_rows += v.as_array().unwrap().len();
        }
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn split_array_handles_nested_structures() {
        // Nested arrays and objects inside values must not split mid-record.
        let json = br#"[{"a":[1,2,3]},{"a":{"b":1}},{"a":null}]"#;
        let ranges = find_array_split_ranges(json, 3);
        let mut total_rows = 0;
        for (start, end) in &ranges {
            let slice = &json[*start..*end];
            let mut wrapped = Vec::with_capacity(slice.len() + 2);
            wrapped.push(b'[');
            wrapped.extend_from_slice(slice);
            wrapped.push(b']');
            let v = simd_json::to_owned_value(&mut wrapped).unwrap();
            use simd_json::prelude::ValueAsArray;
            total_rows += v.as_array().unwrap().len();
        }
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn ndjson_batch_drains_correctly() {
        let json = b"{\"i\":1,\"s\":\"a\",\"b\":true}\n{\"i\":2,\"s\":\"b\",\"b\":false}\n{\"i\":3,\"s\":\"c\",\"b\":true}\n";
        let mut reader = io::Cursor::new(&json[..]);
        let opts = JsonDecodeOptions {
            schema: Some(schema()),
            ..Default::default()
        };
        let b1 = decode_ndjson_batch(&mut reader, &opts, 2).unwrap().unwrap();
        assert_eq!(b1.n_rows, 2);
        let b2 = decode_ndjson_batch(&mut reader, &opts, 2).unwrap().unwrap();
        assert_eq!(b2.n_rows, 1);
        let b3 = decode_ndjson_batch(&mut reader, &opts, 2).unwrap();
        assert!(b3.is_none());
    }
}
