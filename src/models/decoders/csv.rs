//! # CSV Decoder for Minarrow Tables
//!
//! - Accepts a CSV byte slice or any [`BufRead`].
//! - Infers schema or uses a provided schema (optional).
//! - Supports: `Int32`, `Int64`, `UInt32`, `UInt64`, `Float32`, `Float64`, `Boolean`, `String32`, `Categorical32`.
//! - Custom delimiter, nulls, quoting, and dictionary mapping for categoricals.
//! - Produces a single [`Table`] via [`decode_csv`], or multiple batches via repeated calls to [`decode_csv_batch`].
//! - No external dependencies.
//!
//! Notes:
//! - Input is treated as UTF-8; invalid byte sequences are lossily decoded via `String::from_utf8_lossy`.
//! - See [`CsvDecodeOptions`] for configurable delimiter, quoting, header handling, and schema control.

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
    let CsvDecodeOptions {
        delimiter,
        nulls,
        quote,
        has_header,
        schema,
        all_as_text,
        categorical_cols,
    } = options.clone();

    let mut header: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut buf = Vec::new();

    // --- Read and split lines (basic stateful CSV parser, no allocation on fields) ---
    let mut first_row_is_header = false;
    let mut col_count = 0;
    loop {
        buf.clear();
        let n = reader.read_until(b'\n', &mut buf)?;
        if n == 0 {
            break;
        }
        let mut quote_balance = buf.iter().filter(|&&b| b == quote).count() % 2;
        while quote_balance == 1
        /* we are inside an open quote */
        {
            let m = reader.read_until(b'\n', &mut buf)?;
            if m == 0 {
                break;
            } // EOF inside quotes -> let parse fail later
            quote_balance ^= buf[n..].iter().filter(|&&b| b == quote).count() % 2;
        }

        // Strip trailing \r\n or \n
        let line = {
            let l = if let Some(&b'\r') = buf.get(buf.len().saturating_sub(2)) {
                &buf[..buf.len() - 2]
            } else if buf.last() == Some(&b'\n') {
                &buf[..buf.len() - 1]
            } else {
                &buf[..]
            };
            l
        };

        if line.is_empty() && rows.is_empty() {
            continue;
        } // skip blank leading lines

        let fields = parse_csv_line(line, delimiter, quote);
        if fields.is_empty() {
            continue;
        }

        if header.is_empty() && has_header {
            // first non-blank line is the header
            header = fields;
            col_count = header.len();
            first_row_is_header = true;
        } else {
            // actual data rows
            if col_count == 0 {
                col_count = fields.len();
            }
            if fields.len() != col_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "inconsistent row length",
                ));
            }
            rows.push(fields);
        }
    }

    // Use header or default names
    let col_names: Vec<String> = if first_row_is_header {
        header
    } else {
        (0..col_count).map(|i| format!("col{}", i + 1)).collect()
    };

    let n_rows = rows.len();

    // --- Infer schema if needed ---
    let schema: Vec<Field> = if let Some(fields) = schema {
        fields
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
        infer_schema(&rows, &col_names, &categorical_cols, &nulls)
    };

    // --- Build columns ---
    let mut cols: Vec<FieldArray> = Vec::with_capacity(col_count);
    for (col_idx, field) in schema.iter().enumerate() {
        let mut null_mask = vec![true; n_rows]; // Arrow: true=valid, false=null
        let mut str_values: Vec<Option<&str>> = Vec::with_capacity(n_rows);

        for row in 0..n_rows {
            let val = rows[row][col_idx].trim();
            let is_null = nulls.iter().any(|n| n.eq_ignore_ascii_case(val));
            if is_null {
                null_mask[row] = false; // Arrow: false=null
                str_values.push(None);
            } else {
                str_values.push(Some(val));
            }
        }

        let array = match &field.dtype {
            ArrowType::Int32 => parse_numeric_column::<i32>(&str_values, &null_mask)?,
            ArrowType::Int64 => parse_numeric_column::<i64>(&str_values, &null_mask)?,
            ArrowType::UInt32 => parse_numeric_column::<u32>(&str_values, &null_mask)?,
            ArrowType::UInt64 => parse_numeric_column::<u64>(&str_values, &null_mask)?,
            ArrowType::Float32 => parse_numeric_column::<f32>(&str_values, &null_mask)?,
            ArrowType::Float64 => parse_numeric_column::<f64>(&str_values, &null_mask)?,
            ArrowType::Boolean => parse_bool_column(&str_values, &null_mask)?,
            ArrowType::String => parse_string_column(&str_values, &null_mask)?,
            ArrowType::Dictionary(_) => {
                // Always use categorical inference if requested or if declared
                parse_categorical_column(&str_values, &null_mask)?
            }
            _ => {
                // Fallback: treat as string
                parse_string_column(&str_values, &null_mask)?
            }
        };

        // Count false values in null_mask (false = null, true = valid)
        let null_count = null_mask.iter().filter(|x| !**x).count();

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
    })
}

/// Parse a CSV line into fields (no heap per field, only String vec return).
#[inline]
fn parse_csv_line(line: &[u8], delimiter: u8, quote: u8) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field = Vec::with_capacity(32);
    let mut in_quotes = false;
    let mut i = 0;
    while i < line.len() {
        let b = line[i];
        if in_quotes {
            if b == quote {
                if i + 1 < line.len() && line[i + 1] == quote {
                    // Escaped quote
                    field.push(quote);
                    i += 1;
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(b);
            }
        } else if b == quote {
            in_quotes = true;
        } else if b == delimiter {
            fields.push(String::from_utf8_lossy(&field).into_owned());
            field.clear();
        } else {
            field.push(b);
        }
        i += 1;
    }
    fields.push(String::from_utf8_lossy(&field).into_owned());
    fields
}

/// Infer schema from sampled rows, prefer smallest type supporting all data.
/// Dictionary if column is categorical.
fn infer_schema(
    rows: &[Vec<String>],
    col_names: &[String],
    categorical_cols: &HashSet<String>,
    nulls: &[&'static str],
) -> Vec<Field> {
    let n_cols = col_names.len();
    let mut types: Vec<ArrowType> = vec![ArrowType::String; n_cols];
    for col in 0..n_cols {
        let mut is_bool = true;
        let mut is_i32 = true;
        let mut is_i64 = true;
        let mut is_u32 = true;
        let mut is_u64 = true;
        let mut is_f32 = true;
        let mut is_f64 = true;
        let is_cat = categorical_cols.contains(&col_names[col]);

        for row in rows {
            let val = row[col].trim();
            if nulls.iter().any(|n| n.eq_ignore_ascii_case(val)) {
                continue;
            }
            if is_bool && !matches!(val, "true" | "false" | "1" | "0" | "t" | "f" | "T" | "F") {
                is_bool = false;
            }
            if is_i32 && val.parse::<i32>().is_err() {
                is_i32 = false;
            }
            if is_i64 && val.parse::<i64>().is_err() {
                is_i64 = false;
            }
            if is_u32 && val.parse::<u32>().is_err() {
                is_u32 = false;
            }
            if is_u64 && val.parse::<u64>().is_err() {
                is_u64 = false;
            }
            if is_f32 && val.parse::<f32>().is_err() {
                is_f32 = false;
            }
            if is_f64 && val.parse::<f64>().is_err() {
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
        } else if is_f32 {
            ArrowType::Float32
        } else if is_f64 {
            ArrowType::Float64
        } else if is_cat {
            ArrowType::Dictionary(CategoricalIndexType::UInt32)
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
}

// -- Column parsers --

fn mask_to_bitmask(mask: &[bool]) -> Bitmask {
    Bitmask::from_bools(mask)
}

// ------- Numeric (Integer/Floating) -------
fn parse_numeric_column<T: std::str::FromStr + Copy + Default + 'static>(
    values: &[Option<&str>],
    null_mask: &[bool],
) -> std::io::Result<Array> {
    let mut out = vec64![T::default(); values.len()];
    for (i, v) in values.iter().enumerate() {
        if null_mask[i] {
            // Arrow: true=valid
            out[i] = v.unwrap().parse::<T>().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "failed to parse number")
            })?;
        }
    }

    // Construct correct NumericArray variant for T
    let arr = if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
        Array::NumericArray(NumericArray::Int32(
            IntegerArray {
                data: Buffer::from(
                    // SAFETY: Cast is valid because T == i32
                    unsafe { std::mem::transmute::<Vec64<T>, Vec64<i32>>(out) },
                ),
                null_mask: Some(mask_to_bitmask(null_mask)),
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
        Array::NumericArray(NumericArray::Int64(
            IntegerArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<i64>>(out) }),
                null_mask: Some(mask_to_bitmask(null_mask)),
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
        Array::NumericArray(NumericArray::UInt32(
            IntegerArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<u32>>(out) }),
                null_mask: Some(mask_to_bitmask(null_mask)),
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
        Array::NumericArray(NumericArray::UInt64(
            IntegerArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<u64>>(out) }),
                null_mask: Some(mask_to_bitmask(null_mask)),
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f32>() {
        Array::NumericArray(NumericArray::Float32(
            FloatArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<f32>>(out) }),
                null_mask: Some(mask_to_bitmask(null_mask)),
            }
            .into(),
        ))
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f64>() {
        Array::NumericArray(NumericArray::Float64(
            FloatArray {
                data: Buffer::from(unsafe { std::mem::transmute::<Vec64<T>, Vec64<f64>>(out) }),
                null_mask: Some(mask_to_bitmask(null_mask)),
            }
            .into(),
        ))
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "unsupported numeric type",
        ));
    };

    Ok(arr)
}

// ------- Boolean -------
fn parse_bool_column(values: &[Option<&str>], null_mask: &[bool]) -> std::io::Result<Array> {
    let mut out = vec64![false; values.len()];
    for (i, v) in values.iter().enumerate() {
        if null_mask[i] {
            // Arrow: true=valid
            let s = v.unwrap().to_ascii_lowercase();
            out[i] = s == "true" || s == "1" || s == "t";
        }
    }
    Ok(Array::BooleanArray(
        minarrow::BooleanArray::new(Bitmask::from_bools(&out), Some(mask_to_bitmask(null_mask)))
            .into(),
    ))
}

fn parse_string_column(values: &[Option<&str>], null_mask: &[bool]) -> io::Result<Array> {
    let mut offsets = vec64![0u32; values.len() + 1];
    let mut data = Vec64::with_capacity(values.len() * 8);
    let mut pos = 0u32;
    for (i, v) in values.iter().enumerate() {
        if null_mask[i] {
            // Arrow: true=valid
            let s = v.unwrap().as_bytes();
            data.extend_from_slice(s);
            pos += s.len() as u32;
        }
        offsets[i + 1] = pos;
    }
    Ok(Array::TextArray(TextArray::String32(
        minarrow::StringArray {
            offsets: Buffer::from(offsets),
            data: Buffer::from(data),
            null_mask: Some(mask_to_bitmask(null_mask)),
        }
        .into(),
    )))
}

fn parse_categorical_column(values: &[Option<&str>], null_mask: &[bool]) -> io::Result<Array> {
    let mut uniques: Vec<String> = Vec::new();
    let mut dict: HashMap<&str, u32> = HashMap::new();
    let mut codes = vec64![0u32; values.len()];

    for (i, v) in values.iter().enumerate() {
        if !null_mask[i] {
            // Arrow: false=null, so skip nulls
            continue;
        }
        let s = v.unwrap();
        let code = if let Some(&idx) = dict.get(s) {
            idx
        } else {
            let idx = uniques.len() as u32;
            dict.insert(s, idx);
            uniques.push(s.to_string());
            idx
        };
        codes[i] = code;
    }
    Ok(Array::TextArray(TextArray::Categorical32(
        minarrow::CategoricalArray {
            data: Buffer::from(codes),
            unique_values: uniques.into(),
            null_mask: Some(mask_to_bitmask(null_mask)),
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
                let actual: Vec<bool> = (0..arr.data.len).map(|i| arr.data.get(i)).collect();
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
        // simple 3‐row CSV with header
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

        // turn off header for next batch so we don't try to re‐consume it
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
        use crate::models::decoders::csv::decode_csv;
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
        use crate::models::decoders::csv::decode_csv;
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
        use crate::models::decoders::csv::{CsvDecodeOptions, decode_csv};
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
        use crate::models::decoders::csv::{CsvDecodeOptions, decode_csv};
        let csv = b"10,20\n30,40\n";
        let opts = CsvDecodeOptions {
            has_header: false,
            ..Default::default()
        };
        let t = decode_csv(std::io::Cursor::new(csv.as_ref()), &opts).unwrap();
        assert_eq!(t.cols[0].field.name, "col1");
        assert_eq!(t.n_rows, 2);
    }
}
