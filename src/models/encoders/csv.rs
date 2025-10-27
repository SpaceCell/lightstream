//! CSV Encoder for Minarrow Tables/SuperTables.
//! - Handles all supported types: Int32, Int64, UInt32, UInt64, Float32, Float64, Boolean, String32, Categorical32.
//! - Supports custom delimiter, header row, quoting, and null representation.
//! - Serialises a Table or SuperTable to any Write or Vec<u8>.

use minarrow::{Array, Bitmask, NumericArray, SuperTable, Table, TextArray};
use std::io::{self, Write};

use crate::debug_println;

/// Options for CSV encoding.
#[derive(Debug, Clone)]
pub struct CsvEncodeOptions {
    /// Delimiter (e.g., b',' for CSV, b'\t' for TSV).
    pub delimiter: u8,
    /// Whether to write header row.
    pub write_header: bool,
    /// String to represent nulls.
    pub null_repr: &'static str,
    /// Quote character to use (default: '"').
    pub quote: u8,
}

impl Default for CsvEncodeOptions {
    fn default() -> Self {
        CsvEncodeOptions {
            delimiter: b',',
            write_header: true,
            null_repr: "",
            quote: b'"',
        }
    }
}

#[inline]
fn needs_quotes(s: &str, delimiter: u8, quote: u8) -> bool {
    // Needs quotes if contains delimiter, quote char, newline, or leading/trailing whitespace
    s.as_bytes().contains(&delimiter)
        || s.as_bytes().contains(&quote)
        || s.contains('\n')
        || s.contains('\r')
        || s.starts_with(' ')
        || s.ends_with(' ')
}

#[inline]
fn escape_and_quote<'a>(s: &'a str, delimiter: u8, quote: u8) -> String {
    // If quoting is needed, escape quotes by doubling, wrap in quotes.
    if needs_quotes(s, delimiter, quote) {
        let mut out = Vec::with_capacity(s.len() + 2);
        out.push(quote);
        for &b in s.as_bytes() {
            if b == quote {
                out.push(quote);
            }
            out.push(b);
        }
        out.push(quote);
        unsafe { String::from_utf8_unchecked(out) }
    } else {
        s.to_string()
    }
}

/// Serialises a Minarrow `Table` (i.e., Arrow `RecordBatch`) to any `Write` as CSV.
/// - Supports custom delimiter, null representation, header.
/// - Escapes/quotes fields as needed.
/// - Errors propagate from writer.
///
/// # Arguments
/// - `table`: The Table to encode.
/// - `mut writer`: Any io::Write.
/// - `options`: Encoding options.
///
/// # Errors
/// Returns any io error from the writer.
pub fn encode_table_csv<W: Write>(
    table: &Table,
    mut writer: W,
    options: &CsvEncodeOptions,
) -> io::Result<()> {
    let CsvEncodeOptions {
        delimiter,
        write_header,
        null_repr,
        quote,
    } = *options;

    debug_println!(
        "Encoding Table to CSV: rows = {}, cols = {}",
        table.n_rows,
        table.cols.len()
    );

    // Write header
    if write_header {
        for (i, col) in table.cols.iter().enumerate() {
            if i > 0 {
                writer.write_all(&[delimiter])?;
            }
            let header = escape_and_quote(&col.field.name, delimiter, quote);
            writer.write_all(header.as_bytes())?;
        }
        writer.write_all(b"\n")?;
    }

    // Precompute null bitmasks for columns if present
    let mut null_masks: Vec<Option<&Bitmask>> = Vec::with_capacity(table.cols.len());
    for col in &table.cols {
        match &col.array {
            Array::NumericArray(arr) => null_masks.push(arr.null_mask()),
            Array::BooleanArray(arr) => null_masks.push(arr.null_mask.as_ref()),
            Array::TextArray(TextArray::String32(arr)) => null_masks.push(arr.null_mask.as_ref()),
            Array::TextArray(TextArray::Categorical32(arr)) => {
                null_masks.push(arr.null_mask.as_ref())
            }
            #[cfg(feature = "large_string")]
            Array::TextArray(TextArray::String64(arr)) => null_masks.push(arr.null_mask.as_ref()),
            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical8(arr)) => {
                null_masks.push(arr.null_mask.as_ref())
            }
            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical16(arr)) => {
                null_masks.push(arr.null_mask.as_ref())
            }
            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical64(arr)) => {
                null_masks.push(arr.null_mask.as_ref())
            }
            #[cfg(feature = "datetime")]
            Array::TemporalArray(arr) => {
                let null_mask = match arr {
                    minarrow::TemporalArray::Datetime32(arr) => arr.null_mask.as_ref(),
                    minarrow::TemporalArray::Datetime64(arr) => arr.null_mask.as_ref(),
                    minarrow::TemporalArray::Null => None,
                };
                null_masks.push(null_mask)
            }
            _ => null_masks.push(None),
        }
    }

    // Categorical - build unique value tables if needed for fast lookup
    let mut cat_maps: Vec<Option<&[String]>> = Vec::with_capacity(table.cols.len());
    for col in &table.cols {
        match &col.array {
            Array::TextArray(TextArray::Categorical32(arr)) => {
                cat_maps.push(Some(&arr.unique_values))
            }
            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical8(arr)) => {
                cat_maps.push(Some(&arr.unique_values))
            }
            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical16(arr)) => {
                cat_maps.push(Some(&arr.unique_values))
            }
            #[cfg(feature = "extended_categorical")]
            Array::TextArray(TextArray::Categorical64(arr)) => {
                cat_maps.push(Some(&arr.unique_values))
            }
            _ => cat_maps.push(None),
        }
    }

    for row in 0..table.n_rows {
        for (col_idx, col) in table.cols.iter().enumerate() {
            if col_idx > 0 {
                writer.write_all(&[delimiter])?;
            }
            // Null check - optimise for common case of no nulls
            let is_null = if col.null_count == 0 {
                false // Definitely no nulls, skip expensive mask operations
            } else {
                match null_masks[col_idx] {
                    Some(mask) => !mask.get(row), // 1=valid, 0=null
                    None => false,
                }
            };
            if is_null {
                writer.write_all(null_repr.as_bytes())?;
                continue;
            }
            match &col.array {
                Array::NumericArray(n) => match n {
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::Int8(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::Int16(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    NumericArray::Int32(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    NumericArray::Int64(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::UInt8(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::UInt16(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    NumericArray::UInt32(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    NumericArray::UInt64(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    NumericArray::Float32(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    NumericArray::Float64(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    _ => {
                        writer.write_all(b"<unsupported>")?;
                    }
                },
                Array::BooleanArray(arr) => {
                    let b = arr.data.get(row);
                    if b {
                        writer.write_all(b"true")?;
                    } else {
                        writer.write_all(b"false")?;
                    }
                }
                Array::TextArray(TextArray::String32(arr)) => {
                    let start = arr.offsets.as_ref()[row] as usize;
                    let mut end = arr.offsets.as_ref()[row + 1] as usize;
                    if row + 1 == arr.offsets.len() - 1 && end < arr.data.len() {
                        end = arr.data.len();
                    }
                    let raw = &arr.data.as_ref()[start..end];
                    // strip embedded NULs (only for full strings)
                    let s: String = raw
                        .iter()
                        .copied()
                        .filter(|&b| b != 0)
                        .map(|b| b as char)
                        .collect();
                    let q = escape_and_quote(&s, delimiter, quote);
                    writer.write_all(q.as_bytes())?;
                }
                #[cfg(feature = "large_string")]
                Array::TextArray(TextArray::String64(arr)) => {
                    let start = arr.offsets.as_ref()[row] as usize;
                    let mut end = arr.offsets.as_ref()[row + 1] as usize;
                    if row + 1 == arr.offsets.len() - 1 && end < arr.data.len() {
                        end = arr.data.len();
                    }
                    let raw = &arr.data.as_ref()[start..end];
                    // strip embedded NULs (only for full strings)
                    let s: String = raw
                        .iter()
                        .copied()
                        .filter(|&b| b != 0)
                        .map(|b| b as char)
                        .collect();
                    let q = escape_and_quote(&s, delimiter, quote);
                    writer.write_all(q.as_bytes())?;
                }
                Array::TextArray(TextArray::Categorical32(arr)) => {
                    // dictionary lookup—always a clean UTF-8
                    let idx = arr.data.as_ref()[row] as usize;
                    let val = arr
                        .unique_values
                        .get(idx)
                        .map(String::as_str)
                        .unwrap_or("<invalid>");
                    let q = escape_and_quote(val, delimiter, quote);
                    writer.write_all(q.as_bytes())?;
                }
                #[cfg(feature = "extended_categorical")]
                Array::TextArray(TextArray::Categorical8(arr)) => {
                    // dictionary lookup—always a clean UTF-8
                    let idx = arr.data.as_ref()[row] as usize;
                    let val = arr
                        .unique_values
                        .get(idx)
                        .map(String::as_str)
                        .unwrap_or("<invalid>");
                    let q = escape_and_quote(val, delimiter, quote);
                    writer.write_all(q.as_bytes())?;
                }
                #[cfg(feature = "extended_categorical")]
                Array::TextArray(TextArray::Categorical16(arr)) => {
                    // dictionary lookup—always a clean UTF-8
                    let idx = arr.data.as_ref()[row] as usize;
                    let val = arr
                        .unique_values
                        .get(idx)
                        .map(String::as_str)
                        .unwrap_or("<invalid>");
                    let q = escape_and_quote(val, delimiter, quote);
                    writer.write_all(q.as_bytes())?;
                }
                #[cfg(feature = "extended_categorical")]
                Array::TextArray(TextArray::Categorical64(arr)) => {
                    // dictionary lookup—always a clean UTF-8
                    let idx = arr.data.as_ref()[row] as usize;
                    let val = arr
                        .unique_values
                        .get(idx)
                        .map(String::as_str)
                        .unwrap_or("<invalid>");
                    let q = escape_and_quote(val, delimiter, quote);
                    writer.write_all(q.as_bytes())?;
                }
                #[cfg(feature = "datetime")]
                Array::TemporalArray(temp) => match temp {
                    minarrow::TemporalArray::Datetime32(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    minarrow::TemporalArray::Datetime64(arr) => {
                        let v = arr.data.as_ref()[row];
                        write!(writer, "{}", v)?;
                    }
                    minarrow::TemporalArray::Null => {
                        writer.write_all(b"<null_temporal>")?;
                    }
                },
                _ => {
                    writer.write_all(b"<unsupported>")?;
                }
            }
        }
        writer.write_all(b"\n")?;
    }

    Ok(())
}

/// Serialises a *Minarrow* `SuperTable` (i.e., *Arrow* multiple *RecordBatches*) as a CSV, with all batches concatenated.  
/// Each batch will write headers only if `write_header` is set and is the first batch.
/// Use for multi-batch output.
///
/// # Arguments
/// - `supertable`: The SuperTable to encode.
/// - `mut writer`: Any io::Write.
/// - `options`: Encoding options.
///
/// # Errors
/// Returns any io error from the writer.
pub fn encode_supertable_csv<W: Write>(
    supertable: &SuperTable,
    mut writer: W,
    options: &CsvEncodeOptions,
) -> io::Result<()> {
    let mut opts = options.clone();
    for (i, batch) in supertable.batches.iter().enumerate() {
        opts.write_header = if i == 0 { options.write_header } else { false };
        encode_table_csv(batch, &mut writer, &opts)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use minarrow::{
        Array, ArrowType, Bitmask, Buffer, Field, FieldArray, NumericArray, Table, TextArray, vec64,
    };

    use super::*;

    fn make_test_table() -> Table {
        let int_col = FieldArray {
            field: Field {
                name: "ints".to_string(),
                dtype: minarrow::ArrowType::Int32,
                nullable: true, // Change to true to allow nulls
                metadata: Default::default(),
            }
            .into(),
            array: Array::NumericArray(NumericArray::Int32(
                minarrow::IntegerArray {
                    data: Buffer::from(vec64![1, 2, 3, 4]),
                    null_mask: Some(Bitmask::from_bools(&[true, false, true, true])), // row 1 is null
                }
                .into(),
            )),
            null_count: 1,
        };
        let str_col = FieldArray {
            field: Field {
                name: "strings".to_string(),
                dtype: minarrow::ArrowType::String,
                nullable: true,
                metadata: Default::default(),
            }
            .into(),
            array: Array::TextArray(TextArray::String32(
                minarrow::StringArray {
                    offsets: Buffer::from(vec64![0u32, 5, 9, 14, 18]),
                    data: Buffer::from_vec64("helloabcdworldrust".as_bytes().into()),
                    null_mask: Some(Bitmask::from_bools(&[true, false, true, true])),
                }
                .into(),
            )),
            null_count: 1,
        };
        Table {
            name: "test".to_string(),
            cols: vec![int_col, str_col],
            n_rows: 4,
        }
    }

    #[test]
    fn test_encode_table_csv_basic() {
        let table = make_test_table();
        let mut out = Vec::new();
        let opts = CsvEncodeOptions::default();
        encode_table_csv(&table, &mut out, &opts).unwrap();
        let csv = String::from_utf8(out).unwrap();
        println!("CSV Output:\n{}", csv);
        assert!(csv.contains("ints,strings"));
        assert!(csv.contains("hello"));
        assert!(csv.contains("\n,\n"));
    }

    #[test]
    fn test_encode_table_csv_custom_delim() {
        let table = make_test_table();
        let mut out = Vec::new();
        let mut opts = CsvEncodeOptions::default();
        opts.delimiter = b'\t';
        encode_table_csv(&table, &mut out, &opts).unwrap();
        let csv = String::from_utf8(out).unwrap();
        assert!(csv.contains("\t"));
    }

    #[test]
    fn encode_quotes_field_with_delimiter() {
        use minarrow::{Array, Buffer, Field, FieldArray, NumericArray, Table, TextArray, vec64};

        use crate::models::encoders::csv::{CsvEncodeOptions, encode_table_csv};
        let col1 = FieldArray {
            field: Field::new("id", minarrow::ArrowType::Int32, false, None).into(),
            array: Array::NumericArray(NumericArray::Int32(
                minarrow::IntegerArray {
                    data: Buffer::from(vec64![1]),
                    null_mask: None,
                }
                .into(),
            )),
            null_count: 0,
        };

        let col2_str = "needs,quotes"; // contains delimiter
        let col2 = FieldArray {
            field: Field::new("txt", minarrow::ArrowType::String, false, None).into(),
            array: Array::TextArray(TextArray::String32(
                minarrow::StringArray {
                    offsets: Buffer::from(vec64![0u32, col2_str.len() as u32]),
                    data: Buffer::from_vec64(col2_str.as_bytes().into()),
                    null_mask: None,
                }
                .into(),
            )),
            null_count: 0,
        };

        let tbl = Table {
            name: "".into(),
            cols: vec![col1, col2],
            n_rows: 1,
        };
        let mut out = Vec::new();
        encode_table_csv(&tbl, &mut out, &CsvEncodeOptions::default()).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"needs,quotes\"")); // quoted and preserved
    }

    #[test]
    fn encode_decode_custom_null() {
        use crate::models::decoders::csv::*;
        use crate::models::encoders::csv::*;
        let mut opts = CsvEncodeOptions::default();
        opts.null_repr = "NULL";
        // build a 1-row table with a null value in the first column
        use minarrow::{
            Array, ArrowType, Bitmask, Field, FieldArray, IntegerArray, NumericArray, Table,
        };
        use std::sync::Arc;

        let field = Field {
            name: "int32".to_string(),
            dtype: ArrowType::Int32,
            nullable: true,
            metadata: Default::default(),
        };

        let null_mask = Bitmask::from_bytes(&[0b00000000], 1); // First bit is 0 = null
        let array = Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(minarrow::Vec64::from_slice(&[42i32])), // Value doesn't matter since it's null
            null_mask: Some(null_mask),
        })));

        let col = FieldArray::new(field, array);
        let tbl = Table {
            cols: vec![col],
            n_rows: 1,
            name: "test_null".to_string(),
        };
        let mut buf = Vec::new();
        encode_table_csv(&tbl, &mut buf, &opts).unwrap();

        // decode with matching null option
        let mut dec = CsvDecodeOptions::default();
        dec.nulls = vec!["NULL"];
        let parsed = decode_csv(std::io::Cursor::new(&buf), &dec).unwrap();
        assert_eq!(parsed.cols[0].null_count, 1);
    }

    #[test]
    fn test_csv_decoder_mask_semantics() {
        // Test that CSV decoder creates correct Arrow-semantic null masks
        use crate::models::decoders::csv::*;
        use minarrow::MaskedArray;

        let csv = b"col\nvalid\n\nvalid2\n"; // 2 valid, 1 null (empty string)
        let opts = CsvDecodeOptions::default();
        let table = decode_csv(std::io::Cursor::new(csv.as_ref()), &opts).unwrap();

        println!("Table decoded: {:?}", table);

        // Debug: what does the table think the null_count is?
        println!("Table null_count: {}", table.cols[0].null_count);

        // Check the actual mask bits
        if let Array::TextArray(TextArray::String32(arr)) = &table.cols[0].array {
            let mask = arr.null_mask.as_ref().unwrap();
            println!(
                "Mask: len={}, ones={}, zeros={}",
                mask.len(),
                mask.count_ones(),
                mask.count_zeros()
            );
            println!("Direct null_count call: {}", arr.null_count());

            // Check individual bits: [valid, null, valid] = [true, false, true]
            for i in 0..3 {
                println!("  Bit {}: {}", i, mask.get(i));
            }

            // The issue might be here - let's see what's happening
            println!(
                "count_zeros() = {}, count_ones() = {}",
                mask.count_zeros(),
                mask.count_ones()
            );

            // Don't assert yet, just investigate
        } else {
            panic!("Expected String32 array");
        }
    }

    #[test]
    fn test_null_mask_interpretation_mixed_nulls() {
        // Test with a mix of null and valid values to ensure mask interpretation is correct
        use minarrow::{
            Array, ArrowType, Bitmask, Field, FieldArray, IntegerArray, NumericArray, Table,
        };
        use std::sync::Arc;

        let field = Field {
            name: "mixed_nulls".to_string(),
            dtype: ArrowType::Int32,
            nullable: true,
            metadata: Default::default(),
        };

        // Create mask: [valid, null, valid, null] = [1, 0, 1, 0] = 0b0101 = 5
        let null_mask = Bitmask::from_bytes(&[0b00000101], 4);
        let array = Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(minarrow::Vec64::from_slice(&[10i32, 999i32, 30i32, 999i32])),
            null_mask: Some(null_mask),
        })));

        let col = FieldArray::new(field, array);
        let tbl = Table {
            cols: vec![col],
            n_rows: 4,
            name: "mixed_null_test".to_string(),
        };

        // Verify null_count is correct
        assert_eq!(tbl.cols[0].null_count, 2, "Expected 2 nulls");

        let mut opts = CsvEncodeOptions::default();
        opts.null_repr = "NULL";
        let mut buf = Vec::new();
        encode_table_csv(&tbl, &mut buf, &opts).unwrap();
        let csv_output = String::from_utf8(buf).unwrap();

        println!("Mixed nulls CSV: {}", csv_output);

        // Should be: "mixed_nulls\n10\nNULL\n30\nNULL\n"
        assert_eq!(csv_output, "mixed_nulls\n10\nNULL\n30\nNULL\n");
    }

    #[test]
    fn test_null_mask_interpretation_all_nulls() {
        // Test with all nulls to verify mask interpretation
        use minarrow::{
            Array, ArrowType, Bitmask, Field, FieldArray, IntegerArray, NumericArray, Table,
        };
        use std::sync::Arc;

        let field = Field {
            name: "all_nulls".to_string(),
            dtype: ArrowType::Int32,
            nullable: true,
            metadata: Default::default(),
        };

        // Create mask with all nulls: [0, 0, 0] = 0b000 = 0
        let null_mask = Bitmask::from_bytes(&[0b00000000], 3);
        let array = Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(minarrow::Vec64::from_slice(&[999i32, 999i32, 999i32])),
            null_mask: Some(null_mask),
        })));

        let col = FieldArray::new(field, array);
        let tbl = Table {
            cols: vec![col],
            n_rows: 3,
            name: "all_null_test".to_string(),
        };

        // Verify null_count is correct
        assert_eq!(tbl.cols[0].null_count, 3, "Expected 3 nulls");

        let mut opts = CsvEncodeOptions::default();
        opts.null_repr = "NULL";
        let mut buf = Vec::new();
        encode_table_csv(&tbl, &mut buf, &opts).unwrap();
        let csv_output = String::from_utf8(buf).unwrap();

        println!("All nulls CSV: {}", csv_output);

        // Should be: "all_nulls\nNULL\nNULL\nNULL\n"
        assert_eq!(csv_output, "all_nulls\nNULL\nNULL\nNULL\n");
    }

    #[test]
    fn categorical_roundtrip() {
        use crate::models::decoders::csv::*;
        use crate::models::encoders::csv::*;
        let csv = b"id,fruit\n1,apple\n2,banana\n3,apple\n";
        let mut opts = CsvDecodeOptions::default();
        opts.categorical_cols.insert("fruit".into());
        let tbl = decode_csv(std::io::Cursor::new(csv.as_ref()), &opts).unwrap();
        // println!("Rows parsed: {:?}", tbl);

        // ensure dictionary detected
        assert!(matches!(tbl.cols[1].field.dtype, ArrowType::Dictionary(_)));

        let mut out = Vec::new();
        encode_table_csv(&tbl, &mut out, &CsvEncodeOptions::default()).unwrap();
        let out_str = String::from_utf8(out).unwrap();
        println!("{:?}", out_str);
        assert!(out_str.contains("apple"));
        assert!(out_str.contains("banana"));
    }
}
