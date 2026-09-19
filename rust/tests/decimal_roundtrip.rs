// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Round-trip tests for decimal columns through IPC write, full read,
//! windowed read, and parquet type mapping.
//!
//! Each test constructs a table with one column per decimal width
//! (Decimal32, Decimal64, Decimal128) including null entries, writes it
//! through the IPC file protocol, reads it back, and asserts that the
//! storage values, null mask, precision and scale are preserved.

#![cfg(feature = "decimal")]

use std::sync::Arc;

use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::file_table::FileTableReader;
use lightstream::models::writers::ipc::sync_table::write_tables_to_file_sync;
use minarrow::{
    Array, ArrowType, Bitmask, DecimalArray, Field, FieldArray, NumericArray, Table,
};
use tempfile::NamedTempFile;

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

/// Decimal32 column: storage values [100, -250, 0, 9999], second entry null.
/// Precision 7, scale 2 (values represent 1.00, null, 0.00, 99.99).
fn decimal32_col() -> FieldArray {
    let mask = Bitmask::from_bools(&[true, false, true, true]);
    let arr = DecimalArray::<i32> {
        data: minarrow::Buffer::from(minarrow::Vec64::from_slice(&[100i32, -250, 0, 9999])),
        null_mask: Some(mask),
        precision: 7,
        scale: 2,
    };
    FieldArray::new(
        Field::new("dec32", ArrowType::Decimal32(7, 2), true, None),
        Array::NumericArray(NumericArray::Decimal32(Arc::new(arr))),
    )
}

/// Decimal64 column: storage values [123456789, -987654321, 0, 42],
/// third entry null. Precision 18, scale 4.
fn decimal64_col() -> FieldArray {
    let mask = Bitmask::from_bools(&[true, true, false, true]);
    let arr = DecimalArray::<i64> {
        data: minarrow::Buffer::from(minarrow::Vec64::from_slice(&[
            123456789i64,
            -987654321,
            0,
            42,
        ])),
        null_mask: Some(mask),
        precision: 18,
        scale: 4,
    };
    FieldArray::new(
        Field::new("dec64", ArrowType::Decimal64(18, 4), true, None),
        Array::NumericArray(NumericArray::Decimal64(Arc::new(arr))),
    )
}

/// Decimal128 column: storage values [10_000_000_000, -1, 0, 77777],
/// first entry null. Precision 38, scale 6.
fn decimal128_col() -> FieldArray {
    let mask = Bitmask::from_bools(&[false, true, true, true]);
    let arr = DecimalArray::<i128> {
        data: minarrow::Buffer::from(minarrow::Vec64::from_slice(&[
            10_000_000_000i128,
            -1,
            0,
            77777,
        ])),
        null_mask: Some(mask),
        precision: 38,
        scale: 6,
    };
    FieldArray::new(
        Field::new("dec128", ArrowType::Decimal128(38, 6), true, None),
        Array::NumericArray(NumericArray::Decimal128(Arc::new(arr))),
    )
}

/// Build a 4-row table holding one column of each decimal width.
fn decimal_table() -> Table {
    Table {
        cols: vec![decimal32_col(), decimal64_col(), decimal128_col()],
        n_rows: 4,
        name: "decimal_test".into(),
        ..Default::default()
    }
}

fn schema_for(table: &Table) -> Vec<Field> {
    table
        .cols
        .iter()
        .map(|c| (*c.field).clone())
        .collect()
}

// ---------------------------------------------------------------------------
// IPC file round-trip
// ---------------------------------------------------------------------------

#[test]
fn decimal_ipc_file_roundtrip() {
    let table = decimal_table();
    let schema = schema_for(&table);

    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();
    write_tables_to_file_sync(&path, std::slice::from_ref(&table), schema).unwrap();

    let reader = FileTableReader::open(&path).unwrap();
    assert_eq!(reader.num_batches(), 1);
    let mut got = reader.read_batch(0).unwrap();
    got.name = table.name.clone();

    assert_eq!(got.n_rows, 4);
    assert_eq!(got.cols.len(), 3);

    // Decimal32
    match &got.cols[0].array {
        Array::NumericArray(NumericArray::Decimal32(arr)) => {
            assert_eq!(arr.data.as_slice(), &[100i32, -250, 0, 9999]);
            assert_eq!(arr.precision, 7);
            assert_eq!(arr.scale, 2);
            let mask = arr.null_mask.as_ref().unwrap();
            assert!(mask.get(0));
            assert!(!mask.get(1));
            assert!(mask.get(2));
            assert!(mask.get(3));
        }
        other => panic!("Expected Decimal32, got {:?}", other),
    }
    assert_eq!(got.cols[0].field.dtype, ArrowType::Decimal32(7, 2));

    // Decimal64
    match &got.cols[1].array {
        Array::NumericArray(NumericArray::Decimal64(arr)) => {
            assert_eq!(
                arr.data.as_slice(),
                &[123456789i64, -987654321, 0, 42]
            );
            assert_eq!(arr.precision, 18);
            assert_eq!(arr.scale, 4);
            let mask = arr.null_mask.as_ref().unwrap();
            assert!(mask.get(0));
            assert!(mask.get(1));
            assert!(!mask.get(2));
            assert!(mask.get(3));
        }
        other => panic!("Expected Decimal64, got {:?}", other),
    }
    assert_eq!(got.cols[1].field.dtype, ArrowType::Decimal64(18, 4));

    // Decimal128
    match &got.cols[2].array {
        Array::NumericArray(NumericArray::Decimal128(arr)) => {
            assert_eq!(
                arr.data.as_slice(),
                &[10_000_000_000i128, -1, 0, 77777]
            );
            assert_eq!(arr.precision, 38);
            assert_eq!(arr.scale, 6);
            let mask = arr.null_mask.as_ref().unwrap();
            assert!(!mask.get(0));
            assert!(mask.get(1));
            assert!(mask.get(2));
            assert!(mask.get(3));
        }
        other => panic!("Expected Decimal128, got {:?}", other),
    }
    assert_eq!(got.cols[2].field.dtype, ArrowType::Decimal128(38, 6));
}

// ---------------------------------------------------------------------------
// IPC stream round-trip (in-memory)
// ---------------------------------------------------------------------------

#[test]
fn decimal_ipc_stream_roundtrip() {
    use lightstream::models::readers::ipc::table::TableReader;
    use lightstream::models::writers::ipc::table_stream::TableStreamWriter;
    use minarrow::Vec64;

    let table = decimal_table();
    let schema = schema_for(&table);

    let mut writer =
        TableStreamWriter::<Vec64<u8>>::new(schema.clone(), IPCMessageProtocol::Stream, None);
    writer.write(table.clone()).unwrap();
    writer.finish().unwrap();

    let mut all_bytes = Vec::new();
    while let Some(frame) = writer.next_frame() {
        all_bytes.extend_from_slice(frame.unwrap().as_ref());
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader: TableReader<Vec64<u8>> = TableReader::new(
        std::io::Cursor::new(all_bytes),
        8 * 1024,
        IPCMessageProtocol::Stream,
        None,
    );
    let tables = rt.block_on(reader.read_all_tables()).unwrap();
    assert_eq!(tables.len(), 1);

    let mut got = tables.into_iter().next().unwrap();
    got.name = table.name.clone();

    assert_eq!(got.n_rows, 4);
    assert_eq!(got.cols.len(), 3);

    // Verify Decimal32
    match &got.cols[0].array {
        Array::NumericArray(NumericArray::Decimal32(arr)) => {
            assert_eq!(arr.data.as_slice(), &[100i32, -250, 0, 9999]);
            assert_eq!(arr.precision, 7);
            assert_eq!(arr.scale, 2);
        }
        other => panic!("Expected Decimal32, got {:?}", other),
    }

    // Verify Decimal64
    match &got.cols[1].array {
        Array::NumericArray(NumericArray::Decimal64(arr)) => {
            assert_eq!(arr.data.as_slice(), &[123456789i64, -987654321, 0, 42]);
            assert_eq!(arr.precision, 18);
            assert_eq!(arr.scale, 4);
        }
        other => panic!("Expected Decimal64, got {:?}", other),
    }

    // Verify Decimal128
    match &got.cols[2].array {
        Array::NumericArray(NumericArray::Decimal128(arr)) => {
            assert_eq!(arr.data.as_slice(), &[10_000_000_000i128, -1, 0, 77777]);
            assert_eq!(arr.precision, 38);
            assert_eq!(arr.scale, 6);
        }
        other => panic!("Expected Decimal128, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Windowed read
// ---------------------------------------------------------------------------

#[test]
fn decimal_windowed_read() {
    // Write a 1024-row table so the mmap/file reader can window it.
    // Row count must be a multiple of 512 for the windowing constraint.
    let n = 1024usize;
    let vals: Vec<i32> = (0..n as i32).collect();
    let mask = Bitmask::from_bools(&(0..n).map(|i| i % 3 != 0).collect::<Vec<_>>());
    let arr = DecimalArray::<i32> {
        data: minarrow::Buffer::from(minarrow::Vec64::from_slice(&vals)),
        null_mask: Some(mask),
        precision: 9,
        scale: 3,
    };
    let table = Table {
        cols: vec![FieldArray::new(
            Field::new("dec", ArrowType::Decimal32(9, 3), true, None),
            Array::NumericArray(NumericArray::Decimal32(Arc::new(arr))),
        )],
        n_rows: n,
        name: "windowed".into(),
        ..Default::default()
    };
    let schema = schema_for(&table);

    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();
    write_tables_to_file_sync(&path, std::slice::from_ref(&table), schema).unwrap();

    let reader = FileTableReader::open(&path).unwrap();
    // Read window [512..1024)
    let window = reader.read_batch_window(0, 512, 512).unwrap();
    assert_eq!(window.n_rows, 512);

    match &window.cols[0].array {
        Array::NumericArray(NumericArray::Decimal32(arr)) => {
            assert_eq!(arr.data.len(), 512);
            assert_eq!(arr.data.as_slice()[0], 512i32);
            assert_eq!(*arr.data.as_slice().last().unwrap(), 1023i32);
            assert_eq!(arr.precision, 9);
            assert_eq!(arr.scale, 3);
            // Null mask should be present and correctly windowed
            let mask = arr.null_mask.as_ref().unwrap();
            assert_eq!(mask.len(), 512);
        }
        other => panic!("Expected Decimal32, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// CSV encoding of decimal values
// ---------------------------------------------------------------------------

#[cfg(feature = "csv")]
mod csv_encoding {
    use super::*;
    use lightstream::models::encoders::csv::{CsvEncodeOptions, encode_table_csv};

    #[test]
    fn decimal_csv_renders_scaled_values() {
        // Decimal64 with scale=2: [12345, -67890] -> "123.45", "-678.90"
        let arr = DecimalArray::<i64> {
            data: minarrow::Buffer::from(minarrow::Vec64::from_slice(&[12345i64, -67890])),
            null_mask: None,
            precision: 10,
            scale: 2,
        };
        let table = Table {
            cols: vec![FieldArray::new(
                Field::new("amount", ArrowType::Decimal64(10, 2), false, None),
                Array::NumericArray(NumericArray::Decimal64(Arc::new(arr))),
            )],
            n_rows: 2,
            name: "csv_test".into(),
            ..Default::default()
        };

        let mut out = Vec::new();
        encode_table_csv(&table, &mut out, &CsvEncodeOptions::default()).unwrap();
        let csv = String::from_utf8(out).unwrap();
        assert!(csv.contains("123.45"), "csv was: {csv}");
        assert!(csv.contains("-678.90"), "csv was: {csv}");
    }

    #[test]
    fn decimal_csv_scale_zero() {
        // Decimal32 with scale=0: values written as integers
        let arr = DecimalArray::<i32> {
            data: minarrow::Buffer::from(minarrow::Vec64::from_slice(&[42i32, 100])),
            null_mask: None,
            precision: 5,
            scale: 0,
        };
        let table = Table {
            cols: vec![FieldArray::new(
                Field::new("qty", ArrowType::Decimal32(5, 0), false, None),
                Array::NumericArray(NumericArray::Decimal32(Arc::new(arr))),
            )],
            n_rows: 2,
            name: "csv_test".into(),
            ..Default::default()
        };

        let mut out = Vec::new();
        encode_table_csv(&table, &mut out, &CsvEncodeOptions::default()).unwrap();
        let csv = String::from_utf8(out).unwrap();
        assert!(csv.contains("42"), "csv was: {csv}");
        assert!(csv.contains("100"), "csv was: {csv}");
    }

    #[test]
    fn decimal_csv_with_nulls() {
        let mask = Bitmask::from_bools(&[true, false, true]);
        let arr = DecimalArray::<i64> {
            data: minarrow::Buffer::from(minarrow::Vec64::from_slice(&[999i64, 0, 500])),
            null_mask: Some(mask),
            precision: 10,
            scale: 1,
        };
        let table = Table {
            cols: vec![FieldArray::new(
                Field::new("val", ArrowType::Decimal64(10, 1), true, None),
                Array::NumericArray(NumericArray::Decimal64(Arc::new(arr))),
            )],
            n_rows: 3,
            name: "csv_null_test".into(),
            ..Default::default()
        };

        let mut opts = CsvEncodeOptions::default();
        opts.null_repr = "NULL";
        let mut out = Vec::new();
        encode_table_csv(&table, &mut out, &opts).unwrap();
        let csv = String::from_utf8(out).unwrap();
        assert!(csv.contains("99.9"), "csv was: {csv}");
        assert!(csv.contains("NULL"), "csv was: {csv}");
        assert!(csv.contains("50.0"), "csv was: {csv}");
    }
}

// ---------------------------------------------------------------------------
// Multiple batches
// ---------------------------------------------------------------------------

#[test]
fn decimal_multi_batch_roundtrip() {
    use lightstream::models::writers::ipc::sync_table::SyncTableWriter;
    use minarrow::Vec64;

    let arr1 = DecimalArray::<i64> {
        data: minarrow::Buffer::from(Vec64::from_slice(&[100i64, 200, 300])),
        null_mask: None,
        precision: 10,
        scale: 2,
    };
    let arr2 = DecimalArray::<i64> {
        data: minarrow::Buffer::from(Vec64::from_slice(&[400i64, 500])),
        null_mask: None,
        precision: 10,
        scale: 2,
    };
    let field = Field::new("val", ArrowType::Decimal64(10, 2), false, None);
    let t1 = Table {
        cols: vec![FieldArray::new(
            field.clone(),
            Array::NumericArray(NumericArray::Decimal64(Arc::new(arr1))),
        )],
        n_rows: 3,
        name: "multi".into(),
        ..Default::default()
    };
    let t2 = Table {
        cols: vec![FieldArray::new(
            field.clone(),
            Array::NumericArray(NumericArray::Decimal64(Arc::new(arr2))),
        )],
        n_rows: 2,
        name: "multi".into(),
        ..Default::default()
    };

    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();
    let schema = vec![field];

    let mut writer = SyncTableWriter::<_, Vec64<u8>>::new(
        std::fs::File::create(&path).unwrap(),
        schema,
        IPCMessageProtocol::File,
        None,
    );
    writer.write_table(t1).unwrap();
    writer.write_table(t2).unwrap();
    writer.finish().unwrap();

    let reader = FileTableReader::open(&path).unwrap();
    assert_eq!(reader.num_batches(), 2);

    let b0 = reader.read_batch(0).unwrap();
    assert_eq!(b0.n_rows, 3);
    match &b0.cols[0].array {
        Array::NumericArray(NumericArray::Decimal64(arr)) => {
            assert_eq!(arr.data.as_slice(), &[100i64, 200, 300]);
            assert_eq!(arr.precision, 10);
            assert_eq!(arr.scale, 2);
        }
        other => panic!("Expected Decimal64, got {:?}", other),
    }

    let b1 = reader.read_batch(1).unwrap();
    assert_eq!(b1.n_rows, 2);
    match &b1.cols[0].array {
        Array::NumericArray(NumericArray::Decimal64(arr)) => {
            assert_eq!(arr.data.as_slice(), &[400i64, 500]);
            assert_eq!(arr.precision, 10);
            assert_eq!(arr.scale, 2);
        }
        other => panic!("Expected Decimal64, got {:?}", other),
    }
}
