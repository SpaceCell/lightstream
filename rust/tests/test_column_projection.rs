// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Column projection tests for Arrow IPC and Parquet file readers.
//!
//! Verifies that `read_batch_cols` / `load_parquet_table_cols` materialise only
//! the requested columns while preserving correct values, row counts, and
//! schema ordering.

#[cfg(feature = "mmap")]
use lightstream::models::readers::ipc::mmap_table::MmapTableReader;

use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::file_table::FileTableReader;
use lightstream::models::writers::ipc::table::TableWriter;
use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{
    Array, Buffer, Field, FieldArray, FloatArray, IntegerArray, NumericArray, StringArray, Table,
    TextArray, Vec64,
};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::fs::File;

/// Build a test table with int32, float64, string32, and bool columns.
fn make_test_table() -> Table {
    let int_col = FieldArray::new(
        Field::new("id", ArrowType::Int32, false, None),
        Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(Vec64::from_slice(&[10, 20, 30])),
            null_mask: None,
        }))),
    );
    let float_col = FieldArray::new(
        Field::new("score", ArrowType::Float64, false, None),
        Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
            data: Buffer::from(Vec64::from_slice(&[1.5, 2.5, 3.5])),
            null_mask: None,
        }))),
    );
    let str_col = FieldArray::new(
        Field::new("name", ArrowType::String, false, None),
        Array::TextArray(TextArray::String32(Arc::new(StringArray::from_slice(&[
            "alice", "bob", "charlie",
        ])))),
    );
    let bool_col = FieldArray::new(
        Field::new("active", ArrowType::Boolean, false, None),
        Array::BooleanArray(Arc::new(minarrow::BooleanArray::from_slice(&[
            true, false, true,
        ]))),
    );
    Table {
        cols: vec![int_col, float_col, str_col, bool_col],
        n_rows: 3,
        name: "test".into(),
        ..Default::default()
    }
}

/// Write a table to a temp Arrow IPC file and return the handle.
async fn write_to_file(table: &Table) -> NamedTempFile {
    let temp = NamedTempFile::new().unwrap();
    let schema: Vec<Field> = table
        .cols
        .iter()
        .map(|c| c.field.as_ref().clone())
        .collect();
    let file = File::create(temp.path()).await.unwrap();
    let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File, None).unwrap();
    writer.write_table(table.clone()).await.unwrap();
    writer.finish().await.unwrap();
    temp
}

// ── FileTableReader ──────────────────────────────────────────────

#[tokio::test]
async fn file_read_batch_cols_single_numeric() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = FileTableReader::open(temp.path()).unwrap();

    let projected = rdr.read_batch_cols(0, &["id"]).unwrap();
    assert_eq!(projected.n_rows, 3);
    assert_eq!(projected.cols.len(), 1);
    assert_eq!(projected.cols[0].field.name, "id");
    match &projected.cols[0].array {
        Array::NumericArray(NumericArray::Int32(arr)) => {
            let vals: Vec<i32> = arr.data.as_ref().iter().copied().collect();
            assert_eq!(vals, vec![10, 20, 30]);
        }
        _ => panic!("expected Int32"),
    }
}

#[tokio::test]
async fn file_read_batch_cols_multiple_in_schema_order() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = FileTableReader::open(temp.path()).unwrap();

    // Request in reverse order - result should still be schema order
    let projected = rdr.read_batch_cols(0, &["active", "id"]).unwrap();
    assert_eq!(projected.n_rows, 3);
    assert_eq!(projected.cols.len(), 2);
    assert_eq!(projected.cols[0].field.name, "id");
    assert_eq!(projected.cols[1].field.name, "active");
}

#[tokio::test]
async fn file_read_batch_cols_with_string() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = FileTableReader::open(temp.path()).unwrap();

    let projected = rdr.read_batch_cols(0, &["name"]).unwrap();
    assert_eq!(projected.cols.len(), 1);
    match &projected.cols[0].array {
        Array::TextArray(TextArray::String32(arr)) => {
            let vals: Vec<&str> = arr.iter_str().collect();
            assert_eq!(vals, vec!["alice", "bob", "charlie"]);
        }
        _ => panic!("expected String32"),
    }
}

#[tokio::test]
async fn file_read_batch_cols_unknown_name_errors() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = FileTableReader::open(temp.path()).unwrap();

    let err = rdr.read_batch_cols(0, &["nonexistent"]).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
async fn file_read_batch_cols_all_equals_read_batch() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = FileTableReader::open(temp.path()).unwrap();

    let all_names: Vec<&str> = rdr.schema().iter().map(|f| f.name.as_str()).collect();
    let projected = rdr.read_batch_cols(0, &all_names).unwrap();
    let full = rdr.read_batch(0).unwrap();

    assert_eq!(projected.n_rows, full.n_rows);
    assert_eq!(projected.cols.len(), full.cols.len());
    for (p, f) in projected.cols.iter().zip(full.cols.iter()) {
        assert_eq!(p.field.name, f.field.name);
    }
}

// ── MmapTableReader ──────────────────────────────────────────────

#[tokio::test]
#[cfg(feature = "mmap")]
async fn mmap_read_batch_cols_single_numeric() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = MmapTableReader::open(temp.path()).unwrap();

    let projected = rdr.read_batch_cols(0, &["score"]).unwrap();
    assert_eq!(projected.n_rows, 3);
    assert_eq!(projected.cols.len(), 1);
    assert_eq!(projected.cols[0].field.name, "score");
    match &projected.cols[0].array {
        Array::NumericArray(NumericArray::Float64(arr)) => {
            let vals: Vec<f64> = arr.data.as_ref().iter().copied().collect();
            assert_eq!(vals, vec![1.5, 2.5, 3.5]);
        }
        _ => panic!("expected Float64"),
    }
}

#[tokio::test]
#[cfg(feature = "mmap")]
async fn mmap_read_batch_cols_multiple_in_schema_order() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = MmapTableReader::open(temp.path()).unwrap();

    let projected = rdr.read_batch_cols(0, &["name", "id"]).unwrap();
    assert_eq!(projected.cols.len(), 2);
    assert_eq!(projected.cols[0].field.name, "id");
    assert_eq!(projected.cols[1].field.name, "name");
}

#[tokio::test]
#[cfg(feature = "mmap")]
async fn mmap_read_batch_cols_with_string() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = MmapTableReader::open(temp.path()).unwrap();

    let projected = rdr.read_batch_cols(0, &["name"]).unwrap();
    assert_eq!(projected.cols.len(), 1);
    match &projected.cols[0].array {
        Array::TextArray(TextArray::String32(arr)) => {
            let vals: Vec<&str> = arr.iter_str().collect();
            assert_eq!(vals, vec!["alice", "bob", "charlie"]);
        }
        _ => panic!("expected String32"),
    }
}

#[tokio::test]
#[cfg(feature = "mmap")]
async fn mmap_read_batch_cols_unknown_name_errors() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = MmapTableReader::open(temp.path()).unwrap();

    let err = rdr.read_batch_cols(0, &["nonexistent"]).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
#[cfg(feature = "mmap")]
async fn mmap_read_batch_cols_all_equals_read_batch() {
    let table = make_test_table();
    let temp = write_to_file(&table).await;
    let rdr = MmapTableReader::open(temp.path()).unwrap();

    let all_names: Vec<&str> = rdr.schema().iter().map(|f| f.name.as_str()).collect();
    let projected = rdr.read_batch_cols(0, &all_names).unwrap();
    let full = rdr.read_batch(0).unwrap();

    assert_eq!(projected.n_rows, full.n_rows);
    assert_eq!(projected.cols.len(), full.cols.len());
    for (p, f) in projected.cols.iter().zip(full.cols.iter()) {
        assert_eq!(p.field.name, f.field.name);
    }
}

// ── Parquet ──────────────────────────────────────────────────────

#[cfg(feature = "parquet")]
mod parquet {
    use lightstream::models::readers::parquet::{load_parquet_table_cols, load_parquet_table};
    use lightstream::models::writers::parquet::write_parquet_table;
    use minarrow::{
        Array, ArrowType, Buffer, Field, FieldArray, FloatArray, IntegerArray, NumericArray,
        StringArray, Table, TextArray, Vec64,
    };
    use std::io::{Cursor, Seek, SeekFrom};
    use std::sync::Arc;

    fn make_parquet_table() -> Table {
        let int_col = FieldArray::new(
            Field::new("id", ArrowType::Int32, false, None),
            Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
                data: Buffer::from(Vec64::from_slice(&[10, 20, 30])),
                null_mask: None,
            }))),
        );
        let float_col = FieldArray::new(
            Field::new("score", ArrowType::Float64, false, None),
            Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
                data: Buffer::from(Vec64::from_slice(&[1.5, 2.5, 3.5])),
                null_mask: None,
            }))),
        );
        let str_col = FieldArray::new(
            Field::new("name", ArrowType::String, false, None),
            Array::TextArray(TextArray::String32(Arc::new(StringArray::from_slice(&[
                "alice", "bob", "charlie",
            ])))),
        );
        Table {
            cols: vec![int_col, float_col, str_col],
            n_rows: 3,
            name: "test".into(),
            ..Default::default()
        }
    }

    fn write_and_rewind(table: &Table) -> Cursor<Vec<u8>> {
        let mut buf = Cursor::new(Vec::new());
        write_parquet_table(table, &mut buf, None).unwrap();
        buf.seek(SeekFrom::Start(0)).unwrap();
        buf
    }

    #[test]
    fn parquet_read_batch_cols_single_numeric() {
        let table = make_parquet_table();
        let mut buf = write_and_rewind(&table);

        let projected = load_parquet_table_cols(&mut buf, &["id"]).unwrap();
        assert_eq!(projected.n_rows, 3);
        assert_eq!(projected.cols.len(), 1);
        assert_eq!(projected.cols[0].field.name, "id");
        match &projected.cols[0].array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                assert_eq!(arr.data.as_slice(), &[10, 20, 30]);
            }
            _ => panic!("expected Int32"),
        }
    }

    #[test]
    fn parquet_read_batch_cols_multiple_in_schema_order() {
        let table = make_parquet_table();
        let mut buf = write_and_rewind(&table);

        // Request in reverse order - result should still be schema order
        let projected = load_parquet_table_cols(&mut buf, &["name", "id"]).unwrap();
        assert_eq!(projected.cols.len(), 2);
        assert_eq!(projected.cols[0].field.name, "id");
        assert_eq!(projected.cols[1].field.name, "name");
    }

    #[test]
    fn parquet_read_batch_cols_with_string() {
        let table = make_parquet_table();
        let mut buf = write_and_rewind(&table);

        let projected = load_parquet_table_cols(&mut buf, &["name"]).unwrap();
        assert_eq!(projected.cols.len(), 1);
        match &projected.cols[0].array {
            Array::TextArray(TextArray::String32(arr)) => {
                let vals: Vec<&str> = arr.iter_str().collect();
                assert_eq!(vals, vec!["alice", "bob", "charlie"]);
            }
            #[cfg(feature = "large_string")]
            Array::TextArray(TextArray::String64(arr)) => {
                let vals: Vec<&str> = arr.iter_str().collect();
                assert_eq!(vals, vec!["alice", "bob", "charlie"]);
            }
            other => panic!("expected String32 or String64, got {:?}", other),
        }
    }

    #[test]
    fn parquet_read_batch_cols_unknown_name_errors() {
        let table = make_parquet_table();
        let mut buf = write_and_rewind(&table);

        let err = load_parquet_table_cols(&mut buf, &["nonexistent"]);
        assert!(err.is_err());
    }

    #[test]
    fn parquet_read_batch_cols_all_equals_full_read() {
        let table = make_parquet_table();
        let mut buf = write_and_rewind(&table);
        let full = load_parquet_table(&mut buf).unwrap();

        buf.seek(SeekFrom::Start(0)).unwrap();
        let projected = load_parquet_table_cols(&mut buf, &["id", "score", "name"]).unwrap();

        assert_eq!(projected.n_rows, full.n_rows);
        assert_eq!(projected.cols.len(), full.cols.len());
        for (p, f) in projected.cols.iter().zip(full.cols.iter()) {
            assert_eq!(p.field.name, f.field.name);
        }
    }
}
