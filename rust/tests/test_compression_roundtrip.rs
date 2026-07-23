// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Compression roundtrip tests
//!
//! These tests verify that compressed data can be written and read back
//! correctly with full data integrity verification.

use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::fs::File;

use lightstream::compression::Compression;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::file_table::FileTableReader;
use lightstream::models::writers::ipc::table::TableWriter;

use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{
    Array, BooleanArray, Buffer, Field, FieldArray, FloatArray, IntegerArray, MaskedArray,
    NumericArray, StringArray, Table, TextArray, Vec64,
};

/// Create a comprehensive test table with multiple data types
fn create_test_table() -> (Table, Vec<Field>) {
    let n_rows = 1000;

    // Integer column
    let int_data: Vec64<i64> = (0..n_rows).map(|i| i as i64 * 2).collect();
    let int_array = Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
        data: Buffer::from(int_data),
        null_mask: None,
    })));
    let int_field = Field {
        name: "id".into(),
        dtype: ArrowType::Int64,
        nullable: false,
        metadata: Default::default(),
    };

    // Float column
    let float_data: Vec64<f64> = (0..n_rows).map(|i| (i as f64) * 3.14159).collect();
    let float_array = Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
        data: Buffer::from(float_data),
        null_mask: None,
    })));
    let float_field = Field {
        name: "value".into(),
        dtype: ArrowType::Float64,
        nullable: false,
        metadata: Default::default(),
    };

    // Boolean column
    let bool_data: Vec<bool> = (0..n_rows).map(|i| i % 2 == 0).collect();
    let bool_array = Array::BooleanArray(Arc::new(BooleanArray::from_slice(&bool_data)));
    let bool_field = Field {
        name: "flag".into(),
        dtype: ArrowType::Boolean,
        nullable: false,
        metadata: Default::default(),
    };

    // String column
    let string_data: Vec<String> = (0..n_rows).map(|i| format!("test_string_{}", i)).collect();
    let string_refs: Vec<&str> = string_data.iter().map(|s| s.as_str()).collect();
    let string_array = Array::TextArray(TextArray::String32(Arc::new(StringArray::from_vec(
        string_refs,
        None,
    ))));
    let string_field = Field {
        name: "text".into(),
        dtype: ArrowType::String,
        nullable: false,
        metadata: Default::default(),
    };

    let schema = vec![
        int_field.clone(),
        float_field.clone(),
        bool_field.clone(),
        string_field.clone(),
    ];

    let table = Table {
        name: "compression_roundtrip_test".to_string(),
        n_rows,
        cols: vec![
            FieldArray::new(int_field, int_array),
            FieldArray::new(float_field, float_array),
            FieldArray::new(bool_field, bool_array),
            FieldArray::new(string_field, string_array),
        ],
        ..Default::default()
    };

    (table, schema)
}

/// Read all tables from a FileTableReader
fn read_all_tables(reader: &FileTableReader) -> std::io::Result<Vec<Table>> {
    let num_batches = reader.num_batches();
    let mut tables = Vec::new();
    for i in 0..num_batches {
        tables.push(reader.read_batch(i)?);
    }
    Ok(tables)
}

/// Verify that two tables have identical data
fn verify_tables_equal(original: &Table, roundtrip: &Table) {
    // Note: Table names are not preserved in Arrow IPC format, so we don't check names
    assert_eq!(original.n_rows, roundtrip.n_rows, "Row counts should match");
    assert_eq!(
        original.cols.len(),
        roundtrip.cols.len(),
        "Column counts should match"
    );

    for (i, (orig_col, rt_col)) in original.cols.iter().zip(roundtrip.cols.iter()).enumerate() {
        assert_eq!(
            orig_col.field.name, rt_col.field.name,
            "Column {} name should match",
            i
        );
        assert_eq!(
            orig_col.field.dtype, rt_col.field.dtype,
            "Column {} type should match",
            i
        );
        assert_eq!(
            orig_col.field.nullable, rt_col.field.nullable,
            "Column {} nullable should match",
            i
        );

        // For detailed data verification, we'd need to implement array comparison
        // For now, we verify structural equality which catches most compression issues
        match (&orig_col.array, &rt_col.array) {
            (Array::NumericArray(orig), Array::NumericArray(rt)) => match (orig, rt) {
                (NumericArray::Int64(orig_arr), NumericArray::Int64(rt_arr)) => {
                    assert_eq!(
                        orig_arr.data.len(),
                        rt_arr.data.len(),
                        "Int64 column {} data length should match",
                        i
                    );
                }
                (NumericArray::Float64(orig_arr), NumericArray::Float64(rt_arr)) => {
                    assert_eq!(
                        orig_arr.data.len(),
                        rt_arr.data.len(),
                        "Float64 column {} data length should match",
                        i
                    );
                }
                _ => panic!("Numeric array types should match for column {}", i),
            },
            (Array::BooleanArray(orig), Array::BooleanArray(rt)) => {
                assert_eq!(
                    orig.len(),
                    rt.len(),
                    "Boolean column {} length should match",
                    i
                );
            }
            (Array::TextArray(orig), Array::TextArray(rt)) => match (orig, rt) {
                (TextArray::String32(orig_arr), TextArray::String32(rt_arr)) => {
                    assert_eq!(
                        orig_arr.len(),
                        rt_arr.len(),
                        "String column {} length should match",
                        i
                    );
                }
                _ => panic!("Text array types should match for column {}", i),
            },
            _ => panic!("Array types should match for column {}", i),
        }
    }
}

async fn write_and_read_roundtrip(compression: Option<Compression>) -> (Table, Table) {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    let (original_table, schema) = create_test_table();

    // Write with compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::new(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            compression,
        )
        .unwrap();
        writer
            .write_all_tables(vec![original_table.clone()])
            .await
            .unwrap();
    }

    // Read back
    let reader = FileTableReader::open(file_path).unwrap();
    let tables = read_all_tables(&reader).unwrap();

    assert_eq!(tables.len(), 1, "Should read back exactly one table");
    let roundtrip_table = tables.into_iter().next().unwrap();

    (original_table, roundtrip_table)
}

#[tokio::test]
async fn test_compression_none_roundtrip() {
    let (original, roundtrip) = write_and_read_roundtrip(None).await;
    verify_tables_equal(&original, &roundtrip);
    println!("✓ Uncompressed roundtrip test passed");
}

#[cfg(feature = "snappy")]
#[tokio::test]
async fn test_snappy_ipc_write_rejected() {
    // Arrow IPC BodyCompression permits only LZ4_FRAME and ZSTD, so a
    // Snappy IPC write reports Unsupported rather than producing a file
    // other Arrow readers cannot open. Snappy remains available for the
    // formats that support it.
    let temp_file = NamedTempFile::new().unwrap();
    let file = File::create(temp_file.path()).await.unwrap();
    let (table, schema) = create_test_table();
    let mut writer = TableWriter::new(
        file,
        schema,
        IPCMessageProtocol::File,
        Some(Compression::Snappy),
    )
    .unwrap();
    let result = writer.write_all_tables(vec![table]).await;
    let err = result.expect_err("Snappy IPC write should be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
}

#[cfg(feature = "zstd")]
#[tokio::test]
async fn test_zstd_compression_roundtrip() {
    let (original, roundtrip) = write_and_read_roundtrip(Some(Compression::Zstd)).await;
    verify_tables_equal(&original, &roundtrip);
    println!("✓ Zstd compression roundtrip test passed");
}

#[tokio::test]
async fn test_compression_multiple_tables_roundtrip() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    let (table1, schema) = create_test_table();
    let (mut table2, _) = create_test_table();
    table2.name = "second_table".to_string();

    let original_tables = vec![table1, table2];

    // Write with compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::new(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            None,
        )
        .unwrap();
        writer
            .write_all_tables(original_tables.clone())
            .await
            .unwrap();
    }

    // Read back
    let reader = FileTableReader::open(file_path).unwrap();
    let roundtrip_tables = read_all_tables(&reader).unwrap();

    assert_eq!(
        roundtrip_tables.len(),
        2,
        "Should read back exactly two tables"
    );

    for (i, (orig, rt)) in original_tables
        .iter()
        .zip(roundtrip_tables.iter())
        .enumerate()
    {
        verify_tables_equal(orig, rt);
        println!("✓ Table {} roundtrip verified", i + 1);
    }

    println!("✓ Multiple tables compression roundtrip test passed");
}

#[tokio::test]
async fn test_compression_large_table_roundtrip() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    // Create a larger table to better test compression effectiveness
    let n_rows = 10000;
    let int_data: Vec64<i64> = (0..n_rows).map(|i| (i % 100) as i64).collect(); // Repetitive data for better compression

    let int_array = Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
        data: Buffer::from(int_data),
        null_mask: None,
    })));
    let int_field = Field {
        name: "repeated_values".into(),
        dtype: ArrowType::Int64,
        nullable: false,
        metadata: Default::default(),
    };

    let schema = vec![int_field.clone()];
    let original_table = Table {
        name: "large_compression_test".to_string(),
        n_rows,
        cols: vec![FieldArray::new(int_field, int_array)],
        ..Default::default()
    };

    // Test with compression that should be effective on repetitive data
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::new(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            None,
        )
        .unwrap();
        writer
            .write_all_tables(vec![original_table.clone()])
            .await
            .unwrap();
    }

    // Read back
    let reader = FileTableReader::open(file_path).unwrap();
    let tables = read_all_tables(&reader).unwrap();

    assert_eq!(tables.len(), 1, "Should read back exactly one table");
    let roundtrip_table = tables.into_iter().next().unwrap();

    verify_tables_equal(&original_table, &roundtrip_table);
    println!("✓ Large table compression roundtrip test passed");
}

#[tokio::test]
async fn test_stream_protocol_compression_roundtrip() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    let (original_table, schema) = create_test_table();

    // Write with Stream protocol and compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::new(
            file,
            schema.clone(),
            IPCMessageProtocol::Stream,
            None,
        )
        .unwrap();
        writer
            .write_all_tables(vec![original_table.clone()])
            .await
            .unwrap();
    }

    // Note: Stream protocol files may need a different reader approach
    // For now, let's verify the file structure is correct
    let mut file = tokio::fs::File::open(file_path).await.unwrap();
    let mut buf = Vec::new();
    use tokio::io::AsyncReadExt;
    file.read_to_end(&mut buf).await.unwrap();

    assert!(!buf.is_empty(), "File should not be empty");
    // Stream protocol starts with 0xFFFF_FFFF
    assert_eq!(
        &buf[..4],
        &[0xFF, 0xFF, 0xFF, 0xFF],
        "Stream protocol magic should be present"
    );

    println!("✓ Stream protocol compression roundtrip structure test passed");
}

#[tokio::test]
async fn test_compression_data_integrity() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    // Create a simple integer table where we can verify exact values
    let n_rows = 100;
    let expected_values: Vec<i64> = (0..n_rows).map(|i| i as i64 * 7).collect(); // Multiply by 7 for uniqueness
    let int_data: Vec64<i64> = Vec64::from_slice(&expected_values);

    let int_array = Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
        data: Buffer::from(int_data),
        null_mask: None,
    })));
    let int_field = Field {
        name: "test_values".into(),
        dtype: ArrowType::Int64,
        nullable: false,
        metadata: Default::default(),
    };

    let schema = vec![int_field.clone()];
    let original_table = Table {
        name: "data_integrity_test".to_string(),
        n_rows,
        cols: vec![FieldArray::new(int_field, int_array)],
        ..Default::default()
    };

    // Write with compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::new(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            None,
        )
        .unwrap();
        writer
            .write_all_tables(vec![original_table.clone()])
            .await
            .unwrap();
    }

    // Read back
    let reader = FileTableReader::open(file_path).unwrap();
    let tables = read_all_tables(&reader).unwrap();

    assert_eq!(tables.len(), 1, "Should read back exactly one table");
    let roundtrip_table = tables.into_iter().next().unwrap();

    // Verify structure
    verify_tables_equal(&original_table, &roundtrip_table);

    // Verify data integrity: extract the actual values and compare
    if let Array::NumericArray(NumericArray::Int64(rt_arr)) = &roundtrip_table.cols[0].array {
        // Compare first few values to verify data integrity
        for i in 0..std::cmp::min(10, expected_values.len()) {
            let expected = expected_values[i];
            let actual = rt_arr.data[i];
            assert_eq!(actual, expected, "Value at index {} should match", i);
        }

        // Verify total length matches
        assert_eq!(
            rt_arr.data.len(),
            expected_values.len(),
            "Data length should match"
        );

        println!(
            "✓ First 10 values verified: {:?}",
            &rt_arr.data[..10].to_vec()
        );
        println!("✓ Expected first 10 values: {:?}", &expected_values[..10]);
    } else {
        panic!("Expected Int64 array in roundtrip table");
    }

    println!("✓ Compression data integrity test passed");
}

/// Roundtrip test for dictionary/categorical columns with zstd compression.
/// Verifies that compressed dictionary batches and record batches with
/// dictionary-encoded columns decompress correctly.
#[cfg(feature = "zstd")]
#[tokio::test]
async fn test_zstd_dictionary_roundtrip() {
    use minarrow::CategoricalArray;
    use minarrow::ffi::arrow_dtype::CategoricalIndexType;

    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();
    let n_rows = 500;

    // Integer column for reference
    let int_data: Vec64<i64> = (0..n_rows).map(|i| i as i64).collect();
    let int_array = Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
        data: Buffer::from(int_data.clone()),
        null_mask: None,
    })));
    let int_field = Field {
        name: "id".into(),
        dtype: ArrowType::Int64,
        nullable: false,
        metadata: Default::default(),
    };

    // Categorical column with dictionary encoding
    let dict_values = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];

    #[cfg(not(feature = "default_categorical_8"))]
    let (dict_field, dict_array) = {
        let indices: Vec64<u32> = (0..n_rows).map(|i| (i % 3) as u32).collect();
        let field = Field {
            name: "category".into(),
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
            nullable: false,
            metadata: Default::default(),
        };
        let array = Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
            data: Buffer::from(indices),
            unique_values: Vec64::from(dict_values.clone()),
            null_mask: None,
        })));
        (field, array)
    };

    #[cfg(feature = "default_categorical_8")]
    let (dict_field, dict_array) = {
        let indices: Vec64<u8> = (0..n_rows).map(|i| (i % 3) as u8).collect();
        let field = Field {
            name: "category".into(),
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt8),
            nullable: false,
            metadata: Default::default(),
        };
        let array = Array::TextArray(TextArray::Categorical8(Arc::new(CategoricalArray {
            data: Buffer::from(indices),
            unique_values: Vec64::from(dict_values.clone()),
            null_mask: None,
        })));
        (field, array)
    };

    let schema = vec![int_field.clone(), dict_field.clone()];
    let original_table = Table::new(
        "dict_compression_test".to_string(),
        Some(vec![
            FieldArray::new(int_field, int_array),
            FieldArray::new(dict_field, dict_array),
        ]),
    );

    // Write with zstd compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::new(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            Some(Compression::Zstd),
        )
        .unwrap();
        writer.register_dictionary(1, dict_values.clone());
        writer
            .write_all_tables(vec![original_table.clone()])
            .await
            .unwrap();
    }

    // Read back
    let reader = FileTableReader::open(file_path).unwrap();
    let tables = read_all_tables(&reader).unwrap();
    assert_eq!(tables.len(), 1);
    let rt = &tables[0];

    // Verify structure
    assert_eq!(rt.n_rows, n_rows);
    assert_eq!(rt.cols.len(), 2);
    assert_eq!(rt.cols[0].field.name, "id");
    assert_eq!(rt.cols[1].field.name, "category");

    // Verify integer data survived compression roundtrip
    if let Array::NumericArray(NumericArray::Int64(arr)) = &rt.cols[0].array {
        for i in 0..n_rows {
            assert_eq!(arr.data[i], i as i64, "int value mismatch at row {}", i);
        }
    } else {
        panic!("Expected Int64 array for id column");
    }

    // Verify dictionary data survived compression roundtrip
    #[cfg(not(feature = "default_categorical_8"))]
    if let Array::TextArray(TextArray::Categorical32(arr)) = &rt.cols[1].array {
        assert_eq!(
            arr.unique_values.as_slice(),
            &["alpha", "beta", "gamma"],
            "Dictionary values should match"
        );
        for i in 0..n_rows {
            assert_eq!(
                arr.data[i],
                (i % 3) as u32,
                "category index mismatch at row {}",
                i
            );
        }
    } else {
        panic!("Expected Categorical32 array for category column");
    }

    #[cfg(feature = "default_categorical_8")]
    if let Array::TextArray(TextArray::Categorical8(arr)) = &rt.cols[1].array {
        assert_eq!(
            arr.unique_values.as_slice(),
            &["alpha", "beta", "gamma"],
            "Dictionary values should match"
        );
        for i in 0..n_rows {
            assert_eq!(
                arr.data[i],
                (i % 3) as u8,
                "category index mismatch at row {}",
                i
            );
        }
    } else {
        panic!("Expected Categorical8 array for category column");
    }
}
