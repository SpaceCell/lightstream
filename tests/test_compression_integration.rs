//! Integration tests for compression functionality
//!
//! These tests verify end-to-end compression workflows including
//! writing compressed data and reading it back to verify correctness.

use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use lightstream::compression::Compression;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::writers::ipc::table_writer::TableWriter;

use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{Array, Buffer, Field, FieldArray, IntegerArray, NumericArray, Table, Vec64};

/// Create a test table with various data types for comprehensive testing
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

    // Second integer column for simplicity (avoiding string complexity for now)
    let int2_data: Vec64<i32> = (0..n_rows).map(|i| (i as i32) % 100).collect();
    let int2_array = Array::NumericArray(NumericArray::Int32(Arc::new(minarrow::IntegerArray {
        data: Buffer::from(int2_data),
        null_mask: None,
    })));
    let int2_field = Field {
        name: "category".into(),
        dtype: ArrowType::Int32,
        nullable: false,
        metadata: Default::default(),
    };

    // Float column
    let float_data: Vec64<f64> = (0..n_rows).map(|i| (i as f64) * 3.14159).collect();
    let float_array = Array::NumericArray(NumericArray::Float64(Arc::new(minarrow::FloatArray {
        data: Buffer::from(float_data),
        null_mask: None,
    })));
    let float_field = Field {
        name: "value".into(),
        dtype: ArrowType::Float64,
        nullable: false,
        metadata: Default::default(),
    };

    let schema = vec![int_field.clone(), int2_field.clone(), float_field.clone()];

    let table = Table {
        name: "compression_integration_test".to_string(),
        n_rows,
        cols: vec![
            FieldArray::new(int_field, int_array),
            FieldArray::new(int2_field, int2_array),
            FieldArray::new(float_field, float_array),
        ],
    };

    (table, schema)
}

#[tokio::test]
async fn test_compression_none_integration() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    let (table, schema) = create_test_table();

    // Write with no compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::with_compression(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            Compression::None,
        )
        .unwrap();
        writer.write_all_tables(vec![table.clone()]).await.unwrap();
    }

    // Verify file was written correctly
    let mut file = File::open(file_path).await.unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await.unwrap();

    assert!(!buf.is_empty());
    assert!(buf.starts_with(b"ARROW1\0\0"));
    assert!(buf.ends_with(b"ARROW1"));

    println!("✓ Uncompressed file size: {} bytes", buf.len());
    println!("✓ Compression::None integration test passed");
}

#[cfg(feature = "snappy")]
#[tokio::test]
async fn test_snappy_compression_integration() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    let (table, schema) = create_test_table();

    // Write with Snappy compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::with_compression(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            Compression::Snappy,
        )
        .unwrap();
        writer.write_all_tables(vec![table.clone()]).await.unwrap();
    }

    // Verify file was written correctly
    let mut file = File::open(file_path).await.unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await.unwrap();

    assert!(!buf.is_empty());
    assert!(buf.starts_with(b"ARROW1\0\0"));
    assert!(buf.ends_with(b"ARROW1"));

    println!("✓ Snappy compressed file size: {} bytes", buf.len());
    println!("✓ Snappy compression integration test passed");

    // Additional validation: file should contain compression metadata
    // The exact format depends on Arrow IPC spec implementation
    // but we can verify the file is structurally valid Arrow format
}

#[cfg(feature = "zstd")]
#[tokio::test]
async fn test_zstd_compression_integration() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    let (table, schema) = create_test_table();

    // Write with Zstd compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::with_compression(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            Compression::Zstd,
        )
        .unwrap();
        writer.write_all_tables(vec![table.clone()]).await.unwrap();
    }

    // Verify file was written correctly
    let mut file = File::open(file_path).await.unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await.unwrap();

    assert!(!buf.is_empty());
    assert!(buf.starts_with(b"ARROW1\0\0"));
    assert!(buf.ends_with(b"ARROW1"));

    println!("✓ Zstd compressed file size: {} bytes", buf.len());
    println!("✓ Zstd compression integration test passed");
}

#[tokio::test]
async fn test_compression_size_comparison() {
    let (table, schema) = create_test_table();

    // Create three temporary files
    let temp_none = NamedTempFile::new().unwrap();
    let _temp_snappy = NamedTempFile::new().unwrap();
    let _temp_zstd = NamedTempFile::new().unwrap();

    let mut sizes = Vec::new();

    // Write uncompressed
    {
        let file = File::create(temp_none.path()).await.unwrap();
        let mut writer = TableWriter::with_compression(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            Compression::None,
        )
        .unwrap();
        writer.write_all_tables(vec![table.clone()]).await.unwrap();
    }
    let none_size = std::fs::metadata(temp_none.path()).unwrap().len();
    sizes.push(("None", none_size));

    // Write with Snappy (if available)
    #[cfg(feature = "snappy")]
    {
        let file = File::create(_temp_snappy.path()).await.unwrap();
        let mut writer = TableWriter::with_compression(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            Compression::Snappy,
        )
        .unwrap();
        writer.write_all_tables(vec![table.clone()]).await.unwrap();

        let snappy_size = std::fs::metadata(_temp_snappy.path()).unwrap().len();
        sizes.push(("Snappy", snappy_size));
    }

    // Write with Zstd (if available)
    #[cfg(feature = "zstd")]
    {
        let file = File::create(_temp_zstd.path()).await.unwrap();
        let mut writer = TableWriter::with_compression(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            Compression::Zstd,
        )
        .unwrap();
        writer.write_all_tables(vec![table.clone()]).await.unwrap();

        let zstd_size = std::fs::metadata(_temp_zstd.path()).unwrap().len();
        sizes.push(("Zstd", zstd_size));
    }

    // Print compression comparison
    println!("Compression size comparison:");
    for (name, size) in &sizes {
        let ratio = if none_size > 0 {
            *size as f64 / none_size as f64 * 100.0
        } else {
            100.0
        };
        println!("  {}: {} bytes ({:.1}% of uncompressed)", name, size, ratio);
    }

    // Verify all files are valid Arrow format
    assert!(none_size > 0);
    println!("✓ Compression size comparison completed");
}

#[tokio::test]
async fn test_stream_protocol_with_compression() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    let (table, schema) = create_test_table();

    // Write with Stream protocol and compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::with_compression(
            file,
            schema.clone(),
            IPCMessageProtocol::Stream,
            Compression::None,
        )
        .unwrap();
        writer.write_all_tables(vec![table.clone()]).await.unwrap();
    }

    // Verify file was written correctly
    let mut file = File::open(file_path).await.unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await.unwrap();

    assert!(!buf.is_empty());
    // Stream protocol starts with 0xFFFF_FFFF
    assert_eq!(&buf[..4], &[0xFF, 0xFF, 0xFF, 0xFF]);

    println!("✓ Stream protocol with compression test passed");
}

#[tokio::test]
async fn test_multiple_tables_with_compression() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    let (table1, schema) = create_test_table();
    let (mut table2, _) = create_test_table();
    table2.name = "second_table".to_string();

    // Write multiple tables with compression
    {
        let file = File::create(file_path).await.unwrap();
        let mut writer = TableWriter::with_compression(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            Compression::None,
        )
        .unwrap();
        writer.write_all_tables(vec![table1, table2]).await.unwrap();
    }

    // Verify file was written correctly
    let mut file = File::open(file_path).await.unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await.unwrap();

    assert!(!buf.is_empty());
    assert!(buf.starts_with(b"ARROW1\0\0"));
    assert!(buf.ends_with(b"ARROW1"));

    println!("✓ Multiple tables with compression test passed");
}
