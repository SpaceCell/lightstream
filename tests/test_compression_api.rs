//! Test compression API integration without full implementation
//!
//! This test verifies that the compression APIs are properly wired up
//! and can be used without compilation errors.

use tempfile::NamedTempFile;
use tokio::fs::File;

use lightstream::compression::Compression;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::writers::ipc::table_writer::TableWriter;

use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{Array, Buffer, Field, FieldArray, IntegerArray, NumericArray, Table, Vec64};
use std::sync::Arc;

/// Test that TableWriter::with_compression API exists and compiles
#[tokio::test]
async fn test_compression_api_compilation() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    // Create a simple test table
    let n_rows = 100;
    let int_data: Vec64<i64> = (0..n_rows).map(|i| i as i64).collect();

    let int_array = Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
        data: Buffer::from(int_data),
        null_mask: None,
    })));
    let int_field = FieldArray::new(
        Field {
            name: "id".into(),
            dtype: ArrowType::Int64,
            nullable: false,
            metadata: Default::default(),
        },
        int_array,
    );

    let table = Table {
        name: "compression_test".to_string(),
        n_rows,
        cols: vec![int_field],
    };

    // Test uncompressed writer
    {
        let file = File::create(file_path).await.unwrap();
        let schema: Vec<Field> = table.cols.iter().map(|col| (*col.field).clone()).collect();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File).unwrap();
        writer.write_all_tables(vec![table.clone()]).await.unwrap();
    }

    // Test compressed writer API
    {
        let file = File::create(file_path).await.unwrap();
        let schema: Vec<Field> = table.cols.iter().map(|col| (*col.field).clone()).collect();

        // Test each compression option compiles
        let _writer_none = TableWriter::with_compression(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            Compression::None,
        )
        .unwrap();

        #[cfg(feature = "snappy")]
        {
            let file = File::create(file_path).await.unwrap();
            let _writer_snappy = TableWriter::with_compression(
                file,
                schema.clone(),
                IPCMessageProtocol::File,
                Compression::Snappy,
            )
            .unwrap();
        }

        #[cfg(feature = "zstd")]
        {
            let file = File::create(file_path).await.unwrap();
            let _writer_zstd = TableWriter::with_compression(
                file,
                schema,
                IPCMessageProtocol::File,
                Compression::Zstd,
            )
            .unwrap();
        }

        println!("✓ Compression APIs compile successfully");
        println!("✓ All compression codecs are accessible");
    }
}
