//! Test that memory-mapped files create SharedBuffer instances for zero-copy access
//!
//! This verifies that our mmap implementation properly creates SharedBuffers
//! instead of owned Buffers, ensuring true zero-copy behavior.
#[cfg(feature = "mmap")]
use lightstream::enums::IPCMessageProtocol;
#[cfg(feature = "mmap")]
use lightstream::models::readers::ipc::mmap_table_reader::MmapTableReader;
#[cfg(feature = "mmap")]
use lightstream::models::writers::ipc::table_writer::TableWriter;
#[cfg(feature = "mmap")]
use minarrow::ffi::arrow_dtype::ArrowType;
#[cfg(feature = "mmap")]
use minarrow::{
    Array, Buffer, Field, FieldArray, FloatArray, IntegerArray, NumericArray, Table, Vec64,
};
#[cfg(feature = "mmap")]
use std::sync::Arc;
#[cfg(feature = "mmap")]
use tempfile::NamedTempFile;
#[cfg(feature = "mmap")]
use tokio::fs::File;

/// Test that mmap-read arrays use SharedBuffer instead of owned buffers
#[tokio::test]
#[cfg(feature = "mmap")]
async fn test_mmap_creates_shared_buffers() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    // Create a test table with Vec64 data (which should be owned)
    let n_rows = 10000;
    let int_data: Vec64<i64> = (0..n_rows).map(|i| i as i64).collect();
    let float_data: Vec64<f64> = (0..n_rows).map(|i| i as f64 * 0.1).collect();

    // Verify the original Vec64 data creates owned buffers
    let original_int_buffer = Buffer::from(int_data);
    let original_float_buffer = Buffer::from(float_data);

    // These should be owned (not shared) since they come from Vec64
    assert!(
        !original_int_buffer.is_shared(),
        "Vec64-based buffer should be owned"
    );
    assert!(
        !original_float_buffer.is_shared(),
        "Vec64-based buffer should be owned"
    );

    let int_array = Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
        data: original_int_buffer,
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

    let float_array = Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
        data: original_float_buffer,
        null_mask: None,
    })));
    let float_field = FieldArray::new(
        Field {
            name: "measurement".into(),
            dtype: ArrowType::Float64,
            nullable: false,
            metadata: Default::default(),
        },
        float_array,
    );

    let table = Table {
        name: "mmap_test".to_string(),
        n_rows,
        cols: vec![int_field, float_field],
    };

    // Write the table to an Arrow IPC file
    {
        let file = File::create(file_path).await.unwrap();
        let schema: Vec<Field> = table.cols.iter().map(|col| (*col.field).clone()).collect();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File).unwrap();
        writer.write_all_tables(vec![table]).await.unwrap();
    }

    // Now read it back using mmap
    #[cfg(feature = "mmap")]
    {
        let mmap_reader = MmapTableReader::open(file_path).unwrap();
        let mmap_table = mmap_reader.read_batch(0).unwrap();

        assert_eq!(mmap_table.n_rows, n_rows);
        assert_eq!(mmap_table.cols.len(), 2);

        // Test that mmap-read data uses SharedBuffers (zero-copy)
        for (col_idx, col) in mmap_table.cols.iter().enumerate() {
            match &col.array {
                Array::NumericArray(NumericArray::Int64(arr)) => {
                    println!(
                        "Column {}: Integer array is_shared: {}",
                        col_idx,
                        arr.data.is_shared()
                    );
                    assert!(
                        arr.data.is_shared(),
                        "Mmap-read integer data should use SharedBuffer for zero-copy"
                    );
                }
                Array::NumericArray(NumericArray::Float64(arr)) => {
                    println!(
                        "Column {}: Float array is_shared: {}",
                        col_idx,
                        arr.data.is_shared()
                    );
                    assert!(
                        arr.data.is_shared(),
                        "Mmap-read float data should use SharedBuffer for zero-copy"
                    );
                }
                _ => panic!("Unexpected array type in test"),
            }
        }

        // Verify the data is correct (spot check)
        if let Array::NumericArray(NumericArray::Int64(arr)) = &mmap_table.cols[0].array {
            assert_eq!(arr.data[0], 0);
            assert_eq!(arr.data[1], 1);
            assert_eq!(arr.data[n_rows - 1], (n_rows - 1) as i64);
        }

        if let Array::NumericArray(NumericArray::Float64(arr)) = &mmap_table.cols[1].array {
            assert!((arr.data[0] - 0.0).abs() < 1e-10);
            assert!((arr.data[1] - 0.1).abs() < 1e-10);
            assert!((arr.data[n_rows - 1] - ((n_rows - 1) as f64 * 0.1)).abs() < 1e-10);
        }

        println!("✓ Mmap successfully created SharedBuffers for zero-copy access");
        println!("✓ Data integrity verified");
    }

    #[cfg(not(feature = "mmap"))]
    {
        println!("Skipping mmap test - mmap feature not enabled");
    }
}

/// Test specifically that our 64-byte alignment works with mmap SharedBuffers
#[tokio::test]
#[cfg(feature = "mmap")]
async fn test_mmap_alignment_with_shared_buffers() {
    let temp_file = NamedTempFile::new().unwrap();
    let file_path = temp_file.path();

    // Create a larger dataset to ensure alignment matters
    let n_rows = 100000;
    let int_data: Vec64<i64> = (0..n_rows).map(|i| i as i64).collect();

    let int_array = Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
        data: Buffer::from(int_data),
        null_mask: None,
    })));
    let int_field = FieldArray::new(
        Field {
            name: "aligned_data".into(),
            dtype: ArrowType::Int64,
            nullable: false,
            metadata: Default::default(),
        },
        int_array,
    );

    let table = Table {
        name: "alignment_test".to_string(),
        n_rows,
        cols: vec![int_field],
    };

    // Write the table
    {
        let file = File::create(file_path).await.unwrap();
        let schema: Vec<Field> = table.cols.iter().map(|col| (*col.field).clone()).collect();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File).unwrap();
        writer.write_all_tables(vec![table]).await.unwrap();
    }

    // Read it back with mmap and check alignment
    let mmap_reader = MmapTableReader::open(file_path).unwrap();
    let mmap_table = mmap_reader.read_batch(0).unwrap();

    if let Array::NumericArray(NumericArray::Int64(arr)) = &mmap_table.cols[0].array {
        // Verify it's using SharedBuffer
        assert!(arr.data.is_shared(), "Mmap data should be shared");

        // Verify 64-byte alignment
        let ptr = arr.data.as_ptr() as usize;
        println!("Mmap SharedBuffer pointer: 0x{:x}", ptr);
        println!("64-byte aligned: {}", ptr % 64 == 0);

        // The alignment check should pass due to our mmap alignment fix
        assert_eq!(
            ptr % 64,
            0,
            "Mmap SharedBuffer should be 64-byte aligned for SIMD"
        );

        // Verify data integrity with alignment
        assert_eq!(arr.data[0], 0);
        assert_eq!(arr.data[n_rows / 2], (n_rows / 2) as i64);
        assert_eq!(arr.data[n_rows - 1], (n_rows - 1) as i64);
    }

    println!("✓ Mmap SharedBuffer is 64-byte aligned and data is correct");
}
