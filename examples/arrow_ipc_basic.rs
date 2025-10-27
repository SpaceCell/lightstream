//! Basic Arrow IPC format example.
//!
//! This example demonstrates how to:
//! - Create a table with sample data
//! - Write it to Arrow IPC format (both File and Stream formats)
//! - Read it back and verify the data

use futures_util::StreamExt;
use lightstream::enums::BufferChunkSize;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::file_table_reader::FileTableReader;
use lightstream::models::readers::ipc::table_stream_reader::TableStreamReader64;
use lightstream::models::streams::disk::DiskByteStream;
use lightstream::models::writers::ipc::table_writer::TableWriter;
use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{
    Array, Bitmask, BooleanArray, Buffer, Field, FieldArray, FloatArray, IntegerArray,
    NumericArray, StringArray, Table, TextArray, Vec64,
};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::fs::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Arrow IPC Example");
    println!("================");

    // Create sample data
    let table = create_sample_table();
    println!(
        "Created table '{}' with {} rows and {} columns",
        table.name,
        table.n_rows,
        table.cols.len()
    );

    // Print table schema
    print_schema(&table);

    // Create a temporary directory for our example
    let temp_dir = tempdir()?;

    // Example 1: Arrow IPC File format
    println!("\n1. Arrow IPC File Format Example");
    let file_path = temp_dir.path().join("sample.arrow");
    arrow_file_example(&table, &file_path).await?;

    // Example 2: Arrow IPC Stream format
    println!("\n2. Arrow IPC Stream Format Example");
    let stream_path = temp_dir.path().join("sample.stream");
    arrow_stream_example(&table, &stream_path).await?;

    println!("\n✓ All Arrow IPC examples completed successfully!");

    Ok(())
}

/// Create a sample table with various Arrow data types
fn create_sample_table() -> Table {
    let n_rows = 1000;

    // Create integer column
    let int_data: Vec<i32> = (0..n_rows).map(|i| i as i32).collect();
    let int_array = Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
        data: Buffer::from(Vec64::from_slice(&int_data)),
        null_mask: None,
    })));
    let int_field = FieldArray::new(
        Field {
            name: "id".into(),
            dtype: ArrowType::Int32,
            nullable: false,
            metadata: Default::default(),
        },
        int_array,
    );

    // Create float column
    let float_data: Vec<f64> = (0..n_rows).map(|i| (i as f64) * 0.1).collect();
    let float_array = Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
        data: Buffer::from(Vec64::from_slice(&float_data)),
        null_mask: None,
    })));
    let float_field = FieldArray::new(
        Field {
            name: "value".into(),
            dtype: ArrowType::Float64,
            nullable: false,
            metadata: Default::default(),
        },
        float_array,
    );

    // Create string column - concatenate all strings and create offset array
    let individual_strings: Vec<String> = (0..n_rows).map(|i| format!("item_{}", i)).collect();
    let mut str_data = Vec::new();
    let mut offsets = Vec::with_capacity(n_rows + 1);
    offsets.push(0u32);
    for s in &individual_strings {
        str_data.extend_from_slice(s.as_bytes());
        offsets.push(str_data.len() as u32);
    }
    let str_array = Array::TextArray(TextArray::String32(Arc::new(StringArray::new(
        Buffer::from(Vec64::from_slice(&str_data)),
        None,
        Buffer::from(Vec64::from_slice(&offsets)),
    ))));
    let str_field = FieldArray::new(
        Field {
            name: "label".into(),
            dtype: ArrowType::String,
            nullable: false,
            metadata: Default::default(),
        },
        str_array,
    );

    // Create boolean column
    let bool_data: Vec<bool> = (0..n_rows).map(|i| i % 2 == 0).collect();
    let bitmask_bytes = {
        let mut bytes = vec![0u8; (n_rows + 7) / 8];
        for (i, &value) in bool_data.iter().enumerate() {
            if value {
                bytes[i / 8] |= 1 << (i % 8);
            }
        }
        bytes
    };
    let bool_array = Array::BooleanArray(Arc::new(BooleanArray {
        data: Bitmask::from_bytes(&bitmask_bytes, n_rows),
        null_mask: None,
        len: n_rows,
        _phantom: std::marker::PhantomData,
    }));
    let bool_field = FieldArray::new(
        Field {
            name: "is_even".into(),
            dtype: ArrowType::Boolean,
            nullable: false,
            metadata: Default::default(),
        },
        bool_array,
    );

    Table {
        name: "performance_test".to_string(),
        n_rows,
        cols: vec![int_field, float_field, str_field, bool_field],
    }
}

/// Print the schema of the table
fn print_schema(table: &Table) {
    println!("Schema:");
    for (i, col) in table.cols.iter().enumerate() {
        println!("  {}: {} ({:?})", i, col.field.name, col.field.dtype);
    }
}

/// Demonstrate Arrow IPC File format
async fn arrow_file_example(
    table: &Table,
    file_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Write to Arrow IPC File format
    let start = std::time::Instant::now();
    {
        let file = File::create(file_path).await?;
        let schema: Vec<Field> = table.cols.iter().map(|col| (*col.field).clone()).collect();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File)?;
        writer.write_all_tables(vec![table.clone()]).await?;
    }
    let write_time = start.elapsed();
    println!("  File write took: {:?}", write_time);

    // Get file size
    let file_size = tokio::fs::metadata(file_path).await?.len();
    println!(
        "  File size: {} bytes ({:.2} KB)",
        file_size,
        file_size as f64 / 1024.0
    );

    // Read from Arrow IPC File format
    let start = std::time::Instant::now();
    let reader = FileTableReader::open(file_path)?;
    let _ = reader.read_batch(0)?;
    let read_time = start.elapsed();
    println!("  File read took: {:?}", read_time);
    Ok(())
}

/// Demonstrate Arrow IPC Stream format
async fn arrow_stream_example(
    table: &Table,
    stream_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Write to Arrow IPC Stream format
    let start = std::time::Instant::now();
    {
        let file = File::create(stream_path).await?;
        let schema: Vec<Field> = table.cols.iter().map(|col| (*col.field).clone()).collect();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::Stream)?;
        writer.write_all_tables(vec![table.clone()]).await?;
    }
    let write_time = start.elapsed();
    println!("  Stream write took: {:?}", write_time);

    // Get file size
    let file_size = tokio::fs::metadata(stream_path).await?.len();
    println!(
        "  Stream size: {} bytes ({:.2} KB)",
        file_size,
        file_size as f64 / 1024.0
    );

    // Read from Arrow IPC Stream format
    let start = std::time::Instant::now();
    let disk_stream = DiskByteStream::open(stream_path, BufferChunkSize::Custom(64 * 1024)).await?;
    let mut reader = TableStreamReader64::new(disk_stream, 64 * 1024, IPCMessageProtocol::Stream);

    if let Some(_) = reader.next().await {
        let read_time = start.elapsed();
        println!("  Stream read took: {:?}", read_time);
    } else {
        return Err("No data read from stream".into());
    }

    Ok(())
}
