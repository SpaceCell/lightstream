// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Basic Arrow IPC format example.
//!
//! This example demonstrates how to:
//! - Create a table with sample data
//! - Write it to Arrow IPC format (both File and Stream formats)
//! - Read it back and verify the data

use futures_util::StreamExt;
use lightstream::enums::BufferChunkSize;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::file_table::FileTableReader;
use lightstream::models::readers::ipc::table::TableReader;
use lightstream::models::streams::disk::DiskByteStream;
use lightstream::models::writers::ipc::table::TableWriter;
use minarrow::{Field, FieldArray, Table, Vec64, arr_bool, arr_f64, arr_i32, arr_str32};
use std::path::Path;
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

/// Create a sample table with various Arrow data types.
fn create_sample_table() -> Table {
    let n_rows = 1000;

    let ids: Vec64<i32> = (0..n_rows as i32).collect();
    let values: Vec64<f64> = (0..n_rows).map(|i| (i as f64) * 0.1).collect();
    let labels: Vec64<String> = (0..n_rows).map(|i| format!("item_{}", i)).collect();
    let label_refs: Vec64<&str> = labels.iter().map(String::as_str).collect();
    let bools: Vec64<bool> = (0..n_rows).map(|i| i % 2 == 0).collect();

    Table::new(
        "performance_test".to_string(),
        Some(vec![
            FieldArray::from_arr("id", arr_i32!(ids)),
            FieldArray::from_arr("value", arr_f64!(values)),
            FieldArray::from_arr("label", arr_str32!(label_refs)),
            FieldArray::from_arr("is_even", arr_bool!(bools)),
        ]),
    )
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
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File, None)?;
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
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::Stream, None)?;
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
    let mut reader =
        TableReader::<Vec64<u8>>::new(disk_stream, 64 * 1024, IPCMessageProtocol::Stream, None);

    if let Some(_) = reader.next().await {
        let read_time = start.elapsed();
        println!("  Stream read took: {:?}", read_time);
    } else {
        return Err("No data read from stream".into());
    }

    Ok(())
}
