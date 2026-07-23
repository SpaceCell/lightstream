// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Memory-mapped Arrow IPC example.
//!
//! This example demonstrates how to:
//! - Create a table with sample data using Vec64 for alignment  
//! - Write it to Arrow IPC File format using TableWriter
//! - Read it back using MmapTableReader for fast-mmap cached access,
//! which can also be useful for 'larger than available RAM' workloads.

use lightstream::enums::IPCMessageProtocol;
#[cfg(feature = "mmap")]
use lightstream::models::readers::ipc::mmap_table::MmapTableReader;
use lightstream::models::writers::ipc::table::TableWriter;
use minarrow::{Field, FieldArray, Table, Vec64, arr_bool, arr_f64, arr_i64, arr_u32};
#[cfg(feature = "mmap")]
use minarrow::{Array, NumericArray};
use std::path::Path;
use tempfile::tempdir;
use tokio::fs::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Memory-Mapped Example");
    println!("==============================");

    // Create large sample data to show performance benefits
    let table = create_large_table();
    println!(
        "Created table '{}' with {} rows and {} columns",
        table.name,
        table.n_rows,
        table.cols.len()
    );

    // Create a temporary directory for our example
    let temp_dir = tempdir()?;
    let file_path = temp_dir.path().join("large_sample.arrow");

    // Write to Arrow IPC File format
    println!("\n1. Writing to Arrow IPC File");
    write_arrow_file(table, &file_path).await?;
    println!("Completed");
    // Read using memory mapping (zero-copy)
    #[cfg(feature = "mmap")]
    {
        println!("\n2. Reading with Memory Mapping (Zero-Copy)");
        read_with_mmap(&file_path)?;
    }

    #[cfg(not(feature = "mmap"))]
    println!("\n2. Memory mapping not available (mmap feature not enabled)");

    println!("\n✓ Memory-mapped zero-copy example completed!");

    Ok(())
}

/// Create a large table for mmap benchmarking.
fn create_large_table() -> Table {
    let n_rows = 100_000_000;

    let ids: Vec64<i64> = (0..n_rows as i64).collect();
    let measurements: Vec64<f64> = (0..n_rows).map(|i| i as f64 * 0.1).collect();
    let extras: Vec64<u32> = (0..n_rows).map(|i| (i % 1000) as u32).collect();
    let evens: Vec64<bool> = (0..n_rows).map(|i| i % 2 == 0).collect();

    Table::new(
        "large_aligned_data".to_string(),
        Some(vec![
            FieldArray::from_arr("id", arr_i64!(ids)),
            FieldArray::from_arr("measurement", arr_f64!(measurements)),
            FieldArray::from_arr("extra", arr_u32!(extras)),
            FieldArray::from_arr("is_even", arr_bool!(evens)),
        ]),
    )
}

/// Write table to Arrow IPC File format
async fn write_arrow_file(
    table: Table,
    file_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    {
        println!("  Creating file and writer...");
        let file = File::create(file_path).await?;
        let schema: Vec<Field> = table.cols.iter().map(|col| (*col.field).clone()).collect();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File, None)?;

        println!("  Starting write_all_tables...");
        writer.write_all_tables(vec![table]).await?;
        println!("  Finished write_all_tables");
    }
    let write_time = start.elapsed();

    let file_size = tokio::fs::metadata(file_path).await?.len();
    println!("  Wrote {} bytes in {:?}", file_size, write_time);
    println!(
        "  File size: {:.2} MB",
        file_size as f64 / (1024.0 * 1024.0)
    );

    Ok(())
}

/// Read using memory mapping
#[cfg(feature = "mmap")]
fn read_with_mmap(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();

    let reader = MmapTableReader::open(file_path)?;
    let table = reader.read_batch(0)?;

    let read_time = start.elapsed();

    println!("  Memory-mapped read: {:?}", read_time);
    println!(
        "  Read {} rows, {} columns (zero-copy)",
        table.n_rows,
        table.cols.len()
    );

    // Show some sample data - access memory-mapped data directly
    if let Array::NumericArray(NumericArray::Int64(int_arr)) = &table.cols[0].array {
        println!(
            "  Sample int data (mmap): {:?}",
            &int_arr.data.as_ref()[0..5]
        );
    }

    println!("  ✓ Data accessed directly from memory-mapped file");

    Ok(())
}
