// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parquet I/O example demonstrating read and write functionality.
//!
//! This example demonstrates how to:
//! - Write Arrow Tables to Parquet format with various compression options
//! - Read Parquet files back into Arrow Tables
//! - Handle different data types (integers, floats, strings, booleans, dates)
//! - Use compression (None, Snappy, Zstd) for file size optimisation
//! - Verify data integrity through round-trip testing

#[cfg(feature = "parquet")]
use lightstream::{
    compression::Compression,
    models::{
        readers::parquet::load_parquet_table, writers::parquet::write_parquet_table,
    },
};

#[cfg(feature = "parquet")]
use minarrow::{
    Array, FieldArray, NumericArray, Table, TextArray, Vec64, arr_bool, arr_f32, arr_f64, arr_i32,
    arr_i64, arr_str32,
};
#[cfg(feature = "parquet")]
use std::fs::File;
#[cfg(feature = "parquet")]
use std::io::{Cursor, Seek, SeekFrom};
#[cfg(feature = "parquet")]
use std::path::Path;
#[cfg(feature = "parquet")]
#[cfg(feature = "parquet")]
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Parquet I/O Example");
    println!("==================");

    #[cfg(feature = "parquet")]
    {
        // Create a temporary directory for our examples
        let temp_dir = tempdir()?;

        // Example 1: Basic Parquet write and read
        println!("\n1. Basic Parquet Write and Read");
        let basic_path = temp_dir.path().join("basic_data.parquet");
        basic_parquet_example(&basic_path).await?;

        // Example 2: Compression comparison
        println!("\n2. Compression Comparison");
        let compression_dir = temp_dir.path().join("compression");
        std::fs::create_dir_all(&compression_dir)?;
        compression_example(&compression_dir).await?;

        // Example 3: Complex data types
        println!("\n3. Complex Data Types");
        let complex_path = temp_dir.path().join("complex_data.parquet");
        complex_types_example(&complex_path).await?;

        // Example 4: Large dataset performance
        println!("\n4. Large Dataset Performance");
        let large_path = temp_dir.path().join("large_dataset.parquet");
        large_dataset_example(&large_path).await?;

        println!("\n✓ Parquet I/O example completed successfully!");
    }

    #[cfg(not(feature = "parquet"))]
    {
        println!("\nParquet feature not enabled. Enable with --features parquet");
        println!("Run: cargo run --example parquet_file_io --features parquet");
    }

    Ok(())
}

#[cfg(feature = "parquet")]
/// Basic Parquet write and read example
async fn basic_parquet_example(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple table
    let table = create_simple_table();
    println!(
        "  Created table '{}' with {} rows",
        table.name, table.n_rows
    );

    // Write to Parquet file
    let start = std::time::Instant::now();
    {
        let mut file = File::create(file_path)?;
        write_parquet_table(&table, &mut file, None)?;
    }
    let write_time = start.elapsed();

    let file_size = std::fs::metadata(file_path)?.len();
    println!("  Wrote to Parquet in {:?}", write_time);
    println!(
        "  File size: {} bytes ({:.2} KB)",
        file_size,
        file_size as f64 / 1024.0
    );

    // Read back from Parquet file
    let start = std::time::Instant::now();
    let read_table = {
        let mut file = File::open(file_path)?;
        load_parquet_table(&mut file)?
    };
    let read_time = start.elapsed();

    println!("  Read from Parquet in {:?}", read_time);
    println!(
        "  Read table '{}' with {} rows",
        read_table.name, read_table.n_rows
    );

    // Verify data integrity
    verify_simple_table(&table, &read_table)?;
    println!("  ✓ Data integrity verified");

    Ok(())
}

#[cfg(feature = "parquet")]
/// Compression comparison example
async fn compression_example(compression_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let table = create_large_string_table();
    println!(
        "  Created table with {} rows for compression testing",
        table.n_rows
    );

    let compressions: Vec<(&str, Option<Compression>)> = vec![
        ("none", None),
        #[cfg(feature = "snappy")]
        ("snappy", Some(Compression::Snappy)),
        #[cfg(feature = "zstd")]
        ("zstd", Some(Compression::Zstd)),
    ];

    println!("  Testing different compression algorithms:");

    for (name, compression) in compressions {
        let file_path = compression_dir.join(format!("data_{}.parquet", name));

        // Write with compression
        let start = std::time::Instant::now();
        {
            let mut file = File::create(&file_path)?;
            write_parquet_table(&table, &mut file, compression)?;
        }
        let write_time = start.elapsed();

        let file_size = std::fs::metadata(&file_path)?.len();

        // Read back and verify
        let start = std::time::Instant::now();
        let read_table = {
            let mut file = File::open(&file_path)?;
            load_parquet_table(&mut file)?
        };
        let read_time = start.elapsed();

        println!(
            "    {}: {} bytes, write {:?}, read {:?}",
            name, file_size, write_time, read_time
        );

        // Verify data integrity
        assert_eq!(table.n_rows, read_table.n_rows);
        assert_eq!(table.cols.len(), read_table.cols.len());
    }

    println!("  ✓ All compression methods verified");
    Ok(())
}

#[cfg(feature = "parquet")]
/// Complex data types example
async fn complex_types_example(_file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let table = create_complex_types_table();
    println!(
        "  Created table with complex data types ({} rows)",
        table.n_rows
    );

    // Print schema
    for (i, col) in table.cols.iter().enumerate() {
        println!(
            "    Column {}: {} ({:?})",
            i, col.field.name, col.field.dtype
        );
    }

    // Round-trip test
    let read_table = roundtrip_parquet(&table, None)?;

    println!("  ✓ Complex types round-trip successful");
    println!(
        "    Original: {} rows, {} columns",
        table.n_rows,
        table.cols.len()
    );
    println!(
        "    Read back: {} rows, {} columns",
        read_table.n_rows,
        read_table.cols.len()
    );

    // Verify some sample data
    if let Array::TextArray(TextArray::String32(str_arr)) = &read_table.cols[2].array {
        if read_table.n_rows > 0 {
            let first_offset = str_arr.offsets[0] as usize;
            let second_offset = str_arr.offsets[1] as usize;
            let first_str = std::str::from_utf8(&str_arr.data[first_offset..second_offset])?;
            println!("    Sample string: '{}'", first_str);
        }
    }

    Ok(())
}

#[cfg(feature = "parquet")]
/// Large dataset performance example
async fn large_dataset_example(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let table = create_large_performance_table();
    println!("  Created large table with {} rows", table.n_rows);

    // Write with best compression for large files
    let compression: Option<Compression> = {
        #[cfg(feature = "zstd")]
        {
            Some(Compression::Zstd)
        }
        #[cfg(all(feature = "snappy", not(feature = "zstd")))]
        {
            Some(Compression::Snappy)
        }
        #[cfg(all(not(feature = "zstd"), not(feature = "snappy")))]
        {
            None
        }
    };

    let start = std::time::Instant::now();
    {
        let mut file = File::create(file_path)?;
        write_parquet_table(&table, &mut file, compression)?;
    }
    let write_time = start.elapsed();

    let file_size = std::fs::metadata(file_path)?.len();
    println!(
        "  Write performance: {} rows in {:?}",
        table.n_rows, write_time
    );
    println!(
        "  File size: {:.2} MB",
        file_size as f64 / (1024.0 * 1024.0)
    );

    // Read back and measure performance
    let start = std::time::Instant::now();
    let read_table = {
        let mut file = File::open(file_path)?;
        load_parquet_table(&mut file)?
    };
    let read_time = start.elapsed();

    println!(
        "  Read performance: {} rows in {:?}",
        read_table.n_rows, read_time
    );
    println!(
        "  Throughput: {:.0} rows/sec (read)",
        read_table.n_rows as f64 / read_time.as_secs_f64()
    );

    // Verify row count
    assert_eq!(table.n_rows, read_table.n_rows);
    println!("  ✓ Large dataset processing completed");

    Ok(())
}

#[cfg(feature = "parquet")]
fn create_simple_table() -> Table {
    let n = 1000;
    let ids: Vec64<i32> = (0..n as i32).collect();
    let values: Vec64<f64> = (0..n).map(|i| (i as f64) * 0.1 + 3.14).collect();
    Table::new(
        "simple_test".to_string(),
        Some(vec![
            FieldArray::from_arr("id", arr_i32!(ids)),
            FieldArray::from_arr("value", arr_f64!(values)),
        ]),
    )
}

#[cfg(feature = "parquet")]
fn create_large_string_table() -> Table {
    let n = 5000;
    let strings: Vec64<String> = (0..n)
        .map(|i| match i % 10 {
            0..=2 => "Common string pattern that appears frequently in the data".to_string(),
            3..=5 => format!("Variable content item number {}", i % 100),
            6..=8 => "Another repeated pattern for compression testing".to_string(),
            _ => format!("Unique entry {}", i),
        })
        .collect();
    let refs: Vec64<&str> = strings.iter().map(String::as_str).collect();
    Table::new(
        "compression_test".to_string(),
        Some(vec![FieldArray::from_arr("description", arr_str32!(refs))]),
    )
}

#[cfg(feature = "parquet")]
fn create_complex_types_table() -> Table {
    let n = 500;
    let ids: Vec64<i64> = (0..n).map(|i| i as i64 * 7 + 42).collect();
    let measurements: Vec64<f32> = (0..n).map(|i| (i as f32) * 0.01 - 25.5).collect();
    let strings: Vec64<String> = (0..n)
        .map(|i| match i % 5 {
            0 => "A".to_string(),
            1 => "Short".to_string(),
            2 => "Medium length string".to_string(),
            3 => "This is a considerably longer string for testing variable-length encoding"
                .to_string(),
            _ => format!("Generated string number {} with some content", i),
        })
        .collect();
    let refs: Vec64<&str> = strings.iter().map(String::as_str).collect();
    let flags: Vec64<bool> = (0..n).map(|i| (i * 3 + 1) % 7 < 3).collect();
    Table::new(
        "complex_types".to_string(),
        Some(vec![
            FieldArray::from_arr("large_id", arr_i64!(ids)),
            FieldArray::from_arr("measurement", arr_f32!(measurements)),
            FieldArray::from_arr("variable_text", arr_str32!(refs)),
            FieldArray::from_arr("flag", arr_bool!(flags)),
        ]),
    )
}

#[cfg(feature = "parquet")]
fn create_large_performance_table() -> Table {
    let n = 50_000;
    let ids: Vec64<i32> = (0..n as i32).collect();
    let values: Vec64<f64> = (0..n)
        .map(|i| (i as f64).sin() * 1000.0 + (i as f64) * 0.001)
        .collect();
    Table::new(
        "large_performance".to_string(),
        Some(vec![
            FieldArray::from_arr("sequence", arr_i32!(ids)),
            FieldArray::from_arr("computed_value", arr_f64!(values)),
        ]),
    )
}

#[cfg(feature = "parquet")]
/// Verify that two simple tables have the same data
fn verify_simple_table(
    original: &Table,
    read_back: &Table,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(original.n_rows, read_back.n_rows, "Row count mismatch");
    assert_eq!(
        original.cols.len(),
        read_back.cols.len(),
        "Column count mismatch"
    );

    // Check integer column
    if let (
        Array::NumericArray(NumericArray::Int32(orig_int)),
        Array::NumericArray(NumericArray::Int32(read_int)),
    ) = (&original.cols[0].array, &read_back.cols[0].array)
    {
        assert_eq!(
            orig_int.data.as_ref(),
            read_int.data.as_ref(),
            "Integer data mismatch"
        );
    }

    // Check float column
    if let (
        Array::NumericArray(NumericArray::Float64(orig_float)),
        Array::NumericArray(NumericArray::Float64(read_float)),
    ) = (&original.cols[1].array, &read_back.cols[1].array)
    {
        // Allow for small floating point differences
        for (orig, read) in orig_float
            .data
            .as_ref()
            .iter()
            .zip(read_float.data.as_ref().iter())
        {
            assert!(
                (orig - read).abs() < 1e-10,
                "Float data mismatch: {} vs {}",
                orig,
                read
            );
        }
    }

    Ok(())
}

#[cfg(feature = "parquet")]
/// Round-trip a table through Parquet format
fn roundtrip_parquet(
    table: &Table,
    compression: Option<Compression>,
) -> Result<Table, Box<dyn std::error::Error>> {
    let mut buf = Cursor::new(Vec::new());
    write_parquet_table(table, &mut buf, compression)?;
    buf.seek(SeekFrom::Start(0))?;
    let read_table = load_parquet_table(&mut buf)?;
    Ok(read_table)
}
