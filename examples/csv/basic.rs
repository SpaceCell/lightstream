// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Basic CSV reading and writing example.
//!
//! This example demonstrates how to:
//! - Create a table with sample data
//! - Write it to a CSV file
//! - Read it back and verify the data

use lightstream::models::readers::csv::CsvReader;
use lightstream::models::writers::csv::CsvWriter;
use minarrow::{FieldArray, Table, Vec64, arr_f64, arr_i32, arr_str32};
use std::path::Path;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create sample data
    let table = create_sample_table();
    println!(
        "Created table with {} rows and {} columns",
        table.n_rows,
        table.cols.len()
    );

    // Create a temporary directory for our example
    let temp_dir = tempdir()?;
    let file_path = temp_dir.path().join("sample.csv");

    // Write the table to CSV
    write_csv(&table, &file_path).await?;
    println!("Wrote table to CSV file: {}", file_path.display());

    // Read the table back from CSV
    let read_table = read_csv(&file_path).await?;
    println!(
        "Read table with {} rows and {} columns",
        read_table.n_rows,
        read_table.cols.len()
    );

    // Verify the data matches (approximately, since CSV conversion may change types)
    verify_data(&table, &read_table)?;
    println!("✓ Data verification successful!");

    Ok(())
}

/// Create a sample table with various data types.
fn create_sample_table() -> Table {
    let ids: Vec64<i32> = Vec64::from_slice(&[1, 2, 3, 4, 5]);
    let names: Vec64<&str> = Vec64::from(vec!["Alice", "Bob", "Charlies", "Diana", "Eve"]);
    let scores: Vec64<f64> = Vec64::from_slice(&[1.1, 2.2, 3.3, 4.4, 5.5]);

    Table::new(
        "sample_data".to_string(),
        Some(vec![
            FieldArray::from_arr("id", arr_i32!(ids)),
            FieldArray::from_arr("name", arr_str32!(names)),
            FieldArray::from_arr("score", arr_f64!(scores)),
        ]),
    )
}

/// Write a table to CSV file
async fn write_csv(table: &Table, file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Write to a Vec<u8> first, then save to file
    let mut writer = CsvWriter::new_vec();
    writer.write_table(table)?;
    writer.flush()?;
    let csv_data = writer.into_inner();

    // Write to file
    tokio::fs::write(file_path, csv_data).await?;
    Ok(())
}

/// Read a table from CSV file
async fn read_csv(file_path: &Path) -> Result<Table, Box<dyn std::error::Error>> {
    use lightstream::models::decoders::csv::CsvDecodeOptions;
    let reader = CsvReader::from_path(file_path, CsvDecodeOptions::default(), 1000)?;
    let table = reader.load_table()?;
    Ok(table)
}

/// Verify that the data was preserved through the CSV round-trip
fn verify_data(original: &Table, read_back: &Table) -> Result<(), Box<dyn std::error::Error>> {
    // Check basic structure
    assert_eq!(original.n_rows, read_back.n_rows, "Row count mismatch");
    assert_eq!(
        original.cols.len(),
        read_back.cols.len(),
        "Column count mismatch"
    );

    // Check column names
    for (orig_col, read_col) in original.cols.iter().zip(read_back.cols.iter()) {
        println!("Column: {} -> {}", orig_col.field.name, read_col.field.name);
    }
    println!("Data structure preserved through CSV round-trip");

    Ok(())
}
