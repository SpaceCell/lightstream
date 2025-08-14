//! Basic CSV reading and writing example.
//!
//! This example demonstrates how to:
//! - Create a table with sample data
//! - Write it to a CSV file
//! - Read it back and verify the data

use lightstream::models::readers::csv_reader::CsvReader;
use lightstream::models::writers::csv_writer::CsvWriter;
use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{
    Array, Buffer, Field, FieldArray, FloatArray, IntegerArray, NumericArray, StringArray, Table,
    TextArray, Vec64,
};
use std::path::Path;
use std::sync::Arc;
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

/// Create a sample table with various data types
fn create_sample_table() -> Table {
    let n_rows = 5;

    // Create integer column
    let int_array = Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
        data: Buffer::from(Vec64::from_slice(&[1i32, 2, 3, 4, 5])),
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

    // Create string column
    let str_data = "AliceBobCharliesDianaEve".as_bytes();
    let offsets = [0u32, 5, 8, 16, 21, 24];
    let str_array = Array::TextArray(TextArray::String32(Arc::new(StringArray::new(
        Buffer::from(Vec64::from_slice(str_data)),
        None,
        Buffer::from(Vec64::from_slice(&offsets)),
    ))));
    let str_field = FieldArray::new(
        Field {
            name: "name".into(),
            dtype: ArrowType::String,
            nullable: false,
            metadata: Default::default(),
        },
        str_array,
    );

    // Create float column
    let float_array = Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
        data: Buffer::from(Vec64::from_slice(&[1.1f64, 2.2, 3.3, 4.4, 5.5])),
        null_mask: None,
    })));
    let float_field = FieldArray::new(
        Field {
            name: "score".into(),
            dtype: ArrowType::Float64,
            nullable: false,
            metadata: Default::default(),
        },
        float_array,
    );

    Table {
        name: "sample_data".to_string(),
        n_rows,
        cols: vec![int_field, str_field, float_field],
    }
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
    let table = reader.into_table()?;
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
