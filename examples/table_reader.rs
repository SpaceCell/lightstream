//! Standard TableReader example for flexible Arrow IPC reading.
//!
//! This example demonstrates how to:
//! - Use the generic TableReader for various data sources
//! - Read all tables vs limited number of tables
//! - Combine tables into SuperTable, preserving batches
//! - Combine tables into single Table, concatenating rows
//! - Handle different stream sources - disk, network, etc.

use lightstream::enums::BufferChunkSize;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::table_reader::TableReader;
use lightstream::models::streams::disk::DiskByteStream;
use lightstream::models::writers::ipc::table_stream_writer::TableStreamWriter;
use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{
    Array, Buffer, Field, FieldArray, FloatArray, IntegerArray, NumericArray, StringArray, Table,
    TextArray, Vec64,
};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Standard TableReader Example");
    println!("===========================");

    // Create a temporary directory for our examples
    let temp_dir = tempdir()?;

    // Example 1: Read all tables
    println!("\n1. Reading All Tables");
    let all_tables_path = temp_dir.path().join("all_tables.stream");
    read_all_tables_example(&all_tables_path).await?;

    // Example 2: Read limited number of tables
    println!("\n2. Reading Limited Number of Tables");
    let limited_path = temp_dir.path().join("limited.stream");
    read_limited_tables_example(&limited_path).await?;

    // Example 3: Combine to SuperTable (preserving batches)
    println!("\n3. Combining to SuperTable");
    let super_table_path = temp_dir.path().join("super_table.stream");
    super_table_example(&super_table_path).await?;

    // Example 4: Combine to single Table (concatenated rows)
    println!("\n4. Combining to Single Table");
    let single_table_path = temp_dir.path().join("single_table.stream");
    single_table_example(&single_table_path).await?;

    println!("\n✓ Standard TableReader example completed successfully!");

    Ok(())
}

/// Demonstrate reading all tables from a stream
async fn read_all_tables_example(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Create test data
    let source_tables = create_test_tables(5);
    write_test_stream(&source_tables, file_path).await?;

    println!("  Created stream with {} tables", source_tables.len());

    // Read all tables using TableReader
    let disk_stream = DiskByteStream::open(file_path, BufferChunkSize::Custom(8192)).await?;
    let reader = TableReader::new(disk_stream, 8192, IPCMessageProtocol::Stream);

    let start = std::time::Instant::now();
    let tables = reader.read_all_tables().await?;
    let duration = start.elapsed();

    println!("  Read {} tables in {:?}", tables.len(), duration);

    // Verify data integrity
    let total_rows: usize = tables.iter().map(|t| t.n_rows).sum();
    let expected_rows: usize = source_tables.iter().map(|t| t.n_rows).sum();

    println!("  Total rows: {} (expected: {})", total_rows, expected_rows);
    assert_eq!(total_rows, expected_rows);

    // Show sample from each table
    for (i, table) in tables.iter().enumerate() {
        println!(
            "    Table {}: '{}' with {} rows",
            i + 1,
            table.name,
            table.n_rows
        );

        if let Array::NumericArray(NumericArray::Int32(int_arr)) = &table.cols[0].array {
            if table.n_rows > 0 {
                println!("      First ID: {}", int_arr.data.as_ref()[0]);
            }
        }
    }

    println!("  ✓ Successfully read all tables");
    Ok(())
}

/// Demonstrate reading a limited number of tables
async fn read_limited_tables_example(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Create larger dataset
    let source_tables = create_test_tables(10);
    write_test_stream(&source_tables, file_path).await?;

    println!("  Created stream with {} tables", source_tables.len());

    // Read only first 3 tables
    let disk_stream = DiskByteStream::open(file_path, BufferChunkSize::Custom(8192)).await?;
    let reader = TableReader::new(disk_stream, 8192, IPCMessageProtocol::Stream);

    let start = std::time::Instant::now();
    let tables = reader.read_tables(Some(3)).await?;
    let duration = start.elapsed();

    println!(
        "  Read {} tables (limited to 3) in {:?}",
        tables.len(),
        duration
    );
    assert_eq!(tables.len(), 3, "Should have read exactly 3 tables");

    // Verify we got the first 3 tables
    for (i, table) in tables.iter().enumerate() {
        println!(
            "    Table {}: '{}' with {} rows",
            i + 1,
            table.name,
            table.n_rows
        );
        assert_eq!(table.name, format!("test_batch_{}", i));
    }

    println!("  ✓ Successfully read limited number of tables");
    Ok(())
}

/// Demonstrate combining tables into a SuperTable
async fn super_table_example(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Create test data with varying sizes
    let source_tables = create_varying_size_tables();
    write_test_stream(&source_tables, file_path).await?;

    println!(
        "  Created stream with {} tables of varying sizes",
        source_tables.len()
    );

    // Read into SuperTable
    let disk_stream = DiskByteStream::open(file_path, BufferChunkSize::Custom(8192)).await?;
    let reader = TableReader::new(disk_stream, 8192, IPCMessageProtocol::Stream);

    let start = std::time::Instant::now();
    let super_table = reader
        .read_to_super_table(Some("CombinedData".to_string()), None)
        .await?;
    let duration = start.elapsed();

    println!(
        "  Created SuperTable '{}' in {:?}",
        super_table.name, duration
    );
    println!("    Total rows: {}", super_table.n_rows);
    println!("    Number of batches: {}", super_table.batches.len());
    println!("    Schema fields: {}", super_table.schema.len());

    // Show batch information
    for (i, batch) in super_table.batches.iter().enumerate() {
        println!("      Batch {}: {} rows", i + 1, batch.n_rows);
    }

    // Verify schema consistency
    for field in &super_table.schema {
        println!("    Field: '{}' ({:?})", field.name, field.dtype);
    }

    println!("  ✓ Successfully created SuperTable preserving batch structure");
    Ok(())
}

/// Demonstrate combining all tables into a single Table
async fn single_table_example(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Create test data
    let source_tables = create_test_tables(4);
    let expected_total_rows: usize = source_tables.iter().map(|t| t.n_rows).sum();
    write_test_stream(&source_tables, file_path).await?;

    println!(
        "  Created stream with {} tables ({} total rows)",
        source_tables.len(),
        expected_total_rows
    );

    // Read into single combined Table
    let disk_stream = DiskByteStream::open(file_path, BufferChunkSize::Custom(8192)).await?;
    let reader = TableReader::new(disk_stream, 8192, IPCMessageProtocol::Stream);

    let start = std::time::Instant::now();
    let combined_table = reader
        .combine_to_table(Some("CombinedBatches".to_string()))
        .await?;
    let duration = start.elapsed();

    println!(
        "  Created combined Table '{}' in {:?}",
        combined_table.name, duration
    );
    println!(
        "    Total rows: {} (expected: {})",
        combined_table.n_rows, expected_total_rows
    );
    println!("    Number of columns: {}", combined_table.cols.len());

    assert_eq!(
        combined_table.n_rows, expected_total_rows,
        "Row count should match"
    );

    // Show sample data from the combined table
    if let Array::NumericArray(NumericArray::Int32(int_arr)) = &combined_table.cols[0].array {
        let sample_size = 10.min(combined_table.n_rows);
        let sample_data = &int_arr.data.as_ref()[0..sample_size];
        println!("    First {} IDs: {:?}", sample_size, sample_data);

        // Verify data continuity (should be sequential across batches)
        if combined_table.n_rows > 0 {
            let first_id = int_arr.data.as_ref()[0];
            let last_id = int_arr.data.as_ref()[combined_table.n_rows - 1];
            println!("    ID range: {} to {}", first_id, last_id);
        }
    }

    println!("  ✓ Successfully combined all batches into single Table");
    Ok(())
}

/// Create test tables with sequential data
fn create_test_tables(num_tables: usize) -> Vec<Table> {
    let mut tables = Vec::new();
    let mut global_id = 0;

    for batch_num in 0..num_tables {
        let n_rows = 1000;

        // Sequential integer IDs across all batches
        let int_data: Vec<i32> = (global_id..global_id + n_rows).map(|i| i as i32).collect();
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

        // Float values with batch-specific pattern
        let float_data: Vec<f64> = (0..n_rows)
            .map(|i| (i as f64 + batch_num as f64 * 10000.0) * 0.001)
            .collect();
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

        // String labels
        let individual_strings: Vec<String> = (0..n_rows)
            .map(|i| format!("batch{}_item{:04}", batch_num, i))
            .collect();
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

        let table = Table {
            name: format!("test_batch_{}", batch_num),
            n_rows,
            cols: vec![int_field, float_field, str_field],
        };

        tables.push(table);
        global_id += n_rows;
    }

    tables
}

/// Create tables with varying sizes for SuperTable demonstration
fn create_varying_size_tables() -> Vec<Table> {
    let sizes = vec![500, 1500, 800, 2000, 300]; // Different batch sizes
    let mut tables = Vec::new();

    for (batch_num, &n_rows) in sizes.iter().enumerate() {
        // Simple integer sequence
        let int_data: Vec<i32> = (0..n_rows)
            .map(|i| (batch_num as i32) * 10000 + i as i32)
            .collect();
        let int_array = Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(Vec64::from_slice(&int_data)),
            null_mask: None,
        })));
        let int_field = FieldArray::new(
            Field {
                name: "batch_value".into(),
                dtype: ArrowType::Int32,
                nullable: false,
                metadata: Default::default(),
            },
            int_array,
        );

        let table = Table {
            name: format!("varying_batch_{}", batch_num),
            n_rows,
            cols: vec![int_field],
        };

        tables.push(table);
    }

    tables
}

/// Write tables to stream for testing
async fn write_test_stream(
    tables: &[Table],
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema: Vec<Field> = tables[0]
        .cols
        .iter()
        .map(|col| (*col.field).clone())
        .collect();
    let mut stream_writer = TableStreamWriter::<Vec64<u8>>::new(schema, IPCMessageProtocol::Stream);

    // Write all tables
    for table in tables {
        stream_writer.write(table)?;
    }
    stream_writer.finish()?;

    // Write to file
    let mut file = File::create(output_path).await?;
    while let Some(frame_result) = stream_writer.next_frame() {
        let frame = frame_result?;
        file.write_all(frame.as_ref()).await?;
    }
    file.flush().await?;

    Ok(())
}
