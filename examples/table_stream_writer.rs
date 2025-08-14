//! TableStreamWriter example for streaming Arrow IPC data.
//!
//! This example demonstrates how to:
//! - Create multiple tables with sample data
//! - Use TableStreamWriter to encode them as streaming Arrow IPC frames
//! - Process frames individually (useful for network streaming, pipes, etc.)
//! - Write the stream to a file and verify it can be read back

use futures_util::StreamExt;
use lightstream::enums::BufferChunkSize;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::table_stream_reader::TableStreamReader64;
use lightstream::models::streams::disk::DiskByteStream;
use lightstream::models::writers::ipc::table_stream_writer::TableStreamWriter;
use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{
    Array, Buffer, Field, FieldArray, IntegerArray, NumericArray, StringArray, Table, TextArray,
    Vec64,
};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TableStreamWriter Example");
    println!("========================");

    // Create multiple tables to demonstrate streaming
    let tables = create_sample_tables();
    println!("Created {} tables for streaming", tables.len());

    // Create a temporary directory for our example
    let temp_dir = tempdir()?;
    let stream_path = temp_dir.path().join("stream_output.arrow");

    // Example 1: Stream encoding with frame-by-frame processing
    println!("\n1. Streaming Encoding with Frame Processing");
    stream_encode_tables(&tables, &stream_path).await?;

    // Example 2: Read back the streamed data
    println!("\n2. Reading Back Streamed Data");
    read_streamed_data(&stream_path).await?;

    println!("\n✓ TableStreamWriter example completed successfully!");

    Ok(())
}

/// Create sample tables for streaming demonstration
fn create_sample_tables() -> Vec<Table> {
    let mut tables = Vec::new();

    // Create 3 batches of data
    for batch_num in 0..3 {
        let start_id = batch_num * 1000;
        let n_rows = 1000;

        // Create integer column
        let int_data: Vec<i32> = (start_id..start_id + n_rows).map(|i| i as i32).collect();
        let int_array = Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(Vec64::from_slice(&int_data)),
            null_mask: None,
        })));
        let int_field = FieldArray::new(
            Field {
                name: "batch_id".into(),
                dtype: ArrowType::Int32,
                nullable: false,
                metadata: Default::default(),
            },
            int_array,
        );

        // Create string column with batch-specific data
        let individual_strings: Vec<String> = (0..n_rows)
            .map(|i| format!("batch_{}_item_{:04}", batch_num, i))
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
                name: "description".into(),
                dtype: ArrowType::String,
                nullable: false,
                metadata: Default::default(),
            },
            str_array,
        );

        let table = Table {
            name: format!("batch_{}", batch_num),
            n_rows,
            cols: vec![int_field, str_field],
        };

        tables.push(table);
    }

    tables
}

/// Use TableStreamWriter to encode tables and write frames to file
async fn stream_encode_tables(
    tables: &[Table],
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // Extract schema from first table
    let schema: Vec<Field> = tables[0]
        .cols
        .iter()
        .map(|col| (*col.field).clone())
        .collect();

    // Create stream writer
    let mut stream_writer = TableStreamWriter::<Vec64<u8>>::new(schema, IPCMessageProtocol::Stream);

    println!("  Writing {} tables to stream...", tables.len());

    // Write all tables
    for (i, table) in tables.iter().enumerate() {
        stream_writer.write(table)?;
        println!("    Added table {} to stream ('{}')", i + 1, table.name);
    }

    // Finish the stream (adds EOS marker)
    stream_writer.finish()?;
    println!("  Stream finished, processing frames...");

    // Open output file
    let mut file = File::create(output_path).await?;
    let mut frame_count = 0;
    let mut total_bytes = 0;

    // Process frames one by one (this is useful for network streaming, etc.)
    while let Some(frame_result) = stream_writer.next_frame() {
        let frame = frame_result?;
        let frame_size = frame.len();

        // Write frame to file
        file.write_all(frame.as_ref()).await?;

        frame_count += 1;
        total_bytes += frame_size;

        println!("    Processed frame {}: {} bytes", frame_count, frame_size);
    }

    file.flush().await?;

    println!(
        "  ✓ Wrote {} frames, {} total bytes",
        frame_count, total_bytes
    );

    // Verify stream is finished
    assert!(stream_writer.is_finished(), "Stream should be finished");

    Ok(())
}

/// Read back the streamed data to verify it works
async fn read_streamed_data(stream_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("  Opening streamed file for reading...");

    // Open the stream file
    let disk_stream = DiskByteStream::open(stream_path, BufferChunkSize::Custom(64 * 1024)).await?;
    let mut reader = TableStreamReader64::new(disk_stream, 64 * 1024, IPCMessageProtocol::Stream);

    let mut table_count = 0;
    let mut total_rows = 0;

    // Read tables from stream
    while let Some(result) = reader.next().await {
        let table = result?;
        table_count += 1;
        total_rows += table.n_rows;

        println!(
            "    Read table {}: '{}' with {} rows",
            table_count, table.name, table.n_rows
        );

        // Show some sample data from each table
        if let Array::NumericArray(NumericArray::Int32(int_arr)) = &table.cols[0].array {
            let sample_ids = &int_arr.data.as_ref()[0..3.min(table.n_rows)];
            println!("      Sample IDs: {:?}", sample_ids);
        }

        if let Array::TextArray(TextArray::String32(str_arr)) = &table.cols[1].array {
            // Extract first string to show
            if table.n_rows > 0 {
                let first_offset = str_arr.offsets[0] as usize;
                let second_offset = str_arr.offsets[1] as usize;
                let first_str = std::str::from_utf8(&str_arr.data[first_offset..second_offset])?;
                println!("      Sample description: '{}'", first_str);
            }
        }
    }

    println!(
        "  ✓ Read {} tables with {} total rows",
        table_count, total_rows
    );

    Ok(())
}
