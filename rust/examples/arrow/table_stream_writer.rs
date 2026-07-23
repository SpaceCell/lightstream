// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TableStreamWriter example for streaming Arrow IPC data.
//!
//! Demonstrates encoding tables as streaming Arrow IPC frames and
//! processing them individually - useful for network streaming, pipes, etc.

use futures_util::StreamExt;
use lightstream::enums::BufferChunkSize;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::table::TableReader;
use lightstream::models::streams::disk::DiskByteStream;
use lightstream::models::writers::ipc::table_stream::TableStreamWriter;
use minarrow::{Field, FieldArray, Table, Vec64, arr_i32, arr_str32};
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let stream_path = temp_dir.path().join("stream_output.arrow");

    let tables = create_sample_tables();
    println!("Created {} tables for streaming", tables.len());

    // Write tables to stream, processing frames individually
    let schema: Vec<Field> = tables[0].schema().iter().map(|f| (**f).clone()).collect();
    let mut writer = TableStreamWriter::<Vec64<u8>>::new(schema, IPCMessageProtocol::Stream, None);

    for table in &tables {
        writer.write(&table.clone().into())?;
    }
    writer.finish()?;

    let mut file = File::create(&stream_path).await?;
    let mut frame_count = 0;
    let mut total_bytes = 0;

    while let Some(frame_result) = writer.next_frame() {
        let frame = frame_result?;
        file.write_all(frame.as_ref()).await?;
        total_bytes += frame.len();
        frame_count += 1;
    }
    file.flush().await?;
    println!("Wrote {} frames, {} bytes", frame_count, total_bytes);

    // Read back and verify
    let disk_stream =
        DiskByteStream::open(&stream_path, BufferChunkSize::Custom(64 * 1024)).await?;
    let mut reader =
        TableReader::<Vec64<u8>>::new(disk_stream, 64 * 1024, IPCMessageProtocol::Stream, None);

    let mut read_count = 0;
    while let Some(result) = reader.next().await {
        let table = result?;
        read_count += 1;
        println!(
            "  Read batch {}: {} rows, {} cols",
            read_count,
            table.n_rows,
            table.cols.len()
        );
    }
    println!("Read back {} batches", read_count);

    Ok(())
}

fn create_sample_tables() -> Vec<Table> {
    (0..3)
        .map(|batch| {
            let start = batch * 1000;
            let ids: Vec64<i32> = (start..start + 1000).map(|i| i as i32).collect();
            let descs: Vec64<String> = (0..1000)
                .map(|i| format!("batch_{}_item_{:04}", batch, i))
                .collect();
            let refs: Vec64<&str> = descs.iter().map(String::as_str).collect();
            Table::new(
                format!("batch_{}", batch),
                Some(vec![
                    FieldArray::from_arr("batch_id", arr_i32!(ids)),
                    FieldArray::from_arr("description", arr_str32!(refs)),
                ]),
            )
        })
        .collect()
}
