// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TableStreamReader example for reading streaming Arrow IPC data.
//!
//! Demonstrates reading Arrow IPC streams chunk-by-chunk, both in
//! Stream and File protocols, and processing large datasets without
//! loading everything into memory.

use futures_util::StreamExt;
use lightstream::enums::BufferChunkSize;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::file_table::FileTableReader;
use lightstream::models::readers::ipc::table::TableReader;
use lightstream::models::streams::disk::DiskByteStream;
use lightstream::models::writers::ipc::table_stream::TableStreamWriter;
use minarrow::{Field, FieldArray, Table, Vec64, arr_i32, arr_str32};
use std::path::Path;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;

    // Stream protocol: read batches one at a time
    println!("1. Stream Protocol");
    let stream_path = temp_dir.path().join("data.stream");
    let tables = make_tables(3, 100, "small");
    write_stream(&tables, &stream_path, IPCMessageProtocol::Stream).await?;

    let stream = DiskByteStream::open(&stream_path, BufferChunkSize::Custom(8192)).await?;
    let mut reader = TableReader::<Vec64<u8>>::new(stream, 8192, IPCMessageProtocol::Stream, None);
    let mut count = 0;
    while let Some(batch) = reader.next().await {
        let t = batch?;
        count += 1;
        println!("  Batch {}: {} rows", count, t.n_rows);
    }
    println!("  Read {} batches\n", count);

    // File protocol: random-access batch reading
    println!("2. File Protocol");
    let file_path = temp_dir.path().join("data.arrow");
    write_stream(&tables, &file_path, IPCMessageProtocol::File).await?;

    let reader = FileTableReader::open(&file_path)?;
    for i in 0..reader.num_batches() {
        let t = reader.read_batch(i)?;
        println!("  Batch {}: {} rows", i + 1, t.n_rows);
    }
    println!("  Read {} batches\n", reader.num_batches());

    // Large streaming dataset: process and discard each batch
    println!("3. Large Dataset Streaming");
    let large_path = temp_dir.path().join("large.stream");
    let large = make_tables(10, 5000, "large");
    write_stream(&large, &large_path, IPCMessageProtocol::Stream).await?;

    let stream = DiskByteStream::open(&large_path, BufferChunkSize::Custom(4096)).await?;
    let mut reader = TableReader::<Vec64<u8>>::new(stream, 4096, IPCMessageProtocol::Stream, None);
    let start = std::time::Instant::now();
    let mut total_rows = 0;
    while let Some(batch) = reader.next().await {
        total_rows += batch?.n_rows;
    }
    println!("  Processed {} rows in {:?}", total_rows, start.elapsed());

    Ok(())
}

fn make_tables(n: usize, rows: usize, prefix: &str) -> Vec<Table> {
    (0..n)
        .map(|i| {
            let start = i * rows;
            let ids: Vec64<i32> = (start..start + rows).map(|x| x as i32).collect();
            let labels: Vec64<String> = (0..rows)
                .map(|j| format!("{}_{}_row_{}", prefix, i, j))
                .collect();
            let refs: Vec64<&str> = labels.iter().map(String::as_str).collect();
            Table::new(
                format!("{}_{}", prefix, i),
                Some(vec![
                    FieldArray::from_arr("id", arr_i32!(ids)),
                    FieldArray::from_arr("label", arr_str32!(refs)),
                ]),
            )
        })
        .collect()
}

async fn write_stream(
    tables: &[Table],
    path: &Path,
    protocol: IPCMessageProtocol,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema: Vec<Field> = tables[0].schema().iter().map(|f| (**f).clone()).collect();
    let mut writer = TableStreamWriter::<Vec64<u8>>::new(schema, protocol, None);
    for table in tables {
        writer.write(&table.clone().into())?;
    }
    writer.finish()?;

    let mut file = File::create(path).await?;
    while let Some(frame) = writer.next_frame() {
        file.write_all(frame?.as_ref()).await?;
    }
    file.flush().await?;
    Ok(())
}
