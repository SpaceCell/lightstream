// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TableReader example - flexible Arrow IPC reading.
//!
//! Demonstrates read_all_tables, read_tables with limits,
//! SuperTable aggregation, and single-table combination.

use lightstream::enums::BufferChunkSize;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::table::TableReader;
use lightstream::models::streams::disk::DiskByteStream;
use lightstream::models::writers::ipc::table_stream::TableStreamWriter;
use minarrow::{Field, FieldArray, Table, Vec64, arr_f64, arr_i32, arr_str32};
use std::path::Path;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;

    // 1. Read all tables
    println!("1. Read All Tables");
    let path = temp_dir.path().join("all.stream");
    let source = make_tables(5);
    write_stream(&source, &path).await?;

    let reader = open_reader(&path).await?;
    let tables = reader.read_all_tables().await?;
    let total: usize = tables.iter().map(|t| t.n_rows).sum();
    println!("  Read {} batches, {} rows\n", tables.len(), total);

    // 2. Read limited batches
    println!("2. Read Limited");
    let path = temp_dir.path().join("limited.stream");
    write_stream(&make_tables(10), &path).await?;

    let reader = open_reader(&path).await?;
    let tables = reader.read_tables(Some(3)).await?;
    assert_eq!(tables.len(), 3);
    println!("  Read {} of 10 batches\n", tables.len());

    // 3. SuperTable - preserves batch boundaries
    println!("3. SuperTable");
    let path = temp_dir.path().join("super.stream");
    write_stream(&make_varying_tables(), &path).await?;

    let reader = open_reader(&path).await?;
    let st = reader
        .read_to_super_table(Some("Combined".to_string()), None)
        .await?;
    println!("  {} batches, {} total rows", st.batches.len(), st.n_rows);
    for (i, batch) in st.batches.iter().enumerate() {
        println!("    Batch {}: {} rows", i, batch.n_rows);
    }
    println!();

    // 4. Combine to single table - concatenates rows
    println!("4. Combine to Single Table");
    let path = temp_dir.path().join("combined.stream");
    let source = make_tables(4);
    let expected: usize = source.iter().map(|t| t.n_rows).sum();
    write_stream(&source, &path).await?;

    let reader = open_reader(&path).await?;
    let combined = reader.combine_to_table(Some("Merged".to_string())).await?;
    assert_eq!(combined.n_rows, expected);
    println!(
        "  {} rows across {} columns",
        combined.n_rows,
        combined.cols.len()
    );

    Ok(())
}

fn make_tables(n: usize) -> Vec<Table> {
    let mut next_id = 0i32;
    (0..n)
        .map(|batch| {
            let rows = 1000;
            let ids: Vec64<i32> = (next_id..next_id + rows).collect();
            next_id += rows;
            let values: Vec64<f64> = (0..rows as usize)
                .map(|i| (i as f64 + batch as f64 * 10000.0) * 0.001)
                .collect();
            let labels: Vec64<String> = (0..rows as usize)
                .map(|i| format!("b{}_item{:04}", batch, i))
                .collect();
            let refs: Vec64<&str> = labels.iter().map(String::as_str).collect();
            Table::new(
                format!("test_batch_{}", batch),
                Some(vec![
                    FieldArray::from_arr("id", arr_i32!(ids)),
                    FieldArray::from_arr("value", arr_f64!(values)),
                    FieldArray::from_arr("label", arr_str32!(refs)),
                ]),
            )
        })
        .collect()
}

fn make_varying_tables() -> Vec<Table> {
    [500, 1500, 800, 2000, 300]
        .iter()
        .enumerate()
        .map(|(i, &rows)| {
            let ids: Vec64<i32> = (0..rows).map(|j| (i as i32) * 10000 + j as i32).collect();
            Table::new(
                format!("batch_{}", i),
                Some(vec![FieldArray::from_arr("value", arr_i32!(ids))]),
            )
        })
        .collect()
}

async fn open_reader(path: &Path) -> Result<TableReader<Vec64<u8>>, Box<dyn std::error::Error>> {
    let stream = DiskByteStream::open(path, BufferChunkSize::Custom(8192)).await?;
    Ok(TableReader::new(stream, 8192, IPCMessageProtocol::Stream, None))
}

async fn write_stream(tables: &[Table], path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema: Vec<Field> = tables[0].schema().iter().map(|f| (**f).clone()).collect();
    let mut writer = TableStreamWriter::<Vec64<u8>>::new(schema, IPCMessageProtocol::Stream, None);
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
