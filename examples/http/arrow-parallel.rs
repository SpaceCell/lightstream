// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parallel HTTP/2 Arrow IPC Example
//!
//! Fans one table sequence across several concurrent HTTP/2 request
//! streams on a single client connection. The server accepts the same
//! number of request streams and merges them back into one table
//! sequence. Ordering holds within a stream, but not across the set, so the
//! merged tables can interleave between streams.
//!
//! Run with:
//! ```sh
//! cargo run --example http_arrow_parallel --features http
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use helpers::{make_table, table_schema};
use lightstream::models::readers::parallel::http::HttpParallelTableReader;
use lightstream::models::writers::parallel::http::HttpParallelTableWriter;
use lightstream::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};
use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;
use tokio::net::TcpListener;

const STREAMS: usize = 4;
const TABLES: usize = 12;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Parallel HTTP/2 Arrow IPC Example");
    println!("=================================\n");

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("HTTP/2 server listening on http://{addr} across {STREAMS} streams");

    let server = tokio::spawn(async move {
        let (tcp, peer) = listener.accept().await.expect("accept");
        println!("Server accepted TCP from {peer}");
        // from_tcp runs the h2 handshake with upload-sized flow-control
        // windows, then accepts STREAMS request streams and merges them.
        let reader = HttpParallelTableReader::from_tcp(tcp, STREAMS, SortBehaviour::Ordered, None)
            .await
            .expect("accept streams");
        reader.read_all_tables().await.expect("read tables")
    });

    let url = format!("http://{addr}/ingest");
    let mut writer =
        HttpParallelTableWriter::connect(&url, STREAMS, table_schema(), Vec::new(), None).await?;
    println!(
        "Client POSTing {TABLES} tables across {} streams",
        writer.stream_count()
    );

    // Tables round-robin across the streams in open order.
    for i in 0..TABLES {
        writer.write_table(make_table(&format!("batch_{i}"), 5)).await?;
    }
    writer.finish().await?;

    let tables = server.await?;
    let total_rows: usize = tables.iter().map(|(t, _)| t.n_rows).sum();
    println!(
        "Server merged {} tables ({total_rows} rows) from {STREAMS} streams.",
        tables.len()
    );
    assert_eq!(tables.len(), TABLES);

    println!("\nParallel HTTP/2 Arrow IPC example completed successfully!");
    Ok(())
}
