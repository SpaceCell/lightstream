// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Unix Domain Socket Arrow IPC Example
//!
//! Streams Arrow tables over Unix domain sockets using Arrow IPC framing,
//! without the Lightstream multiplexing protocol.
//!
//! 1. Create a temporary socket path via `tempfile`
//! 2. Client writes Arrow tables via `UdsTableWriter`
//! 3. Server reads and verifies via `UdsTableReader`
//!
//! Run with:
//! ```sh
//! cargo run --example uds_arrow --features uds
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use helpers::{make_table, table_schema};
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::uds::UdsTableReader;
use lightstream::models::streams::uds::UdsByteStream;
use lightstream::models::writers::uds::UdsTableWriter;
use lightstream::traits::transport_reader::IPCTransportReader;
use lightstream::traits::transport_writer::IPCTransportWriter;
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Unix Domain Socket Arrow IPC Example");
    println!("=====================================\n");

    let schema = table_schema();

    // --- Transport setup: temporary socket path ---
    let temp_dir = tempfile::tempdir()?;
    let socket_path = temp_dir.path().join("lightstream.sock");
    println!("Socket path: {}", socket_path.display());

    let listener = UnixListener::bind(&socket_path)?;
    println!("Listener bound.");

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        println!("Server accepted connection.");

        let (read_half, _write_half) = stream.into_split();
        let byte_stream =
            UdsByteStream::from_read_half(read_half, lightstream::enums::BufferChunkSize::Http);
        let reader = UdsTableReader::from_stream(byte_stream, IPCMessageProtocol::Stream, None);
        let tables = reader.read_all_tables().await.unwrap();

        for table in &tables {
            println!(
                "  Server got table: {} rows, {} cols",
                table.n_rows,
                table.cols.len()
            );
        }

        assert_eq!(tables.len(), 3);
        println!("Server received all {} tables.", tables.len());
    });

    // --- Client: connect and write ---
    let mut writer = UdsTableWriter::connect(&socket_path, schema, None).await?;
    println!("Client connected.");

    writer.write_table(make_table("batch_1", 5)).await?;
    writer.write_table(make_table("batch_2", 3)).await?;
    writer.write_table(make_table("batch_3", 7)).await?;
    writer.finish().await?;

    server.await?;

    println!("\nUDS Arrow IPC example completed successfully!");
    Ok(())
}
