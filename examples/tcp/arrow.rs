//! TCP Arrow IPC Example
//!
//! Streams Arrow tables over a raw TCP connection using Arrow IPC framing,
//! without the Lightstream multiplexing protocol.
//!
//! 1. Bind a TCP listener and accept a connection
//! 2. Client writes Arrow tables via `TcpTableWriter`
//! 3. Server reads and verifies via `TcpTableReader`
//!
//! Run with:
//! ```sh
//! cargo run --example tcp_arrow --features tcp
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use helpers::{make_table, table_schema};
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::tcp::TcpTableReader;
use lightstream::models::streams::tcp::TcpByteStream;
use lightstream::models::writers::tcp::TcpTableWriter;
use lightstream::traits::transport_reader::TransportReader;
use lightstream::traits::transport_writer::TransportWriter;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TCP Arrow IPC Example");
    println!("=====================\n");

    let schema = table_schema();

    // --- Server: listen and accept ---
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("Listener bound to {}", addr);

    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.unwrap();
        println!("Server accepted connection from {}", peer);

        let (read_half, _write_half) = stream.into_split();
        let byte_stream =
            TcpByteStream::from_read_half(read_half, lightstream::enums::BufferChunkSize::Http);
        let reader = TcpTableReader::from_stream(byte_stream, IPCMessageProtocol::Stream);
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
    let mut writer = TcpTableWriter::connect(addr, schema).await?;
    println!("Client connected to {}", addr);

    writer.write_table(make_table("batch_1", 5)).await?;
    writer.write_table(make_table("batch_2", 3)).await?;
    writer.write_table(make_table("batch_3", 7)).await?;
    writer.finish().await?;

    server.await?;

    println!("\nTCP Arrow IPC example completed successfully!");
    Ok(())
}
