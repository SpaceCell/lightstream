//! WebSocket Arrow IPC Example
//!
//! Streams Arrow tables over a WebSocket connection using Arrow IPC framing,
//! without the Lightstream multiplexing protocol.
//!
//! 1. Accept a WebSocket connection via `tokio_tungstenite::accept_async`
//! 2. Client writes Arrow tables via `WebSocketTableWriter`
//! 3. Server reads and verifies via `WebSocketTableReader`
//!
//! Run with:
//! ```sh
//! cargo run --example websocket_arrow --features websocket
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use helpers::{make_table, table_schema};
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::websocket::WebSocketTableReader;
use lightstream::models::streams::websocket::WebSocketByteStream;
use lightstream::models::writers::websocket::WebSocketTableWriter;
use lightstream::traits::transport_reader::TransportReader;
use lightstream::traits::transport_writer::TransportWriter;
use tokio::net::TcpListener;
use tokio_tungstenite::MaybeTlsStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("WebSocket Arrow IPC Example");
    println!("===========================\n");

    let schema = table_schema();

    // --- Server: TCP listener for WebSocket upgrade ---
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = tcp_listener.local_addr()?;
    println!("TCP listener bound to {} (for WebSocket upgrade)", addr);

    let server = tokio::spawn(async move {
        let (tcp_stream, peer) = tcp_listener.accept().await.unwrap();
        println!(
            "Server accepted TCP from {}, upgrading to WebSocket...",
            peer
        );

        // Wrap in MaybeTlsStream so the split type matches WebSocketTableReader
        let ws_stream = tokio_tungstenite::accept_async(MaybeTlsStream::Plain(tcp_stream))
            .await
            .unwrap();
        println!("WebSocket handshake complete.");

        let (_, read_half) = futures_util::StreamExt::split(ws_stream);
        let byte_stream = WebSocketByteStream::new(read_half);
        let reader = WebSocketTableReader::from_stream(byte_stream, IPCMessageProtocol::Stream);
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

    // --- Client: connect via WebSocket and write ---
    let url = format!("ws://{}", addr);
    let mut writer = WebSocketTableWriter::connect(&url, schema).await?;
    println!("Client WebSocket connected to {}", url);

    writer.write_table(make_table("batch_1", 5)).await?;
    writer.write_table(make_table("batch_2", 3)).await?;
    writer.write_table(make_table("batch_3", 7)).await?;
    writer.finish().await?;

    server.await?;

    println!("\nWebSocket Arrow IPC example completed successfully!");
    Ok(())
}
