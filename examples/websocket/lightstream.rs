//! WebSocket Transport Example
//!
//! Demonstrates the Lightstream protocol over WebSocket.
//! TCP accept followed by WebSocket upgrade on server, `connect_async` on client.
//!
//! Run with:
//! ```sh
//! cargo run --example websocket_lightstream --features "protocol,websocket,msgpack"
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use helpers::{recv_and_print_all, register_demo_types, send_demo_messages};
use lightstream::models::protocol::connection::WebSocketLightstreamConnection;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("WebSocket Transport Example");
    println!("===========================\n");

    let tcp_listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = tcp_listener.local_addr()?;
    println!("TCP listener bound to {} (for WebSocket upgrade)", addr);

    let server = tokio::spawn(async move {
        let (tcp_stream, peer) = tcp_listener.accept().await.unwrap();
        println!(
            "Server accepted TCP from {}, upgrading to WebSocket...",
            peer
        );

        let ws_stream = tokio_tungstenite::accept_async(tcp_stream).await.unwrap();
        println!("WebSocket handshake complete.");

        let mut conn = WebSocketLightstreamConnection::from_websocket(ws_stream);
        register_demo_types(&mut conn);
        recv_and_print_all(&mut conn).await;
        println!("Server connection closed.");
    });

    let url = format!("ws://{}", addr);
    let (ws_stream, _response) = tokio_tungstenite::connect_async(&url).await?;
    println!("Client WebSocket connected to {}", url);

    let mut client = WebSocketLightstreamConnection::from_websocket(ws_stream);
    register_demo_types(&mut client);
    send_demo_messages(&mut client, "websocket").await?;

    server.await?;
    println!("\nWebSocket transport example completed successfully!");
    Ok(())
}
