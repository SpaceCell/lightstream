//! TCP Transport Example
//!
//! Demonstrates the Lightstream protocol over a TCP connection.
//! TCP is the simplest transport: bind, accept, connect.
//!
//! Run with:
//! ```sh
//! cargo run --example tcp_lightstream --features "protocol,tcp,msgpack"
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use helpers::{recv_and_print_all, register_demo_types, send_demo_messages};
use lightstream::models::protocol::connection::TcpLightstreamConnection;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TCP Transport Example");
    println!("=====================\n");

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("Listener bound to {}", addr);

    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.unwrap();
        println!("Server accepted connection from {}", peer);

        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        register_demo_types(&mut conn);
        recv_and_print_all(&mut conn).await;
        println!("Server connection closed.");
    });

    let client_stream = tokio::net::TcpStream::connect(addr).await?;
    println!("Client connected to {}", addr);

    let mut client = TcpLightstreamConnection::from_tcp(client_stream);
    register_demo_types(&mut client);
    send_demo_messages(&mut client, "tcp").await?;

    server.await?;
    println!("\nTCP transport example completed successfully!");
    Ok(())
}
