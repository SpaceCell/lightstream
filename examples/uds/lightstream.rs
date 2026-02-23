//! Unix Domain Socket Transport Example
//!
//! Demonstrates the Lightstream protocol over Unix domain sockets.
//! Uses a temporary directory for the socket path.
//!
//! Run with:
//! ```sh
//! cargo run --example uds_lightstream --features "protocol,uds,msgpack"
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use helpers::{recv_and_print_all, register_demo_types, send_demo_messages};
use lightstream::models::protocol::connection::UdsLightstreamConnection;
use tokio::net::UnixListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Unix Domain Socket Transport Example");
    println!("=====================================\n");

    let temp_dir = tempfile::tempdir()?;
    let socket_path = temp_dir.path().join("lightstream.sock");
    println!("Socket path: {}", socket_path.display());

    let listener = UnixListener::bind(&socket_path)?;
    println!("Listener bound.");

    let path_clone = socket_path.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        println!("Server accepted connection.");

        let mut conn = UdsLightstreamConnection::from_uds(stream);
        register_demo_types(&mut conn);
        recv_and_print_all(&mut conn).await;
        println!("Server connection closed.");
    });

    let client_stream = tokio::net::UnixStream::connect(&path_clone).await?;
    println!("Client connected.");

    let mut client = UdsLightstreamConnection::from_uds(client_stream);
    register_demo_types(&mut client);
    send_demo_messages(&mut client, "uds").await?;

    server.await?;
    println!("\nUDS transport example completed successfully!");
    Ok(())
}
