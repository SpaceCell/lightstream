//! WebTransport Transport Example
//!
//! Demonstrates the Lightstream protocol over WebTransport.
//! Uses self-signed identity with certificate hash pinning on the client.
//!
//! Run with:
//! ```sh
//! cargo run --example webtransport_lightstream --features "protocol,webtransport,msgpack"
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use helpers::{recv_and_print_all, register_demo_types, send_demo_messages};
use lightstream::models::protocol::connection::WebTransportLightstreamConnection;
use wtransport::tls::Sha256Digest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("WebTransport Transport Example");
    println!("==============================\n");

    // --- Identity and certificate pinning ---
    let identity = wtransport::Identity::self_signed(["localhost", "127.0.0.1", "::1"])?;
    let cert_hash: Sha256Digest = identity.certificate_chain().as_slice()[0].hash();
    println!("Generated self-signed identity, cert hash: {:?}", cert_hash);

    // --- Server endpoint ---
    let server_config = wtransport::ServerConfig::builder()
        .with_bind_default(0)
        .with_identity(identity)
        .build();
    let server_endpoint = wtransport::Endpoint::server(server_config)?;
    let addr = server_endpoint.local_addr()?;
    println!("WebTransport server listening on {}", addr);

    let server = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await;
        let session_request = incoming.await.unwrap();
        let connection = session_request.accept().await.unwrap();
        println!("Server accepted WebTransport session.");

        let recv_stream = connection.accept_uni().await.unwrap();
        let send_stream = connection.open_uni().await.unwrap().await.unwrap();

        let mut conn =
            WebTransportLightstreamConnection::from_webtransport(recv_stream, send_stream);
        register_demo_types(&mut conn);
        recv_and_print_all(&mut conn).await;
        println!("Server connection closed.");
    });

    // --- Client endpoint with certificate pinning ---
    let client_config = wtransport::ClientConfig::builder()
        .with_bind_default()
        .with_server_certificate_hashes([cert_hash])
        .build();
    let client_endpoint = wtransport::Endpoint::client(client_config)?;

    let url = format!("https://127.0.0.1:{}", addr.port());
    let connection = client_endpoint.connect(&url).await?;
    println!("Client connected via WebTransport to {}", url);

    let send_stream = connection.open_uni().await?.await?;
    let recv_stream = connection.accept_uni().await?;

    let mut client = WebTransportLightstreamConnection::from_webtransport(recv_stream, send_stream);
    register_demo_types(&mut client);
    send_demo_messages(&mut client, "webtransport").await?;

    server.await?;
    println!("\nWebTransport transport example completed successfully!");
    Ok(())
}
