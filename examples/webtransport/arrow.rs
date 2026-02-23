//! WebTransport Arrow IPC Example
//!
//! Streams Arrow tables over WebTransport using Arrow IPC framing,
//! without the Lightstream multiplexing protocol.
//!
//! 1. Generate a self-signed identity via `wtransport::Identity`
//! 2. Pin the certificate hash on the client via `Sha256Digest`
//! 3. Create server and client `wtransport::Endpoint`s
//! 4. Open a unidirectional stream from client to server
//! 5. Client writes Arrow tables via `WebTransportTableWriter`
//! 6. Server reads and verifies via `WebTransportTableReader`
//!
//! Run with:
//! ```sh
//! cargo run --example webtransport_arrow --features webtransport
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use helpers::{make_table, table_schema};
use lightstream::models::readers::webtransport::WebTransportTableReader;
use lightstream::models::writers::webtransport::WebTransportTableWriter;
use lightstream::traits::transport_reader::TransportReader;
use lightstream::traits::transport_writer::TransportWriter;
use wtransport::tls::Sha256Digest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("WebTransport Arrow IPC Example");
    println!("==============================\n");

    let schema = table_schema();

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
        // Accept incoming session request
        let incoming = server_endpoint.accept().await;
        let session_request = incoming.await.unwrap();
        let connection = session_request.accept().await.unwrap();
        println!("Server accepted WebTransport session.");

        // Accept a unidirectional stream from the client
        let recv_stream = connection.accept_uni().await.unwrap();

        let reader = WebTransportTableReader::from_recv(recv_stream);
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

    // --- Client endpoint with certificate pinning ---
    let client_config = wtransport::ClientConfig::builder()
        .with_bind_default()
        .with_server_certificate_hashes([cert_hash])
        .build();
    let client_endpoint = wtransport::Endpoint::client(client_config)?;

    let url = format!("https://127.0.0.1:{}", addr.port());
    let connection = client_endpoint.connect(&url).await?;
    println!("Client connected via WebTransport to {}", url);

    // Open a unidirectional stream to the server
    let send_stream = connection.open_uni().await?.await?;

    let mut writer = WebTransportTableWriter::new(send_stream, schema)?;

    writer.write_table(make_table("batch_1", 5)).await?;
    writer.write_table(make_table("batch_2", 3)).await?;
    writer.write_table(make_table("batch_3", 7)).await?;
    writer.finish().await?;

    server.await?;

    println!("\nWebTransport Arrow IPC example completed successfully!");
    Ok(())
}
