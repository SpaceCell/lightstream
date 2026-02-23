//! QUIC Arrow IPC Example
//!
//! Streams Arrow tables over QUIC using Arrow IPC framing,
//! without the Lightstream multiplexing protocol.
//!
//! 1. Generate a self-signed TLS certificate via `rcgen`
//! 2. Create server and client `quinn::Endpoint`s with custom TLS config
//! 3. Open a bidirectional stream
//! 4. Client writes Arrow tables via `QuicTableWriter`
//! 5. Server reads and verifies via `QuicTableReader`
//!
//! Run with:
//! ```sh
//! cargo run --example quic_arrow --features quic
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use std::sync::Arc;

use helpers::{make_table, table_schema};
use lightstream::models::readers::quic::QuicTableReader;
use lightstream::models::writers::quic::QuicTableWriter;
use lightstream::traits::transport_reader::TransportReader;
use lightstream::traits::transport_writer::TransportWriter;

// ---------------------------------------------------------------------------
// TLS helpers
// ---------------------------------------------------------------------------

/// Certificate verifier that accepts any server certificate.
/// For examples and testing only.
#[derive(Debug)]
struct SkipVerification;

impl rustls::client::danger::ServerCertVerifier for SkipVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

/// Generate a self-signed certificate and return (cert_chain, private_key).
fn generate_self_signed_cert(
) -> (Vec<rustls::pki_types::CertificateDer<'static>>, rustls::pki_types::PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
    let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    (vec![cert_der], rustls::pki_types::PrivateKeyDer::Pkcs8(key_der))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("QUIC Arrow IPC Example");
    println!("======================\n");

    let schema = table_schema();

    // --- TLS setup ---
    let (certs, key) = generate_self_signed_cert();

    // Server TLS config
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    server_crypto.alpn_protocols = vec![b"ls".to_vec()];
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)?,
    ));
    let transport = Arc::get_mut(&mut server_config.transport).unwrap();
    transport.max_concurrent_bidi_streams(1u8.into());

    // Client TLS config
    let mut client_crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipVerification))
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"ls".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)?,
    ));

    // --- Server endpoint ---
    let server_endpoint =
        quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap())?;
    let addr = server_endpoint.local_addr()?;
    println!("QUIC server listening on {}", addr);

    let server = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let connection = incoming.await.unwrap();
        println!("Server accepted QUIC connection.");

        // Accept a bidirectional stream opened by the client
        let (_send_stream, recv_stream) = connection.accept_bi().await.unwrap();

        let reader = QuicTableReader::from_recv(recv_stream);
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

        // Gracefully shut down
        connection.close(0u32.into(), b"done");
        server_endpoint.wait_idle().await;
    });

    // --- Client endpoint ---
    let mut client_endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())?;
    client_endpoint.set_default_client_config(client_config);

    let connection = client_endpoint.connect(addr, "localhost")?.await?;
    println!("Client connected to QUIC server.");

    // Open a bidirectional stream to the server
    let (send_stream, _recv_stream) = connection.open_bi().await?;

    let mut writer = QuicTableWriter::new(send_stream, schema)?;

    writer.write_table(make_table("batch_1", 5)).await?;
    writer.write_table(make_table("batch_2", 3)).await?;
    writer.write_table(make_table("batch_3", 7)).await?;
    writer.finish().await?;

    server.await?;

    println!("\nQUIC Arrow IPC example completed successfully!");
    Ok(())
}
