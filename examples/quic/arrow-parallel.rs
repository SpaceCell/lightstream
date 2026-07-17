// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parallel QUIC Arrow IPC Example
//!
//! Fans one table sequence across several concurrent unidirectional QUIC
//! streams on a single connection. The server accepts the same number of
//! streams and merges them back into one table sequence. Ordering holds
//! within a stream, but not across the set, so the merged tables can
//! interleave between streams.
//!
//! 1. Generate a self-signed TLS certificate via `rcgen`
//! 2. Create server and client `quinn::Endpoint`s with custom TLS config
//! 3. Client opens `STREAMS` unidirectional streams via `QuicParallelTableWriter`
//! 4. Server merges them via `QuicParallelTableReader`
//!
//! Run with:
//! ```sh
//! cargo run --example quic_arrow_parallel --features quic
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use std::sync::Arc;

use helpers::{make_table, table_schema};
use lightstream::models::readers::parallel::quic::QuicParallelTableReader;
use lightstream::models::writers::parallel::quic::QuicParallelTableWriter;
use lightstream::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};
use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;

const STREAMS: usize = 4;
const TABLES: usize = 12;

// ---------------------------------------------------------------------------
// TLS helpers
// ---------------------------------------------------------------------------

/// Certificate verifier that accepts any server certificate.
/// For examples and testing only.
#[derive(Debug)]
struct SkipVerification;

impl rustls::client::danger::ServerCertVerifier for SkipVerification {
    // Note in a prod setting these of course need to be completed robustly
    // and extensively, and generally not self-signed
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
fn generate_self_signed_cert() -> (
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
    let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    (
        vec![cert_der],
        rustls::pki_types::PrivateKeyDer::Pkcs8(key_der),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Parallel QUIC Arrow IPC Example");
    println!("===============================\n");

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
    // Admit the concurrent unidirectional streams the parallel writer opens.
    let transport = Arc::get_mut(&mut server_config.transport).unwrap();
    transport.max_concurrent_uni_streams((STREAMS as u32).into());

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
    let server_endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap())?;
    let addr = server_endpoint.local_addr()?;
    println!("QUIC server listening on {addr} across {STREAMS} streams");

    let server = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let connection = incoming.await.unwrap();
        println!("Server accepted QUIC connection.");

        let reader = QuicParallelTableReader::accept(&connection, STREAMS, SortBehaviour::Ordered, None)
            .await
            .unwrap();
        let tables = reader.read_all_tables().await.unwrap();

        connection.close(0u32.into(), b"done");
        server_endpoint.wait_idle().await;
        tables
    });

    // --- Client endpoint ---
    let mut client_endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())?;
    client_endpoint.set_default_client_config(client_config);

    let connection = client_endpoint.connect(addr, "localhost")?.await?;
    println!("Client connected to QUIC server.");

    let mut writer =
        QuicParallelTableWriter::open(&connection, STREAMS, schema, Vec::new(), None).await?;
    println!(
        "Client sending {TABLES} tables across {} streams",
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

    println!("\nParallel QUIC Arrow IPC example completed successfully!");
    Ok(())
}
