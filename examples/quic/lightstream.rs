//! QUIC Transport Example
//!
//! Demonstrates the Lightstream protocol over QUIC. Most of the code here is
//! TLS certificate generation and quinn endpoint setup. The protocol usage
//! itself is minimal.
//!
//! Run with:
//! ```sh
//! cargo run --example quic_lightstream --features "protocol,quic,msgpack"
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use std::sync::Arc;

use helpers::{recv_and_print_all, register_demo_types, send_demo_messages};
use lightstream::models::protocol::connection::QuicLightstreamConnection;

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
fn generate_self_signed_cert() -> (
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
    let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    (
        vec![cert_der],
        rustls::pki_types::PrivateKeyDer::Pkcs8(key_der),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("QUIC Transport Example");
    println!("======================\n");

    // --- TLS setup ---
    let (certs, key) = generate_self_signed_cert();

    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    server_crypto.alpn_protocols = vec![b"ls".to_vec()];
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)?,
    ));
    let transport = Arc::get_mut(&mut server_config.transport).unwrap();
    transport.max_concurrent_bidi_streams(1u8.into());

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
    println!("QUIC server listening on {}", addr);

    let server = tokio::spawn(async move {
        let incoming = server_endpoint.accept().await.unwrap();
        let connection = incoming.await.unwrap();
        println!("Server accepted QUIC connection.");

        let (send_stream, recv_stream) = connection.accept_bi().await.unwrap();

        let mut conn = QuicLightstreamConnection::from_quic(recv_stream, send_stream);
        register_demo_types(&mut conn);
        recv_and_print_all(&mut conn).await;

        connection.close(0u32.into(), b"done");
        server_endpoint.wait_idle().await;
        println!("Server connection closed.");
    });

    // --- Client endpoint ---
    let mut client_endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())?;
    client_endpoint.set_default_client_config(client_config);

    let connection = client_endpoint.connect(addr, "localhost")?.await?;
    println!("Client connected to QUIC server.");

    let (send_stream, recv_stream) = connection.open_bi().await?;

    let mut client = QuicLightstreamConnection::from_quic(recv_stream, send_stream);
    register_demo_types(&mut client);
    send_demo_messages(&mut client, "quic").await?;

    server.await?;
    println!("\nQUIC transport example completed successfully!");
    Ok(())
}
