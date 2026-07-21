// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # TLS configuration
//!
//! Builds the QUIC and WebTransport TLS configurations from PEM files.
//! An accepting peer presents the certificate chain in `tls_cert` with
//! the private key in `tls_key`, and a connecting peer verifies the
//! server against the roots in `tls_ca`.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::RootCertStore;
use wtransport::tls::Identity;

/// Application protocol the QUIC wire negotiates. WebTransport manages
/// its own h3 negotiation inside wtransport.
const QUIC_ALPN: &[u8] = b"ls";

/// Loads the PEM certificate chain from `tls_cert`.
fn certificate_chain(cert: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(cert)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls_cert unreadable: {e}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls_cert unreadable: {e}")))
}

/// Loads the PEM private key from `tls_key`.
fn private_key(key: &Path) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls_key unreadable: {e}")))
}

/// Builds the root store a connecting peer verifies servers against.
fn root_store(ca: &Path) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for root in certificate_chain(ca)? {
        roots
            .add(root)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls_ca invalid: {e}")))?;
    }
    Ok(roots)
}

/// Builds an accepting peer's rustls configuration from PEM files,
/// advertising the given application protocols.
fn server_tls(cert: &Path, key: &Path, alpn: &[&[u8]]) -> io::Result<rustls::ServerConfig> {
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificate_chain(cert)?, private_key(key)?)
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("tls_cert and tls_key mismatch: {e}"),
            )
        })?;
    tls.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(tls)
}

/// Builds a connecting peer's rustls configuration from a PEM root
/// file, negotiating the given application protocols.
fn client_tls(ca: &Path, alpn: &[&[u8]]) -> io::Result<rustls::ClientConfig> {
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(root_store(ca)?)
        .with_no_client_auth();
    tls.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(tls)
}

/// Builds the accepting peer's QUIC configuration from PEM files.
pub fn quic_server(cert: &Path, key: &Path) -> io::Result<quinn::ServerConfig> {
    let crypto = QuicServerConfig::try_from(server_tls(cert, key, &[QUIC_ALPN])?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls configuration rejected: {e}")))?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(crypto)))
}

/// Builds the connecting peer's QUIC configuration from a PEM root file.
pub fn quic_client(ca: &Path) -> io::Result<quinn::ClientConfig> {
    let crypto = QuicClientConfig::try_from(client_tls(ca, &[QUIC_ALPN])?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls configuration rejected: {e}")))?;
    Ok(quinn::ClientConfig::new(Arc::new(crypto)))
}

/// Builds the accepting peer's wss configuration from PEM files.
pub fn wss_server(cert: &Path, key: &Path) -> io::Result<Arc<rustls::ServerConfig>> {
    Ok(Arc::new(server_tls(cert, key, &[])?))
}

/// Builds the connecting peer's wss configuration from a PEM root file.
pub fn wss_client(ca: &Path) -> io::Result<Arc<rustls::ClientConfig>> {
    Ok(Arc::new(client_tls(ca, &[])?))
}

/// Builds the accepting peer's https configuration from PEM files.
/// HTTP/2 over TLS negotiates the h2 application protocol.
pub fn https_server(cert: &Path, key: &Path) -> io::Result<Arc<rustls::ServerConfig>> {
    Ok(Arc::new(server_tls(cert, key, &[b"h2"])?))
}

/// Builds the connecting peer's https configuration from a PEM root
/// file. HTTP/2 over TLS negotiates the h2 application protocol.
pub fn https_client(ca: &Path) -> io::Result<Arc<rustls::ClientConfig>> {
    Ok(Arc::new(client_tls(ca, &[b"h2"])?))
}

/// Builds the accepting peer's WebTransport configuration from PEM
/// files. wtransport derives its TLS setup from the identity.
pub async fn wt_server(
    addr: SocketAddr,
    cert: &Path,
    key: &Path,
) -> io::Result<wtransport::ServerConfig> {
    let identity = Identity::load_pemfiles(cert, key)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls_cert or tls_key unreadable: {e}")))?;
    Ok(wtransport::ServerConfig::builder()
        .with_bind_address(addr)
        .with_identity(identity)
        .build())
}

/// Builds the connecting peer's WebTransport configuration from a PEM
/// root file. WebTransport runs over HTTP/3, so the configuration
/// negotiates the h3 application protocol.
pub fn wt_client(ca: &Path) -> io::Result<wtransport::ClientConfig> {
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(root_store(ca)?)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    Ok(wtransport::ClientConfig::builder()
        .with_bind_default()
        .with_custom_tls(tls)
        .build())
}
