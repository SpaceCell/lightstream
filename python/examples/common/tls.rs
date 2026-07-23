// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TLS server configurations for the transport example servers, built
//! from the PEM files the run-example.py router generates.

#![allow(dead_code)]

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Builds a rustls server configuration from PEM files, advertising
/// the given application protocols.
pub fn rustls_server(
    cert: &Path,
    key: &Path,
    alpn: &[&[u8]],
) -> io::Result<Arc<rustls::ServerConfig>> {
    let chain = CertificateDer::pem_file_iter(cert)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let key = PrivateKeyDer::from_pem_file(key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Ok(Arc::new(config))
}

/// Builds a quinn server configuration from PEM files, on the `ls`
/// application protocol the QUIC wire negotiates.
pub fn quic_server(cert: &Path, key: &Path) -> io::Result<quinn::ServerConfig> {
    let tls = rustls_server(cert, key, &[b"ls"])?;
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls.as_ref().clone())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    Ok(quinn::ServerConfig::with_crypto(Arc::new(crypto)))
}

/// Builds a wtransport server configuration from PEM files, bound to
/// the given address.
pub async fn wt_server(
    addr: SocketAddr,
    cert: &Path,
    key: &Path,
) -> io::Result<wtransport::ServerConfig> {
    let identity = wtransport::tls::Identity::load_pemfiles(cert, key)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    Ok(wtransport::ServerConfig::builder()
        .with_bind_address(addr)
        .with_identity(identity)
        .build())
}
