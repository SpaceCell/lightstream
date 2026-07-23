// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TCP Arrow IPC over TLS Example
//!
//! Same wire shape as the `tcp_arrow` example, but the channel is wrapped
//! with rustls. A self-signed certificate is generated at runtime via
//! `rcgen` and the client pins it as the only trusted root - production
//! callers would supply real roots through their `rustls::ClientConfig`.
//!
//! Run with:
//! ```sh
//! cargo run --example tcp_arrow_tls --features "tcp,tls"
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use std::sync::Arc;

use helpers::{make_table, table_schema};
use lightstream::models::readers::tcp::TcpTableReader;
use lightstream::models::streams::tcp::TcpByteStream;
use lightstream::models::writers::tcp::TcpTableWriter;
use lightstream::traits::transport_reader::IPCTransportReader;
use lightstream::traits::transport_writer::IPCTransportWriter;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TCP Arrow IPC over TLS Example");
    println!("==============================\n");

    // Install ring as the rustls process-wide crypto provider. The library
    // never touches the global; doing it at the example boundary keeps the
    // rest of the demo concise.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install ring crypto provider");

    // --- Create a self-signed cert valid for `localhost`. -----------------
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert_der: CertificateDer<'static> = cert.cert.der().clone();
    let key_der: PrivateKeyDer<'static> = PrivateKeyDer::try_from(cert.signing_key.serialize_der())
        .map_err(|e| format!("private key: {e}"))?;

    // --- Build server-side TLS acceptor. --------------------------------
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)?;
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    // --- Build client-side ClientConfig that pins the self-signed cert. -
    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let client_config = Arc::new(client_config);

    // --- Server: listen, accept, upgrade to TLS, read tables. -----------
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("Listener bound to {addr}");

    let acceptor_for_task = acceptor.clone();
    let server = tokio::spawn(async move {
        let (tcp, peer) = listener.accept().await.expect("accept");
        println!("Server accepted TCP connection from {peer}");
        let tls = acceptor_for_task.accept(tcp).await.expect("tls handshake");
        println!("Server TLS handshake complete");

        let (read_half, _write_half) = tokio::io::split(tls);
        let byte_stream =
            TcpByteStream::from_tls_read_half(read_half, lightstream::enums::BufferChunkSize::Http);
        let reader = TcpTableReader::from_stream(
            byte_stream,
            lightstream::enums::IPCMessageProtocol::Stream,
            None,
        );
        let tables = reader.read_all_tables().await.expect("read all tables");
        for t in &tables {
            println!(
                "  Server got table: {} rows, {} cols",
                t.n_rows,
                t.cols.len()
            );
        }
        assert_eq!(tables.len(), 3);
        println!("Server received all {} tables over TLS.", tables.len());
    });

    // --- Client: connect over TLS using the pinned root. ----------------
    let server_name = ServerName::try_from("localhost".to_string())?;
    let mut writer =
        TcpTableWriter::connect_tls(addr, server_name, client_config, table_schema(), None).await?;
    println!("Client TLS handshake complete to {addr}");

    writer.write_table(make_table("batch_1", 5)).await?;
    writer.write_table(make_table("batch_2", 3)).await?;
    writer.write_table(make_table("batch_3", 7)).await?;
    writer.finish().await?;

    server.await?;

    println!("\nTCP Arrow IPC over TLS example completed successfully!");
    Ok(())
}
