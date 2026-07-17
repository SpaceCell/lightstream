// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! WebSocket Arrow IPC over TLS Example
//!
//! Streams Arrow tables over `wss://` using a runtime-generated
//! self-signed certificate. The client pins that certificate as the only
//! trusted root - production callers supply real roots through their
//! `rustls::ClientConfig` instead.
//!
//! The server runs two handshakes back-to-back on the accepted TCP
//! socket: rustls first, then the tokio-tungstenite WebSocket upgrade
//! over the encrypted channel.
//!
//! Run with:
//! ```sh
//! cargo run --example websocket_arrow_tls --features "websocket,tls"
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use std::sync::Arc;

use helpers::{make_table, table_schema};
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::websocket::WebSocketTableReader;
use lightstream::traits::transport_reader::IPCTransportReader;
use lightstream::traits::transport_writer::IPCTransportWriter;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("WebSocket Arrow IPC over TLS Example");
    println!("====================================\n");

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install ring crypto provider");

    // Self-signed cert for `localhost`. Test-only.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert_der: CertificateDer<'static> = cert.cert.der().clone();
    let key_der: PrivateKeyDer<'static> = PrivateKeyDer::try_from(cert.signing_key.serialize_der())
        .map_err(|e| format!("private key: {e}"))?;

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)?;
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let mut roots = RootCertStore::empty();
    roots.add(cert_der)?;
    let client_config = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );

    let tcp_listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = tcp_listener.local_addr()?;
    println!("TCP listener bound to {addr}");

    let acceptor_for_task = acceptor.clone();
    let server = tokio::spawn(async move {
        let (tcp_stream, peer) = tcp_listener.accept().await.expect("accept");
        println!("Server accepted TCP from {peer}, performing TLS handshake...");
        let tls = acceptor_for_task.accept(tcp_stream).await.expect("tls");
        println!("TLS handshake complete, upgrading to WebSocket...");

        // tokio-tungstenite's MaybeTlsStream wraps only the client TLS
        // variant; on the server we hand accept_async the raw server-side
        // tls stream and let it run the WebSocket upgrade over the
        // already-encrypted channel.
        let ws_stream = tokio_tungstenite::accept_async(tls)
            .await
            .expect("ws upgrade");
        println!("WebSocket handshake complete.");

        let raw = ws_stream.into_inner();
        let (read_half, write_half) = tokio::io::split(raw);
        let reader = WebSocketTableReader::from_halves(
            read_half,
            write_half,
            IPCMessageProtocol::Stream,
            None,
        );
        let tables = reader.read_all_tables().await.expect("read tables");
        for t in &tables {
            println!(
                "  Server got table: {} rows, {} cols",
                t.n_rows,
                t.cols.len()
            );
        }
        assert_eq!(tables.len(), 3);
        println!("Server received all {} tables over wss://.", tables.len());
    });

    let url = format!("wss://localhost:{}", addr.port());
    let mut writer = lightstream::models::writers::websocket::WebSocketTableWriter::connect_tls(
        &url,
        client_config,
        table_schema(),
        None,
    )
    .await?;
    println!("Client wss:// handshake complete to {url}");

    writer.write_table(make_table("batch_1", 5)).await?;
    writer.write_table(make_table("batch_2", 3)).await?;
    writer.write_table(make_table("batch_3", 7)).await?;
    writer.finish().await?;

    server.await?;

    println!("\nWebSocket Arrow IPC over TLS example completed successfully!");
    Ok(())
}
