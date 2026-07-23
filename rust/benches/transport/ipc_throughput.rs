// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Criterion benchmarks measuring raw Arrow IPC streaming throughput
//! across all transport types.
//!
//! Connection setup is excluded from the timed region. Each sample
//! establishes one connection, then streams `iters` batches through it,
//! timing only the encode/decode pipeline.

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;
use bench_helpers::*;

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
#[cfg(any(
    feature = "tcp",
    feature = "uds",
    feature = "websocket",
    feature = "quic",
    feature = "webtransport"
))]
use lightstream::enums::IPCMessageProtocol;
#[cfg(any(
    feature = "tcp",
    feature = "uds",
    feature = "websocket",
    feature = "quic",
    feature = "webtransport"
))]
use lightstream::models::readers::ipc::table::TableReader;
#[cfg(any(
    feature = "tcp",
    feature = "uds",
    feature = "websocket",
    feature = "quic",
    feature = "webtransport"
))]
use lightstream::traits::transport_writer::IPCTransportWriter;
#[cfg(any(
    feature = "tcp",
    feature = "uds",
    feature = "websocket",
    feature = "quic",
    feature = "webtransport"
))]
use minarrow::Field;
#[cfg(any(
    feature = "tcp",
    feature = "uds",
    feature = "websocket",
    feature = "quic",
    feature = "webtransport"
))]
use minarrow::Vec64;
#[cfg(feature = "tcp")]
use tokio::net::TcpListener;
#[cfg(feature = "uds")]
use tokio::net::UnixListener;

#[allow(unused_variables)]
fn bench_ipc_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = Arc::new(make_bench_table(BENCH_ROWS));
    #[cfg(any(
        feature = "tcp",
        feature = "uds",
        feature = "websocket",
        feature = "quic",
        feature = "webtransport"
    ))]
    let schema: Vec<Field> = table.schema().iter().map(|f| (**f).clone()).collect();

    let mut group = c.benchmark_group("ipc_throughput");
    group.throughput(Throughput::Bytes(logical_payload_bytes(BENCH_ROWS)));

    #[cfg(feature = "tcp")]
    group.bench_function("tcp", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio::spawn(async move {
                    let mut writer = lightstream::models::writers::tcp::TcpTableWriter::connect(
                        addr,
                        write_schema,
                        None,
                    )
                    .await
                    .unwrap();
                    writer.register_dictionary(
                        3,
                        vec!["red".to_string(), "green".to_string(), "blue".to_string()],
                    );

                    for _ in 0..n {
                        writer.write_table((*write_table).clone()).await.unwrap();
                    }
                    writer.finish().await.unwrap();
                });

                // Writer uses Vec64<u8> (64-byte alignment), reader must match
                let (socket, _) = listener.accept().await.unwrap();
                let (read_half, _write_half) = socket.into_split();
                let mut reader =
                    TableReader::<Vec64<u8>>::new(read_half, 64 * 1024, IPCMessageProtocol::Stream, None);

                let start = std::time::Instant::now();
                let mut count = 0u64;
                while let Some(batch) = reader.read_next().await.unwrap() {
                    assert!(batch.n_rows > 0);
                    std::hint::black_box(&batch.cols);
                    count += 1;
                }
                let elapsed = start.elapsed();
                assert_eq!(count, n);

                writer.await.unwrap();
                elapsed
            }
        });
    });

    #[cfg(feature = "uds")]
    group.bench_function("uds", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                let tempdir = tempfile::tempdir().unwrap();
                let socket_path = tempdir.path().join("bench_ipc.sock");
                let listener = UnixListener::bind(&socket_path).unwrap();

                let path = socket_path.clone();
                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio::spawn(async move {
                    let mut writer = lightstream::models::writers::uds::UdsTableWriter::connect(
                        &path,
                        write_schema,
                        None,
                    )
                    .await
                    .unwrap();
                    writer.register_dictionary(
                        3,
                        vec!["red".to_string(), "green".to_string(), "blue".to_string()],
                    );

                    for _ in 0..n {
                        writer.write_table((*write_table).clone()).await.unwrap();
                    }
                    writer.finish().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let (read_half, _write_half) = socket.into_split();
                let mut reader =
                    TableReader::<Vec64<u8>>::new(read_half, 64 * 1024, IPCMessageProtocol::Stream, None);

                let start = std::time::Instant::now();
                let mut count = 0u64;
                while let Some(batch) = reader.read_next().await.unwrap() {
                    assert!(batch.n_rows > 0);
                    std::hint::black_box(&batch.cols);
                    count += 1;
                }
                let elapsed = start.elapsed();
                assert_eq!(count, n);

                writer.await.unwrap();
                elapsed
            }
        });
    });

    #[cfg(feature = "websocket")]
    group.bench_function("websocket", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let url = format!("ws://{addr}");

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio::spawn(async move {
                    let mut writer =
                        lightstream::models::writers::websocket::WebSocketTableWriter::connect(
                            &url,
                            write_schema,
                            None,
                        )
                        .await
                        .unwrap();
                    writer.register_dictionary(
                        3,
                        vec!["red".to_string(), "green".to_string(), "blue".to_string()],
                    );
                    for _ in 0..n {
                        writer.write_table((*write_table).clone()).await.unwrap();
                    }
                    writer.finish().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                let raw = ws.into_inner();
                let (read_half, write_half) = tokio::io::split(raw);
                let (shared_writer, _ws_write) =
                    lightstream::models::streams::websocket::WsWrite::new(write_half);
                let ws_read =
                    lightstream::models::streams::websocket::WsRead::new(read_half, shared_writer);
                let mut reader =
                    TableReader::<Vec64<u8>>::new(ws_read, 64 * 1024, IPCMessageProtocol::Stream, None);

                let start = std::time::Instant::now();
                let mut count = 0u64;
                while let Some(batch) = reader.read_next().await.unwrap() {
                    assert!(batch.n_rows > 0);
                    std::hint::black_box(&batch.cols);
                    count += 1;
                }
                let elapsed = start.elapsed();
                assert_eq!(count, n);

                writer.await.unwrap();
                elapsed
            }
        });
    });

    #[cfg(feature = "quic")]
    group.bench_function("quic", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
                let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
                let key_der =
                    rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der())
                        .unwrap();

                let mut server_crypto = rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(vec![cert_der], key_der)
                    .unwrap();
                server_crypto.alpn_protocols = vec![b"ls".to_vec()];
                let server_config = quinn::ServerConfig::with_crypto(Arc::new(
                    quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap(),
                ));

                let endpoint =
                    quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
                let addr = endpoint.local_addr().unwrap();

                let mut client_crypto = rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(BenchSkipVerification))
                    .with_no_client_auth();
                client_crypto.alpn_protocols = vec![b"ls".to_vec()];
                let client_config = quinn::ClientConfig::new(Arc::new(
                    quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto).unwrap(),
                ));

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio::spawn(async move {
                    let mut client_ep =
                        quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
                    client_ep.set_default_client_config(client_config);
                    let conn = client_ep.connect(addr, "localhost").unwrap().await.unwrap();
                    let send = conn.open_uni().await.unwrap();
                    let mut writer = lightstream::models::writers::quic::QuicTableWriter::new(
                        send,
                        write_schema,
                        None,
                    )
                    .unwrap();
                    writer.register_dictionary(
                        3,
                        vec!["red".to_string(), "green".to_string(), "blue".to_string()],
                    );
                    for _ in 0..n {
                        writer.write_table((*write_table).clone()).await.unwrap();
                    }
                    writer.finish().await.unwrap();
                    // Hold the connection until the reader closes it, since
                    // dropping the endpoint discards unacknowledged stream data.
                    conn.closed().await;
                });

                let incoming = endpoint.accept().await.unwrap();
                let conn = incoming.await.unwrap();
                let recv = conn.accept_uni().await.unwrap();
                let mut reader =
                    TableReader::<Vec64<u8>>::new(recv, 64 * 1024, IPCMessageProtocol::Stream, None);

                let start = std::time::Instant::now();
                let mut count = 0u64;
                while let Some(batch) = reader.read_next().await.unwrap() {
                    assert!(batch.n_rows > 0);
                    std::hint::black_box(&batch.cols);
                    count += 1;
                }
                let elapsed = start.elapsed();
                assert_eq!(count, n);

                conn.close(0u32.into(), b"");
                writer.await.unwrap();
                elapsed
            }
        });
    });

    #[cfg(feature = "webtransport")]
    group.bench_function("webtransport", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                let identity =
                    wtransport::Identity::self_signed(["localhost", "127.0.0.1", "::1"]).unwrap();
                let cert_hash = identity.certificate_chain().as_slice()[0].hash();
                let server_config = wtransport::ServerConfig::builder()
                    .with_bind_default(0)
                    .with_identity(identity)
                    .build();
                let server = wtransport::Endpoint::server(server_config).unwrap();
                let addr = server.local_addr().unwrap();

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio::spawn(async move {
                    let client_config = wtransport::ClientConfig::builder()
                        .with_bind_default()
                        .with_server_certificate_hashes([cert_hash])
                        .build();
                    let client = wtransport::Endpoint::client(client_config).unwrap();
                    let conn = client
                        .connect(format!("https://127.0.0.1:{}", addr.port()))
                        .await
                        .unwrap();
                    let send = conn.open_uni().await.unwrap().await.unwrap();
                    let mut writer =
                        lightstream::models::writers::webtransport::WebTransportTableWriter::new(
                            send,
                            write_schema,
                            None,
                        )
                        .unwrap();
                    writer.register_dictionary(
                        3,
                        vec!["red".to_string(), "green".to_string(), "blue".to_string()],
                    );
                    for _ in 0..n {
                        writer.write_table((*write_table).clone()).await.unwrap();
                    }
                    writer.finish().await.unwrap();
                    // Hold the session until the reader closes it, since
                    // dropping the endpoint discards unacknowledged stream data.
                    conn.closed().await;
                });

                let incoming = server.accept().await;
                let session_request = incoming.await.unwrap();
                let conn = session_request.accept().await.unwrap();
                let recv = conn.accept_uni().await.unwrap();
                let mut reader =
                    TableReader::<Vec64<u8>>::new(recv, 64 * 1024, IPCMessageProtocol::Stream, None);

                let start = std::time::Instant::now();
                let mut count = 0u64;
                while let Some(batch) = reader.read_next().await.unwrap() {
                    assert!(batch.n_rows > 0);
                    std::hint::black_box(&batch.cols);
                    count += 1;
                }
                let elapsed = start.elapsed();
                assert_eq!(count, n);

                conn.close(wtransport::VarInt::from_u32(0), b"");
                writer.await.unwrap();
                elapsed
            }
        });
    });

    // ---- zstd compression variants ----------------------------------------

    #[cfg(all(feature = "tcp", feature = "zstd"))]
    group.bench_function("tcp_zstd", |b| {
        use lightstream::compression::Compression;
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio::spawn(async move {
                    let mut writer =
                        lightstream::models::writers::tcp::TcpTableWriter::connect(
                            addr,
                            write_schema,
                            Some(Compression::Zstd),
                        )
                        .await
                        .unwrap();
                    writer.register_dictionary(
                        3,
                        vec!["red".to_string(), "green".to_string(), "blue".to_string()],
                    );

                    for _ in 0..n {
                        writer.write_table((*write_table).clone()).await.unwrap();
                    }
                    writer.finish().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let (read_half, _write_half) = socket.into_split();
                let mut reader =
                    TableReader::<Vec64<u8>>::new(read_half, 64 * 1024, IPCMessageProtocol::Stream, None);

                let start = std::time::Instant::now();
                let mut count = 0u64;
                while let Some(batch) = reader.read_next().await.unwrap() {
                    assert!(batch.n_rows > 0);
                    std::hint::black_box(&batch.cols);
                    count += 1;
                }
                let elapsed = start.elapsed();
                assert_eq!(count, n);

                writer.await.unwrap();
                elapsed
            }
        });
    });

    #[cfg(all(feature = "uds", feature = "zstd"))]
    group.bench_function("uds_zstd", |b| {
        use lightstream::compression::Compression;
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                let tempdir = tempfile::tempdir().unwrap();
                let socket_path = tempdir.path().join("bench_ipc_zstd.sock");
                let listener = UnixListener::bind(&socket_path).unwrap();

                let path = socket_path.clone();
                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio::spawn(async move {
                    let mut writer =
                        lightstream::models::writers::uds::UdsTableWriter::connect(
                            &path,
                            write_schema,
                            Some(Compression::Zstd),
                        )
                        .await
                        .unwrap();
                    writer.register_dictionary(
                        3,
                        vec!["red".to_string(), "green".to_string(), "blue".to_string()],
                    );

                    for _ in 0..n {
                        writer.write_table((*write_table).clone()).await.unwrap();
                    }
                    writer.finish().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let (read_half, _write_half) = socket.into_split();
                let mut reader =
                    TableReader::<Vec64<u8>>::new(read_half, 64 * 1024, IPCMessageProtocol::Stream, None);

                let start = std::time::Instant::now();
                let mut count = 0u64;
                while let Some(batch) = reader.read_next().await.unwrap() {
                    assert!(batch.n_rows > 0);
                    std::hint::black_box(&batch.cols);
                    count += 1;
                }
                let elapsed = start.elapsed();
                assert_eq!(count, n);

                writer.await.unwrap();
                elapsed
            }
        });
    });

    // TODO: add process consumer - current duplex-based bench is in-memory
    // only (no kernel pipe), so throughput numbers are misleadingly high.
    // Need to spawn a child process and pipe through real stdin/stdout.

    // #[cfg(feature = "stdio")]
    // group.bench_function("stdio", |b| { ... });

    group.finish();
}

criterion_group!(benches, bench_ipc_throughput);
criterion_main!(benches);

/// Certificate verifier that accepts any certificate, for bench use only.
#[cfg(feature = "quic")]
#[derive(Debug)]
struct BenchSkipVerification;

#[cfg(feature = "quic")]
impl rustls::client::danger::ServerCertVerifier for BenchSkipVerification {
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
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}
