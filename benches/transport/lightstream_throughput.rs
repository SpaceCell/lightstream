// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Criterion benchmarks measuring Lightstream protocol steady-state streaming
//! throughput over TCP and Unix domain sockets.
//!
//! Connection setup is excluded from the timed region. Each sample establishes
//! one connection, then streams `iters` batches through it, timing only the
//! send/recv pipeline. Criterion reports throughput in bytes/sec.

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;
use bench_helpers::*;

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
#[cfg(feature = "io_uring")]
use lightstream::models::io_uring::{IoUringTcpConnection, IoUringUdsConnection};
#[cfg(feature = "protocol")]
use lightstream::models::protocol::connection::{
    TcpLightstreamConnection, UdsLightstreamConnection,
};
use minarrow::Field;
use tokio::net::{TcpListener, UnixListener};

fn bench_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = Arc::new(make_bench_table(BENCH_ROWS));
    let schema: Vec<Field> = table.schema().iter().map(|f| (**f).clone()).collect();

    let mut group = c.benchmark_group("lightstream_throughput");
    group.throughput(Throughput::Bytes(logical_payload_bytes(BENCH_ROWS)));

    // -----------------------------------------------------------------------
    // Lightstream protocol benchmarks (TLV framing + Arrow IPC)
    // -----------------------------------------------------------------------

    #[cfg(feature = "protocol")]
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
                    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
                    let mut conn = TcpLightstreamConnection::from_tcp(stream, None);
                    conn.register_table("Data", write_schema);

                    for _ in 0..n {
                        conn.send_table("Data", Arc::clone(&write_table)).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                });

                // Accept blocks until the writer connects, excluding connection
                // setup from the timed region.
                let (socket, _) = listener.accept().await.unwrap();
                let mut conn = TcpLightstreamConnection::from_tcp(socket, None);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                    std::hint::black_box(&msg);
                }
                let elapsed = start.elapsed();

                writer.await.unwrap();
                elapsed
            }
        });
    });

    #[cfg(feature = "protocol")]
    group.bench_function("uds", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                let tempdir = tempfile::tempdir().unwrap();
                let socket_path = tempdir.path().join("bench.sock");
                let listener = UnixListener::bind(&socket_path).unwrap();

                let path = socket_path.clone();
                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio::spawn(async move {
                    let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
                    let mut conn = UdsLightstreamConnection::from_uds(stream, None);
                    conn.register_table("Data", write_schema);

                    for _ in 0..n {
                        conn.send_table("Data", Arc::clone(&write_table)).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let mut conn = UdsLightstreamConnection::from_uds(socket, None);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                    std::hint::black_box(&msg);
                }
                let elapsed = start.elapsed();

                writer.await.unwrap();
                elapsed
            }
        });
    });

    #[cfg(all(feature = "protocol", feature = "websocket"))]
    group.bench_function("websocket", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                use lightstream::models::protocol::connection::WebSocketLightstreamConnection;

                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let url = format!("ws://{addr}");

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio::spawn(async move {
                    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
                    let mut conn = WebSocketLightstreamConnection::from_websocket(ws, None);
                    conn.register_table("Data", write_schema);
                    for _ in 0..n {
                        conn.send_table("Data", Arc::clone(&write_table)).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let ws = tokio_tungstenite::accept_async(socket).await.unwrap();
                let mut conn = WebSocketLightstreamConnection::from_websocket(ws, None);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                    std::hint::black_box(&msg);
                }
                let elapsed = start.elapsed();

                writer.await.unwrap();
                elapsed
            }
        });
    });

    #[cfg(all(feature = "protocol", feature = "quic"))]
    group.bench_function("quic", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                use lightstream::models::protocol::connection::QuicLightstreamConnection;

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
                    let qconn = client_ep.connect(addr, "localhost").unwrap().await.unwrap();
                    let (send, recv) = qconn.open_bi().await.unwrap();
                    let mut conn = QuicLightstreamConnection::from_quic(recv, send, None);
                    conn.register_table("Data", write_schema);
                    for _ in 0..n {
                        conn.send_table("Data", Arc::clone(&write_table)).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                    // Hold the connection until the reader closes it, since
                    // dropping the endpoint discards unacknowledged stream data.
                    qconn.closed().await;
                });

                let incoming = endpoint.accept().await.unwrap();
                let qconn = incoming.await.unwrap();
                let (send, recv) = qconn.accept_bi().await.unwrap();
                let mut conn = QuicLightstreamConnection::from_quic(recv, send, None);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                    std::hint::black_box(&msg);
                }
                let elapsed = start.elapsed();

                qconn.close(0u32.into(), b"");
                writer.await.unwrap();
                elapsed
            }
        });
    });

    #[cfg(all(feature = "protocol", feature = "webtransport"))]
    group.bench_function("webtransport", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();
            async move {
                use lightstream::models::protocol::connection::WebTransportLightstreamConnection;

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
                    let session = client
                        .connect(format!("https://127.0.0.1:{}", addr.port()))
                        .await
                        .unwrap();
                    let opening = session.open_bi().await.unwrap();
                    let (send, recv) = opening.await.unwrap();
                    let mut conn =
                        WebTransportLightstreamConnection::from_webtransport(recv, send, None);
                    conn.register_table("Data", write_schema);
                    for _ in 0..n {
                        conn.send_table("Data", Arc::clone(&write_table)).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                    // Hold the session until the reader closes it, since
                    // dropping the endpoint discards unacknowledged stream data.
                    session.closed().await;
                });

                let incoming = server.accept().await;
                let session_request = incoming.await.unwrap();
                let sconn = session_request.accept().await.unwrap();
                let (send, recv) = sconn.accept_bi().await.unwrap();
                let mut conn =
                    WebTransportLightstreamConnection::from_webtransport(recv, send, None);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                    std::hint::black_box(&msg);
                }
                let elapsed = start.elapsed();

                sconn.close(wtransport::VarInt::from_u32(0), b"");
                writer.await.unwrap();
                elapsed
            }
        });
    });

    // TODO: add process consumer - current duplex-based bench is in-memory
    // only (no kernel pipe), so throughput numbers are misleadingly high.
    // Need to spawn a child process and pipe through real stdin/stdout.

    // #[cfg(all(feature = "protocol", feature = "stdio"))]
    // group.bench_function("stdio", |b| { ... });

    #[cfg(feature = "io_uring")]
    group.bench_function("uds_io_uring", |b| {
        b.iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();

            // Run inside tokio_uring runtime for direct io_uring I/O
            tokio_uring::start(async move {
                // Use socketpair to avoid tokio-uring's UnixListener::bind
                // SO_REUSEPORT bug on AF_UNIX
                let (stream_a, stream_b) = {
                    let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
                    a.set_nonblocking(true).unwrap();
                    b.set_nonblocking(true).unwrap();
                    (
                        tokio_uring::net::UnixStream::from_std(a),
                        tokio_uring::net::UnixStream::from_std(b),
                    )
                };

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio_uring::spawn(async move {
                    let mut conn = IoUringUdsConnection::new(stream_a, None);
                    conn.register_table("Data", write_schema);

                    for _ in 0..n {
                        conn.send_table("Data", Arc::clone(&write_table)).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                });

                let mut conn = IoUringUdsConnection::new(stream_b, None);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                    std::hint::black_box(&msg);
                }
                let elapsed = start.elapsed();

                writer.await.unwrap();
                elapsed
            })
        });
    });

    #[cfg(feature = "io_uring")]
    group.bench_function("tcp_io_uring", |b| {
        b.iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();

            tokio_uring::start(async move {
                let listener =
                    tokio_uring::net::TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
                let addr = listener.local_addr().unwrap();

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio_uring::spawn(async move {
                    let stream = tokio_uring::net::TcpStream::connect(addr).await.unwrap();
                    let mut conn = IoUringTcpConnection::new(stream, None);
                    conn.register_table("Data", write_schema);

                    for _ in 0..n {
                        conn.send_table("Data", Arc::clone(&write_table)).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                });

                let (stream, _) = listener.accept().await.unwrap();
                let mut conn = IoUringTcpConnection::new(stream, None);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                    std::hint::black_box(&msg);
                }
                let elapsed = start.elapsed();

                writer.await.unwrap();
                elapsed
            })
        });
    });

    // TODO: add cross-process stdio io_uring bench. Spawn a child process,
    // pass one end of IoUringUdsConnection::socketpair() as the child's
    // stdin/stdout, and measure real inter-process throughput. The in-process
    // socketpair version below is equivalent to uds_io_uring and would be
    // misleading without a real process boundary.

    // #[cfg(feature = "io_uring")]
    // group.bench_function("stdio_io_uring", |b| { ... });

    #[cfg(all(feature = "io_uring", feature = "websocket"))]
    group.bench_function("websocket_io_uring", |b| {
        b.iter_custom(|iters| {
            let table = Arc::clone(&table);
            let schema = schema.clone();

            // Do blocking TCP + WS handshake outside tokio_uring, then
            // convert the raw streams for io_uring I/O.
            use lightstream::models::io_uring::IoUringWsConnection;

            let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = std_listener.local_addr().unwrap();
            std_listener.set_nonblocking(false).unwrap();

            // Client handshake in a thread (blocking)
            let client_handle = std::thread::spawn(move || {
                let tcp = std::net::TcpStream::connect(addr).unwrap();
                let (ws, _) =
                    tokio_tungstenite::tungstenite::client("ws://localhost/", tcp).unwrap();
                let raw = ws.into_inner();
                raw.set_nonblocking(true).unwrap();
                raw
            });

            // Server handshake (blocking)
            let (server_tcp, _) = std_listener.accept().unwrap();
            let ws = tokio_tungstenite::tungstenite::accept(server_tcp).unwrap();
            let server_raw = ws.into_inner();
            server_raw.set_nonblocking(true).unwrap();

            let client_raw = client_handle.join().unwrap();

            // Now run the data path on io_uring
            tokio_uring::start(async move {
                let write_stream = tokio_uring::net::TcpStream::from_std(server_raw);
                let read_stream = tokio_uring::net::TcpStream::from_std(client_raw);

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;

                let writer = tokio_uring::spawn(async move {
                    let mut conn = IoUringWsConnection::new(write_stream, None);
                    conn.register_table("Data", write_schema);

                    for _ in 0..n {
                        conn.send_table("Data", Arc::clone(&write_table)).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                });

                let mut conn = IoUringWsConnection::new_client(read_stream, None);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                    std::hint::black_box(&msg);
                }
                let elapsed = start.elapsed();

                writer.await.unwrap();
                elapsed
            })
        });
    });

    group.finish();
}

criterion_group!(benches, bench_throughput);
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
