//! Criterion benchmarks measuring Arrow IPC streaming throughput via the
//! byte stream adapters (Stream + AsyncRead with arena-backed SharedBuffer).
//!
//! Identical workload to ipc_throughput but routes through TcpByteStream /
//! UdsByteStream instead of the raw read half. A/B comparison for the
//! byte stream wrapper overhead.

mod bench_helpers;
use bench_helpers::*;

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
#[cfg(any(feature = "tcp", feature = "uds"))]
use lightstream::compression::Compression;
#[cfg(any(feature = "tcp", feature = "uds"))]
use lightstream::enums::IPCMessageProtocol;
#[cfg(any(feature = "tcp", feature = "uds"))]
use lightstream::models::codecs::ipc::ArrowIpcCodec;
#[cfg(any(feature = "tcp", feature = "uds"))]
use lightstream::models::readers::ipc::table_reader::TableReader;
#[cfg(any(feature = "tcp", feature = "uds"))]
use minarrow::Field;
#[cfg(any(feature = "tcp", feature = "uds"))]
use minarrow::Vec64;
#[cfg(any(feature = "tcp", feature = "uds"))]
use tokio::io::AsyncWriteExt;

#[allow(unused_variables)]
fn bench_stream_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = Arc::new(make_bench_table(BENCH_ROWS));
    #[cfg(any(feature = "tcp", feature = "uds"))]
    let schema: Vec<Field> = table.schema().iter().map(|f| (**f).clone()).collect();

    let mut group = c.benchmark_group("stream_throughput");
    group.throughput(Throughput::Bytes(logical_payload_bytes(BENCH_ROWS)));

    #[cfg(feature = "tcp")]
    {
        use lightstream::enums::BufferChunkSize;
        use lightstream::models::streams::tcp::TcpByteStream;
        use tokio::net::TcpListener;

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
                        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
                        let mut codec = ArrowIpcCodec::<Vec64<u8>>::new(
                            write_schema,
                            IPCMessageProtocol::Stream,
                            Compression::None,
                        );
                        codec.register_dictionary(
                            3,
                            vec!["red".to_string(), "green".to_string(), "blue".to_string()],
                        );
                        let mut buf = Vec64::with_capacity(0);
                        for _ in 0..n {
                            buf.clear();
                            codec.encode(&write_table, &mut buf, 0).unwrap();
                            stream.write_all(buf.as_ref()).await.unwrap();
                        }
                        buf.clear();
                        codec.finish(&mut buf).unwrap();
                        stream.write_all(buf.as_ref()).await.unwrap();
                        stream.shutdown().await.unwrap();
                    });

                    let (socket, _) = listener.accept().await.unwrap();
                    let (read_half, _write_half) = socket.into_split();
                    let byte_stream =
                        TcpByteStream::from_read_half(read_half, BufferChunkSize::Http);
                    let mut reader = TableReader::<Vec64<u8>>::new(
                        byte_stream,
                        64 * 1024,
                        IPCMessageProtocol::Stream,
                    );

                    let start = std::time::Instant::now();
                    let mut count = 0u64;
                    while let Some(batch) = reader.read_next().await.unwrap() {
                        assert!(batch.n_rows > 0);
                        count += 1;
                    }
                    let elapsed = start.elapsed();
                    assert_eq!(count, n);

                    writer.await.unwrap();
                    elapsed
                }
            });
        });
    }

    #[cfg(feature = "uds")]
    {
        use lightstream::enums::BufferChunkSize;
        use lightstream::models::streams::uds::UdsByteStream;
        use tokio::net::UnixListener;

        group.bench_function("uds", |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let table = Arc::clone(&table);
                let schema = schema.clone();
                async move {
                    let tempdir = tempfile::tempdir().unwrap();
                    let socket_path = tempdir.path().join("bench_stream.sock");
                    let listener = UnixListener::bind(&socket_path).unwrap();

                    let path = socket_path.clone();
                    let write_table = Arc::clone(&table);
                    let write_schema = schema.clone();
                    let n = iters;

                    let writer = tokio::spawn(async move {
                        let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
                        let mut codec = ArrowIpcCodec::<Vec64<u8>>::new(
                            write_schema,
                            IPCMessageProtocol::Stream,
                            Compression::None,
                        );
                        codec.register_dictionary(
                            3,
                            vec!["red".to_string(), "green".to_string(), "blue".to_string()],
                        );
                        let mut buf = Vec64::with_capacity(0);
                        for _ in 0..n {
                            buf.clear();
                            codec.encode(&write_table, &mut buf, 0).unwrap();
                            stream.write_all(buf.as_ref()).await.unwrap();
                        }
                        buf.clear();
                        codec.finish(&mut buf).unwrap();
                        stream.write_all(buf.as_ref()).await.unwrap();
                        stream.shutdown().await.unwrap();
                    });

                    let (socket, _) = listener.accept().await.unwrap();
                    let (read_half, _write_half) = socket.into_split();
                    let byte_stream =
                        UdsByteStream::from_read_half(read_half, BufferChunkSize::Http);
                    let mut reader = TableReader::<Vec64<u8>>::new(
                        byte_stream,
                        64 * 1024,
                        IPCMessageProtocol::Stream,
                    );

                    let start = std::time::Instant::now();
                    let mut count = 0u64;
                    while let Some(batch) = reader.read_next().await.unwrap() {
                        assert!(batch.n_rows > 0);
                        count += 1;
                    }
                    let elapsed = start.elapsed();
                    assert_eq!(count, n);

                    writer.await.unwrap();
                    elapsed
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_stream_throughput);
criterion_main!(benches);

