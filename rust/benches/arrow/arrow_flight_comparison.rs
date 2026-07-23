// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Compares Apache Arrow Flight and Lightstream throughput on loopback.
//!
//! Each [`BenchMatrix`] cell runs the same workload over Arrow Flight and
//! the Lightstream protocol over TCP. Both servers run in-process and
//! listen on `127.0.0.1`. Lightstream's Arrow IPC transport writers over
//! TCP, HTTP/2 and QUIC are also measured as additional comparisons when
//! the feature flags are on, and their labels carry `arrow` to distinguish 
//! them from the protocol.
//!
//! The timer covers the transfer from the moment it is requested until the
//! last batch is drained. Flight's timer starts before the `DoGet` request,
//! and the Lightstream single-stream writers hold every send behind a
//! release signal fired the instant the timer starts, so neither side moves
//! bytes before its clock is running. Connection setup, schema negotiation
//! and per-iteration buffer construction are excluded on both sides.
//!
//! Arrow Flight uses 8 MiB HTTP/2 flow-control windows so flow control
//! imposes no ceiling, and otherwise runs on tonic's default gRPC limits.
//! Flight-data slicing stays at the encoder's default 2 MiB. The encoder
//! resends dictionaries, which ensures Flight uses the more efficient
//! dictionary-encoded representation rather than defaulting to actual
//! strings for compatibility reasons, so both transports carry the same
//! representation. Lightstream uses its default configuration.
//!
//! The workload is one table per shape and scale combination. The Arrow
//! Flight record batch is the table's zero-copy Arrow export, verified
//! for type parity before any measurement, so both transports send
//! identical memory. Lightstream splits each send into 8 MiB views
//! through `get_views_for_target_batch_size` inside the timed region,
//! which uses its native 'slice a zero-copy view from a Table' approach,
//! where Flight's encoder slices its sends to its default 2 MiB, which,
//! under the hood, similarly uses its native zero-copy offsets.
//!
//! Throughput is expressed in logical payload bytes via minarrow's
//! [`minarrow::ByteSize::logical_bytes`], the accounting every benchmark
//! in the suite shares.
//!
//! This benchmark requires the `bench_arrow_flight` feature. Arrow Flight and
//! Tonic are not included in the dependency graph when the feature is disabled.

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;
#[path = "../common/arrow_flight_bench.rs"]
mod arrow_flight_bench;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight_bench::{BenchFlightService, FLIGHT_HTTP2_WINDOW, assert_export_parity};
use bench_helpers::{BenchMatrix, BenchScale, bench_schema, make_bench_table_shape};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures::stream::{StreamExt, TryStreamExt};
use minarrow::{ByteSize, Field, Table, Vec64};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tonic::Request;
use tonic::transport::Server;

use lightstream::enums::{BufferChunkSize, IPCMessageProtocol};
use lightstream::models::readers::ipc::table::TableReader;
use lightstream::traits::transport_writer::IPCTransportWriter;

// Lightstream's max_batch_size. Each send splits the workload table into
// zero-copy batches of this target size inside the timed region, where
// Flight's encoder slices its own sends to its default 2 MiB.
const MAX_BATCH_SIZE: usize = 8 * 1024 * 1024;

// Stream counts for the matched parallel comparison. Each side fans the same
// table sequence across N concurrent streams, one connection per stream.
const PARALLEL_STREAM_COUNTS: &[usize] = &[2, 4, 8, 16];

// ---------------------------------------------------------------------------
// Bench driver
// ---------------------------------------------------------------------------

fn bench_arrow_flight_compare(c: &mut Criterion) {
    // QUIC's rustls config needs a process-wide crypto provider installed
    // before the first handshake.
    #[cfg(feature = "quic")]
    {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    let rt = Runtime::new().unwrap();

    for (shape, scale) in BenchMatrix::from_env().cells() {
        let rows = scale.rows();
        // One table per shape and scale combination. The Flight record
        // batch is the table's zero-copy Arrow export, so both transports
        // send identical memory, verified for type parity before any
        // measurement.
        //
        // In other words - the Minarrow 'Table' (which is its term for 'RecordBatch')
        // gets mapped verbatim to an arrow-rs 'RecordBatch', using the exactly mapped
        // types, given Minarrow implements the public Apache Arrow memory format 
        // for its buffers.
        let table = Arc::new(make_bench_table_shape(shape, rows));
        let arrow_batch = Arc::new(table.to_apache_arrow());
        assert_export_parity(&table, &arrow_batch);
        let schema = bench_schema(&table);
        let dict_regs = shape.dictionary_registrations();

        // Both transports report against the same logical payload figure,
        // taken from the workload table itself.
        let table_bytes = table.logical_bytes() as u64;

        let group_name =
            format!("arrow_flight_vs_lightstream_{}_{}", shape.label(), scale.label());
        let mut group = c.benchmark_group(&group_name);
        group.throughput(Throughput::Bytes(table_bytes));

        if matches!(scale, BenchScale::Medium | BenchScale::Large) {
            group.sample_size(10);
        }

        bench_flight_do_get(&mut group, &rt, &arrow_batch);
        #[cfg(feature = "protocol")]
        bench_lightstream_protocol(&mut group, &rt, &table, &schema);
        bench_lightstream_arrow_tcp(&mut group, &rt, &table, &schema, &dict_regs);

        // Each side fans the same per-stream workload across N concurrent
        // streams on one connection.
        for &streams in PARALLEL_STREAM_COUNTS {
            group.throughput(Throughput::Bytes(table_bytes * streams as u64));
            bench_flight_parallel(&mut group, &rt, &arrow_batch, streams);
            #[cfg(feature = "protocol")]
            bench_lightstream_protocol_parallel(&mut group, &rt, &table, &schema, streams);
            bench_lightstream_arrow_tcp_parallel(&mut group, &rt, &table, &schema, &dict_regs, streams);
            #[cfg(feature = "http")]
            bench_lightstream_arrow_http2_parallel(&mut group, &rt, &table, &schema, &dict_regs, streams);
            #[cfg(feature = "quic")]
            bench_lightstream_arrow_quic_parallel(&mut group, &rt, &table, &schema, &dict_regs, streams);
        }

        group.finish();
    }
}


// Lightstream protocol receiver over TCP, the headline comparison against
// Flight's DoGet. The writer holds every send behind the release signal so
// the split, encode and transfer all happen inside the timed region, playing
// the role Flight's ticket request plays on its side.
#[cfg(feature = "protocol")]
fn bench_lightstream_protocol(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    table: &Arc<Table>,
    schema: &[Field],
) {
    use lightstream::models::readers::lightstream::LightstreamReader;
    use lightstream::models::writers::lightstream::LightstreamWriter;

    const TYPE_NAME: &str = "bench";

    group.bench_function("lightstream_protocol", |b| {
        b.to_async(rt).iter_custom(|iters| {
            let table = Arc::clone(table);
            let schema = schema.to_vec();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let n = iters;
                let windows = table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices.len() as u64;

                let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
                let writer = tokio::spawn(async move {
                    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
                    let mut writer = LightstreamWriter::<_, Vec64<u8>>::new(stream);
                    writer.register_table(TYPE_NAME, write_schema);
                    release_rx.await.unwrap();
                    // Each repetition splits the table inside the timed region.
                    // Note that Arrow Flight does the equivalent under the hood implicitly
                    // to achieve its hardcoded 'ideal default' batch size of 2Mib
                    for _ in 0..n {
                        for view in write_table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices
                        {
                            writer.send_table(TYPE_NAME, view).await.unwrap();
                        }
                    }
                    writer.shutdown().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let (read_half, _write_half) = socket.into_split();
                let mut reader: LightstreamReader = LightstreamReader::new(read_half, None);
                reader.register_table(TYPE_NAME, schema.clone());

                let start = std::time::Instant::now();
                release_tx.send(()).unwrap();
                let mut count = 0u64;
                while let Some(item) = reader.next().await {
                    let msg = item.unwrap();
                    if msg.is_table() {
                        count += 1;
                    }
                    std::hint::black_box(&msg);
                }
                let elapsed = start.elapsed();
                assert_eq!(count, n * windows);

                writer.await.unwrap();
                elapsed
            }
        });
    });
}

// Lightstream's Arrow IPC transport writer over TCP, included as an
// additional comparison alongside the protocol. The writer holds every send
// behind the release signal so the split, encode and transfer all happen
// inside the timed region.
fn bench_lightstream_arrow_tcp(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    table: &Arc<Table>,
    schema: &[Field],
    dict_regs: &[(i64, Vec<String>)],
) {
    group.bench_function("lightstream_arrow_tcp", |b| {
        b.to_async(rt).iter_custom(|iters| {
            let table = Arc::clone(table);
            let schema = schema.to_vec();
            let dict_regs = dict_regs.to_vec();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                let write_table = Arc::clone(&table);
                let write_schema = schema.clone();
                let write_dicts = dict_regs.clone();
                let n = iters;
                let windows = table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices.len() as u64;

                let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
                let writer = tokio::spawn(async move {
                    let mut writer = lightstream::models::writers::tcp::TcpTableWriter::connect(
                        addr,
                        write_schema,
                        None,
                    )
                    .await
                    .unwrap();
                    for (id, values) in write_dicts {
                        writer.register_dictionary(id, values);
                    }
                    release_rx.await.unwrap();
                    // Each repetition splits the table inside the timed region.
                    // Note that Arrow Flight does the equivalent under the hood implicitly
                    // to achieve its hardcoded 'ideal default' batch size of 2Mib
                    for _ in 0..n {
                        for view in write_table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices
                        {
                            writer.write_table(view).await.unwrap();
                        }
                    }
                    writer.finish().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let (read_half, _write_half) = socket.into_split();
                let mut reader = TableReader::<Vec64<u8>>::new(
                    read_half,
                    BufferChunkSize::Http.chunk_size(),
                    IPCMessageProtocol::Stream,
                    None,
                );

                let start = std::time::Instant::now();
                release_tx.send(()).unwrap();
                let mut count = 0u64;
                while let Some(batch) = reader.read_next().await.unwrap() {
                    assert!(batch.n_rows > 0);
                    std::hint::black_box(&batch.cols);
                    count += 1;
                }
                let elapsed = start.elapsed();
                assert_eq!(count, n * windows);

                writer.await.unwrap();
                elapsed
            }
        });
    });
}

// Arrow Flight DoGet over loopback gRPC.
fn bench_flight_do_get(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    batch: &Arc<RecordBatch>,
) {
    group.bench_function("arrow_flight_do_get", |b| {
        b.to_async(rt).iter_custom(|iters| {
            let batch = Arc::clone(batch);
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

                let service = BenchFlightService {
                    batch: Arc::clone(&batch),
                };
                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

                let server = tokio::spawn(async move {
                    Server::builder()
                        .initial_stream_window_size(FLIGHT_HTTP2_WINDOW)
                        .initial_connection_window_size(FLIGHT_HTTP2_WINDOW)
                        .add_service(FlightServiceServer::new(service))
                        .serve_with_incoming_shutdown(incoming, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .unwrap();
                });

                let channel = tonic::transport::Endpoint::try_from(format!("http://{addr}"))
                    .unwrap()
                    .initial_stream_window_size(FLIGHT_HTTP2_WINDOW)
                    .initial_connection_window_size(FLIGHT_HTTP2_WINDOW)
                    .connect()
                    .await
                    .unwrap();
                let mut client = FlightServiceClient::new(channel);

                let ticket = Ticket::new(iters.to_le_bytes().to_vec());

                let start = std::time::Instant::now();

                // Request the ticket  
                let stream = client.do_get(Request::new(ticket)).await.unwrap().into_inner();
                let decoder =
                    arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                        stream.map_err(|err| {
                            arrow_flight::error::FlightError::from_external_error(Box::new(err))
                        }),
                    );

                let mut count = 0u64;
                let mut decoder = std::pin::pin!(decoder);
                while let Some(item) = decoder.next().await {
                    let rb = item.unwrap();
                    assert!(rb.num_rows() > 0);
                    std::hint::black_box(rb.columns());
                    count += 1;
                }
                let elapsed = start.elapsed();
                // Batches above the encoder's default 2 MiB flight-data size
                // split into multiple decoded batches, so the check is a
                // lower bound on the input count.
                assert!(count >= iters);

                let _ = shutdown_tx.send(());
                server.await.unwrap();
                elapsed
            }
        });
    });
}

// This benches the Lightstream custom protocol implementation, writing/sending Arrow.
// 
// Lightstream protocol across N concurrent connections to one endpoint. Frames
// are decoded and dropped as they arrive under `None`, matching Flight's
// per-stream ordering and drop-on-arrival materialisation.
#[cfg(feature = "protocol")]
fn bench_lightstream_protocol_parallel(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    table: &Arc<Table>,
    schema: &[Field],
    streams: usize,
) {
    use lightstream::models::readers::parallel::lightstream::LightstreamParallelReader;
    use lightstream::models::writers::parallel::lightstream::LightstreamParallelWriter;
    use lightstream::traits::parallel_transport_reader::SortBehaviour;

    const TYPE_NAME: &str = "bench";

    group.bench_function(format!("lightstream_protocol_parallel_{streams}"), |b| {
        b.to_async(rt).iter_custom(|iters| {
            let table = Arc::clone(table);
            let schema = schema.to_vec();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                let windows = table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices.len() as u64;
                let total_tables = iters * streams as u64 * windows;

                let server_schema = schema.clone();
                let server = tokio::spawn(async move {
                    use futures::StreamExt;
                    let table_types = [(TYPE_NAME, server_schema)];
                    let mut reader = LightstreamParallelReader::accept(
                        &listener,
                        streams,
                        &[],
                        &table_types,
                        SortBehaviour::None,
                        None,
                    )
                    .await
                    .unwrap();
                    // Drop each frame as it arrives so retention matches
                    // Flight's decode-and-drop loop.
                    let mut received = 0u64;
                    while let Some(item) = reader.next().await {
                        let msg = item.unwrap();
                        if msg.is_table() {
                            received += 1;
                        }
                        std::hint::black_box(&msg);
                    }
                    received
                });

                let table_types = [(TYPE_NAME, schema)];
                let mut writer =
                    LightstreamParallelWriter::connect(addr, streams, &[], &table_types)
                        .await
                        .unwrap();

                let start = std::time::Instant::now();
                // Each repetition splits the table inside the timed region,
                // where Flight's encoder slices its own sends.
                for _ in 0..iters * streams as u64 {
                    for view in table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices {
                        writer.send_table(TYPE_NAME, view).await.unwrap();
                    }
                }
                writer.finish().await.unwrap();
                let received = server.await.unwrap();
                let elapsed = start.elapsed();
                assert_eq!(received, total_tables);
                elapsed
            }
        });
    });
}


// This benchmark's Lightstream libraries' custom Arrow writer implementation
// over TCP. I.e., the stream is Arrow stream protocol compliant.
//
// For the Lightstream *protocol*, see the one above.
//
// Lightstream's Arrow IPC transport writer across N concurrent TCP
// connections to one endpoint. TCP has no in-band multiplexing, so each
// connection carries its own stream. Tables are decoded and dropped as they
// arrive under `None`, matching Flight's per-stream ordering and its
// drop-on-arrival materialisation, so the two parallel comparisons share the
// same ordering and retention contract.
fn bench_lightstream_arrow_tcp_parallel(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    table: &Arc<Table>,
    schema: &[Field],
    dict_regs: &[(i64, Vec<String>)],
    streams: usize,
) {
    use lightstream::models::readers::parallel::tcp::TcpParallelTableReader;
    use lightstream::models::writers::parallel::tcp::TcpParallelTableWriter;
    use lightstream::traits::parallel_transport_reader::SortBehaviour;
    use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;

    group.bench_function(format!("lightstream_arrow_tcp_parallel_{streams}"), |b| {
        b.to_async(rt).iter_custom(|iters| {
            let table = Arc::clone(table);
            let schema = schema.to_vec();
            let dict_regs = dict_regs.to_vec();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                let windows = table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices.len() as u64;
                let total_tables = iters * streams as u64 * windows;

                let server = tokio::spawn(async move {
                    let mut reader =
                        TcpParallelTableReader::accept(&listener, streams, SortBehaviour::None, None)
                            .await
                            .unwrap();
                    // Drop each table as it arrives so retention matches
                    // Flight's decode-and-drop loop.
                    let mut received = 0u64;
                    while let Some(item) = reader.next().await {
                        let (table, _seq) = item.unwrap();
                        std::hint::black_box(table.n_rows);
                        received += 1;
                    }
                    received
                });

                let mut writer =
                    TcpParallelTableWriter::connect(addr, streams, schema, dict_regs, None)
                        .await
                        .unwrap();

                let start = std::time::Instant::now();
                // Each repetition splits the table inside the timed region,
                // where Flight's encoder slices its own sends.
                for _ in 0..iters * streams as u64 {
                    for view in table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices {
                        writer.write_table(view).await.unwrap();
                    }
                }
                writer.finish().await.unwrap();
                let received = server.await.unwrap();
                let elapsed = start.elapsed();
                assert_eq!(received, total_tables);
                elapsed
            }
        });
    });
}

// Arrow Flight across N concurrent DoGet streams, each on its own channel and
// therefore its own TCP connection, matching lightstream's connection-per-stream
// parallel layout. Cloned tonic channels would multiplex every stream onto one
// connection and throttle the aggregate on the shared connection window. Each
// stream requests `iters` batches through a cloned ticket, and `Bytes` clones
// share the one ticket buffer, so only the initial ticket allocates.
fn bench_flight_parallel(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    batch: &Arc<RecordBatch>,
    streams: usize,
) {
    group.bench_function(format!("arrow_flight_parallel_{streams}"), |b| {
        b.to_async(rt).iter_custom(|iters| {
            let batch = Arc::clone(batch);
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

                let service = BenchFlightService {
                    batch: Arc::clone(&batch),
                };
                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

                let server = tokio::spawn(async move {
                    Server::builder()
                        .initial_stream_window_size(FLIGHT_HTTP2_WINDOW)
                        .initial_connection_window_size(FLIGHT_HTTP2_WINDOW)
                        .add_service(FlightServiceServer::new(service))
                        .serve_with_incoming_shutdown(incoming, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .unwrap();
                });

                // Connect every channel before the timer starts so connection
                // setup is not billed against the transfer.
                let endpoint = tonic::transport::Endpoint::try_from(format!("http://{addr}"))
                    .unwrap()
                    .initial_stream_window_size(FLIGHT_HTTP2_WINDOW)
                    .initial_connection_window_size(FLIGHT_HTTP2_WINDOW);
                let mut channels = Vec::with_capacity(streams);
                for _ in 0..streams {
                    channels.push(endpoint.connect().await.unwrap());
                }

                let ticket = Ticket::new(iters.to_le_bytes().to_vec());

                let start = std::time::Instant::now();
                let mut handles = Vec::with_capacity(streams);
                for channel in channels {
                    let mut client = FlightServiceClient::new(channel);
                    let ticket = ticket.clone();
                    handles.push(tokio::spawn(async move {
                        let stream = client
                            .do_get(Request::new(ticket))
                            .await
                            .unwrap()
                            .into_inner();
                        let decoder =
                            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                                stream.map_err(|err| {
                                    arrow_flight::error::FlightError::from_external_error(
                                        Box::new(err),
                                    )
                                }),
                            );
                        let mut decoder = std::pin::pin!(decoder);
                        let mut received = 0u64;
                        while let Some(item) = decoder.next().await {
                            let rb = item.unwrap();
                            std::hint::black_box(rb.columns());
                            received += 1;
                        }
                        received
                    }));
                }
                let mut total = 0u64;
                for handle in handles {
                    total += handle.await.unwrap();
                }
                let elapsed = start.elapsed();
                assert!(total >= iters * streams as u64);

                let _ = shutdown_tx.send(());
                server.await.unwrap();
                elapsed
            }
        });
    });
}

// Extra transports with this crate's implementation of the Arrow stream protocol - just for reference.

// Lightstream's Arrow IPC transport writer across N concurrent HTTP/2
// request streams on one connection.
#[cfg(feature = "http")]
fn bench_lightstream_arrow_http2_parallel(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    table: &Arc<Table>,
    schema: &[Field],
    dict_regs: &[(i64, Vec<String>)],
    streams: usize,
) {
    use lightstream::models::readers::parallel::http::HttpParallelTableReader;
    use lightstream::models::writers::parallel::http::HttpParallelTableWriter;
    use lightstream::traits::parallel_transport_reader::SortBehaviour;
    use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;

    group.bench_function(format!("lightstream_arrow_http2_parallel_{streams}"), |b| {
        b.to_async(rt).iter_custom(|iters| {
            let table = Arc::clone(table);
            let schema = schema.to_vec();
            let dict_regs = dict_regs.to_vec();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let url = format!("http://{addr}/ingest");

                let windows = table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices.len() as u64;
                let total_tables = iters * streams as u64 * windows;

                let server = tokio::spawn(async move {
                    use futures::StreamExt;
                    let (tcp, _peer) = listener.accept().await.unwrap();
                    let mut reader =
                        HttpParallelTableReader::from_tcp(tcp, streams, SortBehaviour::None, None)
                            .await
                            .unwrap();
                    // Drop each table as it arrives so retention matches
                    // Flight's decode-and-drop loop.
                    let mut received = 0u64;
                    while let Some(item) = reader.next().await {
                        let (table, _seq) = item.unwrap();
                        std::hint::black_box(table.n_rows);
                        received += 1;
                    }
                    received
                });

                let mut writer =
                    HttpParallelTableWriter::connect(&url, streams, schema, dict_regs, None)
                        .await
                        .unwrap();

                let start = std::time::Instant::now();
                // Each repetition splits the table inside the timed region,
                // where Flight's encoder slices its own sends.
                for _ in 0..iters * streams as u64 {
                    for view in table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices {
                        writer.write_table(view).await.unwrap();
                    }
                }
                writer.finish().await.unwrap();
                let received = server.await.unwrap();
                let elapsed = start.elapsed();
                assert_eq!(received, total_tables);
                elapsed
            }
        });
    });
}

// Lightstream's Arrow IPC transport writer across N concurrent QUIC
// unidirectional streams on one connection. quinn drives the connection in
// the background, so the merged reader drains as a plain `Stream`.
#[cfg(feature = "quic")]
fn bench_lightstream_arrow_quic_parallel(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    table: &Arc<Table>,
    schema: &[Field],
    dict_regs: &[(i64, Vec<String>)],
    streams: usize,
) {
    use std::net::SocketAddr;

    use lightstream::models::readers::parallel::quic::QuicParallelTableReader;
    use lightstream::models::writers::parallel::quic::QuicParallelTableWriter;
    use lightstream::traits::parallel_transport_reader::SortBehaviour;
    use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;

    group.bench_function(format!("lightstream_arrow_quic_parallel_{streams}"), |b| {
        b.to_async(rt).iter_custom(|iters| {
            let table = Arc::clone(table);
            let schema = schema.to_vec();
            let dict_regs = dict_regs.to_vec();
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
                let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
                    quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap(),
                ));
                Arc::get_mut(&mut server_config.transport)
                    .unwrap()
                    .max_concurrent_uni_streams((streams as u32).into());

                let endpoint = quinn::Endpoint::server(
                    server_config,
                    "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
                )
                .unwrap();
                let addr = endpoint.local_addr().unwrap();

                let mut client_crypto = rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(BenchSkipVerification))
                    .with_no_client_auth();
                client_crypto.alpn_protocols = vec![b"ls".to_vec()];
                let client_config = quinn::ClientConfig::new(Arc::new(
                    quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto).unwrap(),
                ));

                let windows = table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices.len() as u64;
                let total_tables = iters * streams as u64 * windows;

                let server = tokio::spawn(async move {
                    use futures::StreamExt;
                    let incoming = endpoint.accept().await.unwrap();
                    let conn = incoming.await.unwrap();
                    let mut reader =
                        QuicParallelTableReader::accept(&conn, streams, SortBehaviour::None, None)
                            .await
                            .unwrap();
                    // Drop each table as it arrives so retention matches
                    // Flight's decode-and-drop loop. The connection and
                    // endpoint close on drop at the end of this task, keeping
                    // the QUIC idle-drain out of the timed region.
                    let mut received = 0u64;
                    while let Some(item) = reader.next().await {
                        let (table, _seq) = item.unwrap();
                        std::hint::black_box(table.n_rows);
                        received += 1;
                    }
                    received
                });

                let mut client_ep =
                    quinn::Endpoint::client("0.0.0.0:0".parse::<SocketAddr>().unwrap()).unwrap();
                client_ep.set_default_client_config(client_config);
                let conn = client_ep.connect(addr, "localhost").unwrap().await.unwrap();

                let mut writer =
                    QuicParallelTableWriter::open(&conn, streams, schema, dict_regs, None)
                        .await
                        .unwrap();

                let start = std::time::Instant::now();
                // Each repetition splits the table inside the timed region,
                // where Flight's encoder slices its own sends.
                for _ in 0..iters * streams as u64 {
                    for view in table.get_views_for_target_batch_size(MAX_BATCH_SIZE).slices {
                        writer.write_table(view).await.unwrap();
                    }
                }
                writer.finish().await.unwrap();
                let received = server.await.unwrap();
                let elapsed = start.elapsed();
                assert_eq!(received, total_tables);
                elapsed
            }
        });
    });
}

// Bench-only TLS verifier that skips certificate validation. QUIC needs it
// because each bench iteration generates a fresh self-signed cert, so the
// client has no trust root to validate against.
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

criterion_group!(benches, bench_arrow_flight_compare);
criterion_main!(benches);
