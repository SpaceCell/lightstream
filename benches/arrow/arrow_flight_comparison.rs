// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Compares Apache Arrow Flight and Lightstream TCP throughput.
//!
//! Each [`BenchMatrix`] cell runs the same workload over Arrow Flight and
//! Lightstream TCP on loopback. Both servers run in-process and listen on
//! `127.0.0.1`. Only the client receive loop is timed. Connection setup,
//! schema negotiation and per-iteration buffer construction are excluded.
//!
//! Arrow Flight uses 8 MiB HTTP/2 flow-control windows and raised gRPC
//! message limits so no transport ceiling interferes, while flight-data
//! slicing stays at the encoder's default 2 MiB, matching how Arrow Flight
//! ships. Lightstream uses its default configuration.
//!
//! Throughput is calculated from the source columns using
//! [`bench_helpers::logical_payload_bytes_shape`], matching the accounting used
//! by the transport benchmarks.
//!
//! This benchmark requires the `bench_arrow_flight` feature. Arrow Flight and
//! Tonic are not included in the dependency graph when the feature is disabled.

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field as ArrowField, Int32Type, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder,
};
use bench_helpers::{
    BenchMatrix, BenchScale, BenchShape, bench_schema, logical_payload_bytes_shape,
    make_bench_table_shape,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use minarrow::{Field, Table, Vec64};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use lightstream::enums::{BufferChunkSize, IPCMessageProtocol};
use lightstream::models::readers::ipc::table::TableReader;
use lightstream::traits::transport_writer::IPCTransportWriter;

// ---------------------------------------------------------------------------
// Arrow record-batch construction matching each BenchShape
// ---------------------------------------------------------------------------

const STRING_HEAVY_DICT_CARDINALITY: usize = 100;
const WIDE_GROUP_SIZE: usize = 25;

fn make_record_batch(shape: BenchShape, n_rows: usize) -> RecordBatch {
    match shape {
        BenchShape::Mixed => mixed_batch(n_rows),
        BenchShape::NarrowNumeric => narrow_numeric_batch(n_rows),
        BenchShape::StringHeavy => string_heavy_batch(n_rows),
        BenchShape::Wide => wide_batch(n_rows),
    }
}

fn mixed_batch(n_rows: usize) -> RecordBatch {
    let ids = Int32Array::from((0..n_rows as i32).collect::<Vec<_>>());
    let values = Float64Array::from((0..n_rows).map(|i| i as f64 * 0.5).collect::<Vec<_>>());
    let labels =
        StringArray::from((0..n_rows).map(|i| format!("row_{}", i)).collect::<Vec<_>>());
    let dict_keys = Int32Array::from((0..n_rows).map(|i| (i % 3) as i32).collect::<Vec<_>>());
    let dict_values = StringArray::from(vec!["red", "green", "blue"]);
    let category = DictionaryArray::<Int32Type>::try_new(dict_keys, Arc::new(dict_values)).unwrap();

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("ids", DataType::Int32, false),
        ArrowField::new("values", DataType::Float64, false),
        ArrowField::new("labels", DataType::Utf8, false),
        ArrowField::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(values),
            Arc::new(labels),
            Arc::new(category),
        ],
    )
    .unwrap()
}

fn narrow_numeric_batch(n_rows: usize) -> RecordBatch {
    let ids = Int32Array::from((0..n_rows as i32).collect::<Vec<_>>());
    let counters = Int64Array::from((0..n_rows).map(|i| (i as i64) * 7).collect::<Vec<_>>());
    let prices = Float32Array::from((0..n_rows).map(|i| i as f32 * 0.25).collect::<Vec<_>>());
    let values = Float64Array::from((0..n_rows).map(|i| i as f64 * 0.5).collect::<Vec<_>>());

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("ids", DataType::Int32, false),
        ArrowField::new("counters", DataType::Int64, false),
        ArrowField::new("prices", DataType::Float32, false),
        ArrowField::new("values", DataType::Float64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(counters),
            Arc::new(prices),
            Arc::new(values),
        ],
    )
    .unwrap()
}

fn string_heavy_batch(n_rows: usize) -> RecordBatch {
    let ids = Int32Array::from((0..n_rows as i32).collect::<Vec<_>>());
    let long_text = StringArray::from(
        (0..n_rows)
            .map(|i| {
                format!(
                    "row_{:08}_payload_{:08x}_lorem_ipsum_dolor_sit",
                    i,
                    i.wrapping_mul(2_654_435_761usize)
                )
            })
            .collect::<Vec<_>>(),
    );
    let short_text = StringArray::from(
        (0..n_rows)
            .map(|i| format!("s_{:04x}", (i & 0xFFFF) as u16))
            .collect::<Vec<_>>(),
    );
    let dict_keys = Int32Array::from(
        (0..n_rows)
            .map(|i| (i % STRING_HEAVY_DICT_CARDINALITY) as i32)
            .collect::<Vec<_>>(),
    );
    let dict_values = StringArray::from(
        (0..STRING_HEAVY_DICT_CARDINALITY)
            .map(|i| format!("cat_{:03}", i))
            .collect::<Vec<_>>(),
    );
    let category = DictionaryArray::<Int32Type>::try_new(dict_keys, Arc::new(dict_values)).unwrap();

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("ids", DataType::Int32, false),
        ArrowField::new("long_text", DataType::Utf8, false),
        ArrowField::new("short_text", DataType::Utf8, false),
        ArrowField::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(long_text),
            Arc::new(short_text),
            Arc::new(category),
        ],
    )
    .unwrap()
}

fn wide_batch(n_rows: usize) -> RecordBatch {
    let mut fields: Vec<ArrowField> = Vec::with_capacity(WIDE_GROUP_SIZE * 4);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(WIDE_GROUP_SIZE * 4);

    for k in 0..WIDE_GROUP_SIZE {
        fields.push(ArrowField::new(
            format!("i32_{:03}", k),
            DataType::Int32,
            false,
        ));
        let arr = Int32Array::from(
            (0..n_rows)
                .map(|i| (i as i32).wrapping_add(k as i32))
                .collect::<Vec<_>>(),
        );
        columns.push(Arc::new(arr));
    }
    for k in 0..WIDE_GROUP_SIZE {
        fields.push(ArrowField::new(
            format!("i64_{:03}", k),
            DataType::Int64,
            false,
        ));
        let arr = Int64Array::from(
            (0..n_rows)
                .map(|i| (i as i64).wrapping_mul(k as i64 + 1))
                .collect::<Vec<_>>(),
        );
        columns.push(Arc::new(arr));
    }
    for k in 0..WIDE_GROUP_SIZE {
        fields.push(ArrowField::new(
            format!("f32_{:03}", k),
            DataType::Float32,
            false,
        ));
        let arr = Float32Array::from(
            (0..n_rows)
                .map(|i| i as f32 + k as f32 * 0.125)
                .collect::<Vec<_>>(),
        );
        columns.push(Arc::new(arr));
    }
    for k in 0..WIDE_GROUP_SIZE {
        fields.push(ArrowField::new(
            format!("f64_{:03}", k),
            DataType::Float64,
            false,
        ));
        let arr = Float64Array::from(
            (0..n_rows)
                .map(|i| i as f64 + k as f64 * 0.5)
                .collect::<Vec<_>>(),
        );
        columns.push(Arc::new(arr));
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    RecordBatch::try_new(schema, columns).unwrap()
}

// ---------------------------------------------------------------------------
// Minimal Flight service: DoGet returns a pre-built batch repeated `iters`
// times. Every other RPC is unsupported.
// ---------------------------------------------------------------------------

// Flight tuning that mirrors lightstream within Flight's public API. The 8 MiB
// HTTP/2 windows match the lightstream HTTP/2 path, and the raised gRPC
// message limits remove the transport-level ceilings. Flight-data slicing
// stays at the encoder's default 2 MiB, matching how Arrow Flight ships.
const FLIGHT_HTTP2_WINDOW: u32 = 8 * 1024 * 1024;
const FLIGHT_MAX_MESSAGE_BYTES: usize = i32::MAX as usize;

// Stream counts for the matched parallel comparison. Each side fans the same
// table sequence across N concurrent streams on one connection.
const PARALLEL_STREAM_COUNTS: &[usize] = &[2, 4, 8, 16];

// The DoGet ticket carries the batch count the client wants, so each concurrent
// stream in the parallel comparison asks for its own share.
#[derive(Clone)]
struct BenchFlightService {
    batch: Arc<RecordBatch>,
}

#[tonic::async_trait]
impl FlightService for BenchFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let n = u64::from_le_bytes(ticket.ticket.as_ref().try_into().unwrap());
        let batch = Arc::clone(&self.batch);
        let batch_stream = stream::iter((0..n).map(move |_| {
            Ok::<RecordBatch, arrow_flight::error::FlightError>((*batch).clone())
        }));
        // The encoder keeps its default flight-data size, so batches above
        // 2 MiB split into multiple messages per Arrow Flight's own tuning.
        let flight_data = FlightDataEncoderBuilder::new()
            .build(batch_stream)
            .map_err(|err| Status::internal(format!("flight encode failure: {err}")));
        Ok(Response::new(Box::pin(flight_data)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }
}

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
        let arrow_batch = Arc::new(make_record_batch(shape, rows));
        let table = Arc::new(make_bench_table_shape(shape, rows));
        let schema = bench_schema(&table);
        let dict_regs = shape.dictionary_registrations();

        let group_name =
            format!("arrow_flight_vs_lightstream_{}_{}", shape.label(), scale.label());
        let mut group = c.benchmark_group(&group_name);
        group.throughput(Throughput::Bytes(logical_payload_bytes_shape(shape, rows, 1)));

        if matches!(scale, BenchScale::Medium | BenchScale::Large) {
            group.sample_size(10);
        }

        bench_flight_do_get(&mut group, &rt, &arrow_batch);
        bench_lightstream_tcp(&mut group, &rt, &table, &schema, &dict_regs);

        // Each side fans the same per-stream workload across N concurrent
        // streams on one connection.
        for &streams in PARALLEL_STREAM_COUNTS {
            group.throughput(Throughput::Bytes(logical_payload_bytes_shape(shape, rows, streams)));
            bench_flight_parallel(&mut group, &rt, &arrow_batch, streams);
            bench_lightstream_tcp_parallel(&mut group, &rt, &table, &schema, &dict_regs, streams);
            #[cfg(feature = "protocol")]
            bench_lightstream_protocol_parallel(&mut group, &rt, &table, &schema, streams);
            #[cfg(feature = "http")]
            bench_lightstream_http2_parallel(&mut group, &rt, &table, &schema, &dict_regs, streams);
            #[cfg(feature = "quic")]
            bench_lightstream_quic_parallel(&mut group, &rt, &table, &schema, &dict_regs, streams);
        }

        group.finish();
    }
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
                        .add_service(
                            FlightServiceServer::new(service)
                                .max_encoding_message_size(FLIGHT_MAX_MESSAGE_BYTES)
                                .max_decoding_message_size(FLIGHT_MAX_MESSAGE_BYTES),
                        )
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
                let mut client = FlightServiceClient::new(channel)
                    .max_decoding_message_size(FLIGHT_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(FLIGHT_MAX_MESSAGE_BYTES);

                let ticket = Ticket::new(iters.to_le_bytes().to_vec());

                let start = std::time::Instant::now();
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

// Lightstream TCP receiver for direct head-to-head comparison.
fn bench_lightstream_tcp(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    table: &Arc<Table>,
    schema: &[Field],
    dict_regs: &[(i64, Vec<String>)],
) {
    group.bench_function("lightstream_tcp", |b| {
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
                    for _ in 0..n {
                        writer.write_table((*write_table).clone()).await.unwrap();
                    }
                    writer.finish().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let (read_half, _write_half) = socket.into_split();
                let mut reader = TableReader::<Vec64<u8>>::new(
                    read_half,
                    BufferChunkSize::Http.chunk_size(),
                    IPCMessageProtocol::Stream,
                );

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
}

// Arrow Flight across N concurrent DoGet streams on one channel. Each stream
// requests `iters` batches through a cloned ticket, so the channel carries the
// same per-stream workload as lightstream's N parallel streams. `Bytes` clones
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
                        .add_service(
                            FlightServiceServer::new(service)
                                .max_encoding_message_size(FLIGHT_MAX_MESSAGE_BYTES)
                                .max_decoding_message_size(FLIGHT_MAX_MESSAGE_BYTES),
                        )
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

                let ticket = Ticket::new(iters.to_le_bytes().to_vec());

                let start = std::time::Instant::now();
                let mut handles = Vec::with_capacity(streams);
                for _ in 0..streams {
                    let mut client = FlightServiceClient::new(channel.clone())
                        .max_decoding_message_size(FLIGHT_MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(FLIGHT_MAX_MESSAGE_BYTES);
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

// lightstream TCP across N concurrent connections to one endpoint. TCP has no
// in-band multiplexing, so each connection carries its own stream. The reader
// merges the connections in global write order under `Ordered`.
fn bench_lightstream_tcp_parallel(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    table: &Arc<Table>,
    schema: &[Field],
    dict_regs: &[(i64, Vec<String>)],
    streams: usize,
) {
    use lightstream::models::readers::parallel::tcp::TcpParallelTableReader;
    use lightstream::models::writers::parallel::tcp::TcpParallelTableWriter;
    use lightstream::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};
    use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;

    group.bench_function(format!("lightstream_tcp_parallel_{streams}"), |b| {
        b.to_async(rt).iter_custom(|iters| {
            let table = Arc::clone(table);
            let schema = schema.to_vec();
            let dict_regs = dict_regs.to_vec();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                let total_tables = iters * streams as u64;

                let server = tokio::spawn(async move {
                    let reader =
                        TcpParallelTableReader::accept(&listener, streams, SortBehaviour::Ordered)
                            .await
                            .unwrap();
                    let tables = reader.read_all_tables().await.unwrap();
                    std::hint::black_box(&tables);
                    tables.len() as u64
                });

                let mut writer =
                    TcpParallelTableWriter::connect(addr, streams, schema, dict_regs, None)
                        .await
                        .unwrap();

                let start = std::time::Instant::now();
                for _ in 0..total_tables {
                    writer.write_table((*table).clone()).await.unwrap();
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

// Lightstream protocol across N concurrent connections to one endpoint. The
// reader merges the connections in global write order under `Ordered`.
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

                let total_tables = iters * streams as u64;

                let server_schema = schema.clone();
                let server = tokio::spawn(async move {
                    let table_types = [(TYPE_NAME, server_schema)];
                    let reader = LightstreamParallelReader::accept(
                        &listener,
                        streams,
                        &[],
                        &table_types,
                        SortBehaviour::Ordered,
                    )
                    .await
                    .unwrap();
                    let frames = reader.read_all().await.unwrap();
                    let received = frames.iter().filter(|m| m.is_table()).count() as u64;
                    std::hint::black_box(&frames);
                    received
                });

                let table_types = [(TYPE_NAME, schema)];
                let mut writer =
                    LightstreamParallelWriter::connect(addr, streams, &[], &table_types)
                        .await
                        .unwrap();

                let start = std::time::Instant::now();
                for _ in 0..total_tables {
                    writer.send_table(TYPE_NAME, (*table).clone()).await.unwrap();
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

// lightstream HTTP/2 across N concurrent request streams on one connection.
#[cfg(feature = "http")]
fn bench_lightstream_http2_parallel(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rt: &Runtime,
    table: &Arc<Table>,
    schema: &[Field],
    dict_regs: &[(i64, Vec<String>)],
    streams: usize,
) {
    use lightstream::models::readers::parallel::http::HttpParallelTableReader;
    use lightstream::models::writers::parallel::http::HttpParallelTableWriter;
    use lightstream::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};
    use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;

    group.bench_function(format!("lightstream_http2_parallel_{streams}"), |b| {
        b.to_async(rt).iter_custom(|iters| {
            let table = Arc::clone(table);
            let schema = schema.to_vec();
            let dict_regs = dict_regs.to_vec();
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let url = format!("http://{addr}/ingest");

                let total_tables = iters * streams as u64;

                let server = tokio::spawn(async move {
                    let (tcp, _peer) = listener.accept().await.unwrap();
                    let reader =
                        HttpParallelTableReader::from_tcp(tcp, streams, SortBehaviour::Ordered)
                            .await
                            .unwrap();
                    let tables = reader.read_all_tables().await.unwrap();
                    std::hint::black_box(&tables);
                    tables.len() as u64
                });

                let mut writer =
                    HttpParallelTableWriter::connect(&url, streams, schema, dict_regs, None)
                        .await
                        .unwrap();

                let start = std::time::Instant::now();
                for _ in 0..total_tables {
                    writer.write_table((*table).clone()).await.unwrap();
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

// lightstream QUIC across N concurrent unidirectional streams on one
// connection. quinn drives the connection in the background, so the merged
// reader drains as a plain `Stream`.
#[cfg(feature = "quic")]
fn bench_lightstream_quic_parallel(
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
    use lightstream::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};
    use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;

    group.bench_function(format!("lightstream_quic_parallel_{streams}"), |b| {
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

                let total_tables = iters * streams as u64;

                let server = tokio::spawn(async move {
                    let incoming = endpoint.accept().await.unwrap();
                    let conn = incoming.await.unwrap();
                    let reader =
                        QuicParallelTableReader::accept(&conn, streams, SortBehaviour::Ordered)
                            .await
                            .unwrap();
                    // read_all_tables collects every stream and reassembles the
                    // global write order from the sequence keys. The connection
                    // and endpoint close on drop at the end of this task,
                    // keeping the QUIC idle-drain out of the timed region.
                    let tables = reader.read_all_tables().await.unwrap();
                    std::hint::black_box(&tables);
                    tables.len() as u64
                });

                let mut client_ep =
                    quinn::Endpoint::client("0.0.0.0:0".parse::<SocketAddr>().unwrap()).unwrap();
                client_ep.set_default_client_config(client_config);
                let conn = client_ep.connect(addr, "localhost").unwrap().await.unwrap();

                let mut writer =
                    QuicParallelTableWriter::open(&conn, streams, schema, dict_regs, None)
                        .await
                        .unwrap();

                // Build the tables to send before the timer so the measured
                // region is the transport alone. The clone is an Arc bump on
                // the shared column buffers.
                let tables: Vec<Table> = (0..total_tables).map(|_| (*table).clone()).collect();

                let start = std::time::Instant::now();
                for table in tables {
                    writer.write_table(table).await.unwrap();
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
