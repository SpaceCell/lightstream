//! Arrow Flight head-to-head bench.
//!
//! For each `BenchMatrix` cell the same logical workload runs through
//! Apache Arrow Flight (gRPC + Arrow IPC) and through lightstream TCP
//! on loopback. Both endpoints live in this process, listen on
//! 127.0.0.1, and stream batches to a client whose receive loop is
//! the timed region. Connection setup, schema negotiation, and any
//! per-iteration buffer construction happen outside the timer.
//!
//! Logical-bytes throughput is computed from the source columns in
//! [`bench_helpers::logical_payload_bytes_shape`] so the denominator
//! matches the figure reported by `transport_throughput` for the
//! same cell, letting the two bench files be read side by side.
//!
//! Gated entirely on the `bench_arrow_flight` feature; without it,
//! `arrow-flight` and `tonic` are not pulled into the dependency
//! graph and this file is not built.

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
use lightstream::models::readers::ipc::table_reader::TableReader;
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

#[derive(Clone)]
struct BenchFlightService {
    batch: Arc<RecordBatch>,
    repetitions: u64,
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
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let batch = Arc::clone(&self.batch);
        let n = self.repetitions;
        let batch_stream = stream::iter((0..n).map(move |_| {
            Ok::<RecordBatch, arrow_flight::error::FlightError>((*batch).clone())
        }));
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
                    repetitions: iters,
                };
                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

                let server = tokio::spawn(async move {
                    Server::builder()
                        .add_service(FlightServiceServer::new(service))
                        .serve_with_incoming_shutdown(incoming, async {
                            let _ = shutdown_rx.await;
                        })
                        .await
                        .unwrap();
                });

                let channel = tonic::transport::Endpoint::try_from(format!("http://{addr}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
                let mut client = FlightServiceClient::new(channel);

                let ticket = Ticket {
                    ticket: bytes::Bytes::from_static(b"bench"),
                };

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
                // FlightDataEncoderBuilder slices each input batch when
                // it exceeds the gRPC target size (default 2 MiB), so
                // one input batch can yield several decoded batches.
                // The honest comparison keeps the default chunking; the
                // sanity check is just that we received at least the
                // input count.
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

criterion_group!(benches, bench_arrow_flight_compare);
criterion_main!(benches);
