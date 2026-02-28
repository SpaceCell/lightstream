//! Criterion benchmarks measuring Lightstream protocol steady-state streaming
//! throughput over TCP and Unix domain sockets.
//!
//! Connection setup is excluded from the timed region. Each sample establishes
//! one connection, then streams `iters` batches through it, timing only the
//! send/recv pipeline. Criterion reports throughput in bytes/sec.

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use lightstream::models::protocol::connection::{
    TcpLightstreamConnection, UdsLightstreamConnection,
};
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, Table, TextArray,
    Vec64, arr_f64, arr_i32, arr_str32, ffi::arrow_dtype::CategoricalIndexType,
};
use tokio::net::{TcpListener, UnixListener};

const BENCH_ROWS: usize = 100_000;

fn make_bench_table(n_rows: usize) -> Table {
    let ids: Vec64<i32> = (0..n_rows as i32).collect();
    let values: Vec64<f64> = (0..n_rows).map(|i| i as f64 * 0.5).collect();
    let labels: Vec64<String> = (0..n_rows).map(|i| format!("row_{}", i)).collect();
    let label_refs: Vec64<&str> = labels.iter().map(String::as_str).collect();

    let id_col = FieldArray::from_arr("ids", arr_i32!(ids));
    let value_col = FieldArray::from_arr("values", arr_f64!(values));
    let label_col = FieldArray::from_arr("labels", arr_str32!(label_refs));

    let indices: Vec64<u32> = (0..n_rows).map(|i| (i % 3) as u32).collect();
    let dict_col = FieldArray::new(
        Field {
            name: "category".into(),
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
            nullable: true,
            metadata: Default::default(),
        },
        Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
            data: Buffer::from(indices),
            unique_values: Vec64::from(vec![
                "red".to_string(),
                "green".to_string(),
                "blue".to_string(),
            ]),
            null_mask: Some(Bitmask::new_set_all(n_rows, true)),
        }))),
    );

    Table::new(
        "bench_table".to_string(),
        Some(vec![id_col, value_col, label_col, dict_col]),
    )
}

fn bench_schema(table: &Table) -> Vec<Field> {
    table.schema().iter().map(|f| (**f).clone()).collect()
}

/// Logical payload size of one batch for throughput reporting.
fn logical_payload_bytes(n_rows: usize) -> u64 {
    let ids = n_rows * size_of::<i32>();
    let values = n_rows * size_of::<f64>();
    let label_offsets = (n_rows + 1) * size_of::<u32>();
    let label_data: usize = (0..n_rows).map(|i| format!("row_{}", i).len()).sum();
    let category_indices = n_rows * size_of::<u32>();
    (ids + values + label_offsets + label_data + category_indices) as u64
}

fn bench_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = Arc::new(make_bench_table(BENCH_ROWS));
    let schema = bench_schema(&table);

    let mut group = c.benchmark_group("transport_throughput");
    group.throughput(Throughput::Bytes(logical_payload_bytes(BENCH_ROWS)));

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
                    let mut conn = TcpLightstreamConnection::from_tcp(stream);
                    conn.register_table("Data", write_schema);

                    for _ in 0..n {
                        conn.send_table("Data", &write_table).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                });

                // Accept blocks until the writer connects, excluding connection
                // setup from the timed region.
                let (socket, _) = listener.accept().await.unwrap();
                let mut conn = TcpLightstreamConnection::from_tcp(socket);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                }
                let elapsed = start.elapsed();

                writer.await.unwrap();
                elapsed
            }
        });
    });

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
                    let mut conn = UdsLightstreamConnection::from_uds(stream);
                    conn.register_table("Data", write_schema);

                    for _ in 0..n {
                        conn.send_table("Data", &write_table).await.unwrap();
                    }
                    conn.flush().await.unwrap();
                    conn.shutdown().await.unwrap();
                });

                let (socket, _) = listener.accept().await.unwrap();
                let mut conn = UdsLightstreamConnection::from_uds(socket);
                conn.register_table("Data", schema);

                let start = std::time::Instant::now();
                for _ in 0..n {
                    let msg = conn.recv().await.unwrap().unwrap();
                    assert!(msg.is_table());
                }
                let elapsed = start.elapsed();

                writer.await.unwrap();
                elapsed
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
