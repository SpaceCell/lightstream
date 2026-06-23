//! Criterion benchmarks measuring JSON encode and decode throughput for
//! both array-of-objects and NDJSON output shapes.
//!
//! Decoder is backed by `simd-json` (the `json` feature pulls it in).
//! Reads use the slice variants (`decode_json_slice` / `decode_ndjson_slice`)
//! so the timed body doesn't repeat the kernel-to-user copy already paid
//! when the pre-encoded fixture was built.

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::{BENCH_ROWS, bench_schema, logical_payload_bytes, make_bench_table};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use lightstream::models::decoders::json::{
    JsonDecodeOptions, decode_json_slice, decode_ndjson_slice,
};
use lightstream::models::encoders::json::{JsonEncodeOptions, JsonFormat, encode_table_json};
use minarrow::Table;
use simd_json::Buffers;

const BENCH_BATCHES: usize = 10;

fn encode_one(table: &Table, format: JsonFormat) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 << 20);
    let opts = JsonEncodeOptions {
        format,
        ..Default::default()
    };
    encode_table_json(table, &mut out, &opts).unwrap();
    out
}

fn bench_json(c: &mut Criterion) {
    let table = make_bench_table(BENCH_ROWS);
    let schema = bench_schema(&table);
    let tables: Vec<Table> = (0..BENCH_BATCHES).map(|_| table.clone()).collect();

    let mut group = c.benchmark_group("json_throughput");
    group.throughput(Throughput::Bytes(
        logical_payload_bytes(BENCH_ROWS) * BENCH_BATCHES as u64,
    ));

    // One JSON array per batch. NDJSON concatenates naturally across batches.
    let array_per_batch: Vec<Vec<u8>> = tables
        .iter()
        .map(|t| encode_one(t, JsonFormat::Array { pretty: false }))
        .collect();
    let mut ndjson_bytes = Vec::with_capacity(1 << 20);
    {
        let opts = JsonEncodeOptions {
            format: JsonFormat::Ndjson,
            ..Default::default()
        };
        for t in &tables {
            encode_table_json(t, &mut ndjson_bytes, &opts).unwrap();
        }
    }

    group.bench_function("write_array", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(1 << 20);
            let opts = JsonEncodeOptions {
                format: JsonFormat::Array { pretty: false },
                ..Default::default()
            };
            for t in &tables {
                out.clear();
                encode_table_json(t, &mut out, &opts).unwrap();
                std::hint::black_box(&out);
            }
        });
    });

    group.bench_function("write_ndjson", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(1 << 20);
            let opts = JsonEncodeOptions {
                format: JsonFormat::Ndjson,
                ..Default::default()
            };
            for t in &tables {
                encode_table_json(t, &mut out, &opts).unwrap();
            }
            std::hint::black_box(out);
        });
    });

    let dec_opts = JsonDecodeOptions {
        schema: Some(schema.clone()),
        ..Default::default()
    };

    // simd-json's tape parser mutates the input buffer (in-place string
    // unescape), so each timed iteration needs a fresh copy. The clones
    // run in `iter_batched` setup and don't count toward the measurement.
    group.bench_function("read_array", |b| {
        b.iter_batched(
            || array_per_batch.clone(),
            |mut batches| {
                for bytes in batches.iter_mut() {
                    let tbl = decode_json_slice(bytes.as_mut_slice(), &dec_opts).unwrap();
                    assert_eq!(tbl.n_rows, BENCH_ROWS);
                    std::hint::black_box(&tbl.cols);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("read_ndjson", |b| {
        b.iter(|| {
            let tbl = decode_ndjson_slice(&ndjson_bytes, &dec_opts).unwrap();
            assert_eq!(tbl.n_rows, BENCH_ROWS * BENCH_BATCHES);
            std::hint::black_box(&tbl.cols);
        });
    });

    // Apples-to-apples parser baseline: same simd-json API the decoder
    // uses (`to_tape_with_buffers`), with the buffers reused across
    // iterations and the input clone done in untimed setup.
    group.bench_function("parse_array_tape_only", |b| {
        let mut buffers = Buffers::default();
        b.iter_batched(
            || array_per_batch.clone(),
            |mut batches| {
                for bytes in batches.iter_mut() {
                    let tape = simd_json::to_tape_with_buffers(bytes.as_mut_slice(), &mut buffers)
                        .unwrap();
                    std::hint::black_box(tape);
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_json);
criterion_main!(benches);
