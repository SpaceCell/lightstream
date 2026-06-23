# Lightstream benchmarks

The throughput benchmark suite. Each bench is a Criterion target, run
with `cargo bench --bench <name>` under the features it needs. Criterion
writes its report to `target/criterion/<group>/report/index.html`.

Absolute throughput depends on the host CPU, NIC, and storage.

## Layout

| Directory | Contents |
|-----------|----------|
| `transport/` | Streaming over a transport. |
| `file/` | Arrow IPC file, chunked-file, and memory-mapped reads. |
| `json/` | JSON encode and decode. |
| `arrow/` | Apache Arrow Flight head-to-head comparison. |
| `common/` | `bench_helpers.rs`, the shape and scale matrix shared across benches. |

## Benches

| Bench | Measures |
|-------|----------|
| `transport/transport_bench_matrix.rs` | Matrix-driven run across every enabled transport (TCP, UDS, WebSocket, QUIC, WebTransport, HTTP/2, Lightstream protocol over TCP), with TLS and zstd variants under their features and io_uring TCP and UDS cells under `io_uring`. Connection setup is outside the timed region. |
| `transport/lightstream_throughput.rs` | Lightstream protocol steady-state streaming over TCP and UDS, with io_uring cells under `io_uring`. Single Mixed shape. |
| `transport/ipc_throughput.rs` | Raw Arrow IPC streaming across the transports, single Mixed shape. |
| `file/file_throughput.rs` | Arrow IPC file write and read for the Mixed shape, including mmap reads, with arrow-rs and polars comparators under their bench features. |
| `file/chunked_throughput.rs` | Chunked-directory write and read for Arrow IPC, CSV, and Parquet (under `parquet`), serial and parallel load paths. Linux-only. |
| `file/mmap_streaming.rs` | Larger-than-memory cold-page streaming over a multi-GiB file, sum-and-subtract methodology. Linux-only. |
| `json/json_throughput.rs` | JSON array-of-objects and NDJSON encode and decode through `simd-json`. |
| `arrow/arrow_flight_comparison.rs` | Apache Arrow Flight `DoGet` (gRPC plus Arrow IPC) against lightstream TCP, same workload, loopback, both endpoints in one process. Gated on `bench_arrow_flight`. |

`ipc_throughput` and `lightstream_throughput` run a single Mixed shape.
`transport_bench_matrix` sweeps the full shape and scale grid below.

## Shape and scale matrix

`bench_helpers::BenchMatrix::from_env()` resolves a preset from the
`LIGHTSTREAM_BENCH_MATRIX` environment variable.

| Preset | Cells | Use |
|--------|-------|-----|
| `quick` | Mixed at 100k rows | Local smoke check, finishes in seconds. |
| `standard` (default) | Each shape at 100k plus two mid-scale | Local-run footprint. |
| `full` | All shapes across Tiny, Small, Medium, Large | Publishable grid, minutes per transport. |

Shapes:

- `NarrowNumeric` - i32, i64, f32, f64. Exercises the SIMD-friendly numeric path.
- `Wide` - 100 numeric columns across i32, i64, f32, f64. Exercises schema handling and per-buffer overhead.
- `StringHeavy` - i32 id, long utf8, short utf8, categorical32 with 100 entries. Exercises offset buffers and dictionary roundtripping.
- `Mixed` - i32, f64, short utf8, categorical with three entries. The reference shape.

Scales: `Tiny=1_000`, `Small=100_000`, `Medium=1_000_000`, `Large=100_000_000` rows.

## Running

### Transport matrix

```bash
# Quick smoke, one cell, every enabled transport
LIGHTSTREAM_BENCH_MATRIX=quick \
cargo bench --bench transport_bench_matrix \
    --features "tcp,uds,websocket,zstd,protocol"

# Standard run
cargo bench --bench transport_bench_matrix \
    --features "tcp,uds,websocket,quic,webtransport,zstd,protocol"

# Full matrix, with io_uring cells (Linux)
LIGHTSTREAM_BENCH_MATRIX=full \
cargo bench --bench transport_bench_matrix \
    --features "tcp,uds,websocket,quic,webtransport,zstd,protocol,io_uring"
```

Each `(shape, scale)` cell becomes a Criterion group, and every enabled
transport runs as a bench function inside it, so the report cross-compares
transports for that cell. The `io_uring` feature adds `tcp_io_uring` and
`uds_io_uring` cells, Linux-only.

### Arrow Flight comparison

```bash
LIGHTSTREAM_BENCH_MATRIX=quick \
cargo bench --bench arrow_flight_comparison \
    --features "bench_arrow_flight,tcp" -- --quick
```

Each cell becomes a group named
`arrow_flight_vs_lightstream_<shape>_<scale>` with two bench functions,
`arrow_flight_do_get` and `lightstream_tcp`, both carrying the same
workload on loopback.

The Flight encoder splits batches above the 2 MiB gRPC target, so the
receiver can decode more `RecordBatch` values than were sent. Throughput
accounts for the input logical bytes, so the split does not change the
denominator.

### mmap streaming

```bash
# Generates a 2 GiB file under /var/tmp/lightstream_mmap_bench on first run
cargo bench --bench mmap_streaming --features "mmap"

# Smaller file for a faster run
LIGHTSTREAM_MMAP_BENCH_SIZE_GIB=1 \
cargo bench --bench mmap_streaming --features "mmap" -- --quick

# Compare against polars
cargo bench --bench mmap_streaming --features "mmap,bench_polars"

# Relocate the bench file
LIGHTSTREAM_MMAP_BENCH_DIR=/data/lightstream_bench \
cargo bench --bench mmap_streaming --features "mmap"
```

Linux-only, requires `posix_fadvise`. The bench file lives under
`/var/tmp` by default because many distros mount `/tmp` as tmpfs, where
`posix_fadvise(DONTNEED)` is a no-op and a cold read becomes a warm-RAM
read. The first invocation prints the resolved path.

### Other benches

```bash
# Arrow IPC file read and write with arrow-rs and polars comparators
cargo bench --bench file_throughput --features "mmap,bench_arrow,bench_polars"

# Chunked write and read, Linux only
cargo bench --bench chunked_throughput --features "parquet,zstd"

# JSON encode and decode
cargo bench --bench json_throughput --features "json"
```

## Methodology

The suite measures sustained throughput with connection setup outside the
timed region. It does not measure per-batch latency. Criterion's HTML
output carries the sample distribution and p50/p99 figures under
`target/criterion/<group>/<bench>/report/index.html`.

Throughput is reported in logical bytes through
`bench_helpers::logical_payload_bytes_shape`, the size of the source
columns being shipped rather than the encoded bytes on the wire. Encoded
throughput differs by the IPC framing overhead, which is small for the
numeric shapes and larger for `StringHeavy` because of the offset buffers.

`std::hint::black_box` wraps the decoded `cols` slice, and the protocol
message where one applies, so the reader cannot elide payload
materialisation. Receiver-side throughput is what the suite reports.

## Cross-host rigs

Local benches measure the software ceiling on one host. The rigs under
`bench/` run the sender and receiver on separate hosts over a real
network.

- `bench/cluster/` provisions an ephemeral EKS cluster and places a
  sender and receiver pod on separate nodes via anti-affinity, measuring
  container-to-container throughput across the VPC. See
  `bench/cluster/README.md`.
- `bench/aws/` provisions two EC2 hosts and streams between them over
  plaintext TCP. See `bench/aws/README.md`.
