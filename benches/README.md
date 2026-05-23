# Lightstream benchmarks

A reproduction guide for the throughput numbers reported in the
project readme. Every figure is produced by one of the files in
this directory, and the same command will reproduce the same shape
of result on your hardware. Numbers in absolute terms vary by NIC,
SSD, and CPU; the relative gaps between transports and against the
comparators stay stable.

## File-by-file

| File                       | What it measures                                                                                              |
|----------------------------|---------------------------------------------------------------------------------------------------------------|
| `transport_throughput.rs`  | Matrix-driven head-to-head across every enabled transport (TCP, UDS, WebSocket, QUIC, WebTransport, Lightstream protocol over TCP), with optional zstd. Connection setup excluded from the timed region. |
| `arrow_flight_compare.rs`  | Apache Arrow Flight DoGet (gRPC + Arrow IPC) versus lightstream TCP, same workload, same machine, loopback. Gated on `bench_arrow_flight`. |
| `mmap_streaming.rs`        | Cold-page batched streaming over a multi-GiB file. `posix_fadvise(POSIX_FADV_DONTNEED)` between iterations so reads come from disk. Linux-only. |
| `file_throughput.rs`       | Small-file IPC write/read for the canonical Mixed shape, including arrow-rs and polars comparators under their bench features. |
| `chunked_throughput.rs`    | Chunked-format directory write/read against the Arrow IPC, CSV, and (under `parquet`) Parquet writers. Linux-only. |
| `json_throughput.rs`       | JSON array-of-objects and NDJSON encode/decode through `simd-json`. |
| `bench_helpers.rs`         | Shared shape and scale matrix consumed by `transport_throughput`, `arrow_flight_compare`, and `mmap_streaming`. |
| `*_orig.rs`                | Pre-matrix single-shape benches kept around until the matrix bench has been validated across the full transport surface, after which the `_orig` files will be removed. |

## The matrix

`bench_helpers::BenchMatrix::from_env()` resolves a preset from the
`LIGHTSTREAM_BENCH_MATRIX` environment variable. Three presets:

| Preset     | Cells                                            | Use case                                      |
|------------|--------------------------------------------------|-----------------------------------------------|
| `quick`    | 1 cell (Mixed at 100k rows)                      | Local smoke check; finishes in seconds.       |
| `standard` (default) | 6 cells: each shape at 100k + two mid-scale | Reasonable local-run footprint.       |
| `full`     | 16 cells: all shapes x Tiny/Small/Medium/Large   | Publishable matrix; expect ~minutes per transport. |

Shapes:

- `NarrowNumeric` - i32, i64, f32, f64. Tests the SIMD-friendly
  numeric path.
- `Wide` - 100 numeric columns split evenly across i32/i64/f32/f64.
  Tests schema-handling and per-buffer overhead.
- `StringHeavy` - i32 id, long utf8, short utf8, categorical32 with
  100 entries. Tests offset buffers and dictionary roundtripping.
- `Mixed` - i32, f64, short utf8, categorical with three entries.
  Canonical mixed shape.

Scales: `Tiny=1_000`, `Small=100_000`, `Medium=1_000_000`,
`Large=100_000_000` rows.

## Running locally

The two headline numbers are the consolidated transport matrix and
the Arrow Flight head-to-head. Both expect cargo and a recent stable
Rust.

### Transport matrix

```bash
# Quick smoke (one cell, all enabled transports)
LIGHTSTREAM_BENCH_MATRIX=quick \
cargo bench --bench transport_throughput \
    --features "tcp,uds,websocket,zstd,protocol"

# Standard run (default if env var unset)
cargo bench --bench transport_throughput \
    --features "tcp,uds,websocket,quic,webtransport,zstd,protocol"

# Full matrix - long; produces the publishable grid
LIGHTSTREAM_BENCH_MATRIX=full \
cargo bench --bench transport_throughput \
    --features "tcp,uds,websocket,quic,webtransport,zstd,protocol"
```

Each `(shape, scale)` cell becomes a Criterion group; within the
group, every enabled transport runs as a bench function, so the
HTML report cross-compares them for that cell.

### Arrow Flight head-to-head

```bash
LIGHTSTREAM_BENCH_MATRIX=quick \
cargo bench --bench arrow_flight_compare \
    --features "bench_arrow_flight,tcp" -- --quick
```

Each cell becomes a group named
`arrow_flight_vs_lightstream_<shape>_<scale>` with two bench
functions: `arrow_flight_do_get` (Apache Arrow Flight over gRPC on
loopback) and `lightstream_tcp` (lightstream TCP on loopback). Both
sides carry the same workload; the criterion HTML report puts them
side-by-side.

The Flight encoder splits batches above the default 2 MiB gRPC
target size, so the receiver may see more decoded `RecordBatch`
values than input batches; the throughput accounting still uses the
input logical bytes so the comparison stays honest.

### mmap streaming

```bash
# Generates a 2 GiB file under /var/tmp/lightstream_mmap_bench on
# first run, then iterates cold-page batched reads against it.
cargo bench --bench mmap_streaming --features "mmap"

# Smaller file for a faster smoke run
LIGHTSTREAM_MMAP_BENCH_SIZE_GIB=1 \
cargo bench --bench mmap_streaming --features "mmap" -- --quick

# Compare against polars under bench_polars
cargo bench --bench mmap_streaming --features "mmap,bench_polars"

# Override the bench file location if /var/tmp is unsuitable
LIGHTSTREAM_MMAP_BENCH_DIR=/data/lightstream_bench \
cargo bench --bench mmap_streaming --features "mmap"
```

Linux-only (requires `posix_fadvise`). The bench file lives under
`/var/tmp` by default because many distros mount `/tmp` as tmpfs,
where `posix_fadvise(DONTNEED)` is a no-op and "cold" reads silently
become warm-RAM measurements. The first invocation prints the
resolved path so the choice is visible.

### Other benches

```bash
# Small-file IPC read/write with arrow-rs and polars comparators
cargo bench --bench file_throughput --features "mmap,bench_arrow,bench_polars"

# Chunked write/read (Linux only)
cargo bench --bench chunked_throughput --features "parquet,zstd"

# JSON encode/decode
cargo bench --bench json_throughput --features "json"
```

## AWS A-to-B

Local benches measure software ceilings; cross-host runs measure
what a deployed service actually sees. The rig at `bench/aws/`
includes a `bench_sender` and `bench_receiver` example pair, a
Dockerfile, and an SSH-orchestration script that launches the
sender, runs the receiver, and captures a machine-parsable result
line. See `bench/aws/README.md` for the EC2 setup (instance class,
placement group, security group) and the workflow.

## What the numbers do and do not tell you

The bench surface measures sustained throughput with connection
setup excluded from the timed region; it does not measure per-batch
latency. Sample distributions are visible in Criterion's HTML
output (`target/criterion/<group>/<bench>/report/index.html`) and
include p50/p99 figures if you want to read latency off them.

Throughput is reported in *logical* bytes through
`bench_helpers::logical_payload_bytes_shape` - the size of the
source columns being shipped, not the encoded bytes on the wire.
Encoded throughput differs by the IPC framing overhead, which is
small for the numeric shapes and non-trivial for `StringHeavy`
because of the offset buffers.

`std::hint::black_box` is applied to the decoded `cols` slice and,
where applicable, the protocol message, so the reader cannot elide
payload materialisation. Receiver-side throughput is what's
reported.

## Reproducing the figures in the README

The lightstream readme cites four numbers people most commonly want
to verify:

1. **TCP loopback** - run `LIGHTSTREAM_BENCH_MATRIX=quick cargo
   bench --bench transport_throughput --features "tcp"`. The
   reported `transport_mixed_small_100k/tcp` throughput is the
   figure.
2. **UDS io_uring** - same command with `--features "uds,io_uring"`,
   read off the `uds` cell.
3. **Arrow Flight comparison ratio** - run the Arrow Flight bench
   above; the ratio between the `arrow_flight_do_get` and
   `lightstream_tcp` cells in the same group is what the readme
   cites.
4. **mmap larger-than-memory** - run the mmap streaming bench
   above; the `lightstream_mmap_cold` and `lightstream_file_cold`
   numbers are the headline pair.

Absolute numbers vary by hardware. The relative gaps - lightstream
vs Flight, mmap vs file, UDS vs TCP - are what generalise.
