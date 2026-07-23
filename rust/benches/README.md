# Lightstream benchmarks

The benchmark suite uses Criterion. Run a benchmark with:

```bash
cargo bench --bench <name> --features "<features>"
```

Criterion writes reports under:

```text
target/criterion/<group>/report/index.html
```

Results depend on the host CPU, network interface and storage.
Linux is recommended to match the published benchmark environment and use the Linux-specific optimisations in Minarrow and Lightstream.


## Layout

| Directory    | Contents                                                        |
| ------------ | --------------------------------------------------------------- |
| `transport/` | Transport streaming benchmarks.                                 |
| `file/`      | Arrow IPC file, chunked-file and memory-mapped read benchmarks. |
| `json/`      | JSON encoding and decoding benchmarks.                          |
| `arrow/`     | Apache Arrow Flight comparison.                                 |
| `common/`    | Shared benchmark helpers, data shapes and scale definitions.    |

## Benchmarks

| Benchmark                             | Measures                                                                                                                                                                                                                                                                                             |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `transport/transport_bench_matrix.rs` | Runs the configured shape and scale matrix across each enabled transport: TCP, UDS, WebSocket, QUIC, WebTransport, HTTP/2 and the Lightstream protocol over TCP. TLS, zstd and `io_uring` variants are included when their features are enabled. Connection setup is excluded from the timed region. |
| `transport/lightstream_throughput.rs` | Steady-state Lightstream protocol throughput over TCP and UDS using the `Mixed` shape. Includes `io_uring` variants when enabled.                                                                                                                                                                    |
| `transport/ipc_throughput.rs`         | Raw Arrow IPC streaming throughput across the supported transports using the `Mixed` shape.                                                                                                                                                                                                          |
| `file/file_throughput.rs`             | Arrow IPC file reads and writes using the `Mixed` shape, including memory-mapped reads. Arrow-rs and Polars comparisons are available through their benchmark features.                                                                                                                              |
| `file/chunked_throughput.rs`          | Serial and parallel chunked-directory reads and writes for Arrow IPC, CSV and, when enabled, Parquet. Linux only.                                                                                                                                                                                    |
| `file/mmap_streaming.rs`              | Cold-page streaming from a multi-GiB file using a sum-and-subtract measurement method. Linux only.                                                                                                                                                                                                   |
| `json/json_throughput.rs`             | Array-of-objects JSON and NDJSON encoding and decoding with `simd-json`.                                                                                                                                                                                                                             |
| `arrow/arrow_flight_comparison.rs`    | Compares Apache Arrow Flight `DoGet` with Lightstream TCP using the same loopback workload. Requires `bench_arrow_flight`.                                                                                                                                                                           |

`ipc_throughput` and `lightstream_throughput` use only the `Mixed` shape. `transport_bench_matrix` runs the configured shape and scale matrix.

## Shape and scale matrix

`bench_helpers::BenchMatrix::from_env()` selects a preset using the `LIGHTSTREAM_BENCH_MATRIX` environment variable.

| Preset     | Cells                                                   | Intended use            |
| ---------- | ------------------------------------------------------- | ----------------------- |
| `quick`    | `Mixed` at 100,000 rows                                 | Smoke test.             |
| `standard` | Each shape at 100,000 rows, plus two medium-scale cells | Default local run.      |
| `full`     | Every shape at each defined scale                       | Complete benchmark run. |

### Shapes

| Shape           | Columns                                                                      | Purpose                                |
| --------------- | ---------------------------------------------------------------------------- | -------------------------------------- |
| `NarrowNumeric` | `i32`, `i64`, `f32`, `f64`                                                   | Numeric buffer throughput.             |
| `Wide`          | 100 numeric columns across `i32`, `i64`, `f32` and `f64`                     | Schema and per-buffer overhead.        |
| `StringHeavy`   | `i32` identifier, long UTF-8, short UTF-8 and a 100-entry categorical column | Offset-buffer and dictionary handling. |
| `Mixed`         | `i32`, `f64`, short UTF-8 and a three-entry categorical column               | General reference workload.            |

### Scales

| Scale    |        Rows |
| -------- | ----------: |
| `Tiny`   |       1,000 |
| `Small`  |     100,000 |
| `Medium` |   1,000,000 |
| `Large`  | 100,000,000 |

## Running the benchmarks

### Transport matrix

Run the quick matrix across the enabled transports:

```bash
LIGHTSTREAM_BENCH_MATRIX=quick \
cargo bench --bench transport_bench_matrix \
  --features "tcp,uds,websocket,zstd,protocol"
```

Run the standard matrix:

```bash
cargo bench --bench transport_bench_matrix \
  --features "tcp,uds,websocket,quic,webtransport,zstd,protocol"
```

Run the full matrix with Linux `io_uring` variants:

```bash
LIGHTSTREAM_BENCH_MATRIX=full \
cargo bench --bench transport_bench_matrix \
  --features "tcp,uds,websocket,quic,webtransport,zstd,protocol,io_uring"
```

Each shape and scale pair forms a Criterion group. Each enabled transport is registered as a benchmark within that group.

Enabling `io_uring` adds the Linux-only `tcp_io_uring` and `uds_io_uring` benchmarks.

### Arrow Flight comparison

```bash
LIGHTSTREAM_BENCH_MATRIX=quick \
cargo bench --bench arrow_flight_comparison \
  --features "bench_arrow_flight,tcp,protocol" \
  -- --quick
```

Each matrix cell creates a group named:

```text
arrow_flight_vs_lightstream_<shape>_<scale>
```

The group contains:

```text
arrow_flight_do_get
lightstream_protocol
lightstream_arrow_tcp
```

plus the parallel variants at each stream count. `lightstream_protocol` is the Lightstream protocol over TCP, the headline comparison against Flight. The `lightstream_arrow_*` benchmarks measure Lightstream's Arrow IPC transport writers over TCP, HTTP/2 and QUIC as additional comparisons. All benchmarks use the same workload over loopback.
Use the `max_batch_size` to split batches for framing purposes, with 8MiB recommended, but 2MiB also works without issue.

Arrow Flight splits batches that exceed the 2 MiB gRPC target per its default configuration. The receiver may therefore decode more `RecordBatch` values than the sender submitted. Throughput is calculated from the logical size of the input data, so batch splitting does not affect the denominator.

### Memory-mapped streaming

Create and benchmark the default 2 GiB file:

```bash
cargo bench --bench mmap_streaming --features "mmap"
```

Use a smaller file:

```bash
LIGHTSTREAM_MMAP_BENCH_SIZE_GIB=1 \
cargo bench --bench mmap_streaming --features "mmap" -- --quick
```

Include the Polars comparison:

```bash
cargo bench --bench mmap_streaming \
  --features "mmap,bench_polars"
```

Use a different directory:

```bash
LIGHTSTREAM_MMAP_BENCH_DIR=/data/lightstream_bench \
cargo bench --bench mmap_streaming --features "mmap"
```

This benchmark is Linux-only and requires `posix_fadvise`.

The benchmark file is stored under `/var/tmp/lightstream_mmap_bench` by default. `/tmp` is avoided because it is commonly mounted as `tmpfs`; in that configuration, `posix_fadvise(..., DONTNEED)` does not produce a cold storage read.

The resolved file path is printed when the benchmark starts.

### Other benchmarks

Run the Arrow IPC file benchmark with Arrow-rs and Polars comparisons:

```bash
cargo bench --bench file_throughput \
  --features "mmap,bench_arrow,bench_polars"
```

Run the Linux chunked-file benchmark:

```bash
cargo bench --bench chunked_throughput \
  --features "parquet,zstd"
```

Run the JSON benchmark:

```bash
cargo bench --bench json_throughput \
  --features "json"
```

## Methodology

The benchmarks measure sustained throughput. Connection setup is performed outside the timed region, and per-batch latency is not measured, except in the ECS benchmark that outputs more detailed checkpoints (please refer to its dedicated README.md).

Criterion reports are written under:

```text
target/criterion/<group>/<benchmark>/report/index.html
```

Throughput is expressed in logical payload bytes, using minarrow's `ByteSize::logical_bytes` accounting via `bench_helpers::logical_payload_bytes_shape`. This is the size of the source columns rather than the encoded byte count transmitted by the transport.

Wire throughput differs because of framing and encoding overhead. The difference is generally smaller for numeric shapes and larger for `StringHeavy`, which includes offset and dictionary buffers. For the Arrow Flight comparison, it uses `Resend` (see the advice in the `arrow_flight_comparison.rs` file), which ensures strings are kept in this encoding rather than 'hydrated' into actual Strings, which the native implementation does by default (i.e., so that Flight is not unfairly penalised).

Decoded columns and protocol messages are passed through `std::hint::black_box` where applicable to prevent the compiler from removing payload materialisation.

Transport benchmarks report receiver-side throughput.

Two small timing asymmetries apply to the loopback comparisons. The Lightstream writer connects before the receiver's timer starts, so up to one socket buffer of data can be in flight at time zero. The Arrow Flight timed region includes the `DoGet` request round trip, which Lightstream's push model does not incur. Both effects are bounded and shrink towards zero as the batch count grows.

## Cross-host benchmarks

The local benchmarks run both endpoints on one host. The cross-host rigs run the sender and receiver on separate machines.

| Directory        | Description                                                                                                                       |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `benches/ecs/`   | Provisions two EC2 hosts under Amazon ECS and compares Arrow Flight with Lightstream TCP between them. See `benches/ecs/README.md`. |
| `benches/aws/`   | Provisions two EC2 instances and runs the benchmark over plaintext TCP. See `benches/aws/README.md`.                              |
