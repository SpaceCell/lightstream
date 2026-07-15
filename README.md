# Lightstream

Move Arrow tables between processes, services and storage without adding a gRPC stack or writing transport-specific framing.

Lightstream provides a consistent table API across TCP, Unix domain sockets, WebSocket, HTTP/2, QUIC, WebTransport, standard I/O, files, memory maps and chunked datasets. It preserves Minarrow's 64-byte-aligned buffers through supported fast paths while keeping encoding, framing and transport concerns separate.

Representative local benchmark results:

| Workload                     | Throughput |
| ---------------------------- | ---------: |
| TCP streaming (parallel)                | ~8.0 GiB/s |
| TCP streaming (serial)                | ~5.0 GiB/s |
| UDS with `io_uring`          | ~5.5 GiB/s |
| Arrow IPC file read          |   ~9 GiB/s |
| Warm memory-mapped Arrow IPC (RAM speed) | ~170 GiB/s |

The same schema-aware interfaces cover local IPC, service-to-service streaming and persistent storage. Lower-level codecs, frames and byte streams remain available when an application requires direct control.

```rust
use lightstream::models::readers::tcp::TcpTableReader;
use lightstream::models::writers::http::HttpTableWriter;
use lightstream::models::writers::tcp::TcpTableWriter;

let reader = TcpTableReader::connect("data.feed:9000").await?;
let writer = TcpTableWriter::connect("data.feed:9000", schema.clone(), None).await?;
let upload = HttpTableWriter::post("https://api/ingest", schema, None).await?;
```

## Capabilities

Lightstream includes:

- Lightstream protocol - multiplex Arrow, Protobuf and MessagePack over a single connection
- Dedicated Arrow IPC stream and file implementations- CSV, JSON, NDJSON and Parquet
- TCP, Unix domain sockets, standard I/O, WebSocket, HTTP/2, QUIC and WebTransport
- Linux `io_uring` support for fast asynchronous I/O
- Warm memory-mapped Arrow IPC reads - measured at approximately 170 GiB/s with warm pages on the benchmark fixture; see Performance
- Chunked Arrow IPC, CSV and Parquet readers/writers that preserve 64-byte SIMD alignment 
- Parallel TCP, QUIC and HTTP/2 streams with optional ordering
- TLV framing
- Optional zstd and Snappy compression
- Configurable decode limits for untrusted input
- A minimal, opt-in dependency footprint, with core functionality implemented from scratch

The table encoding and framing layers are independent of the transport, so applications can move the same table model and protocol between connection types without changing the analytical data structures.

Applications can also work at any layer directly, replacing or extending the framing, buffering or transport logic where required.

## Installation

Enable only the formats and transports required by the application:

```toml
[dependencies]
lightstream = {
    version = "0.5.0",
    features = ["tcp", "mmap", "zstd"]
}
```

See [Feature flags](#feature-flags) for the complete list.

Lightstream currently requires Rust Nightly; supporting Stable Rust is an active priority.

## TCP table streaming

### Receiver

```rust
use futures_util::StreamExt;
use lightstream::models::readers::tcp::TcpTableReader;

let mut reader = TcpTableReader::connect("127.0.0.1:9000").await?;

while let Some(result) = reader.next().await {
    let table = result?;
    process(table);
}
```

### Sender

```rust
use lightstream::models::writers::tcp::TcpTableWriter;

let mut writer =
    TcpTableWriter::connect("127.0.0.1:9000", schema, None).await?;

writer.write_table(batch_1).await?;
writer.write_table(batch_2).await?;
writer.finish().await?;
```

Equivalent table readers and writers are available for the other supported transports. Constructors vary where required by the underlying protocol, while the table-oriented read and write interfaces remain consistent.

## Compression

Writers accept an optional compression mode:

```rust
use lightstream::compression::Compression;
use lightstream::models::writers::tcp::TcpTableWriter;

let writer = TcpTableWriter::connect(
    address,
    schema,
    Some(Compression::Zstd),
).await?;
```

Compression variants are enabled through Cargo features such as `zstd` and `snappy`. Readers inspect the stream metadata and apply the declared decompression mode automatically.

## Memory-mapped Arrow IPC

```rust
use lightstream::models::readers::ipc::mmap_table::MmapTableReader;

let reader = MmapTableReader::open("data.arrow")?;

for index in 0..reader.num_batches() {
    let table = reader.read_batch(index)?;
    process(table);
}
```

Where the file layout and alignment permit it, column buffers refer directly to the mapped file region.

## Writing an Arrow IPC file

```rust
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::writers::ipc::table::TableWriter;
use minarrow::{arr_i32, arr_str32, FieldArray, Table};
use tokio::fs::File;

let table = Table::new(
    "demo".into(),
    vec![
        FieldArray::from_arr("id", arr_i32![1, 2, 3]),
        FieldArray::from_arr("name", arr_str32!["a", "b", "c"]),
    ]
    .into(),
);

let file = File::create("demo.arrow").await?;
let schema: Vec<_> = table.schema().iter().map(|field| (**field).clone()).collect();

let mut writer =
    TableWriter::new(file, schema, IPCMessageProtocol::File, None)?;

writer.write_table(table).await?;
writer.finish().await?;
```

## Lightstream protocol

The `protocol` feature enables framed connections that carry named messages and Arrow tables over the same transport.

Both peers register the same channel definitions in the same order. The connection can then exchange raw messages, MessagePack values, Protobuf messages and tables.

```rust
use lightstream::models::protocol::connection::TcpLightstreamConnection;
use lightstream::models::protocol::LightstreamMessage;

let mut connection = TcpLightstreamConnection::from_tcp(stream);

connection.register_message("event");
connection.register_message("command");
connection.register_table("metrics", schema);

connection.send("event", b"user-login").await?;
connection.send_msgpack("command", &command).await?;
connection.send_table("metrics", &table).await?;
connection.flush().await?;

while let Some(result) = connection.recv().await {
    match result? {
        LightstreamMessage::Message { tag, payload } => {
            handle_message(tag, payload);
        }
        LightstreamMessage::Table { table, .. } => {
            handle_table(table);
        }
    }
}
```

Frames use a compact TLV header:

```text
[tag: u8][length: u32 little-endian][payload]
```

The first frame for a table channel includes its schema and dictionaries. Subsequent frames contain record batches.

MessagePack support requires `msgpack`. Protobuf support requires `protobuf`. Both features enable `protocol`.

Message and table channels can also be distributed across several connections, with their original order restored on read. See [Parallel streams](#parallel-streams).

A max_batch_size of approximately 8 MiB is recommended. Larger Tables are transparently and efficiently split into smaller batches, balancing framing overhead against predictable latency.

## Parallel streams

TCP, QUIC and HTTP/2 provide parallel readers and writers that distribute a table sequence across several connections. This can increase throughput when one connection is the limiting factor.

The Lightstream protocol also has a parallel form, allowing messages and tables to travel across the same group of connections. Readers can restore send order or return items in arrival order when ordering is unnecessary.

## Transports

| Transport           | Feature        | Status       | Notes                                                              |
| ------------------- | -------------- | ------------ | ------------------------------------------------------------------ |
| TCP                 | `tcp`          | Stable       | Raw TCP with optional TLS.                                         |
| Unix domain sockets | `uds`          | Stable       | Local inter-process communication.                                 |
| Standard I/O        | `stdio`        | Stable       | Arrow streams over standard input and output. E.g. pipe data into terminal processes. |
| WebSocket           | `websocket`    | Stable       | Binary WebSocket transport with optional TLS.                      |
| HTTP/2              | `http`         | Stable       | Direct `h2` integration for streaming request and response bodies. |
| QUIC                | `quic`         | Stable       | Multiplexed QUIC transport with protocol-level TLS.                |
| WebTransport        | `webtransport` | Experimental | WebTransport support through the `wtransport` crate.               |

Lightstream handles table encoding, framing and decoding. Applications remain responsible for listener setup, authentication, authorisation, routing and connection lifecycle.

Readers and writers can wrap accepted connections or protocol-specific stream halves through constructors such as `from_stream`, `from_recv` and `from_halves`.

### Additional I/O backends

| Backend     | Feature      | Status       | Notes |
| ----------- | ------------ | ------------ | ----- |
| `io_uring`  | `io_uring`   | Experimental | Linux-only asynchronous I/O. Because `io_uring` exposes a broad kernel interface and increases the host attack surface, it should be enabled only in trusted environments. |

## TLS

Lightstream is primarily intended for controlled environments, such as private Kubernetes networks where TLS is terminated at the ingress or service-mesh boundary and internal services exchange data with minimal overhead.

Where transport-level encryption is required, the `tls` feature enables TLS for TCP, WebSocket and HTTP/2.

```rust
use std::sync::Arc;

use lightstream::models::writers::tcp::TcpTableWriter;
use rustls::ClientConfig;
use rustls_pki_types::ServerName;

let config: Arc<ClientConfig> = build_client_config();
let server_name = ServerName::try_from("api.example.com")?;

let writer = TcpTableWriter::connect_tls(
    "api.example.com:9443",
    server_name,
    config,
    schema,
    None,
)
.await?;
```

Use `rustls::ClientConfig` for custom roots, certificate pinning, client authentication or a custom verifier.

QUIC and WebTransport include TLS at the protocol level and accept their corresponding security configuration through their own constructors.

## File and chunked I/O

Lightstream provides readers and writers for:

- Arrow IPC files and streams
- CSV
- JSON arrays and NDJSON
- Parquet
- Chunked Arrow IPC directories
- Chunked CSV directories
- Chunked Parquet directories

Chunked datasets store each table in a numbered file:

```text
<base>-0000000000.<extension>
<base>-0000000001.<extension>
<base>-0000000002.<extension>
```

The corresponding readers provide ordered serial iteration and parallel loading where supported.

## Architecture

Lightstream separates table access, protocols, formats, framing, buffering and transport.

| Layer     | Components                                                                               |
| --------- | ---------------------------------------------------------------------------------------- |
| Table API | Transport readers and writers, parallel readers and writers, chunked readers and writers |
| Protocol  | Lightstream message multiplexing                                                         |
| Formats   | Arrow IPC, CSV, JSON, Parquet and TLV                                                    |
| Framing   | IPC messages, TLV frames and WebSocket frames                                            |
| Encoding  | One-shot codecs, stream encoders and stream decoders                                     |
| Buffering | `Vec<u8>`, Minarrow `Vec64<u8>` and stream arenas                                        |
| Transport | TCP, UDS, standard I/O, WebSocket, HTTP/2, QUIC, WebTransport and `io_uring`             |
| Storage   | Files, memory maps, chunked directories and asynchronous disk streams                    |

Applications can use complete table readers and writers or assemble lower-level encoders, decoders, frames and byte streams.

## Buffering and decode safety

Minarrow-native readers, writers and transports preserve 64-byte alignment, allowing downstream SIMD processing without an additional realignment step.

Byte-oriented decoders accept configurable limits through `DecodeLimits`. These limits cover values derived from input metadata, including:

- Frame size
- Row count
- Field count
- Buffer count
- Dictionary entries
- String data
- Decompressed data
- Allocation size

Decode paths validate signed descriptors before converting them to `usize`, use checked arithmetic for offsets and lengths, and return structured errors for malformed input.

Applications processing untrusted data should set limits appropriate to the expected workload rather than relying solely on the defaults.

## Performance

Representative results from the repository benchmarks:

| Localhost loopback                       | Throughput |
| ---------------------------------------- | ---------: |
| Lightstream TCP with epoll parallelism   | ~8 GiB/s   |
| Lightstream TCP with epoll               | ~5.0 GiB/s |
| Lightstream UDS with epoll               | ~5.1 GiB/s |
| Lightstream UDS with `io_uring`          | ~5.5 GiB/s |
| Lightstream TCP with `io_uring`          | ~5.5 GiB/s |
| Lightstream WebSocket with `io_uring`    | ~4.7 GiB/s |
| Arrow IPC file read                      |   ~9 GiB/s |
| Arrow IPC memory-mapped read, warm pages | ~170 GiB/s |
| Arrow IPC memory-mapped read, cold pages |   ~6 GiB/s |
| Arrow IPC file write                     |   ~1 GiB/s |

Results from the same file-read fixture:

| Implementation                    | Throughput |
| --------------------------------- | ---------: |
| Lightstream Arrow IPC file reader |   ~9 GiB/s |
| Arrow-rs                          |   ~7 GiB/s |
| Polars                            | ~1.3 GiB/s |

These figures were produced on a single consumer-class laptop with an Intel Core Ultra 7 155H, 32 GB of RAM and Ubuntu 24.04, compiled in release mode using Rust 1.97 nightly.

Results depend on the processor, operating system, transport, storage device, workload shape, enabled features and page-cache state.

The repository includes Criterion benchmarks for:

- Transport throughput
- Lightstream protocol throughput
- Arrow IPC streaming
- Arrow IPC file reads and writes
- Memory-mapped reads
- Chunked Arrow IPC, CSV and Parquet
- JSON encoding and decoding
- Apache Arrow Flight comparison

Cross-host benchmark rigs are also provided for Amazon EC2.

See [`benches/README.md`](benches/README.md) for the benchmark matrix, methodology and commands.

Runtime chunk sizes can be configured without recompiling:

```text
LIGHTSTREAM_HTTP_CHUNK_SIZE=262144
LIGHTSTREAM_WEBSOCKET_CHUNK_SIZE=131072
LIGHTSTREAM_WEBTRANSPORT_CHUNK_SIZE=262144
LIGHTSTREAM_FILE_IO_CHUNK_SIZE=1048576
LIGHTSTREAM_INMEMORY_CHUNK_SIZE=524288
```

## Format support

| Format           | Support                                                              |
| ---------------- | -------------------------------------------------------------------- |
| Arrow IPC        | File and stream protocols, schemas, dictionaries and aligned buffers |
| TLV              | Compact framed messages                                              |
| CSV              | `Table` (`RecordBatch`) and `SuperTable` (`Vec<RecordBatch>`) encoding and decoding                           |
| JSON             | Arrays of objects and NDJSON                                          |
| Parquet          | Feature-gated reader and writer support                              |
| Memory maps      | Arrow IPC batch access through mapped files                          |
| Chunked datasets | Arrow IPC, CSV and Parquet directory layouts                         |

## Feature flags

| Feature                  | Description                                                  |
| ------------------------ | ------------------------------------------------------------ |
| `tcp`                    | TCP transport                                                |
| `uds`                    | Unix domain socket transport                                 |
| `stdio`                  | Standard input and output transport                          |
| `websocket`              | WebSocket transport                                          |
| `http`                   | HTTP/2 transport                                             |
| `quic`                   | QUIC transport                                               |
| `webtransport`           | WebTransport support                                         |
| `io_uring`               | Linux `io_uring` support                                     |
| `tls`                    | TLS for supported transports                                 |
| `mmap`                   | Memory-mapped Arrow IPC reads                                |
| `csv`                    | CSV encoding and decoding                                    |
| `json`                   | JSON and NDJSON encoding and decoding                        |
| `parquet`                | Parquet encoding and decoding                                |
| `zstd`                   | Zstandard compression                                        |
| `snappy`                 | Snappy compression                                           |
| `protocol`               | Lightstream framed protocol                                  |
| `protobuf`               | Protobuf messages through `prost`; enables `protocol`        |
| `msgpack`                | MessagePack messages through `rmp-serde`; enables `protocol` |
| `datetime`               | Date32 and Date64 columns                                    |
| `large_string`           | 64-bit string offsets                                        |
| `extended_numeric_types` | Additional integer widths                                    |
| `extended_categorical`   | Additional categorical index widths                          |
| `lbuffer`                | LBuffer integration                                          |
| `bench_arrow_flight`     | Apache Arrow Flight benchmark support                        |
| `bench_arrow`            | Arrow-rs benchmark comparison                                |
| `bench_polars`           | Polars benchmark comparison (file IO)                                  |

## Examples

The `examples/` directory contains runnable transport and format examples.

```bash
cargo run --example tcp_arrow \
  --features tcp

cargo run --example tcp_arrow_tls \
  --features "tcp,tls"

cargo run --example websocket_arrow \
  --features websocket

cargo run --example websocket_arrow_tls \
  --features "websocket,tls"

cargo run --example http_arrow \
  --features http

cargo run --example uds_arrow \
  --features uds

cargo run --example quic_arrow \
  --features quic
```

## Licence

Copyright © 2025–2026 Peter Garfield Bower.

Licensed under the Mozilla Public License 2.0. See [`LICENSE`](LICENSE) for the full terms.

## Affiliation notice

Lightstream is not affiliated with Apache Arrow or the Apache Software Foundation.

It implements public Arrow formats through Minarrow and uses FlatBuffers schema definitions derived from Arrow-rs. See `THIRD_PARTY_LICENSES` for the applicable third-party licences.

## SpaceCell

`lightstream` is maintained by [SpaceCell](https://spacecell.com) and forms part of its open-source foundation for high-performance data computing.
