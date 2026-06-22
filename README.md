# Lightstream

Zero-copy Arrow IPC streaming over any transport, with 64-byte SIMD-aligned buffers preserved from wire to kernel.

```rust
use lightstream::models::readers::tcp::TcpTableReader;
use lightstream::models::writers::http::HttpTableWriter;
use lightstream::models::writers::tcp::TcpTableWriter;

// Same constructor shape across every transport
let reader = TcpTableReader::connect("data.feed:9000").await?;
let writer = TcpTableWriter::connect("data.feed:9000", schema, None).await?;
let upload = HttpTableWriter::post("https://api/ingest", schema, None).await?;
```

Streams reach ~4.7 GiB/s on TCP epoll, ~5.5 GiB/s on UDS io_uring, and ~170 GiB/s on warm Arrow mmap. Beats `arrow-rs` (~7 GiB/s file read) and `polars` (~1.3 GiB/s) on the same fixture without a re-serialisation hop.

## What it is

A Rust library for shipping Arrow tables over the wire. Provides:

- **Eight transports**: TCP, WebSocket, HTTP/2, QUIC, UDS, stdio, WebTransport, io_uring. All speak the same Arrow IPC framing; switch transport without touching encode/decode logic.
- **Zero-copy decode**: `SharedBuffer` views into a recycled `StreamArena`. Column buffers point at the network bytes that produced them.
- **64-byte SIMD alignment**: maintained from socket read through to the kernel that consumes the column. The uncompressed fast path pays no overhead for compression support existing.
- **Optional TLS**: rustls integration for TCP, WebSocket, and HTTP/2 under the `tls` feature. QUIC and WebTransport mandate TLS at the protocol level and already accept their own rustls config.
- **Optional protocol multiplexing**: the `protocol` feature provides typed message channels (Protobuf, MessagePack, or raw bytes) interleaved with Arrow tables on one socket. An Arrow Flight replacement without the gRPC layer (see below).
- **Hardened decoders**: every byte-consuming entry point caps untrusted lengths via `DecodeLimits`, sign-checks i64 buffer descriptors before casting to `usize`, and surfaces malformed input as `io::Error` rather than panicking or OOMing.

## Where it sits

| Scenario | Lightstream | gRPC + Protobuf | Arrow Flight | arrow-rs + custom framing |
|----------|-------------|-----------------|--------------|---------------------------|
| Polyglot codegen (Java, Python, Go, ...) | Rust-only | yes | yes | varies |
| Row-oriented RPC contracts | wrong tool | yes | partial | wrong tool |
| Multi-GiB/s columnar streaming, Rust <-> Rust | yes | costly protobuf encoding tax | gRPC framing overhead | yes (you write the framing) |
| Arrow-aware mmap reads at memory bandwidth | yes | n/a | no | partial |
| Transport choice (TCP, WS, HTTP/2, QUIC, UDS, stdio, ...) | yes | HTTP/2 only | HTTP/2 only | bring your own |
| Cross-process on the same host via UDS or stdio | yes | no | no | rare |
| Mesh integration, deadlines, observability | bring your own | yes | yes (inherited from gRPC) | bring your own |
| Setup complexity | minimal (no .proto, no codegen) | high | high | low but you build everything |

If you need polyglot RPC contracts between heterogeneous services, gRPC is a different tool for a different problem. For Arrow streaming workloads in Rust - whether between services, between processes on the same host, or piped through stdin/stdout - lightstream is the direct path. The Lightstream protocol layer (below) replaces Arrow Flight specifically: same multiplexed-control-and-data shape on one connection, no gRPC server, no `.proto` file, no codegen step, and substantially higher throughput.

Stdio and UDS as first-class streaming transports for Arrow are unusual; most libraries reach for HTTP/2 by default. Lightstream treats them the same as any other transport, which makes it a natural fit for sidecar processes, ML feature loaders, pipe-based ETL stages, and any deployment that wants kernel-bypass on the same box without TCP loopback.

## Quick start

### Stream tables over TCP

```rust
use futures_util::StreamExt;
use lightstream::models::readers::tcp::TcpTableReader;
use lightstream::models::writers::tcp::TcpTableWriter;

// Receiver
let mut reader = TcpTableReader::connect("127.0.0.1:9000").await?;
while let Some(result) = reader.next().await {
    let table = result?;
    process(table);
}

// Sender
let mut writer = TcpTableWriter::connect("127.0.0.1:9000", schema, None).await?;
writer.write_table(batch_1).await?;
writer.write_table(batch_2).await?;
writer.finish().await?;
```

Swap `TcpTableWriter` for `WebSocketTableWriter::connect("ws://...", schema, None)`, `HttpTableWriter::post("http://api/ingest", schema, None)`, or `UdsTableWriter::connect(path, schema, None)`. Same shape.

### Compressed batches

```rust
use lightstream::compression::Compression;

let writer = TcpTableWriter::connect(addr, schema, Some(Compression::Zstd)).await?;
```

`Compression` variants are feature-gated (`zstd`, `snappy`). With neither feature on, the enum is uninhabited and only `None` typechecks for the trailing argument, so callers writing `None` stay portable across feature sets.

### Memory-mapped reads

```rust
use lightstream::models::readers::ipc::mmap_table::MmapTableReader;

let reader = MmapTableReader::open("data.arrow")?;
for i in 0..reader.num_batches() {
    let table = reader.read_batch(i)?;
    // Column buffers point directly into the mmap region.
}
```

### Write an Arrow file

```rust
use minarrow::{arr_i32, arr_str32, FieldArray, Table};
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::writers::ipc::table::TableWriter;
use tokio::fs::File;

let table = Table::new("demo".into(), vec![
    FieldArray::from_arr("id", arr_i32![1, 2, 3]),
    FieldArray::from_arr("name", arr_str32!["a", "b", "c"]),
].into());

let file = File::create("demo.arrow").await?;
let schema: Vec<_> = table.schema().iter().map(|f| (**f).clone()).collect();
let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File, None)?;
writer.write_table(table).await?;
writer.finish().await?;
```

## Lightstream protocol

A direct replacement for Arrow Flight, minus the gRPC layer.

The `protocol` feature flag enables a TLV-multiplexed connection that carries typed messages (raw bytes, Protobuf, or MessagePack) alongside Arrow tables on one socket. Both sides register the same type vocabulary up front; thereafter each side calls `send`, `send_table`, `send_protobuf`, or `send_msgpack` freely.

```rust
use lightstream::models::protocol::connection::TcpLightstreamConnection;
use lightstream::models::protocol::LightstreamMessage;

let mut conn = TcpLightstreamConnection::from_tcp(stream);

// Both sides register types in the same order
conn.register_message("event");           // tag 0: raw bytes
conn.register_message("command");         // tag 1: msgpack-encoded structs
conn.register_table("metrics", schema);   // tag 2: Arrow table channel

conn.send("event", b"user-login").await?;
conn.send_msgpack("command", &cmd).await?;
conn.send_table("metrics", &table).await?;
conn.flush().await?;

while let Some(Ok(msg)) = conn.recv().await {
    match msg {
        LightstreamMessage::Message { tag, payload } => { /* dispatch on tag */ }
        LightstreamMessage::Table { table, .. } => { /* full Arrow table */ }
    }
}
```

Wire format: `[tag: u8][len: u32 LE][payload]`. The first table send carries schema and dictionaries; subsequent sends carry only record batches. The whole multiplexer works over any of the eight transports.

**Versus Arrow Flight.** Flight ships Arrow tables over gRPC, which requires a `.proto` file describing the service surface, a codegen step, a tonic/grpc server stack, and the gRPC framing overhead on every batch. The Lightstream protocol gets you the same "interleaved control messages plus Arrow tables on one connection" shape without any of that: pick a transport, register types, send. Setup fits in a screen of code, the wire format is a 5-byte TLV header per frame, and the throughput beats Flight by a wide margin on the documented benches. Rust-to-Rust only at the moment. Enable with the `protocol` feature, plus `msgpack` or `protobuf` for typed message encodings.

## Transports

| Transport | Feature flag | Stability | Notes |
|-----------|--------------|-----------|-------|
| TCP | `tcp` | stable | Raw TCP sockets, optional TLS |
| WebSocket | `websocket` | stable | Browser-compatible streaming, `wss://` via tokio-tungstenite |
| HTTP/2 | `http` | stable | `h2` directly (no hyper). h2c plaintext or h2 over TLS |
| QUIC | `quic` | stable | UDP-based, multiplexed (RFC 9000). Always TLS |
| Unix domain socket | `uds` | stable | Local IPC. First-class Arrow streaming on the same host |
| Stdio | `stdio` | stable | Pipe-based communication. Stream Arrow through stdin/stdout |
| WebTransport | `webtransport` | unstable | Spec still pre-RFC, `wtransport` crate at 0.x. Safari does not implement it |
| io_uring (UDS) | `io_uring` | unstable | Linux only. `tokio-uring` at 0.x. Highest documented UDS throughput |

The library handles framing, encoding, and decoding. The caller handles connection lifecycle (bind, accept, auth, routing). Every transport exposes a `from_stream` / `from_recv` / `from_halves` constructor so a hand-rolled accept loop can hand the accepted stream over to lightstream.

### Constructor vocabulary

Identical verbs across transports:

- `connect` / `connect_tls` - dial out (TCP, UDS, WebSocket, HTTP).
- `new` - wrap a pre-built send/recv stream (QUIC, WebTransport, Stdio).
- `from_recv` / `from_stream` / `from_halves` - wrap a server-side accepted stream or pre-split halves.

Writers take a trailing `compression: Option<Compression>` argument. Readers do not have a compression parameter; they decompress whatever the stream declares.

### TLS

Build with the `tls` feature to enable encrypted TCP, WebSocket, and HTTP/2.

```rust
use std::sync::Arc;
use rustls::ClientConfig;
use rustls_pki_types::ServerName;

let config: Arc<ClientConfig> = build_my_client_config();
let server_name = ServerName::try_from("api.example.com")?;

let writer = TcpTableWriter::connect_tls(
    "api.example.com:9443",
    server_name,
    config,
    schema,
    None,
).await?;
```

Plain `connect("wss://...")` and `get("https://...")` paths use tokio-tungstenite's bundled webpki-roots verifier (also gated by `tls`). For pinned roots, custom verifiers, or client-auth keys, supply a `rustls::ClientConfig` directly via `connect_tls`. QUIC and WebTransport already accept their own rustls config. No default root store is bundled; the caller provides one.

## Performance

Single consumer-laptop runs, no warm-up tricks:

| Workload | Throughput |
|----------|-----------|
| Lightstream TCP epoll | ~5 GiB/s |
| Lightstream UDS epoll | ~5.1 GiB/s |
| Lightstream UDS io_uring | ~5.5 GiB/s |
| Lightstream TCP io_uring | ~5.5 GiB/s |
| Lightstream WebSocket io_uring | ~4.7 GiB/s |
| Arrow IPC file read (on-demand per-batch) | ~9 GiB/s |
| Arrow IPC mmap warm (page cache) | ~170 GiB/s |
| Arrow IPC mmap cold (SSD-bound) | ~6 GiB/s |
| Arrow IPC file write | ~1 GiB/s |

On the same fixture: `arrow-rs` file read ~7 GiB/s, `polars` ~1.3 GiB/s.

Chunk sizes are tunable via env vars without recompiling:

```
LIGHTSTREAM_HTTP_CHUNK_SIZE=262144
LIGHTSTREAM_WEBSOCKET_CHUNK_SIZE=131072
LIGHTSTREAM_WEBTRANSPORT_CHUNK_SIZE=262144
LIGHTSTREAM_FILE_IO_CHUNK_SIZE=1048576
LIGHTSTREAM_INMEMORY_CHUNK_SIZE=524288
```

## Hardening

Production deployments consume bytes from untrusted peers. The decoders take this seriously:

- **`DecodeLimits`** caps the bytes a single decode may allocate from any length read out of the wire (frame size, row count, field count, buffer count, dictionary entries, string bytes, decompressed bytes). Defaults are generous; tighten via `Option<DecodeLimits>` on `TLVDecoder::new`, `ArrowIPCFrameDecoder::new`, `ArrowIpcCodec::new`, and `TableStreamDecoder::new`.
- **Panic-to-Result.** Flatbuffer `unwrap()` sites on the live decode path now return `io::Error::InvalidData` rather than aborting the process.
- **Buffer-descriptor sanity.** Every i64 offset / length read from untrusted metadata is sign-checked and `checked_add`-ed before being cast to `usize`. `Vector::get` is bound-checked before the call so a missing-buffer flatbuffer cannot trigger a panic.
- **Decompression-bomb cap.** `decompress_ipc_body` rejects an `uncompressed_len` prefix or accumulated total above `max_decompressed_bytes` before any allocation that scales with it.
- **mmap window check.** `MmapTableReader::open` refuses `offset + len > file_size`, surfacing an `InvalidInput` error rather than letting a runtime read SIGBUS on the unmapped tail.
- **`unsafe` audit.** Live hand-written `unsafe` sites carry `// SAFETY:` invariants describing what they rely on.

## Architecture

Layered. Replace any layer without rewriting the stack:

| Layer | Implementation | Replaceable |
|-------|----------------|-------------|
| Transport | TCP, WebSocket, HTTP/2, QUIC, UDS, WebTransport, Stdio, io_uring | yes |
| Protocol | `LightstreamConnection` - typed multiplexing | optional |
| Framing | `TlvFrame`, `IpcMessage` | yes |
| Buffering | `StreamBuffer` (Vec64 = 64-byte SIMD, Vec<u8> = 8-byte interop) | yes |
| Encoding | `FrameEncoder`, `FrameDecoder` | yes |
| Formats | Arrow IPC, Parquet, CSV, JSON, TLV | yes |

## Formats

| Format | Description |
|--------|-------------|
| Arrow IPC | SIMD-aligned File and Stream protocols with schema + dictionaries |
| TLV | Minimal type-length-value for lightweight transport |
| CSV | Streaming readers/writers with null handling |
| JSON | Array-of-objects and NDJSON via simd-json |
| Parquet | Columnar with Zstd / Snappy compression (feature-gated) |
| Memory maps | Zero-copy ingestion, millions of rows in microseconds |

## Feature flags

| Feature | Description |
|---------|-------------|
| `tcp` | TCP transport |
| `websocket` | WebSocket transport |
| `http` | HTTP/2 transport (h2 directly, no hyper) |
| `tls` | TLS layer for TCP, WebSocket, and HTTP via tokio-rustls (ring provider) |
| `quic` | QUIC transport |
| `uds` | Unix domain socket transport |
| `stdio` | Stdin/stdout transport |
| `webtransport` | WebTransport (unstable - see Transports table) |
| `io_uring` | io_uring UDS transport (Linux only, unstable) |
| `mmap` | Memory-mapped file reads |
| `parquet` | Parquet reader and writer |
| `csv` | CSV reader and writer |
| `json` | JSON reader and writer (simd-json) |
| `zstd` | Zstd compression |
| `snappy` | Snappy compression |
| `protocol` | Lightstream protocol multiplexing |
| `protobuf` | Protobuf message encoding via `prost` (implies `protocol`) |
| `msgpack` | MessagePack encoding via `rmp-serde` (implies `protocol`) |
| `datetime` | Date32 / Date64 column types |
| `large_string` | LargeString offsets (i64) |
| `extended_numeric_types` | Int8 / UInt8 / Int16 / UInt16 columns |
| `extended_categorical` | Additional categorical index widths |

## Examples

The `examples/` directory contains runnable round-trip demos for each transport. Run any of them with the matching feature flag:

```
cargo run --example tcp_arrow             --features tcp
cargo run --example tcp_arrow_tls         --features "tcp,tls"
cargo run --example websocket_arrow       --features websocket
cargo run --example websocket_arrow_tls   --features "websocket,tls"
cargo run --example http_arrow            --features http
cargo run --example uds_arrow             --features uds
cargo run --example quic_arrow            --features quic
```

## License

Copyright Peter Garfield Bower 2025-2026.

## Affiliation notice

Lightstream is not affiliated with Apache Arrow or the Apache Software Foundation. It serialises the public Arrow format via Minarrow, using Flatbuffers schemas from Arrow-RS for schema type generation (see `THIRD_PARTY_LICENSES`).
