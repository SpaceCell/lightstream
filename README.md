TODO - Fix/Rewrite README make it much better

# Lightstream

**Zero-copy Arrow streaming over any transport.**

Lightstream gives you Arrow IPC streaming with SIMD-aligned buffers across TCP, QUIC, WebSocket, Unix sockets, and stdio. No FlightRPC overhead, no Protobuf copies, no heavy infrastructure. Plug into Tokio, stream tables, stay zero-copy from wire to SIMD kernel.

## Why Lightstream?

**The problem:** Arrow has great in-memory format, but getting data in and out efficiently is painful. DIY Protobuf copies everything. FlightRPC is heavyweight. Most solutions break SIMD alignment somewhere in the pipeline.

**The solution:** Lightstream maintains 64-byte alignment from source to sink. Memory-mapped reads hit 100M rows in ~4.5ms. Async streams integrate with Tokio. Pick your transport, pick your format, keep your alignment.

## Transports

| Transport | Feature Flag | Stability | Description |
|-----------|--------------|-----------|-------------|
| TCP | `tcp` | stable | Raw TCP sockets |
| WebSocket | `websocket` | stable | Browser-compatible streaming |
| HTTP/2 | `http` | stable | h2 GET/POST (h2c plaintext or h2 over TLS) |
| QUIC | `quic` | stable | UDP-based, multiplexed connections (RFC 9000) |
| Unix Domain Socket | `uds` | stable | Local IPC |
| Stdio | `stdio` | stable | Pipe-based communication |
| WebTransport | `webtransport` | unstable | Wire format still pre-RFC; `wtransport` crate at 0.x |
| io_uring (UDS) | `io_uring` | unstable | Linux-only; `tokio-uring` at 0.x |

All transports use the same codec layer. Switch transports without
changing your framing logic.

The library handles framing/encoding; the caller handles connection
lifecycle (bind / accept / auth / routing). Each transport exposes
`from_stream` / `from_recv` / `from_halves` constructors so a
caller-built accept loop can hand off the accepted stream.

### Constructor vocabulary

Across all transports the constructor verbs are consistent:

- `connect` / `connect_tls` - dial out (TCP, UDS, WebSocket, HTTP).
- `new` - wrap a pre-built send/recv stream (QUIC, WebTransport, stdio).
- `from_recv` / `from_stream` / `from_halves` - wrap a server-side
  accepted stream or split halves.
- `*_with_compression` - same shape as the base method, plus a
  `Compression` argument. Writers only.

### TLS

Build with the `tls` feature to enable encrypted TCP, WebSocket and
HTTP/2 transports. `connect_tls(addr, name, config, schema)` and
`connect_tls_with_compression(..., compression)` form the TLS pair on
writers; readers expose `connect_tls(addr, name, config)` (or the
URL-based equivalent for WS/HTTP). For pinned roots, custom verifiers
or client-auth keys, supply a `rustls::ClientConfig` directly. Plain
`connect("wss://...")` / `get("https://...")` paths use
tokio-tungstenite's bundled webpki-roots verifier (gated by the same
`tls` feature). QUIC and WebTransport mandate TLS at the protocol level
and already accept their own rustls config. No default root store is
bundled - supply one through your `ClientConfig`.

## Formats

| Format | Description |
|--------|-------------|
| Arrow IPC | SIMD-aligned File and Stream protocols with schema + dictionaries |
| TLV | Minimal type-length-value for lightweight transport |
| CSV | Streaming readers/writers with null handling |
| Parquet | Columnar with Zstd/Snappy compression (feature-gated) |
| Memory Maps | Zero-copy ingestion, millions of rows in microseconds |

## Lightstream Protocol

Need more than raw tables? The optional protocol layer multiplexes typed messages and Arrow tables on a single connection. Register named types on both sides, then send and receive freely - raw bytes, Protobuf, MessagePack, or Arrow tables, all interleaved on one stream.

- **TLV wire format** - `[tag: u8][len: u32 LE][payload]`, 5-byte header per frame
- **Persistent Arrow state** - first table send carries schema and dictionaries; subsequent sends carry only record batches
- **Format-agnostic payloads** - raw `&[u8]`, Protobuf via `prost`, or MessagePack via `rmp-serde`
- **Works on every transport** - same API across TCP, WebSocket, QUIC, UDS, WebTransport, and stdio

```rust
use lightstream::models::protocol::connection::TcpLightstreamConnection;
use lightstream::models::protocol::LightstreamMessage;

let mut conn = TcpLightstreamConnection::from_tcp(stream);

// Both sides register types in the same order
conn.register_message("event");           // tag 0: opaque byte payloads
conn.register_message("command");         // tag 1: msgpack-encoded structs
conn.register_table("metrics", schema);   // Arrow table channel

// Send a mix of message types on one connection
conn.send("event", b"user-login").await?;
conn.send_msgpack("command", &cmd).await?;
conn.send_table("metrics", &table).await?;
conn.flush().await?;

// Receive and dispatch
while let Some(Ok(msg)) = conn.recv().await {
    match msg {
        LightstreamMessage::Message { tag, payload } => {
            // Decode based on tag: raw bytes, protobuf, or msgpack
        }
        LightstreamMessage::Table { table, .. } => {
            // Full Arrow table with schema decoded automatically
        }
    }
}
```

Enable with the `protocol` feature flag, plus `msgpack` or `protobuf` if you want typed serialisation.

## Quick Start

### Stream Tables over TCP

```rust
use futures_util::StreamExt;
use lightstream::models::readers::tcp::TcpTableReader;

let mut reader = TcpTableReader::connect("127.0.0.1:9000").await?;
while let Some(result) = reader.next().await {
    let table = result?;
    process(table);
}
```

### Write Arrow Files

```rust
use minarrow::{arr_i32, arr_str32, FieldArray, Table};
use lightstream::models::writers::ipc::table_writer::TableWriter;
use lightstream::enums::IPCMessageProtocol;
use tokio::fs::File;

let table = Table::new("demo".into(), vec![
    FieldArray::from_arr("id", arr_i32![1, 2, 3]),
    FieldArray::from_arr("name", arr_str32!["a", "b", "c"]),
].into());

let file = File::create("demo.arrow").await?;
let schema: Vec<_> = table.schema().iter().map(|f| (**f).clone()).collect();
let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File)?;
writer.write_table(table).await?;
writer.finish().await?;
```

### Custom Protocol

```rust
use lightstream::traits::frame_decoder::FrameDecoder;
use lightstream::enums::DecodeResult;
use lightstream::models::streams::framed_byte_stream::FramedByteStream;

pub struct MyFramer;

impl FrameDecoder for MyFramer {
    type Frame = Vec<u8>;
    fn decode(&mut self, buf: &[u8]) -> Result<DecodeResult<Self::Frame>, std::io::Error> {
        // Your framing logic
    }
}

let framed = FramedByteStream::new(socket, MyFramer, 64 * 1024);
```

## Architecture

Lightstream is layered and composable. Swap any layer without rewriting the stack:

| Layer | Implementation | Replaceable |
|-------|----------------|-------------|
| Transport | TCP, QUIC, WebSocket, UDS, WebTransport, Stdio | Yes |
| Protocol | `LightstreamConnection` - typed multiplexing | Optional |
| Framing | `TlvFrame`, `IpcMessage` | Yes |
| Buffering | `StreamBuffer` | Yes |
| Encoding | `FrameEncoder`, `FrameDecoder` | Yes |
| Formats | IPC, Parquet, CSV, TLV | Yes |

## Design Principles

- **Zero-copy** - 64-byte aligned buffers via `Vec64`, no reallocation to fix alignment
- **Composable** - Layered codecs, mix and match transports and formats
- **Async-native** - Built for Tokio and futures with backpressure-aware sinks
- **Minimal** - Fast compile times, few dependencies

## Feature Flags

| Feature | Description |
|---------|-------------|
| `tcp` | TCP transport |
| `websocket` | WebSocket transport |
| `http` | HTTP/2 transport (h2 directly, no hyper) |
| `tls` | TLS layer for TCP, WebSocket and HTTP via tokio-rustls (ring provider) |
| `quic` | QUIC transport |
| `uds` | Unix domain socket transport |
| `stdio` | Stdin/stdout transport |
| `mmap` | Memory-mapped file reads |
| `parquet` | Parquet writer |
| `zstd` | Zstd compression |
| `snappy` | Snappy compression |
| `webtransport` | WebTransport support |
| `protocol` | Lightstream protocol multiplexing |
| `protobuf` | Protobuf message encoding via `prost` (implies `protocol`) |
| `msgpack` | MessagePack encoding via `rmp-serde` (implies `protocol`) |

## Performance

Memory-mapped reads: **~4.5ms for 100M rows × 4 columns** on a consumer laptop.

The only Arrow-compatible Rust crate providing 64-byte SIMD-aligned readers/writers.

## License

Copyright Peter Garfield Bower 2025-2026.

Released under MIT. See [LICENSE](LICENSE) for details, and [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES) for Apache-licensed dependencies.

## Affiliation Notice

Lightstream is not affiliated with Apache Arrow or the Apache Software Foundation. It serialises the public Arrow format via Minarrow, using Flatbuffers schemas from Arrow-RS for schema type generation (see `THIRD_PARTY_LICENSES`).
