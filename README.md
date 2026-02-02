# Lightstream

**Zero-copy Arrow streaming over any transport.**

Lightstream gives you Arrow IPC streaming with SIMD-aligned buffers across TCP, QUIC, WebSocket, Unix sockets, and stdio. No FlightRPC overhead, no Protobuf copies, no heavy infrastructure. Plug into Tokio, stream tables, stay zero-copy from wire to SIMD kernel.

## Why Lightstream?

**The problem:** Arrow has great in-memory format, but getting data in and out efficiently is painful. DIY Protobuf copies everything. FlightRPC is heavyweight. Most solutions break SIMD alignment somewhere in the pipeline.

**The solution:** Lightstream maintains 64-byte alignment from source to sink. Memory-mapped reads hit 100M rows in ~4.5ms. Async streams integrate with Tokio. Pick your transport, pick your format, keep your alignment.

## Transports

| Transport | Feature Flag | Description |
|-----------|--------------|-------------|
| TCP | `tcp` | Raw TCP sockets |
| WebSocket | `websocket` | Browser-compatible streaming |
| QUIC | `quic` | Modern UDP-based, multiplexed connections |
| Unix Domain Socket | `uds` | Fast local IPC |
| Stdio | `stdio` | Pipe-based communication |

All transports use the same codec layer. Switch transports without changing your framing logic.

## Formats

| Format | Description |
|--------|-------------|
| Arrow IPC | SIMD-aligned File and Stream protocols with schema + dictionaries |
| TLV | Minimal type-length-value for lightweight transport |
| CSV | Streaming readers/writers with null handling |
| Parquet | Columnar with Zstd/Snappy compression (feature-gated) |
| Memory Maps | Zero-copy ingestion, millions of rows in microseconds |

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
| Transport | TCP, QUIC, WebSocket, UDS, Stdio | Yes |
| Framing | `TlvFrame`, `IpcMessage` | Yes |
| Buffering | `StreamBuffer` | Yes |
| Encoding | `FrameEncoder`, `FrameDecoder` | Yes |
| Formats | IPC, Parquet, CSV, TLV | Yes |

## Design Principles

- **Zero-copy** — 64-byte aligned buffers via `Vec64`, no reallocation to fix alignment
- **Composable** — Layered codecs, mix and match transports and formats
- **Async-native** — Built for Tokio and futures with backpressure-aware sinks
- **Minimal** — Fast compile times, few dependencies

## Feature Flags

| Feature | Description |
|---------|-------------|
| `tcp` | TCP transport |
| `websocket` | WebSocket transport |
| `quic` | QUIC transport |
| `uds` | Unix domain socket transport |
| `stdio` | Stdin/stdout transport |
| `mmap` | Memory-mapped file reads |
| `parquet` | Parquet writer |
| `zstd` | Zstd compression |
| `snappy` | Snappy compression |

## Performance

Memory-mapped reads: **~4.5ms for 100M rows × 4 columns** on a consumer laptop.

The only Arrow-compatible Rust crate providing 64-byte SIMD-aligned readers/writers.

## License

Copyright Peter Garfield Bower 2025-2026.

Released under MIT. See [LICENSE](LICENSE) for details, and [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES) for Apache-licensed dependencies.

## Affiliation Notice

Lightstream is not affiliated with Apache Arrow or the Apache Software Foundation. It serialises the public Arrow format via Minarrow, using Flatbuffers schemas from Arrow-RS for schema type generation (see `THIRD_PARTY_LICENSES`).
