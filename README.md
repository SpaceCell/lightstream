# Lightstream – *Arrow IPC streaming without compromise*

## Why this crate exists
When building high-performance, full-stack, data-driven applications, I repeatedly hit the same barriers.  

- **Protobuf/gRPC**: convenient, but introduces memory copies.  
- **FlightRPC**: powerful, but heavyweight and overengineered for most cases.  

I wanted a crate that would:  
1. Integrate Arrow IPC memory format seamlessly into any async context  
2. Provide low-level control  
3. Establish composable patterns for arbitrary wire formats  
4. Require no heavy infrastructure to get started  
5. Offer a true zero-copy path from wire -> SIMD kernels without re-allocation  
6. Maintain reasonable compile times  

**Lightstream** is the result: an extension to [Minarrow](https://crates.io/crates/minarrow) that adds native streaming and I/O. It enables raw bytestream construction, reading and writing IPC and TLV formats, CSV/Parquet writers, and a high-performance 64-byte SIMD memory-mapped IPC reader.  

### Use cases
* Bypassing library machinery and going straight to the wire
* Streaming Arrow IPC tables over network sockets with zero-copy buffers
* Defining custom binary transport protocols  
* Zero-copy ingestion with mmap for ultra-fast analytics
* Control data alignment at source - SIMD-aligned Arrow IPC writers/readers
* Async data pipelines with backpressure-aware sinks and streams
* Building custom data transport layers and transfer protocols


## Introduction
Lightstream provides composable building blocks for high-performance data I/O in Rust:  

- Asynchronous Arrow IPC streaming and file writing  
- Framed decoders/sinks for IPC, TLV, CSV, and optional Parquet  
- Zero-copy, memory-mapped Arrow file reads **(~4.5ms for 100M rows × 4 columns on a consumer laptop)**  
- Direct integration with Tokio and futures using zero-copy buffers  
- 64-byte SIMD aligned readers/writers *(the only Arrow-compatible crate providing this in 2025)*  

---

## Design Principles
- **Customisable** – *You own the buffer*. Pull-based or sink-driven streaming.  
- **Composable** – Layerable codecs for encoders, decoders, sinks, and stream adapters.  
- **Control** – Wire-level framing: IPC, TLV, CSV, and Parquet are handled at the transport boundary.  
- **Compatible** – Native async support for futures and Tokio.  
- **Power** – 64-byte aligned memory via `Vec64` ensures deterministic SIMD without hot-loop re-allocations.  
- **Extensible** – Primitive building blocks to implement custom wire formats. Contributions welcome.  
- **Efficient** – Minimal dependencies and fast compile times.  

---

## Layered Abstractions

| Layer               | Provided by Lightstream            | Replaceable |
|---------------------|------------------------------------|-------------|
| Framing             | `TlvFrame`, `IpcMessage`           | ✅ |
| Buffering           | `StreamBuffer`                     | ✅ |
| Encoding/Decoding   | `FrameEncoder`, `FrameDecoder`     | ✅ |
| Streaming           | `GenByteStream`, `Sink`            | ✅ |
| Formats             | IPC, Parquet, CSV, TLV             | ✅ |

Each layer is trait-based with a reference implementation. Swap in your own framing, buffering, or encoding logic without re-implementing the stack.  

---

## Supported Formats

- **Arrow IPC** – Full support for SIMD-aligned *File* and *Stream* protocols, schema + dictionaries, streaming or random access.  
- **TLV** – Minimal type-length-value framing for telemetry, control, or lightweight transport.  
- **Parquet** *(feature-gated)* – Compact, columnar, compression-aware writer (Zstd, Snappy) with minimal dependencies.  
- **CSV** – Streaming Arrow/Minarrow table readers/writers with headers, nulls, and custom delimiter/null handling.  
- **Memory Maps** – Ultra-fast, zero-copy ingestion: millions of rows in microseconds, SIMD-ready.  

---

## Examples

### Framed Stream Reader
```rust
use lightstream::models::streams::framed_byte_stream::FramedByteStream;
use lightstream::models::decoders::ipc::table_stream::TableStreamDecoder;
use lightstream::models::readers::ipc::table_stream_reader::TableStreamReader;

let framed = FramedByteStream::new(socket, TableStreamDecoder::default());
let mut reader = TableStreamReader::new(framed);

while let Some(table) = reader.next_table().await? {
    println!("Received table: {:?}", table.name);
}
```

### Custom Protocol
```rust
pub struct MyFramer;

impl FrameDecoder for MyFramer {
    type Frame = Vec<u8>;
    fn decode(&mut self, buf: &[u8]) -> DecodeResult<Self::Frame> {
        // Custom framing logic
    }
}

let stream = FramedByteStream::new(socket, MyFramer);
```

### Write Tables
```rust
use minarrow::{arr_i32, arr_str32, FieldArray, Table};
use lightstream::table_writer::TableWriter;
use lightstream::enums::IPCMessageProtocol;
use tokio::fs::File;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let col1 = FieldArray::from_arr("numbers", arr_i32![1, 2, 3]);
    let col2 = FieldArray::from_arr("letters", arr_str32!["x", "y", "z"]);
    let table = Table::new("demo".into(), vec![col1, col2].into());

    let file = File::create("demo.arrow").await?;
    let schema = table.schema().to_vec();
    let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File)?;

    writer.write_table(table).await?;
    writer.finish().await?;
    Ok(())
}
```

---

## Optional Features
- `parquet` – Parquet writer  
- `mmap` – Memory-mapped files  
- `zstd` – Zstd compression (IPC + Parquet)  
- `snappy` – Snappy compression (IPC + Parquet)  

---

## Licence
This project is licensed under the MIT Licence. See the `LICENCE` file for full terms, and `THIRD_PARTY_LICENSES` for Apache-licensed dependencies.  

---

## Affiliation Notice
This project is not affiliated with Apache Arrow or the Apache Software Foundation.  
It serialises the public Arrow format via a custom implementation (*Minarrow*), while reusing Flatbuffers schemas from Arrow-RS for schema type generation.  
