# Lightstream

**Send and receive typed Arrow and Protobuf data easily across networks and processes at speed.**

```python
for batch in ls.read("tcp://feed.example.com:9000"):
    process(batch)
```

Supported URI schemes include:

| Transport             | URI                     |
| --------------------- | ----------------------- |
| TCP                   | `tcp://host:port`       |
| Unix domain socket    | `uds:///path/to/socket` |
| WebSocket             | `ws://host/path`        |
| Secure WebSocket      | `wss://host/path`       |
| HTTP                  | `http://host/path`      |
| HTTPS                 | `https://host/path`     |
| QUIC                  | `quic://host:port`      |
| WebTransport          | `wt://host/path`        |
| Standard input/output | `stdio://`              |

## Installation

**Rust**
```rust
cargo install lightstream
```
**Python**
```python
pip install lightstream-io
```
See the [Python README](python/README.md) and [Rust README](rust/README.md) for setup details.


## Quick Start

### Rust

**Send Tables**

```rust
use lightstream::models::writers::tcp::TcpTableWriter;

let mut writer = TcpTableWriter::connect("127.0.0.1:9000", schema, None).await?;
writer.write_table(batch_1).await?;
writer.finish().await?;
```

**Receive Tables**

```rust
use futures_util::StreamExt;
use lightstream::models::readers::tcp::TcpTableReader;

let mut reader = TcpTableReader::connect("127.0.0.1:9000").await?;

while let Some(result) = reader.next().await {
    let table = result?;
    process(table);
}
```

**Lightstream protocol**

Multiplex Protobuf messages and Arrow tables on the one connection.

```rust
use lightstream::models::protocol::connection::TcpLightstreamConnection;
use lightstream::models::protocol::LightstreamMessage;

let mut conn = TcpLightstreamConnection::from_tcp(stream);
conn.register_message("event");
conn.register_table("metrics", schema);

conn.send("event", b"user-login").await?;
conn.send_table("metrics", &table).await?;

while let Some(msg) = conn.recv().await {
    match msg? {
        // Protobuf message
        LightstreamMessage::Message { tag, payload } => { /* … */ }
        // Arrow table
        LightstreamMessage::Table { table, .. } => { /* … */ }
    }
}
```

**Memory-mapped Arrow IPC**

The mmap reader is fast. Warm mmap rivals standalone RAM speed. Try the benchmarks.

```rust
use lightstream::models::readers::ipc::mmap_table::MmapTableReader;

let reader = MmapTableReader::open("data.arrow")?;
for i in 0..reader.num_batches() {
    let table = reader.read_batch(i)?;
    process(table);
}
```

### Python

**Read and writing**

```python
# Read the whole dataset.
table = ls.read("quotes.arrow").read_all()

# Stream batches without loading the complete dataset into memory.
for batch in ls.read("large.parquet"):
    process(batch)

# Write any Arrow-compatible object.
with ls.write("output.parquet", compression="zstd") as writer:
    writer.write(table)
```

**Hit your favourite tools**

Every reader implements the Arrow PyCapsule stream protocol, so Arrow-compatible libraries can consume a Lightstream reader directly.

```python
import duckdb
import lightstream as ls

reader = ls.read("quotes.arrow")

result = duckdb.sql(
    "SELECT SUM(qty) FROM reader"
)
```

**Protobuf and Arrow moving across process**

```python
reader = ls.read(
    "uds:///tmp/feed.sock",
    protocol="lightstream",
)

reader.register_table(
    "quotes",
    representative_table,
)
reader.register_message("health")

for frame in reader:
    if frame.is_table():
        on_quotes(frame.table)
    else:
        on_health(frame.payload)
```

## The problem

**TLDR**: *At the time of writing, there wasn't a (at least well-known) 'all-in-one' data transport that makes it effortless to send data and metadata at highly optimised speeds between services and processes, whilst maintaining a dual-sided typing contract compatible with the major data ecosystem.*

**Movement Friction**: Right now, moving Arrow data *(the common tabular interface standard)* between high-throughput services is not as easy as it could be.

You pick a transport such as gRPC, HTTP, TCP or WebSocket. If strongly typed metadata like Protobuf needs to accompany the Arrow data, you often end up writing both onto the wire as adjacent bytes to avoid additional serialisation overhead.

Spinning up a whole Apache Arrow Flight service *(which is otherwise a great and well-engineered system)* can also be infrastructure- and time-intensive. You need to understand the system and implement its architectural contract before getting started. It also only sends Arrow, so your strongly typed metadata contract is lost unless you like attaching strings to your tabular data instead *(which forfeits the dual-sided compilation contract)*.

**Tax**: You end up writing and maintaining your own semi-protocol anyway, to get what you need onto the wire and out the other side. Alternatively, you accept the risk that the typing contract on one side drifts from the other, causing bugs - highly common, when separate teams and/or agents maintain those services.

## The solution

1. Decouple encoding, reading, writing and transports, making them interchangeable.
2. Optimise and ship standard Arrow IPC readers/writers, plus Parquet.
3. Support Arrow (tabular data), Protobuf (metadata) and MessagePack (key/value data) on one connection.
4. Make the Lightstream protocol support globally ordered streams across parallel connections.
5. Make it fast by industry standards.
6. Build it in Rust and bind it in Python (to start).
7. Maintain 64-byte alignment for SIMD across all readers, writers and transports, as supported by Minarrow, without forfeiting the performance gains when reading or writing to disk or the wire.
8. Make it open source.

## The outcome

"Native streaming" becomes straightforward.

The same interface streams raw Arrow IPC over TCP, Unix domain sockets, WebSocket, HTTP, QUIC, WebTransport and standard I/O.

Benefits:

1. Effortlessly send Arrow, Protobuf and MessagePack over the wire via the open Lightstream protocol.
2. Swap between TCP, HTTP, WebSocket, WebTransport, QUIC, UDS and standard I/O trivially.
3. Use 64-byte-aligned Arrow/Parquet stream and file readers and writers, plus Arrow mmap.
4. Stream data batches across processes, or pipe typed data into the terminal *(for example, into Claude Code to monitor exceptions)*.
5. Extensible: implementing a new transport can take only a handful of lines of code.
6. Ergonomic: Tight, declarative syntax.

## Performance

Lightstream performed faster than the industry-standard alternative in every measured comparison on open AWS EC2 benchmarks (see `benchmarks/`). This is despite returning a single globally ordered stream after parallelising connections for delivery, which the other measured framework does not offer natively, and wearing that cost in the reported figures.

![Throughput across levels of core-based streaming parallelism against a variety of tabular workload shapes. Lightstream leads Arrow Flight in every combination.](assets/throughput-vs-parallelism.png)

Its p99 was within 1% of p50: consistent, low-jitter performance.

![Delivery steadiness. Lightstream's p99 sits within 1% of its p50 on every schema, with a tighter per-batch delivery-time tail than Arrow Flight.](assets/delivery-consistency.png)

The full warm-median throughput results across every workload shape and measured level of stream parallelism are included below for reference.

![Full benchmark results. Warm-median throughput in GiB/s of logical payload on a 50 Gbit/s network, with the Lightstream-to-Arrow-Flight ratio for each cell.](assets/full-results-table.png)

<details>
<summary><sub><b>Methodology</b></sub></summary>

<sub>

Both systems serve the same RAM-resident Apache Arrow table, so the measured path is transport and network delivery only. The open benchmark is in this repository.

- **Identical hardware** - two AWS i3en.12xlarge instances, one placement group, and 50 Gbit/s network.
- **Median of five** - every cell is the warm median of five runs, smoothing transient cloud variance.
- **Arrow Flight configuration** - the official arrow-flight crate over gRPC/HTTP/2, Arrow-RS defaults, string data not re-materialised, with multiple connections to avoid multiplex throttling.
- **Receiver-verified** - throughput is logical payload in GiB/s, timed from request to final verified arrival.
- **Four schema shapes** - numeric, mixed, string-heavy and wide. One million rows per batch, from a 300 GB pool (before transport batch framing).
- **Ordered reconstruction** - Lightstream reconstructs one globally ordered stream across all connections, which counts against its own throughput (it wears the cost in the results). Arrow Flight orders only within each endpoint and leaves parallel streams separate. Both therefore use their strongest ordering guarantees and stream configurations without stepping outside the framework boundaries.

</sub>

</details>

## User Guide

See the individual Rust or Python READMEs, or dive straight into the repository examples. A User Guide is under development.

## Like what you see?

Please consider leaving a star on the repository or sharing it. It helps others find it.

## Licence

Mozilla Public License 2.0. © 2025–2026 Peter Garfield Bower.

See [MPL 2.0 FAQ](https://www.mozilla.org/en-US/MPL/2.0/FAQ/) if you are unfamiliar with this open-source licence.

Maintained by **SpaceCell**. Check out the [latest data technology](https://spacecell.com).
