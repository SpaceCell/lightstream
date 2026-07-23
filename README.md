# Lightstream

**Send and receive typed Arrow and Protobuf data easily across the network and processes at speed.**

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

## The problem

**TLDR**: *At the time of writing, there is no (at least known to the author) 'all-in-one' data transport that makes it effortless to send data and metadata at highly optimised speeds between services and processes whilst maintaining a dual-sided typing contract, that is compatible with the major data ecosystem.*

**Engineer's woe**: Right now, if you want to move Arrow data *(the common tabular interface standard)* between high-throughput, fast services, it not as easy as it could be. 

You pick a transport such as gRPC, HTTP, TCP, Websocket, and then if you want strongly typed metadata like Protobuf to accompany the Arrow data, one needs to manually write it onto the wire as bytes next to each other, in order to avoid additional serialisation overhead. 

Also, spinning up a whole Apache Arrow Flight service *(which is otherwise a great and well-engineered system)* can be infrastructure and time intensive, because you need to invest in understanding how the system works and implementing its architectural contract before you can get started. Then, it only sends Arrow - so your strongly typed metadata contract is lost unless you like attaching strings to your tabular data instead *(which forfeits the dual-sided compilation contract)*.

**Tax**: You end up writing (and maintaining) your own semi-protocol anyway, to get what you need on the wire and out the other side. Or, alternatively, you accept the inevitable risk that the typing contract of one side may drift from the other causing bugs, which is, in the author's view, highly likely when separate team(s) and/or agents maintain those services.

## The solution

1. De-couple encoding, reading, writing, transports, and make them interchangeable.
2. Optimise and ship standard Arrow IPC readers/writers + Parquet.
3. Support Arrow (Tabular data), Protobuf (Metadata), and MessagePack (Key/Value data extra) on the one connection.
4. Make the "Lightstream protocol" support globally ordered streams across parallel connections. 
3. Make it fast by industry standards.
4. Build it in Rust and bind it in Python (to start).
5. Ensure all readers, writers and transports maintain 64-byte alignment for SIMD, as supported by Minarrow, so that this extra layer of compounding parallelism for calculations is not forfeited when reading/writing to/from disk or the wire, and make sure this does not detract from the improved performance.
6. Make it open source.

## The outcome

"Native streaming" becomes easy as hell.

The same interface streams raw Arrow IPC over TCP, Unix domain sockets, WebSocket, HTTP, QUIC, WebTransport and standard I/O.

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

Capabilities:
1. Lightstream: Effortlessly send Arrow, Protobuf, MessagePack on the wire via one custom open Lightstream protocol.
2. Transport independence - interchange TCP, HTTP, Websocket, Webtransport, QUIC, UDS, and Stdio trivially
3. Network + Disk-IO: 64-byte aligned Arrow/Parquet stream and file readers and writers + Arrow mmap.
4. Stream data batches across processes, or pipe typed data into the terminal (for e.g., into Claude Code to monitor exceptions)

Plus more.

Side-benefits:
- implementing a new transport can be done in a handful of lines of code 
- tight, declarative syntax helps eliminate friction.

## Performance

Lightstream performed faster than the industry standard alternative in every measured comparison, on open AWS EC2 benchmarks (see `benchmarks/`). This is despite returning a single globally ordered stream after parallelising connections for delivery, which is not offered natively within the other measured framework, and wearing that cost in the reported figures.

![Throughput across levels of core-based streaming parallelism against a variety of tabular workload shapes. Lightstream leads Arrow Flight in every combination.](assets/throughput-vs-parallelism.png)

Its p99 was within 1% of p50 - consistent, low-jitter performance.

![Delivery steadiness. Lightstream's p99 sits within 1% of its p50 on every schema, with a tighter per-batch delivery-time tail than Arrow Flight.](assets/delivery-consistency.png)

The full (warm-median of 5 runs) throughput numbers, across every workload shape and measured level of stream parallelism, are included below for reference.

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
- **Ordered reconstruction** - Lightstream reconstructs one globally ordered stream across all connections, which counts against its own throughput (it wears the cost in the results). Arrow Flight orders only within each endpoint and leaves parallel streams separate. Both therefore provide their strongest ordering guarantees and stream configurations present without stepping outside of the framework boundaries.

</sub>

</details>

## Quick Start

### Rust 

**Receiver**

```rust
use futures_util::StreamExt;
use lightstream::models::readers::tcp::TcpTableReader;

let mut reader = TcpTableReader::connect("127.0.0.1:9000").await?;

while let Some(result) = reader.next().await {
    let table = result?;
    process(table);
}
```

**Sender**

```rust
use lightstream::models::writers::tcp::TcpTableWriter;

let mut writer = TcpTableWriter::connect("127.0.0.1:9000", schema, None).await?;
writer.write_table(batch_1).await?;
writer.finish().await?;
```

**Lightstream protocol** (multiplex protobuf messages and arrow tables)

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

The mmap reader is very fast. Warm mmap rivals standalone RAM speed, as it gets out the way.
Try the benchmark.

```rust
use lightstream::models::readers::ipc::mmap_table::MmapTableReader;

let reader = MmapTableReader::open("data.arrow")?;
for i in 0..reader.num_batches() {
    let table = reader.read_batch(i)?;
    process(table);
}
```

### Python

**Reading and writing**
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

Every reader implements the Arrow PyCapsule stream protocol. Arrow-compatible libraries therefore consume a Lightstream reader no problem.

```python
import duckdb
import lightstream as ls

reader = ls.read("quotes.arrow")

result = duckdb.sql(
    "SELECT SUM(qty) FROM reader"
)
```

**Protobuf and Arrow riding cross-process**

No more boundaries.

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

## User Guide

See the individual Rust or Python READMEs, or instead dive straight into the many repository examples.
 
A User Guide is under development. 

## Like what you see?

Please consider leaving a star on the repository, or sharing it, as it helps others find it.

## Licence

Mozilla Public License 2.0. © 2025–2026 Peter Garfield Bower.

See [MPL 2.0 FAQ](https://www.mozilla.org/en-US/MPL/2.0/FAQ/) if you are unfamiliar with this open-source license.

Maintained by **SpaceCell**. Check out the [latest data technology](https://spacecell.com).