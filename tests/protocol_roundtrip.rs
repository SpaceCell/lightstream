//! Protocol roundtrip integration test.
//!
//! Spins up a local TCP listener, sends mixed Lightstream protocol frames
//! (messages and tables) from one task, reads them back from another, and
//! verifies the data survives the trip.

#![cfg(all(feature = "protocol", feature = "tcp"))]

use std::sync::Arc;

use futures_util::StreamExt;
use lightstream::enums::BufferChunkSize;
use lightstream::models::protocol::connection::TcpLightstreamConnection;
use lightstream::models::readers::lightstream::LightstreamReader;
use lightstream::models::streams::tcp::TcpByteStream;
use lightstream::models::writers::lightstream::LightstreamWriter;
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, FloatArray,
    IntegerArray, NumericArray, StringArray, Table, TextArray, Vec64,
    ffi::arrow_dtype::CategoricalIndexType,
};
use tokio::net::TcpListener;

fn make_schema() -> Vec<Field> {
    vec![
        Field {
            name: "ids".into(),
            dtype: ArrowType::Int32,
            nullable: false,
            metadata: Default::default(),
        },
        Field {
            name: "values".into(),
            dtype: ArrowType::Float64,
            nullable: false,
            metadata: Default::default(),
        },
        Field {
            name: "labels".into(),
            dtype: ArrowType::String,
            nullable: true,
            metadata: Default::default(),
        },
        Field {
            name: "category".into(),
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
            nullable: true,
            metadata: Default::default(),
        },
    ]
}

fn make_test_table() -> Table {
    let int_col = FieldArray::new(
        Field {
            name: "ids".into(),
            dtype: ArrowType::Int32,
            nullable: false,
            metadata: Default::default(),
        },
        Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(Vec64::from_slice(&[10, 20, 30, 40])),
            null_mask: None,
        }))),
    );

    let float_col = FieldArray::new(
        Field {
            name: "values".into(),
            dtype: ArrowType::Float64,
            nullable: false,
            metadata: Default::default(),
        },
        Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
            data: Buffer::from(Vec64::from_slice(&[1.1, 2.2, 3.3, 4.4])),
            null_mask: None,
        }))),
    );

    let str_col = FieldArray::new(
        Field {
            name: "labels".into(),
            dtype: ArrowType::String,
            nullable: true,
            metadata: Default::default(),
        },
        Array::TextArray(TextArray::String32(Arc::new(StringArray::new(
            Buffer::from(Vec64::from_slice("helloworldtest".as_bytes())),
            Some(Bitmask::new_set_all(4, true)),
            Buffer::from(Vec64::from_slice(&[0u32, 5, 10, 14, 14])),
        )))),
    );

    let dict_col = FieldArray::new(
        Field {
            name: "category".into(),
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
            nullable: true,
            metadata: Default::default(),
        },
        Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
            data: Buffer::from(Vec64::from_slice(&[0u32, 1, 2, 0])),
            unique_values: Vec64::from(vec![
                "red".to_string(),
                "green".to_string(),
                "blue".to_string(),
            ]),
            null_mask: Some(Bitmask::new_set_all(4, true)),
        }))),
    );

    Table {
        cols: vec![int_col, float_col, str_col, dict_col],
        n_rows: 4,
        name: "test_table".to_string(),
    }
}

/// Send a mix of messages and tables, verify roundtrip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_protocol_mixed_roundtrip() {
    let table = make_test_table();
    let schema = make_schema();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (_read, write) = stream.into_split();
        let mut writer = LightstreamWriter::<_, Vec<u8>>::new(write);
        writer.register_message("Ping");
        writer.register_table("Events", write_schema);

        // Send: message, table, message, table, table
        writer.send("Ping", b"hello").await.unwrap();
        writer.send_table("Events", &write_table).await.unwrap();
        writer.send("Ping", b"world").await.unwrap();
        writer.send_table("Events", &write_table).await.unwrap();
        writer.send_table("Events", &write_table).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let (read_half, _write_half) = socket.into_split();
    let stream = TcpByteStream::from_read_half(read_half, BufferChunkSize::Http);
    let mut reader = LightstreamReader::<_, Vec<u8>>::new(stream);
    reader.register_message("Ping");
    reader.register_table("Events", schema);

    // 1. Message: "hello"
    let msg = reader.next().await.unwrap().unwrap();
    assert!(msg.is_message());
    assert_eq!(msg.tag(), 0);
    assert_eq!(msg.payload().unwrap(), b"hello");

    // 2. Table
    let msg = reader.next().await.unwrap().unwrap();
    assert!(msg.is_table());
    assert_eq!(msg.tag(), 1);
    let tbl = msg.into_table().unwrap();
    assert_eq!(tbl.n_rows, 4);
    assert_eq!(tbl.cols.len(), 4);

    // 3. Message: "world"
    let msg = reader.next().await.unwrap().unwrap();
    assert!(msg.is_message());
    assert_eq!(msg.payload().unwrap(), b"world");

    // 4. Second table — uses persistent schema from first table
    let msg = reader.next().await.unwrap().unwrap();
    assert!(msg.is_table());
    let tbl = msg.into_table().unwrap();
    assert_eq!(tbl.n_rows, 4);
    assert_eq!(tbl.cols.len(), 4);

    // 5. Third table
    let msg = reader.next().await.unwrap().unwrap();
    assert!(msg.is_table());
    let tbl = msg.into_table().unwrap();
    assert_eq!(tbl.n_rows, 4);
    assert_eq!(tbl.cols.len(), 4);

    // Stream should end
    assert!(reader.next().await.is_none());

    writer_handle.await.unwrap();
}

/// Use the bidirectional TcpLightstreamConnection helper.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_protocol_connection_roundtrip() {
    let table = make_test_table();
    let schema = make_schema();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("Ack");
        conn.register_table("Data", write_schema);

        conn.send("Ack", b"ready").await.unwrap();
        conn.send_table("Data", &write_table).await.unwrap();
        conn.send_table("Data", &write_table).await.unwrap();
        conn.flush().await.unwrap();
        conn.shutdown().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let mut conn = TcpLightstreamConnection::from_tcp(socket);
    conn.register_message("Ack");
    conn.register_table("Data", schema);

    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_message());
    assert_eq!(msg.payload().unwrap(), b"ready");

    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_table());
    assert_eq!(msg.into_table().unwrap().n_rows, 4);

    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_table());
    assert_eq!(msg.into_table().unwrap().n_rows, 4);

    assert!(conn.recv().await.is_none());

    writer_handle.await.unwrap();
}

// ---------------------------------------------------------------------------
// Protobuf types — equivalent to what prost-build generates from a .proto file
// ---------------------------------------------------------------------------

/// Equivalent to:
/// ```protobuf
/// message TradeEvent {
///   uint64 id = 1;
///   string symbol = 2;
///   double price = 3;
///   int32 quantity = 4;
///   bool is_buy = 5;
/// }
/// ```
#[cfg(feature = "protobuf")]
#[derive(Clone, PartialEq, prost::Message)]
pub struct TradeEvent {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub symbol: String,
    #[prost(double, tag = "3")]
    pub price: f64,
    #[prost(int32, tag = "4")]
    pub quantity: i32,
    #[prost(bool, tag = "5")]
    pub is_buy: bool,
}

/// Equivalent to:
/// ```protobuf
/// message Heartbeat {
///   uint64 timestamp_ms = 1;
///   string node_id = 2;
/// }
/// ```
#[cfg(feature = "protobuf")]
#[derive(Clone, PartialEq, prost::Message)]
pub struct Heartbeat {
    #[prost(uint64, tag = "1")]
    pub timestamp_ms: u64,
    #[prost(string, tag = "2")]
    pub node_id: String,
}

// ---------------------------------------------------------------------------
// Protobuf integration tests
// ---------------------------------------------------------------------------

/// Send typed protobuf messages via send_proto, decode via decode_payload.
#[cfg(feature = "protobuf")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_protocol_protobuf_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let writer_handle = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("Trade");
        conn.register_message("Heartbeat");

        let trade = TradeEvent {
            id: 42,
            symbol: "AAPL".into(),
            price: 187.50,
            quantity: 100,
            is_buy: true,
        };
        conn.send_proto("Trade", &trade).await.unwrap();

        let hb = Heartbeat {
            timestamp_ms: 1_700_000_000_000,
            node_id: "node-7".into(),
        };
        conn.send_proto("Heartbeat", &hb).await.unwrap();

        // Send a second trade with different values
        let trade2 = TradeEvent {
            id: 43,
            symbol: "MSFT".into(),
            price: 412.10,
            quantity: 50,
            is_buy: false,
        };
        conn.send_proto("Trade", &trade2).await.unwrap();

        conn.flush().await.unwrap();
        conn.shutdown().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let mut conn = TcpLightstreamConnection::from_tcp(socket);
    conn.register_message("Trade");
    conn.register_message("Heartbeat");

    // 1. First trade — decode by reference
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_message());
    let trade: TradeEvent = msg.decode_payload().unwrap();
    assert_eq!(trade.id, 42);
    assert_eq!(trade.symbol, "AAPL");
    assert!((trade.price - 187.50).abs() < f64::EPSILON);
    assert_eq!(trade.quantity, 100);
    assert!(trade.is_buy);

    // 2. Heartbeat — decode by consuming
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_message());
    let hb: Heartbeat = msg.into_decoded_payload().unwrap();
    assert_eq!(hb.timestamp_ms, 1_700_000_000_000);
    assert_eq!(hb.node_id, "node-7");

    // 3. Second trade
    let msg = conn.recv().await.unwrap().unwrap();
    let trade2: TradeEvent = msg.decode_payload().unwrap();
    assert_eq!(trade2.id, 43);
    assert_eq!(trade2.symbol, "MSFT");
    assert!(!trade2.is_buy);

    assert!(conn.recv().await.is_none());
    writer_handle.await.unwrap();
}

/// Mix protobuf messages and Arrow tables on the same connection.
#[cfg(feature = "protobuf")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_protocol_protobuf_mixed_with_tables() {
    let table = make_test_table();
    let schema = make_schema();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("Trade");
        conn.register_table("Prices", write_schema);

        // Proto, table, proto, table
        let trade = TradeEvent {
            id: 1,
            symbol: "GOOG".into(),
            price: 140.25,
            quantity: 200,
            is_buy: true,
        };
        conn.send_proto("Trade", &trade).await.unwrap();
        conn.send_table("Prices", &write_table).await.unwrap();

        let trade2 = TradeEvent {
            id: 2,
            symbol: "AMZN".into(),
            price: 178.90,
            quantity: 75,
            is_buy: false,
        };
        conn.send_proto("Trade", &trade2).await.unwrap();
        conn.send_table("Prices", &write_table).await.unwrap();

        conn.flush().await.unwrap();
        conn.shutdown().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let mut conn = TcpLightstreamConnection::from_tcp(socket);
    conn.register_message("Trade");
    conn.register_table("Prices", schema);

    // 1. Proto
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_message());
    let trade: TradeEvent = msg.decode_payload().unwrap();
    assert_eq!(trade.symbol, "GOOG");

    // 2. Table
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_table());
    assert_eq!(msg.into_table().unwrap().n_rows, 4);

    // 3. Proto
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_message());
    let trade2: TradeEvent = msg.decode_payload().unwrap();
    assert_eq!(trade2.symbol, "AMZN");
    assert!(!trade2.is_buy);

    // 4. Table — second batch, reuses persistent schema
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_table());
    assert_eq!(msg.into_table().unwrap().n_rows, 4);

    assert!(conn.recv().await.is_none());
    writer_handle.await.unwrap();
}

/// Send only messages — no tables.
#[tokio::test]
async fn test_protocol_messages_only() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let writer_handle = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("Cmd");

        conn.send("Cmd", b"start").await.unwrap();
        conn.send("Cmd", b"stop").await.unwrap();
        conn.flush().await.unwrap();
        conn.shutdown().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let mut conn = TcpLightstreamConnection::from_tcp(socket);
    conn.register_message("Cmd");

    let msg = conn.recv().await.unwrap().unwrap();
    assert_eq!(msg.payload().unwrap(), b"start");

    let msg = conn.recv().await.unwrap().unwrap();
    assert_eq!(msg.payload().unwrap(), b"stop");

    assert!(conn.recv().await.is_none());
    writer_handle.await.unwrap();
}

// ---------------------------------------------------------------------------
// MessagePack types
// ---------------------------------------------------------------------------

#[cfg(feature = "msgpack")]
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
struct SensorReading {
    device_id: String,
    timestamp_ms: u64,
    temperature: f64,
    tags: Vec<String>,
    /// Binary blob — exercises BytesMode::ForceAll.
    raw_payload: Vec<u8>,
}

#[cfg(feature = "msgpack")]
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
struct NestedConfig {
    name: String,
    enabled: bool,
    thresholds: Vec<f64>,
    metadata: std::collections::HashMap<String, String>,
    inner: Option<InnerConfig>,
}

#[cfg(feature = "msgpack")]
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
struct InnerConfig {
    level: u32,
    label: String,
}

// ---------------------------------------------------------------------------
// MessagePack integration tests
// ---------------------------------------------------------------------------

/// Basic msgpack roundtrip with binary data.
#[cfg(feature = "msgpack")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_protocol_msgpack_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let writer_handle = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("Sensor");

        let reading = SensorReading {
            device_id: "thermo-42".into(),
            timestamp_ms: 1_700_000_000_000,
            temperature: 23.7,
            tags: vec!["lab".into(), "floor-3".into()],
            raw_payload: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF],
        };
        conn.send_msgpack("Sensor", &reading).await.unwrap();

        // Send a second reading with different values
        let reading2 = SensorReading {
            device_id: "thermo-43".into(),
            timestamp_ms: 1_700_000_001_000,
            temperature: 19.2,
            tags: vec!["warehouse".into()],
            raw_payload: vec![0x01, 0x02, 0x03],
        };
        conn.send_msgpack("Sensor", &reading2).await.unwrap();

        conn.flush().await.unwrap();
        conn.shutdown().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let mut conn = TcpLightstreamConnection::from_tcp(socket);
    conn.register_message("Sensor");

    // 1. First reading — decode by reference
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_message());
    let reading: SensorReading = msg.decode_msgpack().unwrap();
    assert_eq!(reading.device_id, "thermo-42");
    assert_eq!(reading.timestamp_ms, 1_700_000_000_000);
    assert!((reading.temperature - 23.7).abs() < f64::EPSILON);
    assert_eq!(reading.tags, vec!["lab", "floor-3"]);
    assert_eq!(
        reading.raw_payload,
        vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF]
    );

    // 2. Second reading — decode by consuming
    let msg = conn.recv().await.unwrap().unwrap();
    let reading2: SensorReading = msg.into_decoded_msgpack().unwrap();
    assert_eq!(reading2.device_id, "thermo-43");
    assert_eq!(reading2.raw_payload, vec![0x01, 0x02, 0x03]);

    assert!(conn.recv().await.is_none());
    writer_handle.await.unwrap();
}

/// Nested structs, Options, and HashMaps via msgpack.
#[cfg(feature = "msgpack")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_protocol_msgpack_nested_data() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let writer_handle = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("Config");

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("env".into(), "production".into());
        metadata.insert("region".into(), "ap-southeast-2".into());

        let config = NestedConfig {
            name: "pipeline-alpha".into(),
            enabled: true,
            thresholds: vec![0.5, 0.9, 0.99],
            metadata,
            inner: Some(InnerConfig {
                level: 3,
                label: "critical".into(),
            }),
        };
        conn.send_msgpack("Config", &config).await.unwrap();

        // Send one with None inner
        let config2 = NestedConfig {
            name: "pipeline-beta".into(),
            enabled: false,
            thresholds: vec![],
            metadata: std::collections::HashMap::new(),
            inner: None,
        };
        conn.send_msgpack("Config", &config2).await.unwrap();

        conn.flush().await.unwrap();
        conn.shutdown().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let mut conn = TcpLightstreamConnection::from_tcp(socket);
    conn.register_message("Config");

    // 1. Config with nested inner
    let msg = conn.recv().await.unwrap().unwrap();
    let config: NestedConfig = msg.decode_msgpack().unwrap();
    assert_eq!(config.name, "pipeline-alpha");
    assert!(config.enabled);
    assert_eq!(config.thresholds, vec![0.5, 0.9, 0.99]);
    assert_eq!(config.metadata.get("region").unwrap(), "ap-southeast-2");
    let inner = config.inner.unwrap();
    assert_eq!(inner.level, 3);
    assert_eq!(inner.label, "critical");

    // 2. Config with None inner
    let msg = conn.recv().await.unwrap().unwrap();
    let config2: NestedConfig = msg.decode_msgpack().unwrap();
    assert_eq!(config2.name, "pipeline-beta");
    assert!(!config2.enabled);
    assert!(config2.inner.is_none());

    assert!(conn.recv().await.is_none());
    writer_handle.await.unwrap();
}

/// Mix msgpack messages and Arrow tables on the same connection.
#[cfg(feature = "msgpack")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_protocol_msgpack_mixed_with_tables() {
    let table = make_test_table();
    let schema = make_schema();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut conn = TcpLightstreamConnection::from_tcp(stream);
        conn.register_message("Sensor");
        conn.register_table("Readings", write_schema);

        // Msgpack, table, msgpack, table
        let reading = SensorReading {
            device_id: "probe-1".into(),
            timestamp_ms: 1_000,
            temperature: 20.0,
            tags: vec!["test".into()],
            raw_payload: vec![0xFF],
        };
        conn.send_msgpack("Sensor", &reading).await.unwrap();
        conn.send_table("Readings", &write_table).await.unwrap();

        let reading2 = SensorReading {
            device_id: "probe-2".into(),
            timestamp_ms: 2_000,
            temperature: 25.0,
            tags: vec![],
            raw_payload: vec![],
        };
        conn.send_msgpack("Sensor", &reading2).await.unwrap();
        conn.send_table("Readings", &write_table).await.unwrap();

        conn.flush().await.unwrap();
        conn.shutdown().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let mut conn = TcpLightstreamConnection::from_tcp(socket);
    conn.register_message("Sensor");
    conn.register_table("Readings", schema);

    // 1. Msgpack
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_message());
    let reading: SensorReading = msg.decode_msgpack().unwrap();
    assert_eq!(reading.device_id, "probe-1");

    // 2. Table
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_table());
    assert_eq!(msg.into_table().unwrap().n_rows, 4);

    // 3. Msgpack
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_message());
    let reading2: SensorReading = msg.decode_msgpack().unwrap();
    assert_eq!(reading2.device_id, "probe-2");
    assert!(reading2.raw_payload.is_empty());

    // 4. Table — second batch, reuses persistent schema
    let msg = conn.recv().await.unwrap().unwrap();
    assert!(msg.is_table());
    assert_eq!(msg.into_table().unwrap().n_rows, 4);

    assert!(conn.recv().await.is_none());
    writer_handle.await.unwrap();
}

/// Verify BytesMode::ForceAll produces compact binary encoding.
#[cfg(feature = "msgpack")]
#[tokio::test]
async fn test_protocol_msgpack_binary_efficiency() {
    // A struct with a large binary payload should be encoded compactly.
    // With BytesMode::ForceAll, a 256-byte Vec<u8> takes ~261 bytes (5-byte bin header + data).
    // Without it, serde treats each byte as an array element: ~512+ bytes.
    let reading = SensorReading {
        device_id: "x".into(),
        timestamp_ms: 0,
        temperature: 0.0,
        tags: vec![],
        raw_payload: vec![0xAB; 256],
    };

    // Replicate the encoding logic from the writer to verify size
    let mut buf = Vec::new();
    let mut serializer =
        rmp_serde::Serializer::new(&mut buf).with_bytes(rmp_serde::config::BytesMode::ForceAll);
    serde::Serialize::serialize(&reading, &mut serializer).unwrap();

    // The binary payload is 256 bytes. With bin format: 1 byte type + 2 bytes len + 256 data = 259.
    // Total message should be well under 300 bytes (fields + 259 for the blob).
    assert!(
        buf.len() < 300,
        "encoded size {} is too large; BytesMode::ForceAll may not be active",
        buf.len()
    );

    // Verify roundtrip through rmp_serde::from_slice
    let decoded: SensorReading = rmp_serde::from_slice(&buf).unwrap();
    assert_eq!(decoded.raw_payload, vec![0xAB; 256]);
}
