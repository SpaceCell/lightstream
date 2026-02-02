//! WebSocket roundtrip integration tests.
//!
//! Spins up a local TCP listener, upgrades to WebSocket, writes Arrow IPC
//! tables from a client task, reads them back on the server side, and
//! verifies the data survives the trip.

#![cfg(feature = "websocket")]

use std::sync::Arc;

use futures_util::StreamExt;
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::table_reader::TableReader;
use lightstream::models::streams::websocket::WebSocketByteStream;
use lightstream::models::writers::websocket::WebSocketTableWriter;
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, FloatArray,
    IntegerArray, NumericArray, StringArray, Table, TextArray, Vec64,
    ffi::arrow_dtype::CategoricalIndexType,
};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

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

fn make_schema(table: &Table) -> Vec<Field> {
    table
        .cols
        .iter()
        .map(|fa| fa.field.as_ref().clone())
        .collect()
}

/// Accept a TCP connection and upgrade it to a WebSocket, returning the
/// read half wrapped as a `WebSocketByteStream` for Arrow IPC decoding.
async fn accept_ws_reader(
    listener: &TcpListener,
) -> WebSocketByteStream<
    futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    >,
> {
    let (socket, _) = listener.accept().await.unwrap();
    let ws = accept_async(socket).await.unwrap();
    let (_, read_half) = futures_util::StreamExt::split(ws);
    WebSocketByteStream::new(read_half)
}

/// Basic roundtrip: write one table over WebSocket, read it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ws_single_table_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("ws://{addr}");

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let mut writer = WebSocketTableWriter::connect(&url, write_schema)
            .await
            .unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
    });

    let byte_stream = accept_ws_reader(&listener).await;
    let reader = TableReader::new(byte_stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].n_rows, 4);
    assert_eq!(tables[0].cols.len(), 4);
}

/// Write multiple tables, read them all back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ws_multi_table_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("ws://{addr}");

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let mut writer = WebSocketTableWriter::connect(&url, write_schema)
            .await
            .unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
    });

    let byte_stream = accept_ws_reader(&listener).await;
    let reader = TableReader::new(byte_stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(tables.len(), 3);
    for t in &tables {
        assert_eq!(t.n_rows, 4);
        assert_eq!(t.cols.len(), 4);
    }
}

/// Use the Stream trait for continuous reading over WebSocket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ws_stream_trait() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("ws://{addr}");

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let mut writer = WebSocketTableWriter::connect(&url, write_schema)
            .await
            .unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
    });

    let byte_stream = accept_ws_reader(&listener).await;
    let mut reader = TableReader::new(byte_stream, 64 * 1024, IPCMessageProtocol::Stream);

    let mut count = 0;
    while let Some(result) = reader.next().await {
        let t = result.unwrap();
        assert_eq!(t.n_rows, 4);
        count += 1;
    }

    writer_handle.await.unwrap();
    assert_eq!(count, 2);
}

/// Combine multiple WebSocket batches into a single Table.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ws_combine_to_table() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("ws://{addr}");

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let mut writer = WebSocketTableWriter::connect(&url, write_schema)
            .await
            .unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
    });

    let byte_stream = accept_ws_reader(&listener).await;
    let reader = TableReader::new(byte_stream, 64 * 1024, IPCMessageProtocol::Stream);
    let combined = reader.combine_to_table(Some("merged".into())).await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(combined.n_rows, 8);
    assert_eq!(combined.cols.len(), 4);
    assert_eq!(combined.name, "merged");
}
