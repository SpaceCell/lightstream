//! UDS roundtrip integration test.
//!
//! Spins up a local Unix domain socket listener, writes Arrow IPC tables
//! from one task, reads them back from another, and verifies the data
//! survives the trip.

#![cfg(feature = "uds")]

use std::sync::Arc;

use futures_util::StreamExt;
use lightstream::enums::{BufferChunkSize, IPCMessageProtocol};
use lightstream::models::readers::ipc::table_reader::TableReader;
use lightstream::models::readers::uds::UdsTableReader;
use lightstream::models::streams::uds::UdsByteStream;
use lightstream::models::writers::uds::UdsTableWriter;
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, FloatArray,
    IntegerArray, NumericArray, StringArray, Table, TextArray, Vec64,
    ffi::arrow_dtype::CategoricalIndexType,
};
use tokio::net::UnixListener;

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

/// Basic roundtrip: write one table over UDS, read it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_uds_single_table_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("test.sock");

    let listener = UnixListener::bind(&sock_path).unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_sock = sock_path.clone();
    let writer_handle = tokio::spawn(async move {
        let mut writer = UdsTableWriter::connect(&writer_sock, write_schema).await.unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let (read_half, _write_half) = socket.into_split();
    let stream = UdsByteStream::from_read_half(read_half, BufferChunkSize::Http);
    let reader = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].n_rows, 4);
    assert_eq!(tables[0].cols.len(), 4);

    let t = &tables[0];

    // Verify integer column values
    match &t.cols[0].array {
        Array::NumericArray(NumericArray::Int32(arr)) => {
            assert_eq!(arr.data.as_slice(), &[10, 20, 30, 40]);
        }
        other => panic!("Expected Int32, got {:?}", other),
    }

    // Verify float column values
    match &t.cols[1].array {
        Array::NumericArray(NumericArray::Float64(arr)) => {
            assert_eq!(arr.data.as_slice(), &[1.1, 2.2, 3.3, 4.4]);
        }
        other => panic!("Expected Float64, got {:?}", other),
    }

    // Verify string column values
    match &t.cols[2].array {
        Array::TextArray(TextArray::String32(arr)) => {
            let strs: Vec<_> = arr.iter_str().collect();
            assert_eq!(strs, &["hello", "world", "test", ""]);
        }
        other => panic!("Expected String32, got {:?}", other),
    }

    // Verify categorical column values
    match &t.cols[3].array {
        Array::TextArray(TextArray::Categorical32(arr)) => {
            let cats: Vec<_> = arr.iter_str().collect();
            assert_eq!(cats, &["red", "green", "blue", "red"]);
        }
        other => panic!("Expected Categorical32, got {:?}", other),
    }
}

/// Write multiple tables, read them all back.
#[tokio::test]
async fn test_uds_multi_table_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("test.sock");

    let listener = UnixListener::bind(&sock_path).unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_sock = sock_path.clone();
    let writer_handle = tokio::spawn(async move {
        let mut writer = UdsTableWriter::connect(&writer_sock, write_schema).await.unwrap();
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

    let (socket, _) = listener.accept().await.unwrap();
    let (read_half, _write_half) = socket.into_split();
    let stream = UdsByteStream::from_read_half(read_half, BufferChunkSize::Http);
    let reader = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(tables.len(), 3);
    for t in &tables {
        assert_eq!(t.n_rows, 4);
        assert_eq!(t.cols.len(), 4);
    }
}

/// Use the high-level UdsTableReader with Stream trait for continuous reading.
#[tokio::test]
async fn test_uds_stream_trait() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("test.sock");

    let listener = UnixListener::bind(&sock_path).unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_sock = sock_path.clone();
    let writer_handle = tokio::spawn(async move {
        let mut writer = UdsTableWriter::connect(&writer_sock, write_schema).await.unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let (read_half, _write_half) = socket.into_split();
    let stream = UdsByteStream::from_read_half(read_half, BufferChunkSize::Http);
    let mut reader = UdsTableReader::from_stream(stream, IPCMessageProtocol::Stream);

    let mut count = 0;
    while let Some(result) = reader.next().await {
        let t = result.unwrap();
        assert_eq!(t.n_rows, 4);
        count += 1;
    }

    writer_handle.await.unwrap();
    assert_eq!(count, 2);
}

/// Combine multiple UDS batches into a single Table.
#[tokio::test]
async fn test_uds_combine_to_table() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let dir = tempfile::tempdir().unwrap();
    let sock_path = dir.path().join("test.sock");

    let listener = UnixListener::bind(&sock_path).unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_sock = sock_path.clone();
    let writer_handle = tokio::spawn(async move {
        let mut writer = UdsTableWriter::connect(&writer_sock, write_schema).await.unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
    });

    let (socket, _) = listener.accept().await.unwrap();
    let (read_half, _write_half) = socket.into_split();
    let stream = UdsByteStream::from_read_half(read_half, BufferChunkSize::Http);
    let reader = UdsTableReader::from_stream(stream, IPCMessageProtocol::Stream);
    let combined = reader.combine_to_table(Some("merged".into())).await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(combined.n_rows, 8);
    assert_eq!(combined.cols.len(), 4);
    assert_eq!(combined.name, "merged");
}
