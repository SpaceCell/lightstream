//! WebTransport roundtrip integration test.
//!
//! Spins up a local WebTransport server, writes Arrow IPC tables from one task,
//! reads them back from another, and verifies the data survives the trip.

#![cfg(feature = "webtransport")]

use std::sync::Arc;

use futures_util::StreamExt;
use lightstream::enums::{BufferChunkSize, IPCMessageProtocol};
use lightstream::models::readers::ipc::table_reader::TableReader;
use lightstream::models::readers::webtransport::WebTransportTableReader;
use lightstream::models::streams::webtransport::WebTransportByteStream;
use lightstream::models::writers::webtransport::WebTransportTableWriter;
use lightstream::traits::transport_reader::TransportReader;
use lightstream::traits::transport_writer::TransportWriter;
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, FloatArray,
    IntegerArray, NumericArray, StringArray, Table, TextArray, Vec64,
    ffi::arrow_dtype::CategoricalIndexType,
};
use wtransport::tls::Sha256Digest;
use wtransport::{ClientConfig, Endpoint, Identity, ServerConfig};

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

/// Create a self-signed identity for testing.
///
/// Returns the identity and its certificate hash so the client can verify
/// the server using `with_server_certificate_hashes` — the standard
/// WebTransport certificate pinning mechanism.
fn make_test_identity() -> (Identity, Sha256Digest) {
    let identity = Identity::self_signed(["localhost", "127.0.0.1", "::1"]).unwrap();
    let hash = identity.certificate_chain().as_slice()[0].hash();
    (identity, hash)
}

/// Create a WebTransport server config with self-signed cert for testing.
fn make_server_config(identity: Identity, port: u16) -> ServerConfig {
    ServerConfig::builder()
        .with_bind_default(port)
        .with_identity(identity)
        .build()
}

/// Create a client config that validates the server certificate by hash.
fn make_client_config(server_cert_hash: Sha256Digest) -> ClientConfig {
    ClientConfig::builder()
        .with_bind_default()
        .with_server_certificate_hashes([server_cert_hash])
        .build()
}

/// Basic roundtrip: write one table over WebTransport, read it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_webtransport_single_table_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let (identity, cert_hash) = make_test_identity();
    let server_config = make_server_config(identity, 0);
    let server = Endpoint::server(server_config).unwrap();
    let addr = server.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let client_config = make_client_config(cert_hash);
        let client = Endpoint::client(client_config).unwrap();
        let conn = client
            .connect(format!("https://127.0.0.1:{}", addr.port()))
            .await
            .unwrap();

        let send = conn.open_uni().await.unwrap().await.unwrap();
        let mut writer = WebTransportTableWriter::new(send, write_schema).unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
        conn.closed().await;
    });

    let incoming = server.accept().await;
    let session_request = incoming.await.unwrap();
    let conn = session_request.accept().await.unwrap();
    let recv = conn.accept_uni().await.unwrap();

    let stream = WebTransportByteStream::new(recv, BufferChunkSize::WebTransport);
    let reader = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].n_rows, 4);
    assert_eq!(tables[0].cols.len(), 4);
}

/// Write multiple tables, read them all back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_webtransport_multi_table_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let (identity, cert_hash) = make_test_identity();
    let server_config = make_server_config(identity, 0);
    let server = Endpoint::server(server_config).unwrap();
    let addr = server.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let client_config = make_client_config(cert_hash);
        let client = Endpoint::client(client_config).unwrap();
        let conn = client
            .connect(format!("https://127.0.0.1:{}", addr.port()))
            .await
            .unwrap();

        let send = conn.open_uni().await.unwrap().await.unwrap();
        let mut writer = WebTransportTableWriter::new(send, write_schema).unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
        conn.closed().await;
    });

    let incoming = server.accept().await;
    let session_request = incoming.await.unwrap();
    let conn = session_request.accept().await.unwrap();
    let recv = conn.accept_uni().await.unwrap();

    let stream = WebTransportByteStream::new(recv, BufferChunkSize::WebTransport);
    let reader = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(tables.len(), 3);
    for t in &tables {
        assert_eq!(t.n_rows, 4);
        assert_eq!(t.cols.len(), 4);
    }
}

/// Use the high-level WebTransportTableReader with Stream trait for continuous reading.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_webtransport_stream_trait() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let (identity, cert_hash) = make_test_identity();
    let server_config = make_server_config(identity, 0);
    let server = Endpoint::server(server_config).unwrap();
    let addr = server.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let client_config = make_client_config(cert_hash);
        let client = Endpoint::client(client_config).unwrap();
        let conn = client
            .connect(format!("https://127.0.0.1:{}", addr.port()))
            .await
            .unwrap();

        let send = conn.open_uni().await.unwrap().await.unwrap();
        let mut writer = WebTransportTableWriter::new(send, write_schema).unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
        conn.closed().await;
    });

    let incoming = server.accept().await;
    let session_request = incoming.await.unwrap();
    let conn = session_request.accept().await.unwrap();
    let recv = conn.accept_uni().await.unwrap();

    let stream = WebTransportByteStream::new(recv, BufferChunkSize::WebTransport);
    let mut reader = WebTransportTableReader::from_stream(stream, IPCMessageProtocol::Stream);

    let mut count = 0;
    while let Some(result) = reader.next().await {
        let t = result.unwrap();
        assert_eq!(t.n_rows, 4);
        count += 1;
    }

    writer_handle.await.unwrap();
    assert_eq!(count, 2);
}

/// Collect multiple WebTransport batches into a SuperTable without re-allocation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_webtransport_read_to_super_table() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let (identity, cert_hash) = make_test_identity();
    let server_config = make_server_config(identity, 0);
    let server = Endpoint::server(server_config).unwrap();
    let addr = server.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let client_config = make_client_config(cert_hash);
        let client = Endpoint::client(client_config).unwrap();
        let conn = client
            .connect(format!("https://127.0.0.1:{}", addr.port()))
            .await
            .unwrap();

        let send = conn.open_uni().await.unwrap().await.unwrap();
        let mut writer = WebTransportTableWriter::new(send, write_schema).unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table.clone()).await.unwrap();
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
        conn.closed().await;
    });

    let incoming = server.accept().await;
    let session_request = incoming.await.unwrap();
    let conn = session_request.accept().await.unwrap();
    let recv = conn.accept_uni().await.unwrap();

    let stream = WebTransportByteStream::new(recv, BufferChunkSize::WebTransport);
    let reader = WebTransportTableReader::from_stream(stream, IPCMessageProtocol::Stream);
    let super_table = reader.read_to_super_table(Some("merged".into()), None).await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(super_table.n_rows, 8);
    assert_eq!(super_table.batches.len(), 2);
    assert_eq!(super_table.name, "merged");
    for batch in &super_table.batches {
        assert_eq!(batch.n_rows, 4);
        assert_eq!(batch.cols.len(), 4);
    }
}
