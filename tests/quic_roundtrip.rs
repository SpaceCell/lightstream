//! QUIC roundtrip integration test.
//!
//! Spins up a local QUIC endpoint, writes Arrow IPC tables from one task,
//! reads them back from another, and verifies the data survives the trip.

#![cfg(feature = "quic")]

use std::sync::Arc;

use futures_util::StreamExt;
use lightstream::enums::{BufferChunkSize, IPCMessageProtocol};
use lightstream::models::readers::ipc::table_reader::TableReader;
use lightstream::models::readers::quic::QuicTableReader;
use lightstream::models::streams::quic::QuicByteStream;
use lightstream::models::writers::quic::QuicTableWriter;
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, FloatArray,
    IntegerArray, NumericArray, StringArray, Table, TextArray, Vec64,
    ffi::arrow_dtype::CategoricalIndexType,
};

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

/// Create a self-signed TLS server config for testing.
fn make_server_config() -> quinn::ServerConfig {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
    let key_der = rustls::pki_types::PrivateKeyDer::try_from(cert.key_pair.serialize_der()).unwrap();

    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .unwrap();
    server_crypto.alpn_protocols = vec![b"ls".to_vec()];

    quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto).unwrap(),
    ))
}

/// Create a client config that skips certificate verification for local testing.
fn make_client_config() -> quinn::ClientConfig {
    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipVerification))
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"ls".to_vec()];

    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(std::time::Duration::from_secs(10).try_into().unwrap()));

    let mut client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));
    client_config.transport_config(Arc::new(transport));
    client_config
}

/// Certificate verifier that accepts any certificate, for test use only.
#[derive(Debug)]
struct SkipVerification;

impl rustls::client::danger::ServerCertVerifier for SkipVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

/// Basic roundtrip: write one table over QUIC, read it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_quic_single_table_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let server_config = make_server_config();
    let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let client_endpoint =
            quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        let conn = client_endpoint
            .connect_with(make_client_config(), addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let send = conn.open_uni().await.unwrap();
        let mut writer = QuicTableWriter::new(send, write_schema).unwrap();
        writer.register_dictionary(3, vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);
        writer.write_table(write_table).await.unwrap();
        writer.finish().await.unwrap();
        conn.closed().await;
    });

    let conn = endpoint.accept().await.unwrap().await.unwrap();
    let recv = conn.accept_uni().await.unwrap();
    let stream = QuicByteStream::new(recv, BufferChunkSize::WebTransport);
    let reader = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].n_rows, 4);
    assert_eq!(tables[0].cols.len(), 4);
}

/// Write multiple tables, read them all back.
#[tokio::test]
async fn test_quic_multi_table_roundtrip() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let server_config = make_server_config();
    let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let client_endpoint =
            quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        let conn = client_endpoint
            .connect_with(make_client_config(), addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let send = conn.open_uni().await.unwrap();
        let mut writer = QuicTableWriter::new(send, write_schema).unwrap();
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

    let conn = endpoint.accept().await.unwrap().await.unwrap();
    let recv = conn.accept_uni().await.unwrap();
    let stream = QuicByteStream::new(recv, BufferChunkSize::WebTransport);
    let reader = TableReader::new(stream, 64 * 1024, IPCMessageProtocol::Stream);
    let tables = reader.read_all_tables().await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(tables.len(), 3);
    for t in &tables {
        assert_eq!(t.n_rows, 4);
        assert_eq!(t.cols.len(), 4);
    }
}

/// Use the high-level QuicTableReader with Stream trait for continuous reading.
#[tokio::test]
async fn test_quic_stream_trait() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let server_config = make_server_config();
    let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let client_endpoint =
            quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        let conn = client_endpoint
            .connect_with(make_client_config(), addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let send = conn.open_uni().await.unwrap();
        let mut writer = QuicTableWriter::new(send, write_schema).unwrap();
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

    let conn = endpoint.accept().await.unwrap().await.unwrap();
    let recv = conn.accept_uni().await.unwrap();
    let stream = QuicByteStream::new(recv, BufferChunkSize::WebTransport);
    let mut reader = QuicTableReader::from_stream(stream, IPCMessageProtocol::Stream);

    let mut count = 0;
    while let Some(result) = reader.next().await {
        let t = result.unwrap();
        assert_eq!(t.n_rows, 4);
        count += 1;
    }

    writer_handle.await.unwrap();
    assert_eq!(count, 2);
}

/// Combine multiple QUIC batches into a single Table.
#[tokio::test]
async fn test_quic_combine_to_table() {
    let table = make_test_table();
    let schema = make_schema(&table);

    let server_config = make_server_config();
    let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();

    let write_table = table.clone();
    let write_schema = schema.clone();
    let writer_handle = tokio::spawn(async move {
        let client_endpoint =
            quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        let conn = client_endpoint
            .connect_with(make_client_config(), addr, "localhost")
            .unwrap()
            .await
            .unwrap();
        let send = conn.open_uni().await.unwrap();
        let mut writer = QuicTableWriter::new(send, write_schema).unwrap();
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

    let conn = endpoint.accept().await.unwrap().await.unwrap();
    let recv = conn.accept_uni().await.unwrap();
    let stream = QuicByteStream::new(recv, BufferChunkSize::WebTransport);
    let reader = QuicTableReader::from_stream(stream, IPCMessageProtocol::Stream);
    let combined = reader.combine_to_table(Some("merged".into())).await.unwrap();

    writer_handle.await.unwrap();

    assert_eq!(combined.n_rows, 8);
    assert_eq!(combined.cols.len(), 4);
    assert_eq!(combined.name, "merged");
}
