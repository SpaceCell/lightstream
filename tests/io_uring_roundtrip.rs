// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integration test for io_uring-based UDS transport via tokio-uring.
//!
//! Mirrors the protocol_roundtrip tests but uses IoUringUdsConnection
//! with the tokio-uring runtime.

#![cfg(all(feature = "protocol", feature = "io_uring"))]

use std::sync::Arc;

use lightstream::models::io_uring::IoUringUdsConnection;
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, FloatArray,
    IntegerArray, NumericArray, StringArray, Table, TextArray, Vec64,
    ffi::arrow_dtype::CategoricalIndexType,
};

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
            #[cfg(not(feature = "default_categorical_8"))]
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
            #[cfg(feature = "default_categorical_8")]
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt8),
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

    #[cfg(not(feature = "default_categorical_8"))]
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
    #[cfg(feature = "default_categorical_8")]
    let dict_col = FieldArray::new(
        Field {
            name: "category".into(),
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt8),
            nullable: true,
            metadata: Default::default(),
        },
        Array::TextArray(TextArray::Categorical8(Arc::new(CategoricalArray {
            data: Buffer::from(Vec64::from_slice(&[0u8, 1, 2, 0])),
            unique_values: Vec64::from(vec![
                "red".to_string(),
                "green".to_string(),
                "blue".to_string(),
            ]),
            null_mask: Some(Bitmask::new_set_all(4, true)),
        }))),
    );

    Table::new(
        "test_table".to_string(),
        Some(vec![int_col, float_col, str_col, dict_col]),
    )
}

/// Create a tokio-uring UnixStream pair using std sockets.
/// Works around tokio-uring's UnixListener::bind bug (SO_REUSEPORT on AF_UNIX).
fn make_uring_socketpair() -> (tokio_uring::net::UnixStream, tokio_uring::net::UnixStream) {
    let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
    a.set_nonblocking(true).unwrap();
    b.set_nonblocking(true).unwrap();
    (
        tokio_uring::net::UnixStream::from_std(a),
        tokio_uring::net::UnixStream::from_std(b),
    )
}

/// Send messages and tables via io_uring UDS connection, verify roundtrip.
#[test]
fn test_io_uring_connection_roundtrip() {
    let table = make_test_table();
    let schema = make_schema();

    tokio_uring::start(async move {
        let (stream_a, stream_b) = make_uring_socketpair();

        let write_table = table.clone();
        let write_schema = schema.clone();
        let writer_handle = tokio_uring::spawn(async move {
            let mut conn = IoUringUdsConnection::new(stream_a, None);
            conn.register_message("Ack");
            conn.register_table("Data", write_schema);

            conn.send("Ack", b"ready").await.unwrap();
            conn.send_table("Data", write_table.clone()).await.unwrap();
            conn.send_table("Data", write_table.clone()).await.unwrap();
            conn.flush().await.unwrap();
            conn.shutdown().await.unwrap();
        });

        let mut conn = IoUringUdsConnection::new(stream_b, None);
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
    });
}

/// Send only messages to verify the message path works.
#[test]
fn test_io_uring_messages_only() {
    tokio_uring::start(async move {
        let (stream_a, stream_b) = make_uring_socketpair();

        let writer_handle = tokio_uring::spawn(async move {
            let mut conn = IoUringUdsConnection::new(stream_a, None);
            conn.register_message("Cmd");

            conn.send("Cmd", b"start").await.unwrap();
            conn.send("Cmd", b"stop").await.unwrap();
            conn.flush().await.unwrap();
            conn.shutdown().await.unwrap();
        });

        let mut conn = IoUringUdsConnection::new(stream_b, None);
        conn.register_message("Cmd");

        let msg = conn.recv().await.unwrap().unwrap();
        assert_eq!(msg.payload().unwrap(), b"start");

        let msg = conn.recv().await.unwrap().unwrap();
        assert_eq!(msg.payload().unwrap(), b"stop");

        assert!(conn.recv().await.is_none());

        writer_handle.await.unwrap();
    });
}

/// Multiple batches to verify persistent schema state.
#[test]
fn test_io_uring_multi_batch() {
    let table = make_test_table();
    let schema = make_schema();

    tokio_uring::start(async move {
        let (stream_a, stream_b) = make_uring_socketpair();

        let write_table = table.clone();
        let write_schema = schema.clone();
        let n_batches = 10;

        let writer_handle = tokio_uring::spawn(async move {
            let mut conn = IoUringUdsConnection::new(stream_a, None);
            conn.register_table("Data", write_schema);

            for _ in 0..n_batches {
                conn.send_table("Data", write_table.clone()).await.unwrap();
            }
            conn.flush().await.unwrap();
            conn.shutdown().await.unwrap();
        });

        let mut conn = IoUringUdsConnection::new(stream_b, None);
        conn.register_table("Data", schema);

        for _ in 0..n_batches {
            let msg = conn.recv().await.unwrap().unwrap();
            assert!(msg.is_table());
            assert_eq!(msg.into_table().unwrap().n_rows, 4);
        }

        assert!(conn.recv().await.is_none());

        writer_handle.await.unwrap();
    });
}
