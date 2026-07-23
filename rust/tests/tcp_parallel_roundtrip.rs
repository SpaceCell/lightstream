// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parallel TCP roundtrip integration test.
//!
//! Fans Arrow IPC tables across several concurrent TCP connections from a
//! `TcpParallelTableWriter` (client) into a `TcpParallelTableReader` (server),
//! and verifies round-robin distribution, within-connection ordering, that
//! every table arrives, and that `SortBehaviour::Ordered` recovers global
//! write order.

#![cfg(feature = "tcp")]

use lightstream::models::readers::parallel::tcp::TcpParallelTableReader;
use lightstream::models::writers::parallel::tcp::TcpParallelTableWriter;
use lightstream::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};
use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;
use minarrow::{arr_i32, Field, FieldArray, Table};
use tokio::net::TcpListener;

/// Single Int32 column carrying `marker`, used to track which table lands on
/// which connection and in what order.
fn make_marked_table(marker: i32) -> Table {
    let col = FieldArray::from_arr("marker", arr_i32![&[marker]]);
    Table::new("marked".to_string(), vec![col].into())
}

fn make_schema(table: &Table) -> Vec<Field> {
    table.schema().iter().map(|f| f.as_ref().clone()).collect()
}

/// Fan a handful of tables across the connections and verify every table
/// makes the trip with its shape intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_parallel_roundtrip() {
    const STREAMS: usize = 4;
    const TABLES: i32 = 12;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let reader = TcpParallelTableReader::accept(&listener, STREAMS, SortBehaviour::None, None)
            .await
            .unwrap();
        reader.read_all_tables().await.unwrap()
    });

    let schema = make_schema(&make_marked_table(0));
    let mut writer = TcpParallelTableWriter::connect(addr, STREAMS, schema, Vec::new(), None)
        .await
        .unwrap();
    for i in 0..TABLES {
        writer.write_table(make_marked_table(i)).await.unwrap();
    }
    writer.finish().await.unwrap();

    let tables = server.await.unwrap();
    assert_eq!(tables.len(), TABLES as usize);
    for (t, _) in &tables {
        assert_eq!(t.n_rows, 1);
        assert_eq!(t.cols.len(), 1);
    }
}

/// Table `i` routes to connection `i % STREAMS`, so markers sharing a residue
/// arrive in ascending order even though the merge interleaves connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_parallel_ordering_and_round_robin() {
    const STREAMS: usize = 4;
    const TABLES: i32 = 40;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let reader = TcpParallelTableReader::accept(&listener, STREAMS, SortBehaviour::None, None)
            .await
            .unwrap();
        reader.read_all_tables().await.unwrap()
    });

    let schema = make_schema(&make_marked_table(0));
    let mut writer = TcpParallelTableWriter::connect(addr, STREAMS, schema, Vec::new(), None)
        .await
        .unwrap();
    for i in 0..TABLES {
        writer.write_table(make_marked_table(i)).await.unwrap();
    }
    writer.finish().await.unwrap();

    let tables = server.await.unwrap();
    let markers: Vec<i32> = tables.iter().map(|(t, _)| t.cols[0].array.num().i32().data[0]).collect();
    assert_eq!(markers.len(), TABLES as usize);

    // Every marker arrives once.
    let mut sorted = markers.clone();
    sorted.sort();
    assert_eq!(sorted, (0..TABLES).collect::<Vec<_>>());

    // Markers sharing a residue mod STREAMS came down one connection, so they
    // must stay in ascending order.
    for residue in 0..STREAMS as i32 {
        let stream_markers: Vec<i32> =
            markers.iter().copied().filter(|m| m % STREAMS as i32 == residue).collect();
        let mut ascending = stream_markers.clone();
        ascending.sort();
        assert_eq!(stream_markers, ascending, "connection {residue} arrived out of order");
    }
}

/// An ordered writer paired with a `SortBehaviour::Ordered` reader emits tables
/// in global write order across the connections, even with uneven counts. With
/// 42 tables over 4 connections the connections hold 11/11/10/10 tables, so the
/// rotation must terminate on the short connections without dropping or
/// reordering. Each table's surfaced key equals its write index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tcp_parallel_ordered_uneven_streams() {
    const STREAMS: usize = 4;
    const TABLES: i32 = 42;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let reader = TcpParallelTableReader::accept(&listener, STREAMS, SortBehaviour::Ordered, None)
            .await
            .unwrap();
        reader.read_all_tables().await.unwrap()
    });

    let schema = make_schema(&make_marked_table(0));
    let mut writer =
        TcpParallelTableWriter::connect_ordered(addr, STREAMS, schema, Vec::new(), None)
            .await
            .unwrap();
    for i in 0..TABLES {
        writer.write_table(make_marked_table(i)).await.unwrap();
    }
    writer.finish().await.unwrap();

    let tables = server.await.unwrap();
    assert_eq!(tables.len(), TABLES as usize);
    // Ordered emits in global write order, and the surfaced key matches the
    // write index of each table.
    for (i, (table, seq)) in tables.iter().enumerate() {
        assert_eq!(table.cols[0].array.num().i32().data[0], i as i32, "table {i} arrived out of order");
        assert_eq!(*seq, Some(i as u64));
    }
}
