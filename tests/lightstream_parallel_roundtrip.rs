// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parallel Lightstream protocol roundtrip integration test.
//!
//! Fans Arrow tables across several concurrent Lightstream protocol
//! connections from a `LightstreamParallelWriter` (client) into a
//! `LightstreamParallelReader` (server), and verifies round-robin
//! distribution, within-connection ordering, that every table arrives, and
//! that `SortBehaviour::Ordered` recovers global send order.

#![cfg(all(feature = "protocol", feature = "tcp"))]

use lightstream::models::readers::parallel::lightstream::LightstreamParallelReader;
use lightstream::models::writers::parallel::lightstream::LightstreamParallelWriter;
use lightstream::traits::parallel_transport_reader::SortBehaviour;
use minarrow::{arr_i32, Field, FieldArray, Table};
use tokio::net::TcpListener;

/// Registered type name for the marker table on every connection.
const TYPE_NAME: &str = "marked";

/// Single Int32 column carrying `marker`, used to track which table lands on
/// which connection and in what order.
fn make_marked_table(marker: i32) -> Table {
    let col = FieldArray::from_arr("marker", arr_i32![&[marker]]);
    Table::new("marked".to_string(), vec![col].into())
}

fn make_schema(table: &Table) -> Vec<Field> {
    table.schema().iter().map(|f| f.as_ref().clone()).collect()
}

/// Fan a handful of tables across the connections and verify every table makes
/// the trip with its shape intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lightstream_parallel_roundtrip() {
    const STREAMS: usize = 4;
    const TABLES: i32 = 12;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let schema = make_schema(&make_marked_table(0));

    let server_schema = schema.clone();
    let server = tokio::spawn(async move {
        let table_types = [(TYPE_NAME, server_schema)];
        let reader =
            LightstreamParallelReader::accept(&listener, STREAMS, &[], &table_types, SortBehaviour::None, None)
                .await
                .unwrap();
        reader.read_all().await.unwrap()
    });

    let table_types = [(TYPE_NAME, schema)];
    let mut writer = LightstreamParallelWriter::connect(addr, STREAMS, &[], &table_types)
        .await
        .unwrap();
    for i in 0..TABLES {
        writer.send_table(TYPE_NAME, make_marked_table(i)).await.unwrap();
    }
    writer.finish().await.unwrap();

    let tables: Vec<Table> =
        server.await.unwrap().into_iter().filter_map(|m| m.into_table()).collect();
    assert_eq!(tables.len(), TABLES as usize);
    for t in &tables {
        assert_eq!(t.n_rows, 1);
        assert_eq!(t.cols.len(), 1);
    }
}

/// Table `i` routes to connection `i % STREAMS`, so markers sharing a residue
/// arrive in ascending order even though the merge interleaves connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lightstream_parallel_ordering_and_round_robin() {
    const STREAMS: usize = 4;
    const TABLES: i32 = 40;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let schema = make_schema(&make_marked_table(0));

    let server_schema = schema.clone();
    let server = tokio::spawn(async move {
        let table_types = [(TYPE_NAME, server_schema)];
        let reader =
            LightstreamParallelReader::accept(&listener, STREAMS, &[], &table_types, SortBehaviour::None, None)
                .await
                .unwrap();
        reader.read_all().await.unwrap()
    });

    let table_types = [(TYPE_NAME, schema)];
    let mut writer = LightstreamParallelWriter::connect(addr, STREAMS, &[], &table_types)
        .await
        .unwrap();
    for i in 0..TABLES {
        writer.send_table(TYPE_NAME, make_marked_table(i)).await.unwrap();
    }
    writer.finish().await.unwrap();

    let tables: Vec<Table> =
        server.await.unwrap().into_iter().filter_map(|m| m.into_table()).collect();
    let markers: Vec<i32> = tables.iter().map(|t| t.cols[0].array.num().i32().data[0]).collect();
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

/// An `Ordered` reader emits frames in global send order across the
/// connections, even with uneven counts. With 42 tables over 4 connections the
/// connections hold 11/11/10/10 tables, so the rotation must terminate on the
/// short connections without dropping or reordering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_lightstream_parallel_ordered_uneven_streams() {
    const STREAMS: usize = 4;
    const TABLES: i32 = 42;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let schema = make_schema(&make_marked_table(0));

    let server_schema = schema.clone();
    let server = tokio::spawn(async move {
        let table_types = [(TYPE_NAME, server_schema)];
        let reader = LightstreamParallelReader::accept(
            &listener,
            STREAMS,
            &[],
            &table_types,
            SortBehaviour::Ordered,
            None,
        )
        .await
        .unwrap();
        reader.read_all().await.unwrap()
    });

    let table_types = [(TYPE_NAME, schema)];
    let mut writer = LightstreamParallelWriter::connect(addr, STREAMS, &[], &table_types)
        .await
        .unwrap();
    for i in 0..TABLES {
        writer.send_table(TYPE_NAME, make_marked_table(i)).await.unwrap();
    }
    writer.finish().await.unwrap();

    let tables: Vec<Table> =
        server.await.unwrap().into_iter().filter_map(|m| m.into_table()).collect();
    assert_eq!(tables.len(), TABLES as usize);
    // Ordered emits in global send order.
    for (i, table) in tables.iter().enumerate() {
        assert_eq!(table.cols[0].array.num().i32().data[0], i as i32, "table {i} arrived out of order");
    }
}
