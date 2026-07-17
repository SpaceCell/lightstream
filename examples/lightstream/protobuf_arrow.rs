// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Protobuf messages and Arrow tables sharing one wire over the parallel
//! Lightstream protocol.
//!
//! A client opens several concurrent protocol connections, registers a protobuf
//! `Subscribe` message type and an Arrow `OrderBook` table type, then
//! interleaves a control message with a run of data batches. The server merges
//! the connections in global send order under `SortBehaviour::Ordered` and
//! routes each frame by type - protobuf for control, Arrow for data.
//!
//! Each connection is decoded on its own task, so the connections run in
//! parallel across the runtime's worker threads.
//!
//! Run with:
//!
//! ```text
//! cargo run --example protobuf_arrow_lightstream --features "protocol,tcp,protobuf"
//! ```

use lightstream::models::readers::parallel::lightstream::LightstreamParallelReader;
use lightstream::models::writers::parallel::lightstream::LightstreamParallelWriter;
use lightstream::traits::parallel_transport_reader::SortBehaviour;
use minarrow::{fa_f64, fa_i32, fa_i64, ArrowType, Field, Table};
use tokio::net::TcpListener;

/// Control-plane message, encoded as protobuf via prost.
#[derive(Clone, PartialEq, prost::Message)]
struct Subscribe {
    #[prost(string, tag = "1")]
    symbol: String,
    #[prost(uint32, tag = "2")]
    interval_ms: u32,
}

/// Schema for the data-plane `OrderBook` table.
fn order_book_schema() -> Vec<Field> {
    vec![
        Field {
            name: "price".into(),
            dtype: ArrowType::Float64,
            nullable: false,
            metadata: Default::default(),
        },
        Field {
            name: "quantity".into(),
            dtype: ArrowType::Int64,
            nullable: false,
            metadata: Default::default(),
        },
        Field {
            name: "side".into(),
            dtype: ArrowType::Int32,
            nullable: false,
            metadata: Default::default(),
        },
    ]
}

/// Build a small `OrderBook` batch.
fn make_order_book() -> Table {
    let price = fa_f64!("price", 100.0, 100.5, 101.0);
    let quantity = fa_i64!("quantity", 10, 5, 8);
    let side = fa_i32!("side", 0, 1, 0);
    Table::new("OrderBook".to_string(), vec![price, quantity, side].into())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> std::io::Result<()> {
    const STREAMS: usize = 2;
    const BATCHES: usize = 4;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let schema = order_book_schema();

    // The server accepts the parallel connections, registers the same types in
    // the same order, then routes each merged frame by type.
    let server_schema = schema.clone();
    let server = tokio::spawn(async move {
        let table_types = [("OrderBook", server_schema)];
        let reader = LightstreamParallelReader::accept(
            &listener,
            STREAMS,
            &["Subscribe"],
            &table_types,
            SortBehaviour::Ordered,
            None,
        )
        .await
        .unwrap();

        for frame in reader.read_all().await.unwrap() {
            if frame.is_message() {
                let sub: Subscribe = frame.decode_payload().unwrap();
                println!(
                    "control  Subscribe symbol={} interval_ms={}",
                    sub.symbol, sub.interval_ms
                );
            } else if let Some(table) = frame.into_table() {
                println!("data     OrderBook batch with {} rows", table.n_rows);
            }
        }
    });

    // The client sends one control message then a run of data batches on the
    // same connections.
    let table_types = [("OrderBook", schema)];
    let mut writer =
        LightstreamParallelWriter::connect(addr, STREAMS, &["Subscribe"], &table_types).await?;
    writer
        .send_proto("Subscribe", &Subscribe { symbol: "BTC-USD".into(), interval_ms: 100 })
        .await?;
    for _ in 0..BATCHES {
        writer.send_table("OrderBook", make_order_book()).await?;
    }
    writer.finish().await?;

    server.await.unwrap();
    Ok(())
}
