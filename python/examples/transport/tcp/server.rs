// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! TCP example server streaming the demo table as Arrow IPC.
//!
//! Serves the same table as server.py, so the Python client reads
//! identical results from either backend.
//!
//! Run with:
//! ```sh
//! cargo run --example tcp_server
//! ```

#[path = "../../common/args.rs"]
mod args;
#[path = "../../common/datagen.rs"]
mod datagen;

use std::io;

use lightstream::models::transports::tcp::TcpTransport;
use lightstream::models::writers::tcp::TcpTableWriter;
use lightstream::traits::transport_writer::IPCTransportWriter;
use tokio::runtime::Runtime;

async fn serve() -> io::Result<()> {
    let uri = args::example_uri("tcp://127.0.0.1:9040");
    let listener = TcpTransport::bind(args::authority(&uri)).await?;
    let table = datagen::get_table();
    let schema = datagen::schema(&table);
    println!("Serving get_table on {uri}");

    loop {
        let mut writer = TcpTableWriter::accept(&listener, schema.clone(), None).await?;
        writer.write_table(table.clone()).await?;
        writer.finish().await?;
    }
}

fn main() -> io::Result<()> {
    Runtime::new()?.block_on(serve())
}
