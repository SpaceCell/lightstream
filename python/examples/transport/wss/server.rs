// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! WebSocket over TLS example server streaming the demo table as Arrow IPC.
//!
//! Serves the same table as server.py, so the Python client reads
//! identical results from either backend.
//!
//! Run with:
//! ```sh
//! cargo run --example wss_server
//! ```

#[path = "../../common/args.rs"]
mod args;
#[path = "../../common/datagen.rs"]
mod datagen;
#[path = "../../common/tls.rs"]
mod tls;

use std::io;

use lightstream::models::transports::websocket::WebSocketTransport;
use lightstream::models::writers::websocket::WebSocketTableWriter;
use lightstream::traits::transport_writer::IPCTransportWriter;
use tokio::runtime::Runtime;

async fn serve() -> io::Result<()> {
    let uri = args::example_uri("wss://127.0.0.1:9042");
    let listener = WebSocketTransport::bind(&uri).await?;
    let config = tls::rustls_server(&args::tls_cert(), &args::tls_key(), &[])?;
    let table = datagen::get_table();
    let schema = datagen::schema(&table);
    println!("Serving get_table on {uri}");

    loop {
        let (read_half, write_half) =
            WebSocketTransport::accept_tls(&listener, config.clone()).await?;
        let mut writer =
            WebSocketTableWriter::from_halves(read_half, write_half, schema.clone(), None)?;
        writer.write_table(table.clone()).await?;
        writer.finish().await?;
    }
}

fn main() -> io::Result<()> {
    Runtime::new()?.block_on(serve())
}
