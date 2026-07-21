// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! QUIC example server streaming the demo table as Arrow IPC.
//!
//! Serves the same table as server.py, so the Python client reads
//! identical results from either backend.
//!
//! Run with:
//! ```sh
//! cargo run --example quic_server
//! ```

#[path = "../../common/args.rs"]
mod args;
#[path = "../../common/datagen.rs"]
mod datagen;
#[path = "../../common/tls.rs"]
mod tls;

use std::io;

use lightstream::models::transports::quic::QuicTransport;
use lightstream::models::writers::quic::QuicTableWriter;
use lightstream::traits::transport_writer::IPCTransportWriter;
use tokio::runtime::Runtime;

async fn serve() -> io::Result<()> {
    let uri = args::example_uri("quic://127.0.0.1:9045");
    let addr = args::authority(&uri)
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let config = tls::quic_server(&args::tls_cert(), &args::tls_key())?;
    let listener = QuicTransport::bind(addr, config)?;
    let table = datagen::get_table();
    let schema = datagen::schema(&table);
    println!("Serving get_table on {uri}");

    loop {
        let (_recv, send) = QuicTransport::accept(&listener).await?;
        let mut writer = QuicTableWriter::new(send, schema.clone(), None)?;
        writer.write_table(table.clone()).await?;
        writer.finish().await?;
    }
}

fn main() -> io::Result<()> {
    Runtime::new()?.block_on(serve())
}
