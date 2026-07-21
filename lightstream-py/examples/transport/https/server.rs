// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP/2 over TLS example server streaming the demo table as Arrow IPC.
//!
//! Serves the same table as server.py, so the Python client reads
//! identical results from either backend.
//!
//! Run with:
//! ```sh
//! cargo run --example https_server
//! ```

#[path = "../../common/args.rs"]
mod args;
#[path = "../../common/datagen.rs"]
mod datagen;
#[path = "../../common/tls.rs"]
mod tls;

use std::io;

use lightstream::models::transports::http::HttpTransport;
use lightstream::models::writers::http::HttpTableWriter;
use lightstream::traits::transport_writer::IPCTransportWriter;
use tokio::runtime::Runtime;

async fn serve() -> io::Result<()> {
    let uri = args::example_uri("https://127.0.0.1:9044/get_table");
    let listener = HttpTransport::bind(&uri).await?;
    let config = tls::rustls_server(&args::tls_cert(), &args::tls_key(), &[b"h2"])?;
    let table = datagen::get_table();
    let schema = datagen::schema(&table);
    println!("Serving get_table on {uri}");

    loop {
        let (recv_read, send_write) =
            HttpTransport::accept_tls(&listener, config.clone()).await?;
        let mut writer =
            HttpTableWriter::from_exchange(recv_read, send_write, schema.clone(), None)?;
        writer.write_table(table.clone()).await?;
        writer.finish().await?;
    }
}

fn main() -> io::Result<()> {
    Runtime::new()?.block_on(serve())
}
