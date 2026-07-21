// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! UDS example server streaming the demo table as Arrow IPC.
//!
//! Serves the same table as server.py, so the Python client reads
//! identical results from either backend.
//!
//! Run with:
//! ```sh
//! cargo run --example uds_server
//! ```

#[path = "../../common/args.rs"]
mod args;
#[path = "../../common/datagen.rs"]
mod datagen;

use std::fs;
use std::io;
use std::path::PathBuf;

use lightstream::models::transports::uds::UdsTransport;
use lightstream::models::writers::uds::UdsTableWriter;
use lightstream::traits::transport_writer::IPCTransportWriter;
use tokio::runtime::Runtime;

async fn serve() -> io::Result<()> {
    let uri = args::example_uri("uds:///tmp/lightstream-example/get_table.sock");
    let path = PathBuf::from(uri.strip_prefix("uds://").unwrap_or(&uri));
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let _ = fs::remove_file(&path);
    let listener = UdsTransport::bind(&path)?;
    let table = datagen::get_table();
    let schema = datagen::schema(&table);
    println!("Serving get_table on {uri}");

    loop {
        let mut writer = UdsTableWriter::accept(&listener, schema.clone(), None).await?;
        writer.write_table(table.clone()).await?;
        writer.finish().await?;
    }
}

fn main() -> io::Result<()> {
    Runtime::new()?.block_on(serve())
}
