// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! stdio example server writing the demo table to stdout as Arrow IPC.
//!
//! Run with:
//! ```sh
//! cargo run --example stdio_server
//! ```

#[path = "../../common/datagen.rs"]
mod datagen;

use std::io;

use lightstream::models::writers::stdio::StdoutTableWriter;
use lightstream::traits::transport_writer::IPCTransportWriter;
use tokio::runtime::Runtime;

async fn serve() -> io::Result<()> {
    let table = datagen::get_table();
    let mut writer = StdoutTableWriter::new(datagen::schema(&table), None)?;
    writer.write_table(table).await?;
    writer.finish().await
}

fn main() -> io::Result<()> {
    Runtime::new()?.block_on(serve())
}
