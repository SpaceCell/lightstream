// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP/2 Arrow IPC Example
//!
//! Streams Arrow tables over plaintext HTTP/2 (h2c). The local server
//! accepts a single POST whose body is an Arrow IPC stream and decodes
//! it via the standard table reader; the client posts batches via
//! `HttpTableWriter::post`.
//!
//! Run with:
//! ```sh
//! cargo run --example http_arrow --features http
//! ```

#[path = "../helpers/mod.rs"]
mod helpers;

use std::sync::Arc;

use helpers::{make_table, table_schema};
use lightstream::models::readers::http::HttpTableReader;
use lightstream::models::writers::http::HttpTableWriter;
use lightstream::traits::transport_reader::IPCTransportReader;
use lightstream::traits::transport_writer::IPCTransportWriter;
use tokio::net::TcpListener;
use tokio::sync::Notify;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("HTTP/2 Arrow IPC Example");
    println!("========================\n");

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    println!("HTTP/2 server listening on http://{addr}");

    // Notify the client once the listener is bound; avoids racing accept.
    let ready = Arc::new(Notify::new());
    let ready_for_task = ready.clone();

    let server = tokio::spawn(async move {
        ready_for_task.notify_one();
        let (tcp, peer) = listener.accept().await.expect("accept");
        println!("Server accepted TCP from {peer}, running h2 handshake...");
        // POST throughput is governed by the server's flow-control window;
        // bump from h2's 64 KiB default to 8 MiB so multi-MiB Arrow batches
        // don't pay a WINDOW_UPDATE round-trip every 64 KiB.
        let mut h2 = h2::server::Builder::new()
            .initial_window_size(8 * 1024 * 1024)
            .initial_connection_window_size(2 * 65_535)
            .handshake::<_, bytes::Bytes>(tcp)
            .await
            .expect("h2 handshake");
        println!("h2 handshake complete.");

        let (req, mut respond) = h2.accept().await.expect("accept request").expect("ok");
        println!("Server got {} {}", req.method(), req.uri());

        let response = http::Response::builder().status(200).body(()).unwrap();
        // end_of_stream=true: response is headers only, no body. The
        // client's drain task sees END_STREAM immediately and returns.
        let _send_resp = respond
            .send_response(response, true)
            .expect("send response");

        // The h2 Connection is the I/O driver for all in-flight streams,
        // including the body of the request we just accepted. After
        // accept() returns we have to keep polling it or the RecvStream
        // never receives DATA frames. Spawning accept() in a loop drives
        // the connection until the peer closes.
        let driver = tokio::spawn(async move { while h2.accept().await.is_some() {} });

        let reader = HttpTableReader::from_recv(req.into_body(), None);
        let tables = reader.read_all_tables().await.expect("read tables");
        for t in &tables {
            println!(
                "  Server got table: {} rows, {} cols",
                t.n_rows,
                t.cols.len()
            );
        }
        assert_eq!(tables.len(), 3);
        println!("Server received all {} tables over HTTP/2.", tables.len());

        // h2 connections do not auto-close after their last stream
        // ends; abort the driver so this task exits.
        driver.abort();
        let _ = driver.await;
    });

    ready.notified().await;

    let url = format!("http://{addr}/ingest");
    let mut writer = HttpTableWriter::post(&url, table_schema(), None).await?;
    println!("Client POSTing Arrow IPC stream to {url}");

    writer.write_table(make_table("batch_1", 5)).await?;
    writer.write_table(make_table("batch_2", 3)).await?;
    writer.write_table(make_table("batch_3", 7)).await?;
    writer.finish().await?;

    server.await?;

    println!("\nHTTP/2 Arrow IPC example completed successfully!");
    Ok(())
}
