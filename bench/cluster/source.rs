// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Source side of the cross-host throughput benchmark.
//!
//! Serves the same Arrow payload two ways so the sink can measure both over the
//! pod-to-pod network. It runs an Arrow Flight `DoGet` server and the Lightstream
//! protocol parallel writer that pushes the matched workload on request. Data
//! flows from source to sink for both, and the sink drives and reports.
//!
//! For each stream count the source opens the Lightstream protocol parallel
//! writer to the sink and sends `batches_per_stream * streams` tables. The
//! Flight server stays up for the whole run. A TCP echo answers the sink's
//! round-trip latency measurement.
//!
//! Both transports run plaintext over the trusted-VPC pod-to-pod network. TLS is
//! assumed terminated at the ingress boundary and is excluded, so neither side
//! pays encryption overhead.
//!
//! Run with `--help` for the available options.

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use minarrow::Field;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tonic::transport::Server;

use lightstream::models::writers::parallel::lightstream::LightstreamParallelWriter;
use lightstream::traits::parallel_transport_writer::ParallelTransportWriter;

#[path = "../../benches/common/bench_helpers.rs"]
mod bench_helpers;
#[path = "../../benches/common/arrow_flight_bench.rs"]
mod arrow_flight_bench;

use arrow_flight_bench::{
    BenchFlightService, FLIGHT_HTTP2_WINDOW, FLIGHT_MAX_MESSAGE_BYTES, make_record_batch,
};
use bench_helpers::{BenchShape, bench_schema, make_bench_table_shape};

/// Registered Lightstream protocol table type for the run.
const TYPE_NAME: &str = "vpc_bench";

struct Args {
    shape: BenchShape,
    rows: usize,
    batches_per_stream: u64,
    streams: Vec<usize>,
    flight_bind: String,
    echo_bind: String,
    sink_ls_addr: String,
}

fn parse_shape(s: &str) -> Result<BenchShape, String> {
    match s {
        "mixed" => Ok(BenchShape::Mixed),
        "narrow" | "narrow_numeric" => Ok(BenchShape::NarrowNumeric),
        "string" | "string_heavy" => Ok(BenchShape::StringHeavy),
        "wide" => Ok(BenchShape::Wide),
        other => Err(format!("unknown shape: {other}")),
    }
}

fn parse_streams(s: &str) -> Result<Vec<usize>, String> {
    s.split(',')
        .map(|p| p.trim().parse::<usize>().map_err(|e| format!("--streams: {e}")))
        .collect()
}

fn parse_args() -> Result<Args, String> {
    let mut shape = BenchShape::Mixed;
    let mut rows: usize = 1_000_000;
    let mut batches_per_stream: u64 = 500;
    let mut streams = vec![4usize, 8, 16];
    let mut flight_bind = "0.0.0.0:9101".to_string();
    let mut echo_bind = "0.0.0.0:9102".to_string();
    let mut sink_ls_addr = "127.0.0.1:9103".to_string();

    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        let mut next = || argv.next().ok_or_else(|| format!("{arg} requires a value"));
        match arg.as_str() {
            "--shape" => shape = parse_shape(&next()?)?,
            "--rows" => rows = next()?.parse().map_err(|e| format!("--rows: {e}"))?,
            "--batches-per-stream" => {
                batches_per_stream = next()?.parse().map_err(|e| format!("--batches-per-stream: {e}"))?
            }
            "--streams" => streams = parse_streams(&next()?)?,
            "--flight-bind" => flight_bind = next()?,
            "--echo-bind" => echo_bind = next()?,
            "--sink-ls-addr" => sink_ls_addr = next()?,
            "--help" | "-h" => {
                println!("Usage: bench_vpc_source [options]");
                println!("  --shape SHAPE              mixed | narrow_numeric | string_heavy | wide");
                println!("  --rows N                  rows per table (default 1000000)");
                println!("  --batches-per-stream N    tables per stream per cell (default 500)");
                println!("  --streams LIST            comma-separated stream counts (default 4,8,16)");
                println!("  --flight-bind ADDR        Flight server bind (default 0.0.0.0:9101)");
                println!("  --echo-bind ADDR          latency echo bind (default 0.0.0.0:9102)");
                println!("  --sink-ls-addr ADDR       sink Lightstream address (host:port)");
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    Ok(Args { shape, rows, batches_per_stream, streams, flight_bind, echo_bind, sink_ls_addr })
}

/// Echo every byte back so the sink can time application-level round trips.
async fn run_echo(listener: TcpListener) {
    loop {
        let Ok((mut socket, _peer)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            let mut buf = [0u8; 64];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if socket.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }
}

/// Resolve a `host:port` string to a socket address.
async fn resolve(addr: &str) -> std::io::Result<std::net::SocketAddr> {
    tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, format!("no address for {addr}"))
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    eprintln!(
        "[source] shape={} rows={} batches_per_stream={} streams={:?} sink_ls={}",
        args.shape.label(),
        args.rows,
        args.batches_per_stream,
        args.streams,
        args.sink_ls_addr
    );

    let table = Arc::new(make_bench_table_shape(args.shape, args.rows));
    let schema: Vec<Field> = bench_schema(&table);
    let record_batch = Arc::new(make_record_batch(args.shape, args.rows));

    // Flight DoGet server. Stays up for the whole run.
    let flight_addr = resolve(&args.flight_bind).await?;
    let service = BenchFlightService { batch: Arc::clone(&record_batch) };
    tokio::spawn(async move {
        let incoming = TcpListener::bind(flight_addr).await.unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(incoming);
        Server::builder()
            .initial_stream_window_size(FLIGHT_HTTP2_WINDOW)
            .initial_connection_window_size(FLIGHT_HTTP2_WINDOW)
            .add_service(
                FlightServiceServer::new(service)
                    .max_encoding_message_size(FLIGHT_MAX_MESSAGE_BYTES)
                    .max_decoding_message_size(FLIGHT_MAX_MESSAGE_BYTES),
            )
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Latency echo.
    let echo_listener = TcpListener::bind(&args.echo_bind).await?;
    tokio::spawn(run_echo(echo_listener));

    // Lightstream protocol parallel push, one cell per stream count.
    for &streams in &args.streams {
        let total = args.batches_per_stream * streams as u64;
        let mut writer = connect_retry(&args.sink_ls_addr, streams, &schema).await?;
        for _ in 0..total {
            writer.write_table((*table).clone()).await?;
        }
        writer.finish().await?;
        eprintln!("[source] pushed streams={streams} tables={total}");
    }

    // Keep the Flight server up until the run is stopped. The sink reaches its
    // later Flight phases after the pushes complete, so the source must outlive
    // the push loop.
    eprintln!("[source] push complete - serving Flight until terminated");
    std::future::pending::<()>().await;
    Ok(())
}

/// Open the parallel writer to the sink, resolving and retrying until its
/// reader is up. Resolution is retried too, so the sink Service may appear after
/// the source starts.
async fn connect_retry(
    addr: &str,
    streams: usize,
    schema: &[Field],
) -> Result<LightstreamParallelWriter, Box<dyn std::error::Error>> {
    let deadline = Duration::from_secs(120);
    let step = Duration::from_millis(200);
    let mut waited = Duration::ZERO;
    loop {
        let attempt = match resolve(addr).await {
            Ok(socket_addr) => {
                LightstreamParallelWriter::connect(socket_addr, streams, TYPE_NAME, schema.to_vec())
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
            }
            Err(e) => Err(Box::new(e) as Box<dyn std::error::Error>),
        };
        match attempt {
            Ok(writer) => return Ok(writer),
            Err(e) => {
                if waited >= deadline {
                    return Err(e);
                }
                tokio::time::sleep(step).await;
                waited += step;
            }
        }
    }
}
