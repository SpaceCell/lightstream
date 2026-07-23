// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sender for the AWS A-to-B benchmark.
//!
//! Listens on the configured address, accepts one connection and sends the
//! configured number of table batches. Throughput is measured across the send
//! loop for comparison with the receiver result.
//!
//! See `benches/aws/README.md` for setup instructions. Run with `--help` for
//! the available command-line options.

use std::sync::Arc;
use std::time::Instant;

use minarrow::{Field, Table};
use tokio::net::TcpListener;

use lightstream::models::writers::tcp::TcpTableWriter;
use lightstream::traits::transport_writer::IPCTransportWriter;

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;
use bench_helpers::{BenchShape, bench_schema, make_bench_table_shape};

#[derive(Debug, Clone, Copy)]
enum ShapeArg {
    Mixed,
    NarrowNumeric,
    StringHeavy,
    Wide,
}

impl ShapeArg {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "mixed" => Ok(ShapeArg::Mixed),
            "narrow" | "narrow_numeric" => Ok(ShapeArg::NarrowNumeric),
            "string" | "string_heavy" => Ok(ShapeArg::StringHeavy),
            "wide" => Ok(ShapeArg::Wide),
            other => Err(format!("unknown shape: {other}")),
        }
    }

    fn into_bench(self) -> BenchShape {
        match self {
            ShapeArg::Mixed => BenchShape::Mixed,
            ShapeArg::NarrowNumeric => BenchShape::NarrowNumeric,
            ShapeArg::StringHeavy => BenchShape::StringHeavy,
            ShapeArg::Wide => BenchShape::Wide,
        }
    }
}

struct Args {
    bind: String,
    shape: BenchShape,
    rows: usize,
    batches: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut bind = "0.0.0.0:9001".to_string();
    let mut shape = BenchShape::Mixed;
    let mut rows: usize = 100_000;
    let mut batches: u64 = 1_000;

    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--bind" => {
                bind = argv
                    .next()
                    .ok_or_else(|| "--bind requires value".to_string())?;
            }
            "--shape" => {
                let v = argv
                    .next()
                    .ok_or_else(|| "--shape requires value".to_string())?;
                shape = ShapeArg::parse(&v)?.into_bench();
            }
            "--rows" => {
                let v = argv
                    .next()
                    .ok_or_else(|| "--rows requires value".to_string())?;
                rows = v.parse().map_err(|e| format!("--rows: {e}"))?;
            }
            "--batches" => {
                let v = argv
                    .next()
                    .ok_or_else(|| "--batches requires value".to_string())?;
                batches = v.parse().map_err(|e| format!("--batches: {e}"))?;
            }
            "--help" | "-h" => {
                println!(
                    "Usage: bench_sender [--bind ADDR] [--shape SHAPE] [--rows N] [--batches N]"
                );
                println!("  --bind     listen address (default 0.0.0.0:9001)");
                println!(
                    "  --shape    mixed | narrow_numeric | string_heavy | wide (default mixed)"
                );
                println!("  --rows     rows per batch (default 100_000)");
                println!("  --batches  total batches to send (default 1_000)");
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    Ok(Args {
        bind,
        shape,
        rows,
        batches,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    eprintln!(
        "[sender] bind={} shape={} rows={} batches={}",
        args.bind,
        args.shape.label(),
        args.rows,
        args.batches
    );

    let table = Arc::new(make_bench_table_shape(args.shape, args.rows));
    let schema: Vec<Field> = bench_schema(&table);
    let dict_regs = args.shape.dictionary_registrations();

    let listener = TcpListener::bind(&args.bind).await?;
    eprintln!("[sender] listening on {}", listener.local_addr()?);

    let (socket, peer) = listener.accept().await?;
    eprintln!("[sender] receiver connected from {peer}");

    // Hand the accepted socket's write half to the table writer. The
    // peer is responsible for connecting first, so the timed region
    // covers send-only work.
    let (_read, write) = socket.into_split();
    let mut writer = TcpTableWriter::from_write_half(write, schema, None)?;
    for (id, values) in dict_regs {
        writer.register_dictionary(id, values);
    }

    let start = Instant::now();
    for _ in 0..args.batches {
        let table_ref = Table::clone(&table);
        writer.write_table(table_ref).await?;
    }
    writer.finish().await?;
    let elapsed = start.elapsed();

    let logical_bytes = bench_helpers::logical_payload_bytes_shape(args.shape, args.rows, 1)
        * args.batches;
    let throughput_gib =
        (logical_bytes as f64) / (1024.0 * 1024.0 * 1024.0) / elapsed.as_secs_f64();

    eprintln!(
        "[sender] sent {} batches ({:.2} GiB logical) in {:.3} s = {:.3} GiB/s",
        args.batches,
        (logical_bytes as f64) / (1024.0 * 1024.0 * 1024.0),
        elapsed.as_secs_f64(),
        throughput_gib
    );
    Ok(())
}
