// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Receiver for the AWS A-to-B benchmark.
//!
//! Connects to the sender, receives the configured number of table batches and
//! reports throughput measured across the receive loop.
//!
//! See `benches/aws/README.md` for setup instructions. Run with `--help` for
//! the available command-line options.

use std::time::Instant;

use minarrow::Vec64;
use tokio::net::TcpStream;

use lightstream::enums::{BufferChunkSize, IPCMessageProtocol};
use lightstream::models::readers::ipc::table::TableReader;

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;

use bench_helpers::BenchShape;

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
    connect: String,
    shape: BenchShape,
    rows: usize,
    batches: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut connect = "127.0.0.1:9001".to_string();
    let mut shape = BenchShape::Mixed;
    let mut rows: usize = 100_000;
    let mut batches: u64 = 1_000;

    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--connect" => {
                connect = argv
                    .next()
                    .ok_or_else(|| "--connect requires value".to_string())?;
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
                    "Usage: bench_receiver [--connect ADDR] [--shape SHAPE] [--rows N] [--batches N]"
                );
                println!("  --connect  sender address (default 127.0.0.1:9001)");
                println!(
                    "  --shape    mixed | narrow_numeric | string_heavy | wide (default mixed)"
                );
                println!("  --rows     rows per batch (default 100_000) - must match sender");
                println!("  --batches  total batches expected (default 1_000)");
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    Ok(Args {
        connect,
        shape,
        rows,
        batches,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

    eprintln!(
        "[receiver] connect={} shape={} rows={} batches={}",
        args.connect,
        args.shape.label(),
        args.rows,
        args.batches
    );

    let stream = TcpStream::connect(&args.connect).await?;
    eprintln!("[receiver] connected; reading");

    let (read_half, _write) = stream.into_split();
    let mut reader = TableReader::<Vec64<u8>>::new(
        read_half,
        BufferChunkSize::Http.chunk_size(),
        IPCMessageProtocol::Stream,
        None,
    );

    let start = Instant::now();
    let mut count = 0u64;
    while let Some(batch) = reader.read_next().await? {
        assert!(batch.n_rows > 0);
        std::hint::black_box(&batch.cols);
        count += 1;
    }
    let elapsed = start.elapsed();
    assert_eq!(count, args.batches, "batch count mismatch");

    let logical_bytes = bench_helpers::logical_payload_bytes_shape(args.shape, args.rows, 1)
        * args.batches;
    let logical_gib = (logical_bytes as f64) / (1024.0 * 1024.0 * 1024.0);
    let throughput_gib = logical_gib / elapsed.as_secs_f64();

    eprintln!(
        "[receiver] received {} batches ({:.2} GiB logical) in {:.3} s = {:.3} GiB/s",
        count,
        logical_gib,
        elapsed.as_secs_f64(),
        throughput_gib
    );
    // Print a machine-parsable summary line on stdout so wrapper scripts
    // can capture the result without parsing the log.
    println!(
        "RESULT shape={} rows={} batches={} bytes={} elapsed_s={:.6} gib_per_s={:.3}",
        args.shape.label(),
        args.rows,
        args.batches,
        logical_bytes,
        elapsed.as_secs_f64(),
        throughput_gib
    );
    Ok(())
}
