//! Sink side of the cross-host throughput benchmark.
//!
//! Drives the comparison over the pod-to-pod network. For each stream count it
//! receives the same workload twice and times each transfer independently. One
//! transfer arrives over Arrow Flight (N concurrent `DoGet` streams), the other
//! over the Lightstream protocol parallel reader (N concurrent connections).
//! Both transfers return tables in global write order, so the two transports are
//! compared on an equal ordering contract. Arrow Flight delivers its DoGet stream
//! in order, and the Lightstream parallel reader merges its connections under
//! `Ordered`. Each transfer prints a `RESULT` line giving the transport, shape,
//! stream count and throughput in GiB/s, which the wrapper script greps to build
//! the comparison. It also measures the round-trip latency to the source and
//! reports it on a `RESULT` line of its own.
//!
//! Both transports run plaintext over the trusted-VPC pod-to-pod network. TLS is
//! assumed terminated at the ingress boundary and is excluded, so neither side
//! pays encryption overhead.
//!
//! Run with `--help` for the available options.

use std::time::Instant;

use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::stream::{StreamExt, TryStreamExt};
use minarrow::Field;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::Request;

use lightstream::models::readers::parallel::lightstream::LightstreamParallelReader;
use lightstream::traits::parallel_transport_reader::{ParallelTransportReader, SortBehaviour};

#[path = "../../benches/common/bench_helpers.rs"]
mod bench_helpers;
#[path = "../../benches/common/arrow_flight_bench.rs"]
mod arrow_flight_bench;

use arrow_flight_bench::{FLIGHT_HTTP2_WINDOW, FLIGHT_MAX_MESSAGE_BYTES};
use bench_helpers::{
    BenchShape, bench_schema, logical_payload_bytes_shape, make_bench_table_shape,
};

/// Registered Lightstream protocol table type for the run. Must match the source.
const TYPE_NAME: &str = "vpc_bench";

/// Round trips used to measure pod-to-pod latency.
const RTT_ROUNDS: usize = 50;

struct Args {
    shape: BenchShape,
    rows: usize,
    batches_per_stream: u64,
    streams: Vec<usize>,
    source_flight_addr: String,
    source_echo_addr: String,
    ls_bind: String,
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
    let mut source_flight_addr = "127.0.0.1:9101".to_string();
    let mut source_echo_addr = "127.0.0.1:9102".to_string();
    let mut ls_bind = "0.0.0.0:9103".to_string();

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
            "--source-flight-addr" => source_flight_addr = next()?,
            "--source-echo-addr" => source_echo_addr = next()?,
            "--ls-bind" => ls_bind = next()?,
            "--help" | "-h" => {
                println!("Usage: bench_vpc_sink [options]");
                println!("  --shape SHAPE              mixed | narrow_numeric | string_heavy | wide");
                println!("  --rows N                  rows per table (default 1000000)");
                println!("  --batches-per-stream N    tables per stream per cell (default 500)");
                println!("  --streams LIST            comma-separated stream counts (default 4,8,16)");
                println!("  --source-flight-addr ADDR source Flight address (host:port)");
                println!("  --source-echo-addr ADDR  source latency echo address (host:port)");
                println!("  --ls-bind ADDR            Lightstream reader bind (default 0.0.0.0:9103)");
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    Ok(Args {
        shape,
        rows,
        batches_per_stream,
        streams,
        source_flight_addr,
        source_echo_addr,
        ls_bind,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    eprintln!(
        "[sink] shape={} rows={} batches_per_stream={} streams={:?} source_flight={}",
        args.shape.label(),
        args.rows,
        args.batches_per_stream,
        args.streams,
        args.source_flight_addr
    );

    let table = make_bench_table_shape(args.shape, args.rows);
    let schema: Vec<Field> = bench_schema(&table);
    let per_table_bytes = logical_payload_bytes_shape(args.shape, args.rows, 1);

    let rtt_ms = measure_rtt(&args.source_echo_addr).await?;
    println!(
        "RESULT metric=latency shape={} rtt_ms={:.4}",
        args.shape.label(),
        rtt_ms
    );

    let ls_listener = TcpListener::bind(&args.ls_bind).await?;

    for &streams in &args.streams {
        let total = args.batches_per_stream * streams as u64;
        let logical_bytes = per_table_bytes * total;
        let logical_gib = logical_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

        // Arrow Flight: N concurrent DoGet streams, each pulling
        // `batches_per_stream` batches from the source.
        let flight_gib = flight_phase(
            &args.source_flight_addr,
            streams,
            args.batches_per_stream,
            logical_gib,
        )
        .await?;
        println!(
            "RESULT protocol=flight shape={} rows={} streams={} batches={} gib_per_s={:.3}",
            args.shape.label(),
            args.rows,
            streams,
            total,
            flight_gib
        );

        // Lightstream protocol parallel: accept N connections, the source pushes
        // the same workload, time the receive.
        let reader = LightstreamParallelReader::accept(
            &ls_listener,
            streams,
            TYPE_NAME,
            schema.clone(),
            SortBehaviour::Ordered,
        )
        .await?;
        let start = Instant::now();
        let tables = reader.read_all_tables().await?;
        let elapsed = start.elapsed();
        assert_eq!(tables.len() as u64, total, "lightstream batch count mismatch");
        let ls_gib = logical_gib / elapsed.as_secs_f64();
        println!(
            "RESULT protocol=lightstream shape={} rows={} streams={} batches={} gib_per_s={:.3}",
            args.shape.label(),
            args.rows,
            streams,
            total,
            ls_gib
        );
    }

    eprintln!("[sink] done");
    Ok(())
}

/// Pull `batches_per_stream` batches over each of `streams` concurrent DoGet
/// streams and return the aggregate throughput in GiB/s.
async fn flight_phase(
    source_flight_addr: &str,
    streams: usize,
    batches_per_stream: u64,
    logical_gib: f64,
) -> Result<f64, Box<dyn std::error::Error>> {
    let channel = tonic::transport::Endpoint::try_from(format!("http://{source_flight_addr}"))?
        .initial_stream_window_size(FLIGHT_HTTP2_WINDOW)
        .initial_connection_window_size(FLIGHT_HTTP2_WINDOW)
        .connect()
        .await?;

    let ticket = Ticket::new(batches_per_stream.to_le_bytes().to_vec());

    let start = Instant::now();
    let mut handles = Vec::with_capacity(streams);
    for _ in 0..streams {
        let mut client = FlightServiceClient::new(channel.clone())
            .max_decoding_message_size(FLIGHT_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(FLIGHT_MAX_MESSAGE_BYTES);
        let ticket = ticket.clone();
        handles.push(tokio::spawn(async move {
            let stream = client.do_get(Request::new(ticket)).await.unwrap().into_inner();
            let decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                stream.map_err(|err| {
                    arrow_flight::error::FlightError::from_external_error(Box::new(err))
                }),
            );
            let mut decoder = std::pin::pin!(decoder);
            let mut received = 0u64;
            while let Some(item) = decoder.next().await {
                let rb = item.unwrap();
                std::hint::black_box(rb.columns());
                received += 1;
            }
            received
        }));
    }
    let mut total = 0u64;
    for handle in handles {
        total += handle.await.unwrap();
    }
    let elapsed = start.elapsed();
    assert!(total >= batches_per_stream * streams as u64);
    Ok(logical_gib / elapsed.as_secs_f64())
}

/// Measure the median application-level round-trip latency to the source echo.
async fn measure_rtt(source_echo_addr: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let mut socket = TcpStream::connect(source_echo_addr).await?;
    socket.set_nodelay(true)?;
    let payload = [0u8; 8];
    let mut buf = [0u8; 8];
    let mut samples = Vec::with_capacity(RTT_ROUNDS);
    for _ in 0..RTT_ROUNDS {
        let start = Instant::now();
        socket.write_all(&payload).await?;
        socket.read_exact(&mut buf).await?;
        samples.push(start.elapsed());
    }
    samples.sort();
    let mid = samples.len() / 2;
    let median = if samples.len() % 2 == 0 {
        (samples[mid - 1] + samples[mid]) / 2
    } else {
        samples[mid]
    };
    Ok(median.as_secs_f64() * 1000.0)
}
