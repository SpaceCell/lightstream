// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sink side of the cross-host throughput benchmark.
//!
//! Drives the comparison over the host-to-host network. For each stream count
//! it receives the same workload per transport and times each transfer
//! independently. Arrow Flight obtains its endpoints from `GetFlightInfo`
//! and fetches their `DoGet` streams concurrently. Lightstream runs its
//! protocol parallel reader with one connection per stream, merged in global
//! write order. Before each Lightstream pass the sink pulses one control
//! byte to the source (`C` for cold, `W` for warm or memory), keeping the
//! two sides' phase order in lockstep.
//!
//! Under `memory` (the standard setting that measures the transport and protocol
//! performance), each cell interleaves the transports run by run. Due to cost,
//! most users should consider `memory`, as `nvme` is trivial to run on localhost
//! via the separate benchmarks, and does not an EC2 instance. 
//! 
//! Under `nvme` (included for a full 'get data off disk and send it, using Lightstream's
//! custom native arrow readers and Arrow-Rs's Arrow reader for Flight), 
//! each transport runs as a block per cell: one cold pass with the
//! source's files evicted from the page cache first, then `runs` warm passes
//! over the cached files. Cold passes report `cache=cold`, warm passes
//! `cache=warm`, and the cell medians cover the warm passes only.
//!
//! Each transport provides the strongest ordering guarantee defined by its
//! protocol:
//!
//! * Lightstream returns a single globally ordered result. Each protocol
//!   connection announces its index when opened, so an `Ordered` merge
//!   preserves global order regardless of the order in which connections are
//!   accepted.
//!
//! * Flight endpoints are consumed concurrently and independently because
//!   Flight does not define ordering across endpoints in a partitioned dataset.
//!   Each endpoint contains a contiguous range and preserves the order of its
//!   own batches, but the final set of batches is not globally reordered during
//!   reassembly.
//!
//!   Producing globally ordered Flight output in this benchmark would require
//!   either a single TCP connection, which would unfairly constrain Flight's
//!   parallelism, or custom reordering logic. The latter was found to introduce
//!   head-of-line blocking. Global reordering is therefore intentionally
//!   omitted to avoid distorting Flight's benchmark results and because it is
//!   not provided by the Flight implementation itself.
//!
//! Delivery is verified from the data. Under both data sources every
//! batch's first column carries its global sequence: Lightstream must
//! deliver record batches and row windows in dataset order, which under
//! `nvme` spans the replay files in file order, and each Flight endpoint
//! must deliver its contiguous range in order and complete.
//!
//! Each transfer records:
//!
//! * A `RESULT` line with the transport, workload, run and throughput.
//! * A `RESULT metric=gaps` line summarising adjacent arrival gaps.
//! * `RAW` lines containing every receiver-visible arrival offset.
//!
//! The timing contract is:
//!
//! * Start immediately before the existing client requests the dataset.
//! * Record an arrival offset as each item becomes available.
//! * Stop after the final item has been verified.
//! * Compute statistics and print results after the timed window closes.
//!
//! Each cell closes with a median `RESULT` line carrying the min and max,
//! which the wrapper script uses to build the comparison. The sink reports
//! host-to-host round-trip latency on a separate `RESULT` line.
//!
//! Each table is released after the ordered reader yields it and verification
//! completes. Tables are not collected, so sink memory stays flat at
//! cross-host workload sizes.
//!
//! Both transports run plaintext over the trusted-VPC host-to-host network.
//! TLS is assumed terminated at the ingress boundary and is excluded, so
//! neither side pays encryption overhead.
//!
//! Run with `--help` for the available options.

use std::time::{Duration, Instant};

use arrow::array::{Array as ArrowArray, Int32Array};
use arrow_flight::{FlightClient, FlightDescriptor};
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::future::try_join_all;
use futures::stream::StreamExt;
use minarrow::{Array, Field, NumericArray, Table};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::transport::Channel;

use lightstream::models::readers::parallel::lightstream::LightstreamParallelReader;
use lightstream::traits::parallel_transport_reader::SortBehaviour;

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;
#[path = "../common/arrow_flight_bench.rs"]
mod arrow_flight_bench;

use arrow_flight_bench::FLIGHT_HTTP2_WINDOW;
use bench_helpers::{
    BenchShape, batches_per_stream_for_budget, bench_schema, logical_payload_bytes_shape,
    make_bench_table_shape,
};

/// Table type name registered on every Lightstream protocol connection,
/// matching the source's registration so the wire tags agree.
const TYPE_NAME: &str = "bench";

/// Round trips used to measure host-to-host latency.
const RTT_ROUNDS: usize = 50;

/// Deadline for the source to come up, and the pause between attempts.
const CONNECT_DEADLINE: Duration = Duration::from_secs(120);
const CONNECT_STEP: Duration = Duration::from_millis(200);

/// Deadline for the source's Flight server under the nvme data source, which
/// binds only after the dataset is generated on first use.
const NVME_FLIGHT_DEADLINE: Duration = Duration::from_secs(3600);

#[derive(Clone, Copy, PartialEq, Eq)]
enum DataSource {
    Memory,
    Nvme,
}

impl DataSource {
    fn label(self) -> &'static str {
        match self {
            DataSource::Memory => "memory",
            DataSource::Nvme => "nvme",
        }
    }
}

struct Args {
    shape: BenchShape,
    rows: usize,
    dataset_gb: u64,
    streams: Vec<usize>,
    runs: u32,
    data_source: DataSource,
    max_batch_size: usize,
    source_flight_addr: String,
    source_echo_addr: String,
    source_ctrl_addr: String,
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

fn parse_data_source(s: &str) -> Result<DataSource, String> {
    match s {
        "memory" => Ok(DataSource::Memory),
        "nvme" => Ok(DataSource::Nvme),
        other => Err(format!("unknown data source: {other}")),
    }
}

fn parse_args() -> Result<Args, String> {
    let mut shape = BenchShape::Mixed;
    let mut rows: usize = 1_000_000;
    let mut dataset_gb: u64 = 350;
    let mut streams = vec![1usize, 4, 8, 16];
    let mut runs: u32 = 5;
    let mut data_source = DataSource::Memory;
    let mut max_batch_size: usize = 0;
    let mut source_flight_addr = "127.0.0.1:9101".to_string();
    let mut source_echo_addr = "127.0.0.1:9102".to_string();
    let mut source_ctrl_addr = "127.0.0.1:9104".to_string();
    let mut ls_bind = "0.0.0.0:9103".to_string();

    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        let mut next = || argv.next().ok_or_else(|| format!("{arg} requires a value"));
        match arg.as_str() {
            "--shape" => shape = parse_shape(&next()?)?,
            "--rows" => rows = next()?.parse().map_err(|e| format!("--rows: {e}"))?,
            "--dataset-gb" => {
                dataset_gb = next()?.parse().map_err(|e| format!("--dataset-gb: {e}"))?
            }
            "--streams" => streams = parse_streams(&next()?)?,
            "--runs" => runs = next()?.parse().map_err(|e| format!("--runs: {e}"))?,
            "--data-source" => data_source = parse_data_source(&next()?)?,
            "--max-batch-size" => {
                max_batch_size = next()?.parse().map_err(|e| format!("--max-batch-size: {e}"))?
            }
            "--source-flight-addr" => source_flight_addr = next()?,
            "--source-echo-addr" => source_echo_addr = next()?,
            "--source-ctrl-addr" => source_ctrl_addr = next()?,
            "--ls-bind" => ls_bind = next()?,
            "--help" | "-h" => {
                println!("Usage: bench_ecs_sink [options]");
                println!("  --shape SHAPE              mixed | narrow_numeric | string_heavy | wide");
                println!("  --rows N                  rows per table (default 1000000)");
                println!("  --dataset-gb N            workload gigabytes split across the largest");
                println!("                            stream count (default 350)");
                println!("  --streams LIST            comma-separated stream counts (default 1,4,8,16)");
                println!("  --runs N                  warm runs per cell (default 5)");
                println!("  --data-source SRC         memory | nvme (default memory)");
                println!("  --max-batch-size N        nvme replay batch size limit in bytes, 0 replays");
                println!("                            whole batches (default 0)");
                println!("  --source-flight-addr ADDR source Flight address (host:port)");
                println!("  --source-echo-addr ADDR  source latency echo address (host:port)");
                println!("  --source-ctrl-addr ADDR  source control address (host:port)");
                println!("  --ls-bind ADDR            Lightstream reader bind (default 0.0.0.0:9103)");
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }

    if runs == 0 {
        return Err("--runs must be at least 1".to_string());
    }

    Ok(Args {
        shape,
        rows,
        dataset_gb,
        streams,
        runs,
        data_source,
        max_batch_size,
        source_flight_addr,
        source_echo_addr,
        source_ctrl_addr,
        ls_bind,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    let max_streams = args.streams.iter().copied().max().unwrap_or(1);
    let batches_per_stream =
        batches_per_stream_for_budget(args.shape, args.rows, max_streams, args.dataset_gb);
    eprintln!(
        "[sink] shape={} rows={} dataset_gb={} batches_per_stream={} streams={:?} runs={} data={} max_batch_size={} source_flight={}",
        args.shape.label(),
        args.rows,
        args.dataset_gb,
        batches_per_stream,
        args.streams,
        args.runs,
        args.data_source.label(),
        args.max_batch_size,
        args.source_flight_addr
    );

    let shape = args.shape.label();
    let data = args.data_source.label();
    let per_table_bytes = logical_payload_bytes_shape(args.shape, args.rows, 1);
    // The protocol reader registers the bench table type up front, mirroring
    // the source's registration, so both sides agree on the wire tag.
    let ls_schema: Vec<Field> = bench_schema(&make_bench_table_shape(args.shape, args.rows));

    let rtt_ms = measure_rtt(&args.source_echo_addr).await?;
    println!("RESULT metric=latency shape={shape} data={data} rtt_ms={rtt_ms:.4}");

    let ls_listener = TcpListener::bind(&args.ls_bind).await?;
    let mut ctrl = connect_retry(&args.source_ctrl_addr).await?;

    for &streams in &args.streams {
        let total = batches_per_stream * streams as u64;
        let logical_bytes = per_table_bytes * total;
        let logical_gib = logical_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

        let mut flight_runs = Vec::with_capacity(args.runs as usize);
        let mut ls_runs = Vec::with_capacity(args.runs as usize);
        let mut summary_cache = "";

        match args.data_source {
            DataSource::Memory => {
                for run in 1..=args.runs {
                    // Arrow Flight: N concurrent DoGet streams, each pulling
                    // `batches_per_stream` batches from the source.
                    let (flight_gib, series) = flight_phase(
                        &args.source_flight_addr,
                        args.data_source,
                        streams,
                        batches_per_stream,
                        args.rows,
                        logical_gib,
                        false,
                    )
                    .await?;
                    println!(
                        "RESULT protocol=flight shape={shape} data={data} rows={} streams={streams} batches={total} run={run} gib_per_s={flight_gib:.3}",
                        args.rows
                    );
                    report_series(
                        &format!("protocol=flight shape={shape} data={data} streams={streams} run={run}"),
                        &series,
                    );
                    flight_runs.push(flight_gib);

                    // Lightstream protocol: accept the transport connections
                    // before the existing control connection requests the
                    // dataset.
                    let (ls_gib, series) = lightstream_phase(
                        &ls_listener,
                        &mut ctrl,
                        b'W',
                        args.data_source,
                        streams,
                        batches_per_stream,
                        args.rows,
                        &ls_schema,
                        logical_gib,
                    )
                    .await?;
                    println!(
                        "RESULT protocol=lightstream shape={shape} data={data} rows={} streams={streams} batches={total} run={run} gib_per_s={ls_gib:.3}",
                        args.rows
                    );
                    report_series(
                        &format!("protocol=lightstream shape={shape} data={data} streams={streams} run={run}"),
                        &series,
                    );
                    ls_runs.push(ls_gib);
                }
            }
            DataSource::Nvme => {
                summary_cache = " cache=warm";

                // Arrow Flight block: one cold pass with the files evicted
                // through the ticket flag, then the warm runs.
                let (cold_gib, series) = flight_phase(
                    &args.source_flight_addr,
                    args.data_source,
                    streams,
                    batches_per_stream,
                    args.rows,
                    logical_gib,
                    true,
                )
                .await?;
                println!(
                    "RESULT protocol=flight shape={shape} data={data} cache=cold rows={} streams={streams} batches={total} gib_per_s={cold_gib:.3}",
                    args.rows
                );
                report_series(
                    &format!("protocol=flight shape={shape} data={data} streams={streams} cache=cold"),
                    &series,
                );
                for run in 1..=args.runs {
                    let (flight_gib, series) = flight_phase(
                        &args.source_flight_addr,
                        args.data_source,
                        streams,
                        batches_per_stream,
                        args.rows,
                        logical_gib,
                        false,
                    )
                    .await?;
                    println!(
                        "RESULT protocol=flight shape={shape} data={data} cache=warm rows={} streams={streams} batches={total} run={run} gib_per_s={flight_gib:.3}",
                        args.rows
                    );
                    report_series(
                        &format!("protocol=flight shape={shape} data={data} streams={streams} cache=warm run={run}"),
                        &series,
                    );
                    flight_runs.push(flight_gib);
                }

                // Lightstream block: the cold pulse has the source evict the
                // cell's files before replaying, then the warm runs.
                let (cold_gib, series) = lightstream_phase(
                    &ls_listener,
                    &mut ctrl,
                    b'C',
                    args.data_source,
                    streams,
                    batches_per_stream,
                    args.rows,
                    &ls_schema,
                    logical_gib,
                )
                .await?;
                println!(
                    "RESULT protocol=lightstream shape={shape} data={data} cache=cold rows={} streams={streams} batches={total} gib_per_s={cold_gib:.3}",
                    args.rows
                );
                report_series(
                    &format!("protocol=lightstream shape={shape} data={data} streams={streams} cache=cold"),
                    &series,
                );
                for run in 1..=args.runs {
                    let (ls_gib, series) = lightstream_phase(
                        &ls_listener,
                        &mut ctrl,
                        b'W',
                        args.data_source,
                        streams,
                        batches_per_stream,
                        args.rows,
                        &ls_schema,
                        logical_gib,
                    )
                    .await?;
                    println!(
                        "RESULT protocol=lightstream shape={shape} data={data} cache=warm rows={} streams={streams} batches={total} run={run} gib_per_s={ls_gib:.3}",
                        args.rows
                    );
                    report_series(
                        &format!("protocol=lightstream shape={shape} data={data} streams={streams} cache=warm run={run}"),
                        &series,
                    );
                    ls_runs.push(ls_gib);
                }
            }
        }

        let (min, median, max) = spread(&mut flight_runs);
        println!(
            "RESULT protocol=flight shape={shape} data={data}{summary_cache} rows={} streams={streams} batches={total} stat=median runs={} gib_per_s={median:.3} min_gib_per_s={min:.3} max_gib_per_s={max:.3}",
            args.rows, args.runs
        );
        let (min, median, max) = spread(&mut ls_runs);
        println!(
            "RESULT protocol=lightstream shape={shape} data={data}{summary_cache} rows={} streams={streams} batches={total} stat=median runs={} gib_per_s={median:.3} min_gib_per_s={min:.3} max_gib_per_s={max:.3}",
            args.rows, args.runs
        );
    }

    eprintln!("[sink] done");
    Ok(())
}

/// Request one Flight dataset, fetch its endpoints concurrently over
/// separate Tonic gRPC connections, and consume every stream as it arrives.
///
/// Endpoints are read independently with no cross-endpoint ordering, which
/// is the delivery contract Flight defines for partitioned datasets.
///
/// The gRPC channels open before the timer, matching the Lightstream pass,
/// which accepts its transport connections before its own timer starts.
/// Timing starts immediately before `GetFlightInfo` and ends after the
/// final batch is decoded and verified, so discovery, `DoGet` and decoding
/// are all included.
///
/// Each decoded record batch contributes one arrival timestamp.
async fn flight_phase(
    source_flight_addr: &str,
    data_source: DataSource,
    streams: usize,
    batches_per_stream: u64,
    rows: usize,
    logical_gib: f64,
    evict: bool,
) -> Result<(f64, Vec<u64>), Box<dyn std::error::Error>> {
    let metadata_channel = flight_connect_retry(source_flight_addr, data_source).await?;
    let mut metadata_client = flight_client(metadata_channel);

    // One channel per endpoint opens before the timer so connection setup
    // stays outside the timed region. Connection attempts run concurrently.
    // The source's services publish location-free endpoints, meaning every
    // `DoGet` goes to the source address these channels already reach.
    let channels = try_join_all(
        (0..streams).map(|_| flight_connect_retry(source_flight_addr, data_source)),
    )
    .await?;

    let start = Instant::now();
    let mut command = Vec::with_capacity(17);
    command.extend_from_slice(&batches_per_stream.to_le_bytes());
    command.extend_from_slice(&(streams as u64).to_le_bytes());
    command.push(evict as u8);
    let info = metadata_client
        .get_flight_info(FlightDescriptor::new_cmd(command))
        .await?;
    assert_eq!(
        info.endpoint.len(),
        streams,
        "Flight endpoint count does not match the request"
    );

    // Tickets pair with the pre-opened channels in the endpoint order
    // FlightInfo returned.
    let endpoints = info
        .endpoint
        .into_iter()
        .zip(channels)
        .map(|(endpoint, channel)| {
            assert!(
                endpoint.location.is_empty(),
                "endpoint carries a location, which the pre-opened channel cannot serve"
            );
            let ticket = endpoint
                .ticket
                .clone()
                .ok_or("Flight endpoint has no ticket")?;
            Ok::<_, Box<dyn std::error::Error>>((ticket, channel))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Each endpoint is decoded and verified on its own task, so every
    // stream draws down the wire at the same time. Draining them in
    // `FlightInfo` order would serialise the transfer. Each endpoint still
    // verifies its own range in order.
    let mut handles = Vec::with_capacity(streams);
    for (endpoint_idx, (ticket, channel)) in endpoints.into_iter().enumerate() {
        handles.push(tokio::spawn(async move {
            let mut client = flight_client(channel);
            let mut stream = client.do_get(ticket).await.unwrap();
            let mut stamps: Vec<Instant> = Vec::with_capacity(batches_per_stream as usize);
            let mut batch_rows = 0usize;
            let mut batches_done = 0u64;
            while let Some(item) = stream.next().await {
                let rb = item.unwrap();
                stamps.push(Instant::now());
                let col = rb
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("sequenced batch missing leading i32 column");
                let seq = endpoint_idx as u64 * batches_per_stream + batches_done;
                let expected = (seq as i32).wrapping_add(batch_rows as i32);
                assert_eq!(
                    col.value(0),
                    expected,
                    "sequence verification failed for endpoint {endpoint_idx}"
                );
                batch_rows += rb.num_rows();
                assert!(batch_rows <= rows, "flight record batch crosses a source batch");
                if batch_rows == rows {
                    batches_done += 1;
                    batch_rows = 0;
                }
                std::hint::black_box(rb.columns());
            }
            assert_eq!(
                batches_done, batches_per_stream,
                "flight batch count mismatch for endpoint {endpoint_idx}"
            );
            assert_eq!(
                batch_rows, 0,
                "flight endpoint {endpoint_idx} ended mid-batch"
            );
            stamps
        }));
    }
    let mut stamps: Vec<Instant> = Vec::new();
    for handle in handles {
        stamps.extend(handle.await.unwrap());
    }
    let elapsed = start.elapsed();
    stamps.sort();
    let offsets = stamps
        .iter()
        .map(|t| t.duration_since(start).as_micros() as u64)
        .collect();
    Ok((logical_gib / elapsed.as_secs_f64(), offsets))
}

fn flight_client(channel: Channel) -> FlightClient {
    FlightClient::new_from_inner(FlightServiceClient::new(channel))
}

/// Connect the Flight channel, retrying until the source's server is up. The
/// nvme deadline is long because the source binds its Flight server only
/// after the dataset is generated.
async fn flight_connect_retry(
    source_flight_addr: &str,
    data_source: DataSource,
) -> Result<Channel, Box<dyn std::error::Error>> {
    flight_connect_uri_retry(&format!("http://{source_flight_addr}"), data_source).await
}

async fn flight_connect_uri_retry(
    uri: &str,
    data_source: DataSource,
) -> Result<Channel, Box<dyn std::error::Error>> {
    let deadline = match data_source {
        DataSource::Memory => CONNECT_DEADLINE,
        DataSource::Nvme => NVME_FLIGHT_DEADLINE,
    };
    let endpoint = tonic::transport::Endpoint::try_from(uri.to_string())?
        .initial_stream_window_size(FLIGHT_HTTP2_WINDOW)
        .initial_connection_window_size(FLIGHT_HTTP2_WINDOW);
    let mut waited = Duration::ZERO;
    loop {
        match endpoint.connect().await {
            Ok(channel) => return Ok(channel),
            Err(e) => {
                if waited >= deadline {
                    return Err(Box::new(e));
                }
                tokio::time::sleep(CONNECT_STEP).await;
                waited += CONNECT_STEP;
            }
        }
    }
}

/// Receive the cell's tables over the Lightstream protocol, verify delivery
/// and return the throughput in GiB/s with the per-table arrival offsets in
/// microseconds. Accepts one Lightstream protocol connection per stream and
/// merges them in global write order under `Ordered`. Each connection
/// announces its index at open, so the merge holds regardless of the order
/// the connections were accepted.
///
/// Timing begins after the transport connections exist and immediately before
/// the request pulse. It includes source preparation, decoding, ordered
/// delivery and verification. Each table is released after verification.
async fn lightstream_phase(
    listener: &TcpListener,
    ctrl: &mut TcpStream,
    pulse: u8,
    data_source: DataSource,
    streams: usize,
    batches_per_stream: u64,
    rows: usize,
    schema: &[Field],
    logical_gib: f64,
) -> Result<(f64, Vec<u64>), Box<dyn std::error::Error>> {
    let total = batches_per_stream * streams as u64;
    let mut file_idx = 0usize;
    let mut batch_idx = 0u64;
    let mut row_offset = 0usize;
    let mut stamps: Vec<Instant> = Vec::with_capacity(total as usize);

    let table_types = [(TYPE_NAME, schema.to_vec())];
    let mut reader = LightstreamParallelReader::accept(
        listener,
        streams,
        &[],
        &table_types,
        SortBehaviour::Ordered,
        None,
    )
    .await?;
    // The memory workload forms one contiguous sequence of batches, where
    // nvme spans one replay file per stream. Both verify through the same
    // window checks against the leading sequence column.
    let (files_expected, batches_per_file) = match data_source {
        DataSource::Memory => (1usize, total),
        DataSource::Nvme => (streams, batches_per_stream),
    };

    let start = Instant::now();
    ctrl.write_all(&[pulse]).await?;
    while let Some(item) = reader.next().await {
        let msg = item?;
        let Some(table) = msg.into_table() else {
            return Err("lightstream frame is not a table".into());
        };
        stamps.push(Instant::now());
        verify_sequenced_table(
            &table,
            rows,
            batches_per_file,
            &mut file_idx,
            &mut batch_idx,
            &mut row_offset,
        );
        std::hint::black_box(&table.cols);
    }
    assert_eq!(file_idx, files_expected, "the dataset did not fully arrive");
    assert_eq!(batch_idx, 0, "the transfer ended between batches");
    assert_eq!(row_offset, 0, "the transfer ended within a record batch");
    let elapsed = start.elapsed();
    let offsets = stamps
        .iter()
        .map(|t| t.duration_since(start).as_micros() as u64)
        .collect();
    Ok((logical_gib / elapsed.as_secs_f64(), offsets))
}

/// Verify that a sequenced table arrived in order: its leading column must
/// start at the exact next expected dataset row, so any reordered, missing
/// or duplicated delivery fails the assertion. Advances the file,
/// record-batch and row offsets after a successful check.
fn verify_sequenced_table(
    table: &Table,
    rows: usize,
    batches_per_file: u64,
    file_idx: &mut usize,
    batch_idx: &mut u64,
    row_offset: &mut usize,
) {
    let first = match &table.cols[0].array {
        Array::NumericArray(NumericArray::Int32(a)) => a.data[0] as i64,
        _ => panic!("sequenced batch missing leading i32 column"),
    };
    let global_batch = *file_idx as u64 * batches_per_file + *batch_idx;
    let expected = (global_batch as i32).wrapping_add(*row_offset as i32);
    assert_eq!(first as i32, expected, "sequenced table arrived out of order");
    assert!(
        table.n_rows <= rows - *row_offset,
        "table overruns its batch"
    );
    *row_offset += table.n_rows;
    if *row_offset == rows {
        *row_offset = 0;
        *batch_idx += 1;
        if *batch_idx == batches_per_file {
            *batch_idx = 0;
            *file_idx += 1;
        }
    }
}

/// Values per `RAW` line, sized to keep each log event comfortably under
/// CloudWatch's event limit.
const RAW_CHUNK: usize = 1000;

/// Report a pass's arrival series after the timed window closes.
///
/// * `RESULT metric=gaps` summarises the inter-arrival gaps.
/// * `RAW` lines contain every arrival offset for offline analysis.
/// * Percentiles appear only at sample counts that support them.
fn report_series(tags: &str, offsets_us: &[u64]) {
    if offsets_us.len() >= 2 {
        let mut gaps: Vec<u64> = offsets_us.windows(2).map(|w| w[1] - w[0]).collect();
        gaps.sort_unstable();
        let n = gaps.len();
        let mut line = format!(
            "RESULT metric=gaps {tags} n={n} p50_us={}",
            percentile(&gaps, 0.50)
        );
        if n >= 100 {
            line.push_str(&format!(" p95_us={}", percentile(&gaps, 0.95)));
        }
        if n >= 1000 {
            line.push_str(&format!(" p99_us={}", percentile(&gaps, 0.99)));
        }
        line.push_str(&format!(" max_us={}", gaps[n - 1]));
        println!("{line}");
    }
    let chunks = offsets_us.len().div_ceil(RAW_CHUNK).max(1);
    for (i, chunk) in offsets_us.chunks(RAW_CHUNK).enumerate() {
        let values: Vec<String> = chunk.iter().map(u64::to_string).collect();
        println!(
            "RAW {tags} unit=us n={} chunk={}/{} values={}",
            offsets_us.len(),
            i + 1,
            chunks,
            values.join(",")
        );
    }
}

/// Nearest-rank percentile of an ascending-sorted slice.
fn percentile(sorted: &[u64], q: f64) -> u64 {
    let rank = ((sorted.len() as f64) * q).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// Min, median and max of the run samples. Sorts in place and averages the
/// middle pair for an even count.
fn spread(samples: &mut [f64]) -> (f64, f64, f64) {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = samples.len() / 2;
    let median = if samples.len() % 2 == 0 {
        (samples[mid - 1] + samples[mid]) / 2.0
    } else {
        samples[mid]
    };
    (samples[0], median, samples[samples.len() - 1])
}

/// Measure the median application-level round-trip latency to the source
/// echo, retrying the connection until the source is up.
async fn measure_rtt(source_echo_addr: &str) -> Result<f64, Box<dyn std::error::Error>> {
    let mut socket = connect_retry(source_echo_addr).await?;
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

/// Connect to the source, retrying until it is up. The sink may start before
/// the source finishes binding its listeners.
async fn connect_retry(addr: &str) -> std::io::Result<TcpStream> {
    let mut waited = Duration::ZERO;
    loop {
        match TcpStream::connect(addr).await {
            Ok(socket) => return Ok(socket),
            Err(e) => {
                if waited >= CONNECT_DEADLINE {
                    return Err(e);
                }
                tokio::time::sleep(CONNECT_STEP).await;
                waited += CONNECT_STEP;
            }
        }
    }
}
