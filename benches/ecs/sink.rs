// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sink side of the cross-host throughput benchmark.
//!
//! Drives the comparison over the host-to-host network. For each stream count
//! it receives the same workload per transport and times each transfer
//! independently. One transfer arrives over Arrow Flight (N concurrent
//! `DoGet` streams), the other over Lightstream TCP - one connection for the
//! single-stream cell, otherwise N connections. Before each Lightstream pass
//! the sink pulses one control byte to the source (`C` for cold, `W` for
//! warm or memory), keeping the two sides' phase order in lockstep.
//!
//! Under `memory` each cell interleaves the transports run by run. Under
//! `nvme` each transport runs as a block per cell: one cold pass with the
//! source's files evicted from the page cache first, then `runs` warm passes
//! over the cached files. Cold passes report `cache=cold`, warm passes
//! `cache=warm`, and the cell medians cover the warm passes only.
//!
//! The merge contract follows the data source. Under `memory` the parallel
//! connections merge in global write order under `Ordered`, which follows
//! the writer's round-robin rotation. Under `nvme` each connection is an
//! independent file replay with per-stream ordering, matching the Flight
//! side's independent `DoGet` streams, so arrivals merge in arrival order.
//!
//! Delivery is verified from the data. Under `nvme` every batch's first
//! column carries its global sequence, and the sink asserts each stream
//! arrives ordered and complete with the expected row counts. Under `memory`
//! every send carries the one shared bench table, so the sink asserts the
//! received row totals.
//!
//! Each transfer prints a `RESULT` line giving the transport, shape, data
//! source, stream count, run index and throughput in GiB/s, followed by a
//! `RESULT metric=gaps` line summarising the inter-batch arrival gaps and
//! chunked `RAW` lines carrying every arrival offset in microseconds for
//! offline analysis. Arrival instants are recorded during the transfer and
//! everything else is computed after the timed window closes. Each cell
//! closes with a median `RESULT` line per transport carrying the min and max
//! alongside, which the wrapper script greps to build the comparison. The
//! sink also measures the round-trip latency to the source and reports it on
//! a `RESULT` line of its own.
//!
//! Received tables are verified and dropped as they arrive rather than
//! collected, so sink memory stays flat at cross-host workload sizes.
//!
//! Both transports run plaintext over the trusted-VPC host-to-host network.
//! TLS is assumed terminated at the ingress boundary and is excluded, so
//! neither side pays encryption overhead.
//!
//! Run with `--help` for the available options.

use std::time::{Duration, Instant};

use arrow::array::{Array as ArrowArray, Int32Array};
use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::stream::{StreamExt, TryStreamExt};
use minarrow::{Array, NumericArray, Table, Vec64};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::Request;
use tonic::transport::Channel;

use lightstream::enums::{BufferChunkSize, IPCMessageProtocol};
use lightstream::models::readers::ipc::table::TableReader;
use lightstream::models::readers::parallel::tcp::TcpParallelTableReader;
use lightstream::traits::parallel_transport_reader::SortBehaviour;

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;
#[path = "../common/arrow_flight_bench.rs"]
mod arrow_flight_bench;

use arrow_flight_bench::{FLIGHT_HTTP2_WINDOW, FLIGHT_MAX_MESSAGE_BYTES};
use bench_helpers::{BenchShape, batches_per_stream_for_budget, logical_payload_bytes_shape};

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
    max_chunk_size: usize,
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
    let mut max_chunk_size: usize = 0;
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
            "--max-chunk-size" => {
                max_chunk_size = next()?.parse().map_err(|e| format!("--max-chunk-size: {e}"))?
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
                println!("  --max-chunk-size N           nvme replay chunk size in bytes, 0 replays");
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
        max_chunk_size,
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
        "[sink] shape={} rows={} dataset_gb={} batches_per_stream={} streams={:?} runs={} data={} max_chunk_size={} source_flight={}",
        args.shape.label(),
        args.rows,
        args.dataset_gb,
        batches_per_stream,
        args.streams,
        args.runs,
        args.data_source.label(),
        args.max_chunk_size,
        args.source_flight_addr
    );

    let shape = args.shape.label();
    let data = args.data_source.label();
    // Tags Lightstream nvme RESULT lines so chunked and whole-batch replays
    // are distinguishable in the summary. Flight replays are unaffected by
    // the chunk size, so their lines stay untagged.
    let ls_chunk = if args.max_chunk_size > 0 && args.data_source == DataSource::Nvme {
        format!(" max_chunk_size={}", args.max_chunk_size)
    } else {
        String::new()
    };
    let per_table_bytes = logical_payload_bytes_shape(args.shape, args.rows, 1);

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

                    // Lightstream TCP: the source pushes the same workload on
                    // the control pulse, timed from the moment the
                    // connections are accepted.
                    ctrl.write_all(b"W").await?;
                    let (ls_gib, series) = lightstream_phase(
                        &ls_listener,
                        args.data_source,
                        streams,
                        batches_per_stream,
                        args.rows,
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
                ctrl.write_all(b"C").await?;
                let (cold_gib, series) = lightstream_phase(
                    &ls_listener,
                    args.data_source,
                    streams,
                    batches_per_stream,
                    args.rows,
                    logical_gib,
                )
                .await?;
                println!(
                    "RESULT protocol=lightstream shape={shape} data={data}{ls_chunk} cache=cold rows={} streams={streams} batches={total} gib_per_s={cold_gib:.3}",
                    args.rows
                );
                report_series(
                    &format!("protocol=lightstream shape={shape} data={data} streams={streams} cache=cold"),
                    &series,
                );
                for run in 1..=args.runs {
                    ctrl.write_all(b"W").await?;
                    let (ls_gib, series) = lightstream_phase(
                        &ls_listener,
                        args.data_source,
                        streams,
                        batches_per_stream,
                        args.rows,
                        logical_gib,
                    )
                    .await?;
                    println!(
                        "RESULT protocol=lightstream shape={shape} data={data}{ls_chunk} cache=warm rows={} streams={streams} batches={total} run={run} gib_per_s={ls_gib:.3}",
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
            "RESULT protocol=lightstream shape={shape} data={data}{ls_chunk}{summary_cache} rows={} streams={streams} batches={total} stat=median runs={} gib_per_s={median:.3} min_gib_per_s={min:.3} max_gib_per_s={max:.3}",
            args.rows, args.runs
        );
    }

    eprintln!("[sink] done");
    Ok(())
}

/// Pull `batches_per_stream` batches over each of `streams` concurrent DoGet
/// streams, verify delivery and return the aggregate throughput in GiB/s
/// with the sorted per-message arrival offsets in microseconds. Under
/// `memory` the ticket is the batch count and verification sums row counts,
/// since the flight-data encoder slices large batches into multiple
/// messages. Under `nvme` the ticket also carries the stream index and the
/// evict flag, and every decoded message is checked against the global
/// sequence its batch carries in its first column, proving each stream
/// arrives ordered and complete.
async fn flight_phase(
    source_flight_addr: &str,
    data_source: DataSource,
    streams: usize,
    batches_per_stream: u64,
    rows: usize,
    logical_gib: f64,
    evict: bool,
) -> Result<(f64, Vec<u64>), Box<dyn std::error::Error>> {
    let channel = flight_connect_retry(source_flight_addr, data_source).await?;

    let start = Instant::now();
    let mut handles = Vec::with_capacity(streams);
    for stream_idx in 0..streams {
        let mut client = FlightServiceClient::new(channel.clone())
            .max_decoding_message_size(FLIGHT_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(FLIGHT_MAX_MESSAGE_BYTES);
        let ticket = match data_source {
            DataSource::Memory => Ticket::new(batches_per_stream.to_le_bytes().to_vec()),
            DataSource::Nvme => {
                let mut bytes = Vec::with_capacity(17);
                bytes.extend_from_slice(&batches_per_stream.to_le_bytes());
                bytes.extend_from_slice(&(stream_idx as u64).to_le_bytes());
                bytes.push(evict as u8);
                Ticket::new(bytes)
            }
        };
        handles.push(tokio::spawn(async move {
            let stream = client.do_get(Request::new(ticket)).await.unwrap().into_inner();
            let decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                stream.map_err(|err| {
                    arrow_flight::error::FlightError::from_external_error(Box::new(err))
                }),
            );
            let mut decoder = std::pin::pin!(decoder);
            let mut stamps: Vec<Instant> = Vec::with_capacity(batches_per_stream as usize);
            let mut batch_rows = 0usize;
            let mut batches_done = 0u64;
            let mut total_rows = 0u64;
            while let Some(item) = decoder.next().await {
                let rb = item.unwrap();
                stamps.push(Instant::now());
                if data_source == DataSource::Nvme {
                    let col = rb
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("replay batch missing leading i32 column");
                    let seq = stream_idx as u64 * batches_per_stream + batches_done;
                    let expected = (seq as i32).wrapping_add(batch_rows as i32);
                    assert_eq!(
                        col.value(0),
                        expected,
                        "replay verification failed for stream {stream_idx}"
                    );
                    batch_rows += rb.num_rows();
                    assert!(batch_rows <= rows, "flight message crosses a batch boundary");
                    if batch_rows == rows {
                        batches_done += 1;
                        batch_rows = 0;
                    }
                }
                total_rows += rb.num_rows() as u64;
                std::hint::black_box(rb.columns());
            }
            match data_source {
                DataSource::Memory => assert_eq!(
                    total_rows,
                    batches_per_stream * rows as u64,
                    "flight row count mismatch for stream {stream_idx}"
                ),
                DataSource::Nvme => {
                    assert_eq!(
                        batches_done, batches_per_stream,
                        "flight batch count mismatch for stream {stream_idx}"
                    );
                    assert_eq!(batch_rows, 0, "flight stream {stream_idx} ended mid-batch");
                }
            }
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

/// Connect the Flight channel, retrying until the source's server is up. The
/// nvme deadline is long because the source binds its Flight server only
/// after the dataset is generated.
async fn flight_connect_retry(
    source_flight_addr: &str,
    data_source: DataSource,
) -> Result<Channel, Box<dyn std::error::Error>> {
    let deadline = match data_source {
        DataSource::Memory => CONNECT_DEADLINE,
        DataSource::Nvme => NVME_FLIGHT_DEADLINE,
    };
    let endpoint = tonic::transport::Endpoint::try_from(format!("http://{source_flight_addr}"))?
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

/// Receive the cell's tables over Lightstream TCP, verify delivery and
/// return the throughput in GiB/s with the per-table arrival offsets in
/// microseconds. Accepts one connection when `streams` is 1, otherwise
/// `streams` connections merged per the data source's ordering contract.
/// Timing starts once the connections are accepted. Under `nvme` each
/// table's first column carries its global sequence and every stream must
/// arrive ordered and complete. Tables are verified and dropped as they
/// arrive so memory stays flat.
async fn lightstream_phase(
    listener: &TcpListener,
    data_source: DataSource,
    streams: usize,
    batches_per_stream: u64,
    rows: usize,
    logical_gib: f64,
) -> Result<(f64, Vec<u64>), Box<dyn std::error::Error>> {
    let total = batches_per_stream * streams as u64;
    // Per-stream replay cursors: the global sequence of the batch being
    // received and the rows delivered into it so far. Whole batches and row
    // chunks both continue a cursor, so one verification covers chunked and
    // unchunked replays.
    let mut cursor_global: Vec<u64> = (0..streams as u64)
        .map(|s| s * batches_per_stream)
        .collect();
    let mut cursor_rows: Vec<usize> = vec![0; streams];
    let mut cursor_batches: Vec<u64> = vec![0; streams];
    let mut stamps: Vec<Instant> = Vec::with_capacity(total as usize);

    let (start, elapsed) = if streams == 1 {
        let (socket, _peer) = listener.accept().await?;
        let (read_half, _write_half) = socket.into_split();
        let mut reader = TableReader::<Vec64<u8>>::new(
            read_half,
            BufferChunkSize::Http.chunk_size(),
            IPCMessageProtocol::Stream,
        );
        let start = Instant::now();
        let mut received = 0u64;
        while let Some(table) = reader.read_next().await? {
            stamps.push(Instant::now());
            match data_source {
                DataSource::Memory => assert_eq!(table.n_rows, rows, "row count mismatch"),
                DataSource::Nvme => verify_replay_table(
                    &table,
                    rows,
                    &mut cursor_global,
                    &mut cursor_rows,
                    &mut cursor_batches,
                ),
            }
            std::hint::black_box(&table.cols);
            received += 1;
        }
        if data_source == DataSource::Memory {
            assert_eq!(received, total, "lightstream batch count mismatch");
        }
        (start, start.elapsed())
    } else {
        let sort = match data_source {
            DataSource::Memory => SortBehaviour::Ordered,
            DataSource::Nvme => SortBehaviour::None,
        };
        let mut reader = TcpParallelTableReader::accept(listener, streams, sort).await?;
        let start = Instant::now();
        let mut received = 0u64;
        while let Some(item) = reader.next().await {
            let (table, _seq) = item?;
            stamps.push(Instant::now());
            match data_source {
                DataSource::Memory => assert_eq!(table.n_rows, rows, "row count mismatch"),
                DataSource::Nvme => verify_replay_table(
                    &table,
                    rows,
                    &mut cursor_global,
                    &mut cursor_rows,
                    &mut cursor_batches,
                ),
            }
            std::hint::black_box(&table.cols);
            received += 1;
        }
        if data_source == DataSource::Memory {
            assert_eq!(received, total, "lightstream batch count mismatch");
        }
        (start, start.elapsed())
    };

    if data_source == DataSource::Nvme {
        for (stream, count) in cursor_batches.iter().enumerate() {
            assert_eq!(*count, batches_per_stream, "stream {stream} arrived incomplete");
            assert_eq!(cursor_rows[stream], 0, "stream {stream} ended mid-batch");
        }
    }
    let offsets = stamps
        .iter()
        .map(|t| t.duration_since(start).as_micros() as u64)
        .collect();
    Ok((logical_gib / elapsed.as_secs_f64(), offsets))
}

/// Check one received replay table against the dataset. The first value of
/// the leading `i32` column must continue one stream's cursor, since batch
/// `b` of stream `s` holds `s * batches_per_stream + b + i` at row `i`, and
/// whole batches or row chunks of them both satisfy that. Streams whose
/// cursors expect the same value carry identical content there, so advancing
/// either cursor verifies the same delivery.
fn verify_replay_table(
    table: &Table,
    rows: usize,
    cursor_global: &mut [u64],
    cursor_rows: &mut [usize],
    cursor_batches: &mut [u64],
) {
    let first = match &table.cols[0].array {
        Array::NumericArray(NumericArray::Int32(a)) => a.data[0] as i64,
        _ => panic!("replay batch missing leading i32 column"),
    };
    let stream = cursor_global
        .iter()
        .zip(cursor_rows.iter())
        .position(|(g, r)| *g as i64 + *r as i64 == first)
        .unwrap_or_else(|| panic!("first value {first} continues no stream cursor"));
    assert!(
        table.n_rows <= rows - cursor_rows[stream],
        "chunk overruns its batch"
    );
    cursor_rows[stream] += table.n_rows;
    if cursor_rows[stream] == rows {
        cursor_rows[stream] = 0;
        cursor_global[stream] += 1;
        cursor_batches[stream] += 1;
    }
}

/// Values per `RAW` line, sized to keep each log event comfortably under
/// CloudWatch's event limit.
const RAW_CHUNK: usize = 1000;

/// Report a pass's arrival series: a `RESULT metric=gaps` line summarising
/// the inter-arrival gaps and the raw offsets as chunked `RAW` lines for
/// offline analysis. Runs after the timed window closes. Percentiles appear
/// only at sample counts that support them.
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
