// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Source side of the cross-host throughput benchmark.
//!
//! Serves the same Arrow payload two ways so the sink can measure both over
//! the host-to-host network. It runs an Arrow Flight `DoGet` server and
//! pushes the matched workload over the Lightstream protocol on request.
//! Data flows from source to sink for both, and the sink drives and reports.
//!
//! Two data sources cover the two serving patterns:
//!
//! * `memory` - the table is materialised once in RAM and every send splits
//!   it into zero-copy batches sized towards `--max-batch-size` through its
//!   reference-counted columns, copying no payload bytes and measuring pure
//!   transport throughput with no storage in the path. The Flight server
//!   serves the table's zero-copy Arrow export, verified for type parity,
//!   so both transports send identical memory.
//! * `nvme` - a dataset of `--dataset-gb` gigabytes is written to local NVMe
//!   as one Arrow IPC file per configured stream, and each stream count
//!   selects the first N files and delivers their record batches in
//!   file-index order. This is the replay-server pattern, measuring the
//!   time for the remote server to pull data from disk and then send it.
//!   Flight reads through Arrow's native IPC reader and Lightstream through
//!   its own native implementation of an Arrow reader, both the stock
//!   buffered variants rather than mmap. `--use-mmap true` switches
//!   Lightstream to its zero-copy mmap reader instead, however keep in
//!   mind what this then measures is not directly comparable, as
//!   Lightstream will potentially then be hitting warm cache in more
//!   scenarios than native page caching within the one run. That setting
//!   disqualifies a direct comparison and is provided for informational
//!   benchmarking only.
//!
//! Every batch in the nvme dataset is distinct: its first column carries
//! the batch's global sequence, which the sink verifies on receipt. Each
//! combination runs one cold pass, then `runs` warm passes over the cached
//! files, covering both the first-scan and steady-state replay cases. A
//! cold pass first flushes and drops the selected files from the page
//! cache through `posix_fadvise`, so its reads come off the device even
//! when a prior pass or the dataset generation left those pages resident.
//! Within one pass each byte is read once, so a pass never warms itself.
//!
//! For each stream count the source opens the Lightstream protocol parallel
//! writer to the sink with one connection per stream and sends
//! `batches_per_stream * streams` tables per pass. The
//! sink pulses one control byte before each Lightstream pass (`C` for cold,
//! `W` for warm or memory), and the source asserts the pulse against its own
//! schedule, so an eviction never lands inside a timed Flight run and a
//! desync fails loudly. The Flight server stays up for the whole run. A TCP
//! echo answers the sink's round-trip latency measurement.
//!
//! Both transports run plaintext over the trusted-VPC host-to-host network.
//! TLS is assumed terminated at the ingress boundary and is excluded, so
//! neither side pays encryption overhead.
//!
//! Run with `--help` for the available options.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::encode::{DictionaryHandling, FlightDataEncoderBuilder};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::{self, BoxStream, TryStreamExt};
use minarrow::{Field, Table, Vec64};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::file_table::FileTableReader;
use lightstream::models::readers::ipc::mmap_table::MmapTableReader;
use lightstream::models::writers::ipc::sync_table::SyncTableWriter;
use lightstream::models::writers::parallel::lightstream::LightstreamParallelWriter;

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;
#[path = "../common/arrow_flight_bench.rs"]
mod arrow_flight_bench;

use arrow_flight_bench::{BenchFlightService, FLIGHT_HTTP2_WINDOW, assert_export_parity};
use bench_helpers::{
    BenchShape, batches_per_stream_for_budget, bench_schema, make_bench_table_shape,
    replay_batch_table,
};

/// Deadline for the sink's reader to come up, and the pause between attempts.
const CONNECT_DEADLINE: Duration = Duration::from_secs(120);
const CONNECT_STEP: Duration = Duration::from_millis(200);

/// Table type name registered on every Lightstream protocol connection. The
/// sink registers the same name and schema so the wire tags agree.
const TYPE_NAME: &str = "bench";

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
    dataset_dir: PathBuf,
    max_batch_size: usize,
    use_mmap: bool,
    flight_bind: String,
    echo_bind: String,
    ctrl_bind: String,
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
    let mut dataset_dir = PathBuf::from("/data");
    let mut max_batch_size: usize = 0;
    let mut use_mmap = false;
    let mut flight_bind = "0.0.0.0:9101".to_string();
    let mut echo_bind = "0.0.0.0:9102".to_string();
    let mut ctrl_bind = "0.0.0.0:9104".to_string();
    let mut sink_ls_addr = "127.0.0.1:9103".to_string();

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
            "--dataset-dir" => dataset_dir = PathBuf::from(next()?),
            "--max-batch-size" => {
                max_batch_size = next()?.parse().map_err(|e| format!("--max-batch-size: {e}"))?
            }
            "--use-mmap" => {
                use_mmap = match next()?.as_str() {
                    "true" | "1" => true,
                    "false" | "0" => false,
                    other => return Err(format!("--use-mmap: expected true or false, got {other}")),
                }
            }
            "--flight-bind" => flight_bind = next()?,
            "--echo-bind" => echo_bind = next()?,
            "--ctrl-bind" => ctrl_bind = next()?,
            "--sink-ls-addr" => sink_ls_addr = next()?,
            "--help" | "-h" => {
                println!("Usage: bench_ecs_source [options]");
                println!("  --shape SHAPE              mixed | narrow_numeric | string_heavy | wide");
                println!("  --rows N                  rows per table (default 1000000)");
                println!("  --dataset-gb N            workload gigabytes split across the largest");
                println!("                            stream count (default 350)");
                println!("  --streams LIST            comma-separated stream counts (default 1,4,8,16)");
                println!("  --runs N                  warm runs per cell (default 5)");
                println!("  --data-source SRC         memory | nvme (default memory)");
                println!("  --dataset-dir PATH        nvme dataset directory (default /data)");
                println!("  --max-batch-size N        nvme replay batch size limit in bytes, 0 replays");
                println!("                            whole batches (default 0)");
                println!("  --use-mmap BOOL           replay nvme files through the buffered file");
                println!("                            reader (false, default) or the mmap reader");
                println!("                            (true). true is informational only and");
                println!("                            disqualifies a direct Flight comparison");
                println!("  --flight-bind ADDR        Flight server bind (default 0.0.0.0:9101)");
                println!("  --echo-bind ADDR          latency echo bind (default 0.0.0.0:9102)");
                println!("  --ctrl-bind ADDR          sink control bind (default 0.0.0.0:9104)");
                println!("  --sink-ls-addr ADDR       sink Lightstream address (host:port)");
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
        dataset_dir,
        max_batch_size,
        use_mmap,
        flight_bind,
        echo_bind,
        ctrl_bind,
        sink_ls_addr,
    })
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
async fn resolve(addr: &str) -> io::Result<std::net::SocketAddr> {
    tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("no address for {addr}"))
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args().map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    let max_streams = args.streams.iter().copied().max().unwrap_or(1);
    let batches_per_stream =
        batches_per_stream_for_budget(args.shape, args.rows, max_streams, args.dataset_gb);
    eprintln!(
        "[source] shape={} rows={} dataset_gb={} batches_per_stream={} streams={:?} runs={} data={} max_batch_size={} sink_ls={}",
        args.shape.label(),
        args.rows,
        args.dataset_gb,
        batches_per_stream,
        args.streams,
        args.runs,
        args.data_source.label(),
        args.max_batch_size,
        args.sink_ls_addr,
    );

    let table = Arc::new(make_bench_table_shape(args.shape, args.rows));
    let schema: Vec<Field> = bench_schema(&table);

    // The latency echo and the control listener come up first so the sink can
    // connect and measure round trips while the nvme dataset is still being
    // generated.
    let echo_listener = TcpListener::bind(&args.echo_bind).await?;
    tokio::spawn(run_echo(echo_listener));
    let ctrl_listener = TcpListener::bind(&args.ctrl_bind).await?;

    let flight_addr = resolve(&args.flight_bind).await?;

    let dataset = match args.data_source {
        DataSource::Memory => None,
        DataSource::Nvme => {
            let dir = dataset_dir_for(
                &args.dataset_dir,
                args.shape,
                args.rows,
                batches_per_stream,
                max_streams,
            );
            let gen_table = Arc::clone(&table);
            let gen_schema = schema.clone();
            let files = tokio::task::spawn_blocking(move || {
                generate_dataset(&dir, &gen_table, gen_schema, batches_per_stream, max_streams)
            })
            .await??;
            eprintln!("[source] dataset ready: {} files", files.len());
            let file = std::fs::File::open(&files[0])?;
            let reader = arrow::ipc::reader::FileReader::try_new(file, None)?;
            Some(Arc::new(ReplayDataset {
                files,
                batches_per_stream,
                schema: reader.schema(),
            }))
        }
    };

    // Flight DoGet server. It starts after the dataset is ready so the sink's
    // first DoGet never races generation, and stays up for the whole run.
    match &dataset {
        None => {
            let record_batch = Arc::new(table.to_apache_arrow());
            assert_export_parity(&table, &record_batch);
            let service = BenchFlightService {
                batch: record_batch,
            };
            tokio::spawn(async move {
                serve_flight(flight_addr, FlightServiceServer::new(service)).await;
            });
        }
        Some(dataset) => {
            let service = ReplayFlightService {
                dataset: Arc::clone(dataset),
            };
            tokio::spawn(async move {
                serve_flight(flight_addr, FlightServiceServer::new(service)).await;
            });
        }
    }

    // Lightstream protocol push, one connection set per pass. Each writer
    // connects before waiting for the sink's request pulse. The source checks
    // each pulse against its schedule so both sides remain in lockstep.
    let (mut ctrl, _peer) = ctrl_listener.accept().await?;
    for &streams in &args.streams {
        let total = batches_per_stream * streams as u64;
        match &dataset {
            None => {
                for run in 1..=args.runs {
                    push_memory_workload(
                        &args.sink_ls_addr,
                        streams,
                        total,
                        &schema,
                        &table,
                        args.max_batch_size,
                        &mut ctrl,
                        b'W',
                    )
                    .await?;
                    eprintln!("[source] pushed streams={streams} run={run} tables={total}");
                }
            }
            Some(dataset) => {
                push_replay_workload(
                    &args.sink_ls_addr,
                    streams,
                    &schema,
                    dataset,
                    args.max_batch_size,
                    args.use_mmap,
                    &mut ctrl,
                    b'C',
                    true,
                )
                .await?;
                eprintln!("[source] pushed streams={streams} cache=cold tables={total}");
                for run in 1..=args.runs {
                    push_replay_workload(
                        &args.sink_ls_addr,
                        streams,
                        &schema,
                        dataset,
                        args.max_batch_size,
                        args.use_mmap,
                        &mut ctrl,
                        b'W',
                        false,
                    )
                    .await?;
                    eprintln!(
                        "[source] pushed streams={streams} cache=warm run={run} tables={total}"
                    );
                }
            }
        }
    }

    // Keep the Flight server up until the run is stopped. The sink reaches its
    // later Flight phases after the pushes complete, so the source must outlive
    // the push loop.
    eprintln!("[source] push complete - serving Flight until terminated");
    std::future::pending::<()>().await;
    Ok(())
}

/// Serve a Flight service on `addr` with the benchmark's window and message
/// limits until the process exits.
async fn serve_flight<S>(addr: std::net::SocketAddr, service: FlightServiceServer<S>)
where
    S: FlightService,
{
    let incoming = TcpListener::bind(addr).await.unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(incoming);
    Server::builder()
        .initial_stream_window_size(FLIGHT_HTTP2_WINDOW)
        .initial_connection_window_size(FLIGHT_HTTP2_WINDOW)
        .add_service(service)
        .serve_with_incoming(incoming)
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////
// nvme dataset
////////////////////////////////////////////////////////////////////////////////

/// On-disk replay dataset: one Arrow IPC file per stream, each holding
/// `batches_per_stream` record batches. Batch `b` of stream `s` carries the
/// global sequence `s * batches_per_stream + b` in its first column, so the
/// sink can verify ordered, complete delivery per stream.
struct ReplayDataset {
    files: Vec<PathBuf>,
    batches_per_stream: u64,
    schema: arrow::datatypes::SchemaRef,
}

/// Directory holding the dataset for one workload. The parameters are encoded
/// in the name, so a matching directory can be reused across runs.
fn dataset_dir_for(
    base: &Path,
    shape: BenchShape,
    rows: usize,
    batches_per_stream: u64,
    max_streams: usize,
) -> PathBuf {
    base.join(format!(
        "{}_r{}_b{}_s{}",
        shape.label(),
        rows,
        batches_per_stream,
        max_streams
    ))
}

/// Write one Arrow IPC file per stream, each holding `batches_per_stream`
/// distinct batches built by [`replay_batch_table`]. A `MANIFEST` file marks
/// the dataset complete, so an existing dataset is reused rather than
/// rewritten.
fn generate_dataset(
    dir: &Path,
    table: &Table,
    schema: Vec<Field>,
    batches_per_stream: u64,
    max_streams: usize,
) -> io::Result<Vec<PathBuf>> {
    let files: Vec<PathBuf> = (0..max_streams)
        .map(|i| dir.join(format!("stream-{i:02}.arrow")))
        .collect();

    let manifest = dir.join("MANIFEST");
    if manifest.exists() {
        eprintln!("[source] reusing dataset at {}", dir.display());
        return Ok(files);
    }

    std::fs::create_dir_all(dir)?;
    for (i, path) in files.iter().enumerate() {
        let file = std::fs::File::create(path)?;
        let mut writer = SyncTableWriter::<_, Vec64<u8>>::new(
            file,
            schema.clone(),
            IPCMessageProtocol::File,
            None,
        );
        for b in 0..batches_per_stream {
            let seq = i as u64 * batches_per_stream + b;
            writer.write_table(replay_batch_table(table, seq))?;
        }
        writer.finish()?;
        eprintln!("[source] wrote {} ({}/{})", path.display(), i + 1, max_streams);
    }
    std::fs::write(
        &manifest,
        format!("files={} batches_per_stream={batches_per_stream}\n", files.len()),
    )?;
    Ok(files)
}

/// Flush and drop a replay file's pages from the page cache so the next
/// read comes off the NVMe device rather than RAM. The flush matters
/// because `posix_fadvise` leaves dirty pages in place.
#[cfg(unix)]
fn evict_file(path: &Path) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    let file = std::fs::File::open(path)?;
    file.sync_all()?;
    // SAFETY: the fd is valid for the duration of the call.
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
    }
    Ok(())
}

#[cfg(not(unix))]
fn evict_file(_path: &Path) -> io::Result<()> {
    Ok(())
}

/// Read one control byte from the sink and require it to match the phase the
/// source is about to serve, so a phase-order desync fails loudly rather
/// than silently corrupting the cache state of a timed pass.
async fn read_pulse(ctrl: &mut TcpStream, expected: u8) -> io::Result<()> {
    let mut byte = [0u8; 1];
    ctrl.read_exact(&mut byte).await?;
    if byte[0] != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "control pulse mismatch: got {:?}, expected {:?}",
                byte[0] as char, expected as char
            ),
        ));
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
// Lightstream push
////////////////////////////////////////////////////////////////////////////////

/// Send `total` sequenced batches to the sink over the Lightstream protocol
/// parallel writer, which opens one connection per stream.
///
/// Each batch is built through [`replay_batch_table`] inside the timed
/// region, so its leading column tells the sink where it belongs in the
/// dataset. The sink uses that position to verify the delivery arrived
/// ordered and complete, under the same contract as the nvme replay.
///
/// When `max_batch_size` is nonzero, each send is further split into
/// zero-copy views of that target size through
/// `get_views_for_target_batch_size`, which also happens inside the timed
/// region.
///
/// Each connection announces its index when it opens, so the sink can
/// merge the round-robin rotation back into global write order under
/// `Ordered`.
async fn push_memory_workload(
    addr: &str,
    streams: usize,
    total: u64,
    schema: &[Field],
    table: &Arc<Table>,
    max_batch_size: usize,
    ctrl: &mut TcpStream,
    expected_pulse: u8,
) -> io::Result<()> {
    let mut writer = connect_retry(addr, streams, schema).await?;
    read_pulse(ctrl, expected_pulse).await?;
    for seq in 0..total {
        let batch = replay_batch_table(table, seq);
        if max_batch_size == 0 {
            writer.send_table(TYPE_NAME, batch).await?;
        } else {
            for view in batch.get_views_for_target_batch_size(max_batch_size).slices {
                writer.send_table(TYPE_NAME, view).await?;
            }
        }
    }
    writer.finish().await
}

/// Replay batches from the selected files through the Lightstream protocol
/// parallel writer. Files are visited by index, record batches retain their
/// order within each file, and the writer distributes the resulting tables
/// across its connection tasks round-robin. A nonzero `max_batch_size`
/// replays each record batch as row windows of at most that size through the
/// reader's windowed batch iteration.
///
/// `use_mmap` selects the reader, with the buffered file reader as the
/// default. The buffered reader leaves the pages it reads cheaply
/// reclaimable, so it also suits datasets larger than RAM, where mapped
/// pages would stall page faults in reclaim once memory fills. The mmap
/// reader serves each table zero-copy out of the page cache and each table
/// owns its mapping while queued or in flight, so it suits datasets that
/// fit in RAM.
async fn push_replay_workload(
    addr: &str,
    streams: usize,
    schema: &[Field],
    dataset: &Arc<ReplayDataset>,
    max_batch_size: usize,
    use_mmap: bool,
    ctrl: &mut TcpStream,
    expected_pulse: u8,
    evict: bool,
) -> io::Result<()> {
    let mut writer = connect_retry(addr, streams, schema).await?;
    read_pulse(ctrl, expected_pulse).await?;
    if evict {
        for file in &dataset.files[..streams] {
            evict_file(file)?;
        }
    }
    for file in &dataset.files[..streams] {
        let n_batches = dataset.batches_per_stream as usize;
        if use_mmap {
            let reader = MmapTableReader::open(file)?;
            let n = n_batches.min(reader.num_batches());
            if max_batch_size == 0 {
                for idx in 0..n {
                    writer.send_table(TYPE_NAME, reader.read_batch(idx)?).await?;
                }
            } else {
                for idx in 0..n {
                    for window in reader.batch_windows(idx, max_batch_size)? {
                        writer.send_table(TYPE_NAME, window?).await?;
                    }
                }
            }
        } else {
            let reader = FileTableReader::open(file)?;
            let n = n_batches.min(reader.num_batches());
            if max_batch_size == 0 {
                for idx in 0..n {
                    writer.send_table(TYPE_NAME, reader.read_batch(idx)?).await?;
                }
            } else {
                for idx in 0..n {
                    for window in reader.batch_windows(idx, max_batch_size)? {
                        writer.send_table(TYPE_NAME, window?).await?;
                    }
                }
            }
        }
    }
    writer.finish().await
}

/// Open the Lightstream protocol parallel writer to the sink with one
/// connection per stream, resolving and retrying until its listener is up.
/// Resolution is retried too, so the sink may appear after the source starts.
/// Every connection registers the bench table type, and announces its index
/// at open so the sink pairs it regardless of accept order.
async fn connect_retry(
    addr: &str,
    streams: usize,
    schema: &[Field],
) -> io::Result<LightstreamParallelWriter> {
    let table_types = [(TYPE_NAME, schema.to_vec())];
    let mut waited = Duration::ZERO;
    loop {
        let attempt = match resolve(addr).await {
            Ok(socket_addr) => {
                LightstreamParallelWriter::connect(socket_addr, streams, &[], &table_types).await
            }
            Err(e) => Err(e),
        };
        match attempt {
            Ok(writer) => return Ok(writer),
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

////////////////////////////////////////////////////////////////////////////////
// Replay Flight service
////////////////////////////////////////////////////////////////////////////////

/// Flight service for the nvme data source. `GetFlightInfo` returns one
/// ordered endpoint per selected file. Each endpoint ticket carries the batch
/// count and file index as little-endian `u64`s plus an evict flag byte. When
/// the flag is set the file is flushed and dropped from the page cache before
/// serving, so a cold pass reads the device. `DoGet` opens the file with the
/// `arrow` IPC reader and hands the batch iterator to the flight-data encoder.
#[derive(Clone)]
struct ReplayFlightService {
    dataset: Arc<ReplayDataset>,
}

#[tonic::async_trait]
impl FlightService for ReplayFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let bytes: [u8; 17] = descriptor
            .cmd
            .as_ref()
            .try_into()
            .map_err(|_| Status::invalid_argument("flight descriptor must be 17 bytes"))?;
        let batches_per_endpoint = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let endpoint_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        if endpoint_count == 0 || endpoint_count > self.dataset.files.len() {
            return Err(Status::invalid_argument(format!(
                "requested {endpoint_count} endpoints, dataset has {}",
                self.dataset.files.len()
            )));
        }
        if batches_per_endpoint > self.dataset.batches_per_stream {
            return Err(Status::invalid_argument(format!(
                "requested {batches_per_endpoint} batches per endpoint, dataset has {}",
                self.dataset.batches_per_stream
            )));
        }

        let endpoints = (0..endpoint_count)
            .map(|idx| {
                let mut ticket = Vec::with_capacity(17);
                ticket.extend_from_slice(&batches_per_endpoint.to_le_bytes());
                ticket.extend_from_slice(&(idx as u64).to_le_bytes());
                ticket.push(bytes[16]);
                FlightEndpoint::new().with_ticket(Ticket::new(ticket))
            })
            .collect();
        let info = FlightInfo::new()
            .try_with_schema(self.dataset.schema.as_ref())
            .map_err(|e| Status::internal(format!("encode flight schema: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoints(endpoints)
            .with_ordered(true);
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let bytes: [u8; 17] = ticket
            .ticket
            .as_ref()
            .try_into()
            .map_err(|_| Status::invalid_argument("replay ticket must be 17 bytes"))?;
        let n = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let idx = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let evict = bytes[16] != 0;
        let path = self
            .dataset
            .files
            .get(idx)
            .ok_or_else(|| Status::invalid_argument(format!("no replay file for stream {idx}")))?;

        if evict {
            evict_file(path)
                .map_err(|e| Status::internal(format!("evict {}: {e}", path.display())))?;
        }

        let file = std::fs::File::open(path)
            .map_err(|e| Status::internal(format!("open {}: {e}", path.display())))?;
        let reader = arrow::ipc::reader::FileReader::try_new(file, None)
            .map_err(|e| Status::internal(format!("read {}: {e}", path.display())))?;
        let batch_stream = stream::iter(
            reader
                .take(n)
                .map(|batch| batch.map_err(arrow_flight::error::FlightError::from)),
        );

        // The encoder keeps its default flight-data size, so batches above
        // 2 MiB split into multiple messages per Arrow Flight's own tuning.
        // Resending dictionaries ensures Flight uses the more efficient
        // dictionary-encoded representation rather than defaulting to
        // actual strings for compatibility reasons. Per the upstream
        // documentation at
        // https://docs.rs/arrow-flight/latest/arrow_flight/encode/enum.DictionaryHandling.html
        //
        // "Variants
        //
        //  Hydrate
        //  Expands to the underlying type (default). This likely sends more
        //  data over the network but requires less memory (dictionaries are
        //  not tracked) and is more compatible with other arrow flight
        //  client implementations that may not support DictionaryEncoding
        //
        //  See also:
        //  https://github.com/apache/arrow-rs/issues/1206
        //
        //  Resend
        //  Send dictionary FlightData with every RecordBatch that contains
        //  a DictionaryArray. See Self::Hydrate for more tradeoffs. No
        //  attempt is made to skip sending the same (logical) dictionary
        //  values twice.
        //
        //  This requires identifying the different dictionaries in use and
        //  assigning them unique IDs"
        let builder = FlightDataEncoderBuilder::new()
            .with_dictionary_handling(DictionaryHandling::Resend);
        let flight_data = builder
            .build(batch_stream)
            .map_err(|err| Status::internal(format!("flight encode failure: {err}")));
        Ok(Response::new(Box::pin(flight_data)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }
}
