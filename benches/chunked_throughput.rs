//! Chunked-format throughput bench. **Linux-only.**
//!
//! Writes ~100 MiB worth of batches into a tmp directory as
//! `<base>-NNNNNNNNNN.<ext>` chunk files, reads them back via the serial
//! Iterator path and the parallel `par_load_batched` path, then deletes the
//! directory. Covers each enabled chunked format - Arrow IPC always,
//! CSV always, Parquet under the `parquet` feature.
//!
//! Chunk size is chosen to be representative of what a streaming sink
//! should aim for: small enough that per-batch latency stays low, large
//! enough that per-file overhead amortises. The `make_bench_table`
//! helper produces ~32 bytes per row across 4 columns (i32, f64, utf8,
//! categorical), so 100 K rows ≈ 3 MiB per chunk and 32 chunks ≈ 96 MiB
//! on the wire.
//!
//! ## What the write benchmarks measure
//!
//! Each write bench operates on **32 distinct in-memory Tables** built
//! once at the top of the bench function. Distinct tables (rather than
//! one Table reused 32x) means the encoder reads ~96 MiB of unique
//! buffer memory per iteration, which exceeds typical L2/L3 cache
//! capacity and so does not let cache effects unrealistically inflate
//! the measured throughput.
//!
//! ## Two write benches per operation: logical and physical
//!
//! Every write operation is benched twice with deliberately different
//! workloads. They answer two distinct questions:
//!
//! - `*_logical`: "how long does my process take to hand the data to
//!   the kernel". Encodes the 32 source tables and calls `write()` on
//!   each chunk file. The kernel buffers the bytes in its page cache
//!   and returns immediately - no fsync, no waiting for the storage
//!   device. This matches what a typical streaming producer
//!   experiences: write call returns, process moves on, kernel
//!   flushes in the background. Throughput denominator is the source
//!   byte count - the size of the in-memory column buffers being
//!   consumed: binary numerics (4 bytes per i32, 8 per f64, ...), raw
//!   UTF-8 string bytes plus their offset buffers, categorical index
//!   buffers. Independent of the output format.
//!
//! - `*_physical`: "how long until my output is durably on disk".
//!   Same encode + write as the logical bench, plus an explicit
//!   `fsync` on every chunk file AND on the parent directory before
//!   the timed window ends. Fsyncing the parent directory makes the
//!   chunk dir entries themselves durable - without it, file contents
//!   survive a power cut but the dir entries can vanish, leaving
//!   orphan inodes (this is what databases like postgres and sqlite
//!   do). The timing includes waiting for the storage device to
//!   acknowledge both. Matches a durability-sensitive workload that
//!   needs the data persisted before continuing. Throughput
//!   denominator is the output file byte count, taken by stat'ing the
//!   chunk files after a one-off pre-write.
//!
//! Cleanup of the chunk directory between iterations is NOT included
//! in the timed window: `fresh_dir` in the next iteration's setup
//! wipes the previous run via `remove_dir_all`, which runs in
//! `iter_batched`'s setup phase outside the measurement. The final
//! iteration's directory is reaped at the bottom of each bench fn.
//!
//! Different work being timed, different denominators. The numbers
//! aren't meant to be cross-compared except to see the cost of
//! durability. Read benches use the physical denominator since that
//! is what flows from disk into memory per second.
//!
//! ## Why Linux-only
//!
//! For the read benchmarks to mean "open files I didn't just write" -
//! not "decode files still warm in the kernel page cache from the write
//! that ran milliseconds ago" - every chunk file is evicted from the
//! page cache via `posix_fadvise(POSIX_FADV_DONTNEED)` immediately
//! before each measured iteration. The eviction call runs as
//! `iter_batched` setup so it does not count toward the timed window,
//! and `BatchSize::PerIteration` makes sure each iteration starts cold
//! (otherwise the first read in a batch would warm the cache for the
//! rest). `posix_fadvise` is the Linux interface for this; macOS and
//! Windows have no portable equivalent that gives the same guarantee.
//! `libc` is wired in as a dev-dependency only on Linux, so attempting
//! to build this bench elsewhere is an unambiguous signal that this
//! workload isn't supported there.

use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use lightstream::traits::chunked_table_reader::ChunkedTableReader;
use lightstream::traits::chunked_table_writer::ChunkedTableWriter;

mod bench_helpers;
use bench_helpers::{BENCH_ROWS, logical_payload_bytes, make_bench_table};

const N_CHUNKS: usize = 32;
const BASE: &str = "chunk";

fn fresh_dir(suffix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("lightstream_chunked_bench_{suffix}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Sum the size on disk of every regular file directly under `dir`.
/// Used once per format to derive the real physical byte count of a
/// fully-written chunk set, so the bench can report throughput against
/// both the Arrow logical denominator and the actual physical
/// denominator side by side - no surrogate, no guess.
fn physical_bytes(dir: &Path) -> u64 {
    let mut total: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                }
            }
        }
    }
    total
}

fn total_payload_bytes() -> u64 {
    logical_payload_bytes(BENCH_ROWS) * N_CHUNKS as u64
}

/// Hint the kernel to drop every chunk file in `dir` from the page
/// cache so the next read goes to disk. Best-effort: errors are
/// ignored. Used as `iter_batched` setup so the syscall cost stays out
/// of the timed window.
fn evict_pages(dir: &Path) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        if let Ok(f) = std::fs::File::open(entry.path()) {
            // POSIX_FADV_DONTNEED is advisory; ignore the return.
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
        }
    }
}

fn bench_chunked_arrow(c: &mut Criterion) {
    use lightstream::models::readers::chunked::arrow::ChunkedArrowReader;
    use lightstream::models::writers::chunked::arrow::ChunkedArrowWriter;
    use minarrow::Table;

    // 32 distinct tables in distinct memory so the encoder cannot
    // unrealistically benefit from L2/L3 cache residency.
    let tables: Vec<Table> = (0..N_CHUNKS)
        .map(|_| make_bench_table(BENCH_ROWS))
        .collect();
    let table_refs: Vec<&Table> = tables.iter().collect();

    // Pre-write the dataset once to learn the real physical byte count,
    // which is reported alongside the logical (in-memory) byte count.
    let read_dir = fresh_dir("arrow_read");
    {
        let mut w = ChunkedArrowWriter::new(&read_dir, BASE).unwrap();
        for t in &tables {
            w.write_chunk(t).unwrap();
        }
    }
    let logical_bytes = total_payload_bytes();
    let physical_bytes = physical_bytes(&read_dir);

    let mut group = c.benchmark_group("chunked_arrow");
    group.sample_size(10);

    // `_logical`: process completion time. Encode + hand bytes to the
    // kernel and return. Denominator = source bytes.
    // `_physical`: durable on-disk time. Same encode + write, then
    // fsync every chunk file AND the parent directory so both the
    // file contents and the directory entries are durable before the
    // timed window ends. Denominator = output file bytes.
    //
    // Cleanup of the chunk dir is NOT in the timed region: `fresh_dir`
    // in the next iteration's setup wipes it via `remove_dir_all`. The
    // final iteration's dir is reaped at the end of this fn.
    let write_serial_logical = |dir: PathBuf| {
        let mut w = ChunkedArrowWriter::new(&dir, BASE).unwrap();
        for t in &tables {
            w.write_chunk(t).unwrap();
        }
    };
    let write_serial_physical = |dir: PathBuf| {
        let mut w = ChunkedArrowWriter::new(&dir, BASE).unwrap();
        let mut paths: Vec<PathBuf> = Vec::with_capacity(N_CHUNKS);
        for t in &tables {
            paths.push(w.write_chunk(t).unwrap());
        }
        for p in &paths {
            std::fs::File::open(p).unwrap().sync_all().unwrap();
        }
        // Fsync the parent dir so the chunk dir entries themselves are
        // durable - without this, file contents persist but the dir
        // entries can vanish on power loss, leaving orphan inodes.
        std::fs::File::open(&dir).unwrap().sync_all().unwrap();
    };

    group.throughput(Throughput::Bytes(logical_bytes));
    group.bench_function("write_logical", |b| {
        b.iter_batched(
            || fresh_dir("arrow_write"),
            &write_serial_logical,
            criterion::BatchSize::PerIteration,
        );
    });
    group.throughput(Throughput::Bytes(physical_bytes));
    group.bench_function("write_physical", |b| {
        b.iter_batched(
            || fresh_dir("arrow_write"),
            &write_serial_physical,
            criterion::BatchSize::PerIteration,
        );
    });

    let par_write_logical = |dir: PathBuf| {
        let w = ChunkedArrowWriter::new(&dir, BASE).unwrap();
        let paths = w.par_write_all(&table_refs, None).unwrap();
        assert_eq!(paths.len(), N_CHUNKS);
    };
    let par_write_physical = |dir: PathBuf| {
        let w = ChunkedArrowWriter::new(&dir, BASE).unwrap();
        let paths = w.par_write_all(&table_refs, None).unwrap();
        assert_eq!(paths.len(), N_CHUNKS);
        for p in &paths {
            std::fs::File::open(p).unwrap().sync_all().unwrap();
        }
        std::fs::File::open(&dir).unwrap().sync_all().unwrap();
    };

    group.throughput(Throughput::Bytes(logical_bytes));
    group.bench_function("par_write_logical", |b| {
        b.iter_batched(
            || fresh_dir("arrow_par_write"),
            &par_write_logical,
            criterion::BatchSize::PerIteration,
        );
    });
    group.throughput(Throughput::Bytes(physical_bytes));
    group.bench_function("par_write_physical", |b| {
        b.iter_batched(
            || fresh_dir("arrow_par_write"),
            &par_write_physical,
            criterion::BatchSize::PerIteration,
        );
    });

    // Read benches use the physical denominator since that's what
    // actually flows from disk into memory per second.
    group.throughput(Throughput::Bytes(physical_bytes));

    group.bench_function("serial_read_all", |b| {
        b.iter_batched(
            || evict_pages(&read_dir),
            |_| {
                let r = ChunkedArrowReader::open(&read_dir, BASE, ()).unwrap();
                let st = r.load_batched().unwrap();
                assert_eq!(st.batches.len(), N_CHUNKS);
                assert_eq!(st.n_rows, N_CHUNKS * BENCH_ROWS);
                std::hint::black_box(st);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("par_load_batched", |b| {
        b.iter_batched(
            || evict_pages(&read_dir),
            |_| {
                let st = ChunkedArrowReader::par_load_batched(&read_dir, BASE, (), None).unwrap();
                assert_eq!(st.batches.len(), N_CHUNKS);
                assert_eq!(st.n_rows, N_CHUNKS * BENCH_ROWS);
                std::hint::black_box(st);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    // Reap the write benches' final-iteration directories alongside
    // the read benches' directory now that all timed work is done.
    cleanup(&read_dir);
    cleanup(&std::env::temp_dir().join("lightstream_chunked_bench_arrow_write"));
    cleanup(&std::env::temp_dir().join("lightstream_chunked_bench_arrow_par_write"));
    group.finish();
}

#[cfg(feature = "parquet")]
fn bench_chunked_parquet(c: &mut Criterion) {
    use lightstream::models::readers::chunked::parquet::ChunkedParquetReader;
    use lightstream::models::writers::chunked::parquet::ChunkedParquetWriter;
    use minarrow::Table;

    // 32 distinct tables in distinct memory so the encoder cannot
    // unrealistically benefit from L2/L3 cache residency.
    let tables: Vec<Table> = (0..N_CHUNKS)
        .map(|_| make_bench_table(BENCH_ROWS))
        .collect();
    let table_refs: Vec<&Table> = tables.iter().collect();

    // Pre-write the dataset once to learn the real physical byte count.
    let read_dir = fresh_dir("parquet_read");
    {
        let mut w = ChunkedParquetWriter::new(&read_dir, BASE, None).unwrap();
        for t in &tables {
            w.write_chunk(t).unwrap();
        }
    }
    let logical_bytes = total_payload_bytes();
    let physical_bytes = physical_bytes(&read_dir);

    let mut group = c.benchmark_group("chunked_parquet");
    group.sample_size(10);

    // `_logical`: encode + write, no fsync. Denominator = source bytes.
    // `_physical`: encode + write + fsync per chunk + fsync the parent
    // dir. Denominator = output file bytes. Cleanup of the chunk dir
    // is NOT in the timed region.
    let write_serial_logical = |dir: PathBuf| {
        let mut w = ChunkedParquetWriter::new(&dir, BASE, None).unwrap();
        for t in &tables {
            w.write_chunk(t).unwrap();
        }
    };
    let write_serial_physical = |dir: PathBuf| {
        let mut w = ChunkedParquetWriter::new(&dir, BASE, None).unwrap();
        let mut paths: Vec<PathBuf> = Vec::with_capacity(N_CHUNKS);
        for t in &tables {
            paths.push(w.write_chunk(t).unwrap());
        }
        for p in &paths {
            std::fs::File::open(p).unwrap().sync_all().unwrap();
        }
        // Fsync the parent dir so the chunk dir entries themselves are durable.
        std::fs::File::open(&dir).unwrap().sync_all().unwrap();
    };

    group.throughput(Throughput::Bytes(logical_bytes));
    group.bench_function("write_logical", |b| {
        b.iter_batched(
            || fresh_dir("parquet_write"),
            &write_serial_logical,
            criterion::BatchSize::PerIteration,
        );
    });
    group.throughput(Throughput::Bytes(physical_bytes));
    group.bench_function("write_physical", |b| {
        b.iter_batched(
            || fresh_dir("parquet_write"),
            &write_serial_physical,
            criterion::BatchSize::PerIteration,
        );
    });

    let par_write_logical = |dir: PathBuf| {
        let w = ChunkedParquetWriter::new(&dir, BASE, None).unwrap();
        let paths = w.par_write_all(&table_refs, None).unwrap();
        assert_eq!(paths.len(), N_CHUNKS);
    };
    let par_write_physical = |dir: PathBuf| {
        let w = ChunkedParquetWriter::new(&dir, BASE, None).unwrap();
        let paths = w.par_write_all(&table_refs, None).unwrap();
        assert_eq!(paths.len(), N_CHUNKS);
        for p in &paths {
            std::fs::File::open(p).unwrap().sync_all().unwrap();
        }
        std::fs::File::open(&dir).unwrap().sync_all().unwrap();
    };

    group.throughput(Throughput::Bytes(logical_bytes));
    group.bench_function("par_write_logical", |b| {
        b.iter_batched(
            || fresh_dir("parquet_par_write"),
            &par_write_logical,
            criterion::BatchSize::PerIteration,
        );
    });
    group.throughput(Throughput::Bytes(physical_bytes));
    group.bench_function("par_write_physical", |b| {
        b.iter_batched(
            || fresh_dir("parquet_par_write"),
            &par_write_physical,
            criterion::BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Bytes(physical_bytes));

    group.bench_function("serial_read_all", |b| {
        b.iter_batched(
            || evict_pages(&read_dir),
            |_| {
                let r = ChunkedParquetReader::open(&read_dir, BASE, ()).unwrap();
                let st = r.load_batched().unwrap();
                assert_eq!(st.batches.len(), N_CHUNKS);
                assert_eq!(st.n_rows, N_CHUNKS * BENCH_ROWS);
                std::hint::black_box(st);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("par_load_batched", |b| {
        b.iter_batched(
            || evict_pages(&read_dir),
            |_| {
                let st = ChunkedParquetReader::par_load_batched(&read_dir, BASE, (), None).unwrap();
                assert_eq!(st.batches.len(), N_CHUNKS);
                assert_eq!(st.n_rows, N_CHUNKS * BENCH_ROWS);
                std::hint::black_box(st);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    cleanup(&read_dir);
    cleanup(&std::env::temp_dir().join("lightstream_chunked_bench_parquet_write"));
    cleanup(&std::env::temp_dir().join("lightstream_chunked_bench_parquet_par_write"));
    group.finish();
}

#[cfg(feature = "csv")]
fn bench_chunked_csv(c: &mut Criterion) {
    use lightstream::models::decoders::csv::CsvDecodeOptions;
    use lightstream::models::encoders::csv::CsvEncodeOptions;
    use lightstream::models::readers::chunked::csv::{ChunkedCsvReadOptions, ChunkedCsvReader};
    use lightstream::models::writers::chunked::csv::ChunkedCsvWriter;
    use minarrow::Table;

    // 32 distinct tables in distinct memory so the encoder cannot
    // unrealistically benefit from L2/L3 cache residency.
    let tables: Vec<Table> = (0..N_CHUNKS)
        .map(|_| make_bench_table(BENCH_ROWS))
        .collect();
    let table_refs: Vec<&Table> = tables.iter().collect();

    // Pre-write the dataset once to learn the real physical byte count.
    let read_dir = fresh_dir("csv_read");
    {
        let mut w = ChunkedCsvWriter::new(&read_dir, BASE, CsvEncodeOptions::default()).unwrap();
        for t in &tables {
            w.write_chunk(t).unwrap();
        }
    }
    let logical_bytes = total_payload_bytes();
    let physical_bytes = physical_bytes(&read_dir);

    let mut group = c.benchmark_group("chunked_csv");
    group.sample_size(10);

    // `_logical`: encode + write, no fsync. Denominator = source bytes.
    // `_physical`: encode + write + fsync per chunk + fsync the parent
    // dir. Denominator = output file bytes. Cleanup of the chunk dir
    // is NOT in the timed region.
    let write_serial_logical = |dir: PathBuf| {
        let mut w = ChunkedCsvWriter::new(&dir, BASE, CsvEncodeOptions::default()).unwrap();
        for t in &tables {
            w.write_chunk(t).unwrap();
        }
    };
    let write_serial_physical = |dir: PathBuf| {
        let mut w = ChunkedCsvWriter::new(&dir, BASE, CsvEncodeOptions::default()).unwrap();
        let mut paths: Vec<PathBuf> = Vec::with_capacity(N_CHUNKS);
        for t in &tables {
            paths.push(w.write_chunk(t).unwrap());
        }
        for p in &paths {
            std::fs::File::open(p).unwrap().sync_all().unwrap();
        }
        // Fsync the parent dir so the chunk dir entries themselves are durable.
        std::fs::File::open(&dir).unwrap().sync_all().unwrap();
    };

    group.throughput(Throughput::Bytes(logical_bytes));
    group.bench_function("write_logical", |b| {
        b.iter_batched(
            || fresh_dir("csv_write"),
            &write_serial_logical,
            criterion::BatchSize::PerIteration,
        );
    });
    group.throughput(Throughput::Bytes(physical_bytes));
    group.bench_function("write_physical", |b| {
        b.iter_batched(
            || fresh_dir("csv_write"),
            &write_serial_physical,
            criterion::BatchSize::PerIteration,
        );
    });

    let par_write_logical = |dir: PathBuf| {
        let w = ChunkedCsvWriter::new(&dir, BASE, CsvEncodeOptions::default()).unwrap();
        let paths = w.par_write_all(&table_refs, None).unwrap();
        assert_eq!(paths.len(), N_CHUNKS);
    };
    let par_write_physical = |dir: PathBuf| {
        let w = ChunkedCsvWriter::new(&dir, BASE, CsvEncodeOptions::default()).unwrap();
        let paths = w.par_write_all(&table_refs, None).unwrap();
        assert_eq!(paths.len(), N_CHUNKS);
        for p in &paths {
            std::fs::File::open(p).unwrap().sync_all().unwrap();
        }
        std::fs::File::open(&dir).unwrap().sync_all().unwrap();
    };

    group.throughput(Throughput::Bytes(logical_bytes));
    group.bench_function("par_write_logical", |b| {
        b.iter_batched(
            || fresh_dir("csv_par_write"),
            &par_write_logical,
            criterion::BatchSize::PerIteration,
        );
    });
    group.throughput(Throughput::Bytes(physical_bytes));
    group.bench_function("par_write_physical", |b| {
        b.iter_batched(
            || fresh_dir("csv_par_write"),
            &par_write_physical,
            criterion::BatchSize::PerIteration,
        );
    });

    group.throughput(Throughput::Bytes(physical_bytes));

    let read_opts = || ChunkedCsvReadOptions {
        decode: CsvDecodeOptions::default(),
        // Match the chunk size so each file resolves into one Table on
        // the reader side without further internal splitting.
        batch_size: BENCH_ROWS,
    };

    group.bench_function("serial_read_all", |b| {
        b.iter_batched(
            || evict_pages(&read_dir),
            |_| {
                let r = ChunkedCsvReader::open(&read_dir, BASE, read_opts()).unwrap();
                let st = r.load_batched().unwrap();
                assert_eq!(st.batches.len(), N_CHUNKS);
                assert_eq!(st.n_rows, N_CHUNKS * BENCH_ROWS);
                std::hint::black_box(st);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    group.bench_function("par_load_batched", |b| {
        b.iter_batched(
            || evict_pages(&read_dir),
            |_| {
                let st =
                    ChunkedCsvReader::par_load_batched(&read_dir, BASE, read_opts(), None).unwrap();
                assert_eq!(st.batches.len(), N_CHUNKS);
                assert_eq!(st.n_rows, N_CHUNKS * BENCH_ROWS);
                std::hint::black_box(st);
            },
            criterion::BatchSize::PerIteration,
        );
    });

    cleanup(&read_dir);
    cleanup(&std::env::temp_dir().join("lightstream_chunked_bench_csv_write"));
    cleanup(&std::env::temp_dir().join("lightstream_chunked_bench_csv_par_write"));
    group.finish();
}

#[cfg(all(feature = "parquet", feature = "csv"))]
criterion_group!(
    benches,
    bench_chunked_arrow,
    bench_chunked_parquet,
    bench_chunked_csv
);
#[cfg(all(feature = "parquet", not(feature = "csv")))]
criterion_group!(benches, bench_chunked_arrow, bench_chunked_parquet);
#[cfg(all(not(feature = "parquet"), feature = "csv"))]
criterion_group!(benches, bench_chunked_arrow, bench_chunked_csv);
#[cfg(all(not(feature = "parquet"), not(feature = "csv")))]
criterion_group!(benches, bench_chunked_arrow);
criterion_main!(benches);
