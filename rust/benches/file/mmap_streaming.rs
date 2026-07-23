// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks memory-mapped streaming with cold and warm page-cache states.
//! Linux only.
//!
//! Each reader sums the `values` (`f64`) column across the benchmark file.
//!
//! - `_cold` evicts the file from the page cache before each iteration.
//! - `_warm` primes the page cache before measurement.
//!
//! The difference between cold and warm timings approximates storage and
//! page-fault overhead. Warm timings measure decoding and aggregation with the
//! file resident in memory.
//!
//! ## Column projection
//!
//! Readers that support column projection also include a `_projected` variant
//! that reads only the `values` column:
//!
//! - Polars uses `IpcReader::memory_mapped(None).with_columns(["values"])`.
//! - Arrow-rs uses `FileReader::try_new(file, Some(vec![1]))`.
//! - Lightstream uses `MmapTableReader::read_batch_cols` and
//!   `FileTableReader::read_batch_cols`.
//!
//! Projected results should only be compared with other projected results,
//! because the amount of data read differs from the full-batch variants.
//!
//! ## Benchmark file
//!
//! `LIGHTSTREAM_MMAP_BENCH_DIR` sets the file directory. The default is
//! `/var/tmp/lightstream_mmap_bench`. `/tmp` is avoided because it is commonly
//! mounted as `tmpfs`, where cache eviction produces no cold storage read.
//!
//! `LIGHTSTREAM_MMAP_BENCH_SIZE_GIB` sets the file size in GiB and defaults to
//! 2. An existing file is reused when its size matches the requested size.
//!
//! The file is deleted after the benchmarks complete. Set
//! `LIGHTSTREAM_MMAP_BENCH_CLEANUP=false` to retain it.

// Requires Linux `posix_fadvise` support and the `mmap` Cargo feature.

#[path = "../common/bench_helpers.rs"]
mod bench_helpers;
use bench_helpers::{BenchShape, logical_payload_bytes_shape, make_bench_table_shape};

use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::OnceLock;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use lightstream::enums::IPCMessageProtocol;
use lightstream::models::readers::ipc::file_table::FileTableReader;
use lightstream::models::readers::ipc::mmap_table::MmapTableReader;
use lightstream::models::writers::ipc::table::TableWriter;
use minarrow::{Array, NumericArray, Table};

const SHAPE: BenchShape = BenchShape::Mixed;
const ROWS_PER_BATCH: usize = 100_000;
const DEFAULT_FILE_SIZE_GIB: usize = 2;

// Index of the `values` column in the `Mixed` shape.
const VALUES_COL_INDEX: usize = 1;
const VALUES_COL_NAME: &str = "values";

// Shared benchmark file state, initialised once per process.
static FILE_STATE: OnceLock<FileState> = OnceLock::new();

struct FileState {
    path: PathBuf,
    n_batches: usize,
    bytes_per_batch: u64,
}

fn bench_mmap_streaming(c: &mut Criterion) {
    #[cfg(feature = "bench_logging")]
    let _ = env_logger::try_init();

    let state = FILE_STATE.get_or_init(generate_file);

    let mut group = c.benchmark_group(format!(
        "mmap_streaming_sum_{}_{}GiB",
        SHAPE.label(),
        target_file_size_gib()
    ));
    group.throughput(Throughput::Bytes(
        state.bytes_per_batch * state.n_batches as u64,
    ));
    group.sample_size(10);

    let path = state.path.clone();
    let n_batches = state.n_batches;

    // ---- lightstream mmap, full batch -----------------------------------------
    group.bench_function("lightstream_mmap_cold", |b| {
        b.iter_batched(
            || evict_file_cache(&path),
            |_| {
                let s = sum_lightstream_mmap(&path, n_batches);
                std::hint::black_box(s);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.bench_function("lightstream_mmap_warm", |b| {
        // Prime the page cache once outside the timed region.
        std::hint::black_box(sum_lightstream_mmap(&path, n_batches));
        b.iter(|| {
            let s = sum_lightstream_mmap(&path, n_batches);
            std::hint::black_box(s);
        });
    });

    // ---- lightstream file -----------------------------------------------------
    group.bench_function("lightstream_file_cold", |b| {
        b.iter_batched(
            || evict_file_cache(&path),
            |_| {
                let s = sum_lightstream_file(&path, n_batches);
                std::hint::black_box(s);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.bench_function("lightstream_file_warm", |b| {
        std::hint::black_box(sum_lightstream_file(&path, n_batches));
        b.iter(|| {
            let s = sum_lightstream_file(&path, n_batches);
            std::hint::black_box(s);
        });
    });

    // ---- lightstream mmap and file, projected to the `values` column ----------
    group.bench_function("lightstream_mmap_projected_cold", |b| {
        b.iter_batched(
            || evict_file_cache(&path),
            |_| {
                let s = sum_lightstream_mmap_projected(&path, n_batches);
                std::hint::black_box(s);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.bench_function("lightstream_mmap_projected_warm", |b| {
        std::hint::black_box(sum_lightstream_mmap_projected(&path, n_batches));
        b.iter(|| {
            let s = sum_lightstream_mmap_projected(&path, n_batches);
            std::hint::black_box(s);
        });
    });
    group.bench_function("lightstream_file_projected_cold", |b| {
        b.iter_batched(
            || evict_file_cache(&path),
            |_| {
                let s = sum_lightstream_file_projected(&path, n_batches);
                std::hint::black_box(s);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.bench_function("lightstream_file_projected_warm", |b| {
        std::hint::black_box(sum_lightstream_file_projected(&path, n_batches));
        b.iter(|| {
            let s = sum_lightstream_file_projected(&path, n_batches);
            std::hint::black_box(s);
        });
    });

    // ---- arrow-rs FileReader, no projection -----------------------------------
    #[cfg(feature = "bench_arrow")]
    {
        group.bench_function("arrow_rs_file_cold", |b| {
            b.iter_batched(
                || evict_file_cache(&path),
                |_| {
                    let s = sum_arrow_rs_file(&path, None);
                    std::hint::black_box(s);
                },
                criterion::BatchSize::PerIteration,
            );
        });
        group.bench_function("arrow_rs_file_warm", |b| {
            std::hint::black_box(sum_arrow_rs_file(&path, None));
            b.iter(|| {
                let s = sum_arrow_rs_file(&path, None);
                std::hint::black_box(s);
            });
        });

        // ---- arrow-rs FileReader, projected to the `values` column ---------
        group.bench_function("arrow_rs_file_projected_cold", |b| {
            b.iter_batched(
                || evict_file_cache(&path),
                |_| {
                    let s = sum_arrow_rs_file(&path, Some(vec![VALUES_COL_INDEX]));
                    std::hint::black_box(s);
                },
                criterion::BatchSize::PerIteration,
            );
        });
        group.bench_function("arrow_rs_file_projected_warm", |b| {
            std::hint::black_box(sum_arrow_rs_file(&path, Some(vec![VALUES_COL_INDEX])));
            b.iter(|| {
                let s = sum_arrow_rs_file(&path, Some(vec![VALUES_COL_INDEX]));
                std::hint::black_box(s);
            });
        });
    }

    // ---- polars mmap, no projection -------------------------------------------
    #[cfg(feature = "bench_polars")]
    {
        group.bench_function("polars_mmap_cold", |b| {
            b.iter_batched(
                || evict_file_cache(&path),
                |_| {
                    let s = sum_polars_mmap(&path, None);
                    std::hint::black_box(s);
                },
                criterion::BatchSize::PerIteration,
            );
        });
        group.bench_function("polars_mmap_warm", |b| {
            std::hint::black_box(sum_polars_mmap(&path, None));
            b.iter(|| {
                let s = sum_polars_mmap(&path, None);
                std::hint::black_box(s);
            });
        });

        // ---- polars mmap, projected to the `values` column -----------------
        group.bench_function("polars_mmap_projected_cold", |b| {
            b.iter_batched(
                || evict_file_cache(&path),
                |_| {
                    let s = sum_polars_mmap(&path, Some(vec![VALUES_COL_NAME.to_string()]));
                    std::hint::black_box(s);
                },
                criterion::BatchSize::PerIteration,
            );
        });
        group.bench_function("polars_mmap_projected_warm", |b| {
            std::hint::black_box(sum_polars_mmap(
                &path,
                Some(vec![VALUES_COL_NAME.to_string()]),
            ));
            b.iter(|| {
                let s = sum_polars_mmap(&path, Some(vec![VALUES_COL_NAME.to_string()]));
                std::hint::black_box(s);
            });
        });
    }

    // ---- Decode-only benchmarks (no sum) to localise the warm decode gap -----
    // These benchmarks iterate every batch and `black_box` the column slice
    // without summing it. Subtracting from the corresponding `_warm`
    // sum benchmark isolates the per-batch decode + buffer-setup cost from
    // the f64 add loop. Cold variants are intentionally not included;
    // page-fault cost is already isolated by the cold-warm diff on
    // the sum benchmarks.
    group.bench_function("lightstream_mmap_decode_only_warm", |b| {
        std::hint::black_box(decode_only_lightstream_mmap(&path, n_batches));
        b.iter(|| {
            let n = decode_only_lightstream_mmap(&path, n_batches);
            std::hint::black_box(n);
        });
    });
    group.bench_function("lightstream_file_decode_only_warm", |b| {
        std::hint::black_box(decode_only_lightstream_file(&path, n_batches));
        b.iter(|| {
            let n = decode_only_lightstream_file(&path, n_batches);
            std::hint::black_box(n);
        });
    });
    #[cfg(feature = "bench_arrow")]
    group.bench_function("arrow_rs_file_decode_only_warm", |b| {
        std::hint::black_box(decode_only_arrow_rs_file(&path));
        b.iter(|| {
            let n = decode_only_arrow_rs_file(&path);
            std::hint::black_box(n);
        });
    });

    group.finish();

    // ---- Post-run cleanup ----------------------------------------------------
    if cleanup_on_exit() {
        eprintln!(
            "[mmap_streaming] removing bench file at {} (LIGHTSTREAM_MMAP_BENCH_CLEANUP=true)",
            path.display()
        );
        let _ = std::fs::remove_file(&path);
    } else {
        eprintln!(
            "[mmap_streaming] keeping bench file at {} (LIGHTSTREAM_MMAP_BENCH_CLEANUP=false)",
            path.display()
        );
    }
}

fn cleanup_on_exit() -> bool {
    match std::env::var("LIGHTSTREAM_MMAP_BENCH_CLEANUP")
        .ok()
        .as_deref()
    {
        Some("false") | Some("0") | Some("no") => false,
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Per-reader sum kernels. Each iterates every batch in the file and
// returns the sum of the `values` f64 column. Reading every byte of
// that column forces page faults in the `_cold` benchmarks.
// ---------------------------------------------------------------------------

fn sum_lightstream_mmap(path: &PathBuf, n_batches: usize) -> f64 {
    let reader = MmapTableReader::open(path).unwrap();
    let mut sum = 0.0f64;
    for i in 0..n_batches {
        let batch = reader.read_batch(i).unwrap();
        sum += sum_values_column(&batch, VALUES_COL_INDEX);
    }
    sum
}

fn sum_lightstream_file(path: &PathBuf, n_batches: usize) -> f64 {
    let reader = FileTableReader::open(path).unwrap();
    let mut sum = 0.0f64;
    for i in 0..n_batches {
        let batch = reader.read_batch(i).unwrap();
        sum += sum_values_column(&batch, VALUES_COL_INDEX);
    }
    sum
}

// Projected variants read only the `values` column, which lands at index 0
// of the projected batch.

fn sum_lightstream_mmap_projected(path: &PathBuf, n_batches: usize) -> f64 {
    let reader = MmapTableReader::open(path).unwrap();
    let mut sum = 0.0f64;
    for i in 0..n_batches {
        let batch = reader.read_batch_cols(i, &[VALUES_COL_NAME]).unwrap();
        sum += sum_values_column(&batch, 0);
    }
    sum
}

fn sum_lightstream_file_projected(path: &PathBuf, n_batches: usize) -> f64 {
    let reader = FileTableReader::open(path).unwrap();
    let mut sum = 0.0f64;
    for i in 0..n_batches {
        let batch = reader.read_batch_cols(i, &[VALUES_COL_NAME]).unwrap();
        sum += sum_values_column(&batch, 0);
    }
    sum
}

fn sum_values_column(batch: &Table, col: usize) -> f64 {
    if let Array::NumericArray(NumericArray::Float64(arr)) = &batch.cols[col].array {
        let mut acc = 0.0f64;
        for v in arr.data.iter() {
            acc += *v;
        }
        acc
    } else {
        panic!("expected Float64 array at col {col}");
    }
}

#[cfg(feature = "bench_arrow")]
fn sum_arrow_rs_file(path: &PathBuf, projection: Option<Vec<usize>>) -> f64 {
    use arrow::array::Float64Array;
    use arrow::ipc::reader::FileReader;
    use std::fs::File as StdFile;

    let file = StdFile::open(path).unwrap();
    // When projection is Some([1]), the produced batches have a single
    // column at index 0; when None, the values column stays at its
    // original index 1.
    let target_col = if projection.is_some() {
        0
    } else {
        VALUES_COL_INDEX
    };
    let reader = FileReader::try_new(file, projection).unwrap();
    let mut sum = 0.0f64;
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.column(target_col);
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        for v in arr.values().iter() {
            sum += *v;
        }
    }
    sum
}

// Decode-only kernels: iterate every batch, touch the column slice
// via black_box, do not sum. Used to localise the per-batch decode
// cost from the f64 add loop in the corresponding `_warm` sum benchmarks.

fn decode_only_lightstream_mmap(path: &PathBuf, n_batches: usize) -> u64 {
    let reader = MmapTableReader::open(path).unwrap();
    let mut touched = 0u64;
    for i in 0..n_batches {
        let batch = reader.read_batch(i).unwrap();
        if let Array::NumericArray(NumericArray::Float64(arr)) = &batch.cols[VALUES_COL_INDEX].array
        {
            std::hint::black_box(arr.data.as_ref());
            touched += arr.data.len() as u64;
        }
    }
    touched
}

fn decode_only_lightstream_file(path: &PathBuf, n_batches: usize) -> u64 {
    let reader = FileTableReader::open(path).unwrap();
    let mut touched = 0u64;
    for i in 0..n_batches {
        let batch = reader.read_batch(i).unwrap();
        if let Array::NumericArray(NumericArray::Float64(arr)) = &batch.cols[VALUES_COL_INDEX].array
        {
            std::hint::black_box(arr.data.as_ref());
            touched += arr.data.len() as u64;
        }
    }
    touched
}

#[cfg(feature = "bench_arrow")]
fn decode_only_arrow_rs_file(path: &PathBuf) -> u64 {
    use arrow::array::Float64Array;
    use arrow::ipc::reader::FileReader;
    use std::fs::File as StdFile;

    let file = StdFile::open(path).unwrap();
    let reader = FileReader::try_new(file, None).unwrap();
    let mut touched = 0u64;
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.column(VALUES_COL_INDEX);
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        std::hint::black_box(arr.values());
        touched += arr.values().len() as u64;
    }
    touched
}

#[cfg(feature = "bench_polars")]
fn sum_polars_mmap(path: &PathBuf, projection: Option<Vec<String>>) -> f64 {
    use polars::prelude::*;

    let file = std::fs::File::open(path).unwrap();
    let mut reader = IpcReader::new(file).memory_mapped(None);
    if let Some(cols) = projection {
        reader = reader.with_columns(Some(cols));
    }
    let df = reader.finish().unwrap();
    df.column(VALUES_COL_NAME)
        .unwrap()
        .f64()
        .unwrap()
        .sum()
        .unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// Bench file generation
// ---------------------------------------------------------------------------

fn generate_file() -> FileState {
    let dir = std::env::var("LIGHTSTREAM_MMAP_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/tmp/lightstream_mmap_bench"));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!(
        "{}_{}GiB.arrow",
        SHAPE.label(),
        target_file_size_gib()
    ));

    let template = make_bench_table_shape(SHAPE, ROWS_PER_BATCH);
    let bytes_per_batch = logical_payload_bytes_shape(SHAPE, ROWS_PER_BATCH, 1);
    let target_bytes = (target_file_size_gib() as u64) * (1 << 30);
    let n_batches = ((target_bytes + bytes_per_batch - 1) / bytes_per_batch) as usize;

    // Reuse the existing file when it covers the active
    // `LIGHTSTREAM_MMAP_BENCH_SIZE_GIB`. This keeps multi-GiB writes
    // from happening on every `cargo bench`. The file name encodes the
    // target size, so a different size setting resolves to its own file.
    // Encoded files carry framing overhead above the logical payload,
    // so the check is at-least-target rather than exact.
    if let Ok(meta) = std::fs::metadata(&path) {
        if meta.len() >= target_bytes {
            eprintln!(
                "[mmap_streaming] reusing existing {} GiB bench file at {}",
                target_file_size_gib(),
                path.display()
            );
            return FileState {
                path,
                n_batches,
                bytes_per_batch,
            };
        }
        // Existing path is smaller than the target - remove it before
        // writing a fresh one so we never leave two multi-GiB files
        // sitting next to each other.
        eprintln!(
            "[mmap_streaming] existing file at {} is {} bytes (wanted at least {}); removing before regen",
            path.display(),
            meta.len(),
            target_bytes
        );
        let _ = std::fs::remove_file(&path);
    }

    eprintln!(
        "[mmap_streaming] generating {} GiB bench file at {}",
        target_file_size_gib(),
        path.display()
    );
    let schema = template.schema().iter().map(|f| (**f).clone()).collect();
    let tables: Vec<Table> = (0..n_batches).map(|_| template.clone()).collect();
    let dict_regs = SHAPE.dictionary_registrations();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let file = tokio::fs::File::create(&path).await.unwrap();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File, None).unwrap();
        for (id, values) in dict_regs {
            writer.register_dictionary(id, values);
        }
        writer.write_all_tables(tables).await.unwrap();
    });

    FileState {
        path,
        n_batches,
        bytes_per_batch,
    }
}

fn target_file_size_gib() -> usize {
    std::env::var("LIGHTSTREAM_MMAP_BENCH_SIZE_GIB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_FILE_SIZE_GIB)
}

fn evict_file_cache(path: &PathBuf) {
    let f = std::fs::File::open(path).unwrap();
    // SAFETY: posix_fadvise on an open fd with valid offset/len is safe.
    unsafe {
        libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
    }
}

criterion_group!(benches, bench_mmap_streaming);
criterion_main!(benches);
