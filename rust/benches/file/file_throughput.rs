// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Benchmarks Arrow IPC file read and write throughput.
//!
//! Measures writes to a temporary file, standard file reads and memory-mapped
//! reads as separate Criterion benchmarks.
//!
//! Read benchmarks follow a warm/cold naming convention. Warm reads run
//! with the file resident in the page cache, so they measure the decode
//! path plus the memory recall (which is generally faster for mmap than
//! standard page cache reads), rather than the time to recall the bytes
//! from disk. Cold reads evict the page cache before every iteration,
//! so they measure fresh reads off the disk.

use std::sync::Arc;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use lightstream::models::readers::ipc::file_table::FileTableReader;
use lightstream::models::writers::ipc::table::write_tables_to_file;
use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, Table, TextArray,
    Vec64, arr_f64, arr_i32, arr_str32, ffi::arrow_dtype::CategoricalIndexType,
};
use tempfile::NamedTempFile;

const BENCH_ROWS: usize = 100_000;
const BENCH_BATCHES: usize = 10;

fn make_bench_table(n_rows: usize) -> Table {
    let ids: Vec64<i32> = (0..n_rows as i32).collect();
    let values: Vec64<f64> = (0..n_rows).map(|i| i as f64 * 0.5).collect();
    let labels: Vec64<String> = (0..n_rows).map(|i| format!("row_{}", i)).collect();
    let label_refs: Vec64<&str> = labels.iter().map(String::as_str).collect();

    let id_col = FieldArray::from_arr("ids", arr_i32!(ids));
    let value_col = FieldArray::from_arr("values", arr_f64!(values));
    let label_col = FieldArray::from_arr("labels", arr_str32!(label_refs));

    #[cfg(not(feature = "default_categorical_8"))]
    let dict_col = {
        let indices: Vec64<u32> = (0..n_rows).map(|i| (i % 3) as u32).collect();
        FieldArray::new(
            Field {
                name: "category".into(),
                dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
                nullable: true,
                metadata: Default::default(),
            },
            Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
                data: Buffer::from(indices),
                unique_values: Vec64::from(vec![
                    "red".to_string(),
                    "green".to_string(),
                    "blue".to_string(),
                ]),
                null_mask: Some(Bitmask::new_set_all(n_rows, true)),
            }))),
        )
    };
    #[cfg(feature = "default_categorical_8")]
    let dict_col = {
        let indices: Vec64<u8> = (0..n_rows).map(|i| (i % 3) as u8).collect();
        FieldArray::new(
            Field {
                name: "category".into(),
                dtype: ArrowType::Dictionary(CategoricalIndexType::UInt8),
                nullable: true,
                metadata: Default::default(),
            },
            Array::TextArray(TextArray::Categorical8(Arc::new(CategoricalArray {
                data: Buffer::from(indices),
                unique_values: Vec64::from(vec![
                    "red".to_string(),
                    "green".to_string(),
                    "blue".to_string(),
                ]),
                null_mask: Some(Bitmask::new_set_all(n_rows, true)),
            }))),
        )
    };

    Table::new(
        "bench_table".to_string(),
        Some(vec![id_col, value_col, label_col, dict_col]),
    )
}

fn bench_schema(table: &Table) -> Vec<Field> {
    table.schema().iter().map(|f| (**f).clone()).collect()
}

fn logical_payload_bytes(n_rows: usize, n_batches: usize) -> u64 {
    let ids = n_rows * size_of::<i32>();
    let values = n_rows * size_of::<f64>();
    let label_offsets = (n_rows + 1) * size_of::<u32>();
    let label_data: usize = (0..n_rows).map(|i| format!("row_{}", i).len()).sum();
    let category_indices = n_rows
        * if cfg!(feature = "default_categorical_8") {
            size_of::<u8>()
        } else {
            size_of::<u32>()
        };
    ((ids + values + label_offsets + label_data + category_indices) * n_batches) as u64
}

fn bench_file_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = make_bench_table(BENCH_ROWS);
    let schema = bench_schema(&table);
    let tables: Vec<Table> = (0..BENCH_BATCHES).map(|_| table.clone()).collect();

    let mut group = c.benchmark_group("file_throughput");
    group.throughput(Throughput::Bytes(logical_payload_bytes(
        BENCH_ROWS,
        BENCH_BATCHES,
    )));

    // Write benchmark
    group.bench_function("write", |b| {
        b.to_async(&rt).iter(|| {
            let tables = tables.clone();
            let schema = schema.clone();
            async move {
                let temp = NamedTempFile::new().unwrap();
                let path = temp.path().to_str().unwrap().to_string();
                write_tables_to_file(&path, &tables, schema).await.unwrap();
            }
        });
    });

    // Write a file once for the read benchmarks. It lives under the
    // target directory rather than the system temp dir, which is often
    // tmpfs, so the cold benchmark's page cache eviction reaches a real
    // disk.
    let read_file = NamedTempFile::new_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let read_path = read_file.path().to_path_buf();
    rt.block_on(async {
        write_tables_to_file(read_path.to_str().unwrap(), &tables, schema.clone())
            .await
            .unwrap();
    });
    // Flush the file to disk so the cold benchmark's page cache
    // eviction operates on clean pages.
    read_file.as_file().sync_all().unwrap();

    // File reader benchmark - warm (page cache)
    group.bench_function("read_file_warm", |b| {
        b.iter(|| {
            let reader = FileTableReader::open(&read_path).unwrap();
            assert_eq!(reader.num_batches(), BENCH_BATCHES);
            for i in 0..reader.num_batches() {
                let batch = reader.read_batch(i).unwrap();
                assert_eq!(batch.n_rows, BENCH_ROWS);
                std::hint::black_box(&batch.cols);
            }
        });
    });

    // File reader benchmark - cold. Evicts page cache before each read
    // so every byte is pulled fresh from disk. The pread path copies
    // every byte, so the disk reads land inside the timed region
    // without needing to touch the decoded columns.
    #[cfg(target_os = "linux")]
    group.bench_function("read_file_cold", |b| {
        use std::os::unix::io::AsRawFd;
        b.iter(|| {
            // Evict file from page cache
            let f = std::fs::File::open(&read_path).unwrap();
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
            drop(f);

            let reader = FileTableReader::open(&read_path).unwrap();
            assert_eq!(reader.num_batches(), BENCH_BATCHES);
            for i in 0..reader.num_batches() {
                let batch = reader.read_batch(i).unwrap();
                assert_eq!(batch.n_rows, BENCH_ROWS);
                std::hint::black_box(&batch.cols);
            }
        });
    });

    // Mmap reader benchmark - warm (page cache)
    #[cfg(feature = "mmap")]
    group.bench_function("read_mmap_warm", |b| {
        use lightstream::models::readers::ipc::mmap_table::MmapTableReader;
        b.iter(|| {
            let reader = MmapTableReader::open(&read_path).unwrap();
            assert_eq!(reader.num_batches(), BENCH_BATCHES);
            for i in 0..reader.num_batches() {
                let batch = reader.read_batch(i).unwrap();
                assert_eq!(batch.n_rows, BENCH_ROWS);
                assert_eq!(batch.cols.len(), 4);
                // Keep the decoded columns live so the construction work
                // is not optimised away. Data pages stay untouched, so
                // this measures table construction over cached pages.
                std::hint::black_box(&batch.cols);
            }
        });
    });

    // Mmap reader benchmark - cold. Evicts page cache before each read,
    // then faults every data page back in, so the timing covers the disk
    // I/O that the zero-copy decode defers until the data is used.
    #[cfg(all(feature = "mmap", target_os = "linux"))]
    group.bench_function("read_mmap_cold", |b| {
        use lightstream::models::readers::ipc::mmap_table::MmapTableReader;
        use minarrow::NumericArray;
        use std::os::unix::io::AsRawFd;
        b.iter(|| {
            // Evict file from page cache
            let f = std::fs::File::open(&read_path).unwrap();
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
            drop(f);

            let reader = MmapTableReader::open(&read_path).unwrap();
            assert_eq!(reader.num_batches(), BENCH_BATCHES);
            for i in 0..reader.num_batches() {
                let batch = reader.read_batch(i).unwrap();
                assert_eq!(batch.n_rows, BENCH_ROWS);
                // Read one element per 4 KiB page of each column so the
                // evicted pages are pulled from disk inside the timed
                // region.
                for col in &batch.cols {
                    match &col.array {
                        Array::NumericArray(NumericArray::Int32(a)) => {
                            for idx in (0..a.data.len()).step_by(1024) {
                                std::hint::black_box(a.data[idx]);
                            }
                        }
                        Array::NumericArray(NumericArray::Float64(a)) => {
                            for idx in (0..a.data.len()).step_by(512) {
                                std::hint::black_box(a.data[idx]);
                            }
                        }
                        Array::TextArray(TextArray::String32(a)) => {
                            for idx in (0..a.data.len()).step_by(4096) {
                                std::hint::black_box(a.data[idx]);
                            }
                        }
                        #[cfg(not(feature = "default_categorical_8"))]
                        Array::TextArray(TextArray::Categorical32(a)) => {
                            for idx in (0..a.data.len()).step_by(1024) {
                                std::hint::black_box(a.data[idx]);
                            }
                        }
                        #[cfg(feature = "default_categorical_8")]
                        Array::TextArray(TextArray::Categorical8(a)) => {
                            for idx in (0..a.data.len()).step_by(4096) {
                                std::hint::black_box(a.data[idx]);
                            }
                        }
                        _ => {}
                    }
                }
            }
        });
    });

    // Lightstream zero-copy decode over a buffer preloaded outside the
    // timed region. Excludes file I/O, so compare with
    // read_arrow_rs_inmemory and the warm reads only.
    group.bench_function("read_inmemory", |b| {
        use flatbuffers::root;
        use lightstream::arrow::file::org::apache::arrow::flatbuf as fbf;
        use lightstream::arrow::message::org::apache::arrow::flatbuf as fbm;
        use lightstream::models::decoders::ipc::parser::{
            convert_fb_field_to_arrow, decode_record_batch, handle_dictionary_batch,
        };
        use lightstream::models::decoders::limits::DecodeLimits;
        use minarrow::structs::shared_buffer::SharedBuffer;
        use std::collections::HashMap;

        // Read the file into a 64-byte aligned buffer once, outside the
        // timed region
        let bytes = std::fs::read(&read_path).unwrap();
        let mut aligned: Vec64<u8> = Vec64::with_capacity(bytes.len());
        aligned.extend_from_slice(&bytes);
        let shared = SharedBuffer::from_vec64(aligned);

        b.iter(|| {
            let buf = shared.as_slice();
            let file_len = buf.len();
            let footer_len =
                u32::from_le_bytes(buf[file_len - 10..file_len - 6].try_into().unwrap()) as usize;
            let footer_start = file_len - 10 - footer_len;
            let footer = root::<fbf::Footer>(&buf[footer_start..footer_start + footer_len])
                .unwrap();
            let fb_fields = footer.schema().unwrap().fields().unwrap();
            let fields: Vec<Field> = (0..fb_fields.len())
                .map(|i| convert_fb_field_to_arrow(&fb_fields.get(i)).unwrap())
                .collect();

            let mut dicts = HashMap::new();
            for blk in footer.dictionaries().iter().flatten() {
                let off = blk.offset() as usize;
                let meta_len =
                    u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()) as usize;
                let msg = root::<fbm::Message>(&buf[off + 8..off + 8 + meta_len]).unwrap();
                let dict_batch = msg.header_as_dictionary_batch().unwrap();
                let body_off = off + blk.metaDataLength() as usize;
                let body = &buf[body_off..body_off + blk.bodyLength() as usize];
                handle_dictionary_batch(&dict_batch, body, &mut dicts, DecodeLimits::default())
                    .unwrap();
            }

            let batches = footer.recordBatches().unwrap();
            assert_eq!(batches.len(), BENCH_BATCHES);
            for i in 0..batches.len() {
                let blk = batches.get(i);
                let off = blk.offset() as usize;
                let meta_len =
                    u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()) as usize;
                let msg = root::<fbm::Message>(&buf[off + 8..off + 8 + meta_len]).unwrap();
                let rec = msg.header_as_record_batch().unwrap();
                let body_start = off + blk.metaDataLength() as usize;
                let (table, _) = decode_record_batch(
                    &rec,
                    &fields,
                    &dicts,
                    shared.clone(),
                    body_start,
                    blk.bodyLength() as usize,
                    None,
                    DecodeLimits::default(),
                )
                .unwrap();
                assert_eq!(table.n_rows, BENCH_ROWS);
                std::hint::black_box(&table.cols);
            }
        });
    });

    // arrow-rs file reader for comparison - warm (page cache)
    #[cfg(feature = "bench_arrow")]
    group.bench_function("read_arrow_rs_file_warm", |b| {
        use arrow::ipc::reader::FileReader;
        use std::fs::File as StdFile;
        b.iter(|| {
            let file = StdFile::open(&read_path).unwrap();
            let reader = FileReader::try_new(file, None).unwrap();
            let mut count = 0usize;
            for batch in reader {
                let batch = batch.unwrap();
                assert!(batch.num_rows() > 0);
                std::hint::black_box(batch.columns());
                count += 1;
            }
            assert_eq!(count, BENCH_BATCHES);
        });
    });

    // arrow-rs file reader - cold, with the same page cache eviction as
    // read_file_cold so the two decoders are compared over identical
    // disk reads.
    #[cfg(all(feature = "bench_arrow", target_os = "linux"))]
    group.bench_function("read_arrow_rs_file_cold", |b| {
        use arrow::ipc::reader::FileReader;
        use std::fs::File as StdFile;
        use std::os::unix::io::AsRawFd;
        b.iter(|| {
            // Evict file from page cache
            let f = StdFile::open(&read_path).unwrap();
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
            drop(f);

            let file = StdFile::open(&read_path).unwrap();
            let reader = FileReader::try_new(file, None).unwrap();
            let mut count = 0usize;
            for batch in reader {
                let batch = batch.unwrap();
                assert!(batch.num_rows() > 0);
                std::hint::black_box(batch.columns());
                count += 1;
            }
            assert_eq!(count, BENCH_BATCHES);
        });
    });

    // arrow-rs zero-copy FileDecoder over a buffer preloaded outside the
    // timed region. Excludes file I/O, so compare with warm reads only.
    #[cfg(feature = "bench_arrow")]
    group.bench_function("read_arrow_rs_inmemory", |b| {
        use arrow::buffer::Buffer;
        use arrow::ipc::convert::fb_to_schema;
        use arrow::ipc::reader::{FileDecoder, read_footer_length};
        use arrow::ipc::root_as_footer;
        use std::fs::File as StdFile;
        use std::io::Read as _;

        // Read the file into an arrow Buffer once, outside the timed region
        let mut file = StdFile::open(&read_path).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();
        let buffer = Buffer::from_vec(data);

        b.iter(|| {
            let trailer_start = buffer.len() - 10;
            let footer_len =
                read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
            let footer =
                root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).unwrap();
            let schema = std::sync::Arc::new(fb_to_schema(footer.schema().unwrap()));
            let mut decoder = FileDecoder::new(schema, footer.version());

            for block in footer.dictionaries().iter().flatten() {
                let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
                let data = buffer.slice_with_length(block.offset() as _, block_len);
                decoder.read_dictionary(&block, &data).unwrap();
            }

            let batches = footer.recordBatches().unwrap();
            assert_eq!(batches.len(), BENCH_BATCHES);
            for i in 0..batches.len() {
                let block = batches.get(i);
                let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
                let data = buffer.slice_with_length(block.offset() as _, block_len);
                let batch = decoder.read_record_batch(&block, &data).unwrap().unwrap();
                assert!(batch.num_rows() > 0);
            }
        });
    });

    // Polars file reader for comparison
    #[cfg(feature = "bench_polars")]
    group.bench_function("read_polars_file", |b| {
        use polars::prelude::*;
        b.iter(|| {
            let file = std::fs::File::open(&read_path).unwrap();
            let df = IpcReader::new(file).finish().unwrap();
            assert_eq!(df.height(), BENCH_ROWS * BENCH_BATCHES);
        });
    });

    // Polars mmap reader for comparison
    #[cfg(feature = "bench_polars")]
    group.bench_function("read_polars_mmap", |b| {
        use polars::prelude::*;
        b.iter(|| {
            let file = std::fs::File::open(&read_path).unwrap();
            let df = IpcReader::new(file).memory_mapped(None).finish().unwrap();
            assert_eq!(df.height(), BENCH_ROWS * BENCH_BATCHES);
        });
    });

    // ---- zstd compression variants ----------------------------------------

    #[cfg(feature = "zstd")]
    {
        use lightstream::compression::Compression;
        use lightstream::enums::IPCMessageProtocol;
        use lightstream::models::writers::ipc::table::TableWriter;

        /// Write tables to a file with zstd compression.
        async fn write_compressed(
            path: &str,
            tables: &[Table],
            schema: Vec<Field>,
        ) -> std::io::Result<()> {
            let file = tokio::fs::File::create(path).await?;
            let mut writer = TableWriter::new(
                file,
                schema,
                IPCMessageProtocol::File,
                Some(Compression::Zstd),
            )?;
            // Column 3 is the categorical - register its dictionary
            writer.register_dictionary(
                3,
                vec!["red".to_string(), "green".to_string(), "blue".to_string()],
            );
            writer.write_all_tables(tables.to_vec()).await?;
            Ok(())
        }

        group.bench_function("write_zstd", |b| {
            b.to_async(&rt).iter(|| {
                let tables = tables.clone();
                let schema = schema.clone();
                async move {
                    let temp = NamedTempFile::new().unwrap();
                    let path = temp.path().to_str().unwrap().to_string();
                    write_compressed(&path, &tables, schema).await.unwrap();
                }
            });
        });

        // Write a compressed file for the read benchmark
        let zstd_file = NamedTempFile::new().unwrap();
        let zstd_path = zstd_file.path().to_path_buf();
        rt.block_on(async {
            write_compressed(zstd_path.to_str().unwrap(), &tables, schema.clone())
                .await
                .unwrap();
        });

        group.bench_function("read_file_zstd", |b| {
            b.iter(|| {
                let reader = FileTableReader::open(&zstd_path).unwrap();
                assert_eq!(reader.num_batches(), BENCH_BATCHES);
                for i in 0..reader.num_batches() {
                    let batch = reader.read_batch(i).unwrap();
                    assert_eq!(batch.n_rows, BENCH_ROWS);
                    std::hint::black_box(&batch.cols);
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_file_throughput);
criterion_main!(benches);
