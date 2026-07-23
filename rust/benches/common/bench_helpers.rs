// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared support code for throughput benchmarks.
//!
//! [`BenchShape`] defines the column layout and [`BenchScale`] defines the row
//! count. The resulting matrix is used by the network, file and Arrow Flight
//! benchmarks.
//!
//! Set `LIGHTSTREAM_BENCH_MATRIX` to select a preset:
//!
//! - `quick`: one cell for smoke testing.
//! - `standard`: the default six-cell matrix.
//! - `full`: all sixteen shape and scale combinations.

#![allow(dead_code)]

use std::env;
use std::sync::Arc;

use minarrow::{
    Array, ArrowType, Buffer, ByteSize, CategoricalArray, Field, FieldArray, FloatArray,
    IntegerArray, NumericArray, Table, TextArray, Vec64, arr_f64, arr_i32, arr_str32,
    ffi::arrow_dtype::CategoricalIndexType,
};

// ---------------------------------------------------------------------------
// Shapes and scales
// ---------------------------------------------------------------------------

/// Column layout used by a benchmark workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchShape {
    /// Four fixed-width numeric columns: `i32`, `i64`, `f32` and `f64`.
    NarrowNumeric,

    /// One hundred numeric columns, split evenly across `i32`, `i64`, `f32`
    /// and `f64`.
    Wide,

    /// An `i32` identifier, long and short UTF-8 columns, and a `categorical32`
    /// column containing one hundred distinct values.
    StringHeavy,

    /// An `i32`, an `f64`, a short UTF-8 column and a `categorical32` column
    /// containing three distinct values.
    Mixed,
}


impl BenchShape {
    pub fn label(self) -> &'static str {
        match self {
            BenchShape::NarrowNumeric => "narrow_numeric",
            BenchShape::Wide => "wide",
            BenchShape::StringHeavy => "string_heavy",
            BenchShape::Mixed => "mixed",
        }
    }

    /// Dictionary registrations the writer needs to perform for this shape,
    /// keyed by column index. Returns an empty vector for shapes without
    /// categorical columns.
    pub fn dictionary_registrations(self) -> Vec<(i64, Vec<String>)> {
        match self {
            BenchShape::Mixed => vec![(
                3,
                vec!["red".to_string(), "green".to_string(), "blue".to_string()],
            )],
            BenchShape::StringHeavy => vec![(
                3,
                (0..STRING_HEAVY_DICT_CARDINALITY)
                    .map(|i| format!("cat_{:03}", i))
                    .collect(),
            )],
            BenchShape::NarrowNumeric | BenchShape::Wide => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchScale {
    Tiny,
    Small,
    Medium,
    Large,
}

impl BenchScale {
    pub fn rows(self) -> usize {
        match self {
            BenchScale::Tiny => 1_000,
            BenchScale::Small => 100_000,
            BenchScale::Medium => 1_000_000,
            BenchScale::Large => 100_000_000,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            BenchScale::Tiny => "tiny_1k",
            BenchScale::Small => "small_100k",
            BenchScale::Medium => "medium_1M",
            BenchScale::Large => "large_100M",
        }
    }
}

// ---------------------------------------------------------------------------
// Matrix presets
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchMatrix {
    Quick,
    Standard,
    Full,
}

impl BenchMatrix {
    /// Resolve the matrix preset from `LIGHTSTREAM_BENCH_MATRIX`.
    pub fn from_env() -> Self {
        match env::var("LIGHTSTREAM_BENCH_MATRIX").ok().as_deref() {
            Some("quick") => BenchMatrix::Quick,
            Some("full") => BenchMatrix::Full,
            _ => BenchMatrix::Standard,
        }
    }

    pub fn cells(self) -> Vec<(BenchShape, BenchScale)> {
        use BenchScale::*;
        use BenchShape::*;
        match self {
            BenchMatrix::Quick => vec![(Mixed, Small)],
            BenchMatrix::Standard => vec![
                (Mixed, Small),
                (NarrowNumeric, Small),
                (StringHeavy, Small),
                (Wide, Small),
                (Mixed, Medium),
                (NarrowNumeric, Medium),
            ],
            BenchMatrix::Full => vec![
                (NarrowNumeric, Tiny),
                (NarrowNumeric, Small),
                (NarrowNumeric, Medium),
                (NarrowNumeric, Large),
                (Wide, Tiny),
                (Wide, Small),
                (Wide, Medium),
                (Wide, Large),
                (StringHeavy, Tiny),
                (StringHeavy, Small),
                (StringHeavy, Medium),
                (StringHeavy, Large),
                (Mixed, Tiny),
                (Mixed, Small),
                (Mixed, Medium),
                (Mixed, Large),
            ],
        }
    }
}

// ---------------------------------------------------------------------------
// Backward-compat surface
// ---------------------------------------------------------------------------

pub const BENCH_ROWS: usize = 100_000;

/// Clone the schema fields off a bench table so writers can be constructed
/// without re-deriving the type structure at each call site.
pub fn bench_schema(table: &Table) -> Vec<Field> {
    table.schema().iter().map(|f| (**f).clone()).collect()
}

/// Single-shape constructor preserved so the existing single-shape benches
/// continue to build against the older API.
pub fn make_bench_table(n_rows: usize) -> Table {
    make_bench_table_shape(BenchShape::Mixed, n_rows)
}

/// Single-shape payload accounting preserved for the same reason.
pub fn logical_payload_bytes(n_rows: usize) -> u64 {
    logical_payload_bytes_shape(BenchShape::Mixed, n_rows, 1)
}

// ---------------------------------------------------------------------------
// Shape-aware table construction
// ---------------------------------------------------------------------------

/// Build a bench table of the requested shape and row count.
pub fn make_bench_table_shape(shape: BenchShape, n_rows: usize) -> Table {
    match shape {
        BenchShape::Mixed => mixed_table(n_rows),
        BenchShape::NarrowNumeric => narrow_numeric_table(n_rows),
        BenchShape::StringHeavy => string_heavy_table(n_rows),
        BenchShape::Wide => wide_table(n_rows),
    }
}

/// Compute the logical payload size of `n_batches` of `shape` x `n_rows`.
/// Used as the throughput denominator so reported bytes/sec reflects the
/// raw source columns rather than the encoded bytes on the wire.
///
/// The accounting is minarrow's [`ByteSize::logical_bytes`], so every
/// benchmark in the suite shares one definition of the payload size.
/// Callers that already hold the workload table can call `logical_bytes`
/// on it and get the identical per-batch figure.
pub fn logical_payload_bytes_shape(shape: BenchShape, n_rows: usize, n_batches: usize) -> u64 {
    let per_batch = make_bench_table_shape(shape, n_rows).logical_bytes();
    (per_batch as u64) * (n_batches as u64)
}

// ---------------------------------------------------------------------------
// Replay dataset support
// ---------------------------------------------------------------------------

/// Tables each stream replays when a dataset budget of `dataset_gb` decimal
/// gigabytes is split evenly across `max_streams` streams. The source and
/// sink both derive the batch count from the same workload arguments, so the
/// two sides agree on it without exchanging it.
pub fn batches_per_stream_for_budget(
    shape: BenchShape,
    n_rows: usize,
    max_streams: usize,
    dataset_gb: u64,
) -> u64 {
    let per_batch = logical_payload_bytes_shape(shape, n_rows, 1);
    ((dataset_gb * 1_000_000_000) / (max_streams as u64 * per_batch)).max(1)
}

/// Rebuild `base` with a fresh first column so every replay batch is
/// distinct. The first column of every bench shape is `i32`, and batch
/// `seq` holds `seq + i` at row `i`, so a batch's first value identifies it
/// and the receiver can verify ordered, complete delivery for each stream.
/// The remaining columns stay Arc-shared with `base`.
pub fn replay_batch_table(base: &Table, seq: u64) -> Table {
    let data: Vec64<i32> = (0..base.n_rows)
        .map(|i| (seq as i32).wrapping_add(i as i32))
        .collect();
    let mut table = base.clone();
    table.cols[0] = FieldArray::new(
        (*base.cols[0].field).clone(),
        Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(data),
            null_mask: None,
        }))),
    );
    table
}

// ---------------------------------------------------------------------------
// Mixed (default) shape - i32 + f64 + utf8 + categorical
// ---------------------------------------------------------------------------

fn mixed_table(n_rows: usize) -> Table {
    let ids: Vec64<i32> = (0..n_rows as i32).collect();
    let values: Vec64<f64> = (0..n_rows).map(|i| i as f64 * 0.5).collect();
    let labels: Vec64<String> = (0..n_rows).map(|i| format!("row_{}", i)).collect();
    let label_refs: Vec64<&str> = labels.iter().map(String::as_str).collect();

    let id_col = FieldArray::from_arr("ids", arr_i32!(ids));
    let value_col = FieldArray::from_arr("values", arr_f64!(values));
    let label_col = FieldArray::from_arr("labels", arr_str32!(label_refs));

    #[cfg(not(feature = "default_categorical_8"))]
    let dict_col = mixed_dict_col_u32(n_rows);
    #[cfg(feature = "default_categorical_8")]
    let dict_col = mixed_dict_col_u8(n_rows);

    Table::new(
        "bench_table".to_string(),
        Some(vec![id_col, value_col, label_col, dict_col]),
    )
}

#[cfg(not(feature = "default_categorical_8"))]
fn mixed_dict_col_u32(n_rows: usize) -> FieldArray {
    let indices: Vec64<u32> = (0..n_rows).map(|i| (i % 3) as u32).collect();
    FieldArray::new(
        Field {
            name: "category".into(),
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
            nullable: false,
            metadata: Default::default(),
        },
        Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
            data: Buffer::from(indices),
            unique_values: Vec64::from(vec![
                "red".to_string(),
                "green".to_string(),
                "blue".to_string(),
            ]),
            null_mask: None,
        }))),
    )
}

#[cfg(feature = "default_categorical_8")]
fn mixed_dict_col_u8(n_rows: usize) -> FieldArray {
    let indices: Vec64<u8> = (0..n_rows).map(|i| (i % 3) as u8).collect();
    FieldArray::new(
        Field {
            name: "category".into(),
            dtype: ArrowType::Dictionary(CategoricalIndexType::UInt8),
            nullable: false,
            metadata: Default::default(),
        },
        Array::TextArray(TextArray::Categorical8(Arc::new(CategoricalArray {
            data: Buffer::from(indices),
            unique_values: Vec64::from(vec![
                "red".to_string(),
                "green".to_string(),
                "blue".to_string(),
            ]),
            null_mask: None,
        }))),
    )
}

// ---------------------------------------------------------------------------
// Narrow numeric - i32 + i64 + f32 + f64
// ---------------------------------------------------------------------------

fn narrow_numeric_table(n_rows: usize) -> Table {
    let ids: Vec64<i32> = (0..n_rows as i32).collect();
    let counters: Vec64<i64> = (0..n_rows).map(|i| (i as i64) * 7).collect();
    let prices: Vec64<f32> = (0..n_rows).map(|i| i as f32 * 0.25).collect();
    let values: Vec64<f64> = (0..n_rows).map(|i| i as f64 * 0.5).collect();

    let id_col = FieldArray::new(
        Field {
            name: "ids".into(),
            dtype: ArrowType::Int32,
            nullable: false,
            metadata: Default::default(),
        },
        Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(ids),
            null_mask: None,
        }))),
    );
    let counter_col = FieldArray::new(
        Field {
            name: "counters".into(),
            dtype: ArrowType::Int64,
            nullable: false,
            metadata: Default::default(),
        },
        Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
            data: Buffer::from(counters),
            null_mask: None,
        }))),
    );
    let price_col = FieldArray::new(
        Field {
            name: "prices".into(),
            dtype: ArrowType::Float32,
            nullable: false,
            metadata: Default::default(),
        },
        Array::NumericArray(NumericArray::Float32(Arc::new(FloatArray {
            data: Buffer::from(prices),
            null_mask: None,
        }))),
    );
    let value_col = FieldArray::new(
        Field {
            name: "values".into(),
            dtype: ArrowType::Float64,
            nullable: false,
            metadata: Default::default(),
        },
        Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
            data: Buffer::from(values),
            null_mask: None,
        }))),
    );

    Table::new(
        "bench_narrow_numeric".to_string(),
        Some(vec![id_col, counter_col, price_col, value_col]),
    )
}

// ---------------------------------------------------------------------------
// String-heavy - i32 + long utf8 + short utf8 + dictionary100
// ---------------------------------------------------------------------------

const STRING_HEAVY_DICT_CARDINALITY: usize = 100;

fn string_heavy_table(n_rows: usize) -> Table {
    let ids: Vec64<i32> = (0..n_rows as i32).collect();
    let id_col = FieldArray::from_arr("ids", arr_i32!(ids));

    let long_strings: Vec64<String> = (0..n_rows)
        .map(|i| {
            format!(
                "row_{:08}_payload_{:08x}_lorem_ipsum_dolor_sit",
                i,
                i.wrapping_mul(2_654_435_761)
            )
        })
        .collect();
    let long_refs: Vec64<&str> = long_strings.iter().map(String::as_str).collect();
    let long_col = FieldArray::from_arr("long_text", arr_str32!(long_refs));

    let short_strings: Vec64<String> = (0..n_rows)
        .map(|i| format!("s_{:04x}", (i & 0xFFFF) as u16))
        .collect();
    let short_refs: Vec64<&str> = short_strings.iter().map(String::as_str).collect();
    let short_col = FieldArray::from_arr("short_text", arr_str32!(short_refs));

    let unique: Vec64<String> = (0..STRING_HEAVY_DICT_CARDINALITY)
        .map(|i| format!("cat_{:03}", i))
        .collect();

    #[cfg(not(feature = "default_categorical_8"))]
    let dict_col = {
        let indices: Vec64<u32> = (0..n_rows)
            .map(|i| (i % STRING_HEAVY_DICT_CARDINALITY) as u32)
            .collect();
        FieldArray::new(
            Field {
                name: "category".into(),
                dtype: ArrowType::Dictionary(CategoricalIndexType::UInt32),
                nullable: false,
                metadata: Default::default(),
            },
            Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
                data: Buffer::from(indices),
                unique_values: unique,
                null_mask: None,
            }))),
        )
    };
    #[cfg(feature = "default_categorical_8")]
    let dict_col = {
        // The Categorical8 variant can only address 256 entries; 100 fits.
        let indices: Vec64<u8> = (0..n_rows)
            .map(|i| (i % STRING_HEAVY_DICT_CARDINALITY) as u8)
            .collect();
        FieldArray::new(
            Field {
                name: "category".into(),
                dtype: ArrowType::Dictionary(CategoricalIndexType::UInt8),
                nullable: false,
                metadata: Default::default(),
            },
            Array::TextArray(TextArray::Categorical8(Arc::new(CategoricalArray {
                data: Buffer::from(indices),
                unique_values: unique,
                null_mask: None,
            }))),
        )
    };

    Table::new(
        "bench_string_heavy".to_string(),
        Some(vec![id_col, long_col, short_col, dict_col]),
    )
}

// ---------------------------------------------------------------------------
// Wide - 25 each of i32 / i64 / f32 / f64 = 100 cols
// ---------------------------------------------------------------------------

const WIDE_GROUP_SIZE: usize = 25;
const WIDE_NUM_COLS: usize = WIDE_GROUP_SIZE * 4;

fn wide_table(n_rows: usize) -> Table {
    let mut cols: Vec<FieldArray> = Vec::with_capacity(WIDE_NUM_COLS);

    for k in 0..WIDE_GROUP_SIZE {
        let data: Vec64<i32> = (0..n_rows)
            .map(|i| (i as i32).wrapping_add(k as i32))
            .collect();
        cols.push(FieldArray::new(
            Field {
                name: format!("i32_{:03}", k).into(),
                dtype: ArrowType::Int32,
                nullable: false,
                metadata: Default::default(),
            },
            Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
                data: Buffer::from(data),
                null_mask: None,
            }))),
        ));
    }
    for k in 0..WIDE_GROUP_SIZE {
        let data: Vec64<i64> = (0..n_rows)
            .map(|i| (i as i64).wrapping_mul(k as i64 + 1))
            .collect();
        cols.push(FieldArray::new(
            Field {
                name: format!("i64_{:03}", k).into(),
                dtype: ArrowType::Int64,
                nullable: false,
                metadata: Default::default(),
            },
            Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
                data: Buffer::from(data),
                null_mask: None,
            }))),
        ));
    }
    for k in 0..WIDE_GROUP_SIZE {
        let data: Vec64<f32> = (0..n_rows).map(|i| i as f32 + k as f32 * 0.125).collect();
        cols.push(FieldArray::new(
            Field {
                name: format!("f32_{:03}", k).into(),
                dtype: ArrowType::Float32,
                nullable: false,
                metadata: Default::default(),
            },
            Array::NumericArray(NumericArray::Float32(Arc::new(FloatArray {
                data: Buffer::from(data),
                null_mask: None,
            }))),
        ));
    }
    for k in 0..WIDE_GROUP_SIZE {
        let data: Vec64<f64> = (0..n_rows).map(|i| i as f64 + k as f64 * 0.5).collect();
        cols.push(FieldArray::new(
            Field {
                name: format!("f64_{:03}", k).into(),
                dtype: ArrowType::Float64,
                nullable: false,
                metadata: Default::default(),
            },
            Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
                data: Buffer::from(data),
                null_mask: None,
            }))),
        ));
    }

    Table::new("bench_wide".to_string(), Some(cols))
}
