//! Shared helpers for throughput benchmarks.

use std::sync::Arc;

use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, Table, TextArray,
    Vec64, arr_f64, arr_i32, arr_str32, ffi::arrow_dtype::CategoricalIndexType,
};

pub const BENCH_ROWS: usize = 100_000;

pub fn make_bench_table(n_rows: usize) -> Table {
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

/// Logical payload size of one batch for throughput reporting.
pub fn logical_payload_bytes(n_rows: usize) -> u64 {
    let ids = n_rows * size_of::<i32>();
    let values = n_rows * size_of::<f64>();
    let label_offsets = (n_rows + 1) * size_of::<u32>();
    let label_data: usize = (0..n_rows).map(|i| format!("row_{}", i).len()).sum();
    let category_indices = n_rows * size_of::<u32>();
    (ids + values + label_offsets + label_data + category_indices) as u64
}
