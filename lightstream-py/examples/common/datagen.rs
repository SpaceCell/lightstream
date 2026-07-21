// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared demo table for the transport example servers. Matches
//! datagen.py, so the Python client verifies identical results from
//! either backend.

#![allow(dead_code)]

use minarrow::{Field, FieldArray, Table, Vec64, arr_f64, arr_i64, arr_str32};

pub const ROWS: usize = 1_000_000;

/// Builds the one million row demo table matching datagen.py.
pub fn get_table() -> Table {
    let ids: Vec64<i64> = (0..ROWS as i64).collect();
    let values: Vec64<f64> = (0..ROWS).map(|i| i as f64 * 0.25).collect();
    let labels: Vec64<String> = (0..ROWS).map(|i| format!("row-{}", i % 100)).collect();
    let label_refs: Vec64<&str> = labels.iter().map(String::as_str).collect();
    Table::new(
        "get_table".to_string(),
        Some(vec![
            FieldArray::from_arr("id", arr_i64!(ids)),
            FieldArray::from_arr("value", arr_f64!(values)),
            FieldArray::from_arr("label", arr_str32!(label_refs)),
        ]),
    )
}

/// Extracts a cloned schema for writer construction.
pub fn schema(table: &Table) -> Vec<Field> {
    table.schema().iter().map(|f| (**f).clone()).collect()
}
