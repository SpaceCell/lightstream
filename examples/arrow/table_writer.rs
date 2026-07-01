// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;

use lightstream::models::writers::ipc::table::write_table_to_file;
use minarrow::ffi::arrow_dtype::{ArrowType, CategoricalIndexType};
use minarrow::{
    Array, Buffer, CategoricalArray, Field, FieldArray, Table, TextArray, Vec64, arr_bool, arr_i32,
    arr_str32,
};
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Table 1: integer column
        let ids = Vec64::from_slice(&[1_i32, 2, 3, 4]);
        let tbl1 = Table::new(
            "tbl1".to_string(),
            Some(vec![FieldArray::from_arr("id", arr_i32!(ids))]),
        );
        write_table_to_file(
            "t1.arrow",
            &tbl1,
            tbl1.schema().iter().map(|arc| (**arc).clone()).collect(),
        )
        .await
        .unwrap();

        // Table 2: string + boolean columns
        let names: Vec64<&str> = Vec64::from(vec!["alice", "bob", "cindy", "dan"]);
        let active: Vec64<bool> = Vec64::from(vec![true, false, true, true]);
        let tbl2 = Table::new(
            "tbl2".to_string(),
            Some(vec![
                FieldArray::from_arr("name", arr_str32!(names)),
                FieldArray::from_arr("active", arr_bool!(active)),
            ]),
        );
        write_table_to_file(
            "t2.arrow",
            &tbl2,
            tbl2.schema().iter().map(|arc| (**arc).clone()).collect(),
        )
        .await
        .unwrap();

        // Table 3: categorical column
        let categories = Vec64::from(vec![
            "red".to_string(),
            "green".to_string(),
            "blue".to_string(),
        ]);

        #[cfg(not(feature = "default_categorical_8"))]
        let cat_col = {
            let indices = Vec64::from_slice(&[0u32, 2, 1, 1]);
            FieldArray::new(
                Field::new(
                    "category",
                    ArrowType::Dictionary(CategoricalIndexType::UInt32),
                    true,
                    None,
                ),
                Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
                    data: Buffer::from(indices),
                    unique_values: categories.clone(),
                    null_mask: None,
                }))),
            )
        };

        #[cfg(feature = "default_categorical_8")]
        let cat_col = {
            let indices = Vec64::from_slice(&[0u8, 2, 1, 1]);
            FieldArray::new(
                Field::new(
                    "category",
                    ArrowType::Dictionary(CategoricalIndexType::UInt8),
                    true,
                    None,
                ),
                Array::TextArray(TextArray::Categorical8(Arc::new(CategoricalArray {
                    data: Buffer::from(indices),
                    unique_values: categories.clone(),
                    null_mask: None,
                }))),
            )
        };

        let tbl3 = Table::new("tbl3".to_string(), Some(vec![cat_col]));
        write_table_to_file(
            "t3.arrow",
            &tbl3,
            tbl3.schema().iter().map(|arc| (**arc).clone()).collect(),
        )
        .await
        .unwrap();
    });
}
