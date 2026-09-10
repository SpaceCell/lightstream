// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parquet reader conformance against files written by pyarrow.
//!
//! The fixtures under `pyarrow-roundtrip/` are committed and regenerated
//! with `generate_pyarrow_parquet_files.py`. They cover the page layouts
//! parquet-cpp writes by default (Snappy, dictionary encoding, DataPageV1,
//! multiple row groups) plus the DataPageV2 layouts with and without
//! dictionaries.

#[cfg(feature = "parquet")]
mod pyarrow_parquet_tests {
    use std::fs::File;
    use std::io::BufReader;

    use lightstream::models::readers::parquet::{load_parquet_table, load_parquet_table_cols};
    use minarrow::{Array, ArrowType, MaskedArray, NumericArray, Table, TextArray};

    fn open(name: &str) -> BufReader<File> {
        let path = format!("{}/pyarrow-roundtrip/{name}", env!("CARGO_MANIFEST_DIR"));
        BufReader::new(File::open(&path).unwrap_or_else(|e| panic!("open {path}: {e}")))
    }

    fn column<'a>(table: &'a Table, name: &str) -> &'a Array {
        let col = table
            .cols
            .iter()
            .find(|c| c.field.name == name)
            .unwrap_or_else(|| panic!("column {name} missing"));
        &col.array
    }

    fn int32_values(table: &Table, name: &str) -> Vec<Option<i32>> {
        match column(table, name) {
            Array::NumericArray(NumericArray::Int32(a)) => (0..a.len()).map(|i| a.get(i)).collect(),
            other => panic!("{name}: expected Int32, got {other:?}"),
        }
    }

    fn int64_values(table: &Table, name: &str) -> Vec<Option<i64>> {
        match column(table, name) {
            Array::NumericArray(NumericArray::Int64(a)) => (0..a.len()).map(|i| a.get(i)).collect(),
            other => panic!("{name}: expected Int64, got {other:?}"),
        }
    }

    fn float32_values(table: &Table, name: &str) -> Vec<Option<f32>> {
        match column(table, name) {
            Array::NumericArray(NumericArray::Float32(a)) => {
                (0..a.len()).map(|i| a.get(i)).collect()
            }
            other => panic!("{name}: expected Float32, got {other:?}"),
        }
    }

    fn float64_values(table: &Table, name: &str) -> Vec<Option<f64>> {
        match column(table, name) {
            Array::NumericArray(NumericArray::Float64(a)) => {
                (0..a.len()).map(|i| a.get(i)).collect()
            }
            other => panic!("{name}: expected Float64, got {other:?}"),
        }
    }

    fn bool_values(table: &Table, name: &str) -> Vec<Option<bool>> {
        match column(table, name) {
            Array::BooleanArray(a) => (0..a.len()).map(|i| a.get(i)).collect(),
            other => panic!("{name}: expected Boolean, got {other:?}"),
        }
    }

    /// UTF-8 columns map to `String`, or to `LargeString` when the
    /// `large_string` feature is on.
    fn string_values(table: &Table, name: &str) -> Vec<Option<String>> {
        match column(table, name) {
            Array::TextArray(TextArray::String32(a)) => {
                (0..a.len()).map(|i| a.get(i).map(str::to_owned)).collect()
            }
            #[cfg(feature = "large_string")]
            Array::TextArray(TextArray::String64(a)) => {
                (0..a.len()).map(|i| a.get(i).map(str::to_owned)).collect()
            }
            other => panic!("{name}: expected a UTF-8 column, got {other:?}"),
        }
    }

    /// Assert the seven-row table written by `nullable_table()` in the
    /// generator script, in schema order.
    fn assert_nullable_table(table: &Table) {
        assert_eq!(table.n_rows, 7);
        assert_eq!(table.cols.len(), 6);
        let names: Vec<&str> = table.cols.iter().map(|c| c.field.name.as_str()).collect();
        assert_eq!(
            names,
            ["int32", "int64", "float32", "float64", "bool", "string"]
        );
        assert!(table.cols.iter().all(|c| c.field.nullable));

        assert_eq!(
            int32_values(table, "int32"),
            [Some(1), None, Some(3), Some(4), None, Some(6), Some(7)]
        );
        assert_eq!(
            int64_values(table, "int64"),
            [Some(100), Some(101), None, Some(103), Some(104), Some(105), None]
        );
        assert_eq!(
            float32_values(table, "float32"),
            [Some(0.5), Some(1.5), Some(2.5), None, Some(4.5), Some(5.5), Some(6.5)]
        );
        assert_eq!(
            float64_values(table, "float64"),
            [None, Some(-1.0), Some(-2.0), Some(-3.0), Some(-4.0), None, Some(-6.0)]
        );
        assert_eq!(
            bool_values(table, "bool"),
            [Some(true), Some(false), None, Some(true), Some(false), Some(true), None]
        );
        assert_eq!(
            string_values(table, "string"),
            [
                Some("a".into()),
                Some("b".into()),
                Some("a".into()),
                None,
                Some("c".into()),
                Some("b".into()),
                Some("a".into())
            ]
        );
        for col in &table.cols {
            assert_eq!(col.null_count, col.array.null_count(), "{}", col.field.name);
        }
    }

    /// The three-column file from the original defect report, written
    /// with pyarrow's defaults: Snappy, dictionary encoding, DataPageV1
    /// and REQUIRED columns.
    #[cfg(feature = "snappy")]
    #[test]
    fn reads_pyarrow_default_layout() {
        let table = load_parquet_table(open("pyarrow_simple.parquet")).expect("read");
        assert_eq!(table.n_rows, 5);
        assert_eq!(table.cols.len(), 3);
        assert_eq!(table.cols[0].field.dtype, ArrowType::Int64);
        #[cfg(not(feature = "large_string"))]
        assert_eq!(table.cols[1].field.dtype, ArrowType::String);
        #[cfg(feature = "large_string")]
        assert_eq!(table.cols[1].field.dtype, ArrowType::LargeString);
        assert_eq!(table.cols[2].field.dtype, ArrowType::Float64);

        assert_eq!(
            int64_values(&table, "id"),
            [Some(1), Some(2), Some(3), Some(4), Some(5)]
        );
        assert_eq!(
            string_values(&table, "name"),
            ["Alice", "Bob", "Charlie", "Diana", "Eve"]
                .map(|s| Some(s.to_owned()))
        );
        assert_eq!(
            float64_values(&table, "score"),
            [Some(85.5), Some(92.0), Some(78.3), Some(95.1), Some(88.7)]
        );
        assert!(table.cols.iter().all(|c| c.null_count == 0));
    }

    /// Nullable columns holding nulls, split over three row groups, with
    /// pyarrow's default page layout.
    #[cfg(feature = "snappy")]
    #[test]
    fn reads_pyarrow_nullable_columns_across_row_groups() {
        let table =
            load_parquet_table(open("pyarrow_nullable_row_groups.parquet")).expect("read");
        assert_nullable_table(&table);
    }

    /// DataPageV2 pages with PLAIN values and no compression.
    #[test]
    fn reads_pyarrow_plain_data_page_v2() {
        let table = load_parquet_table(open("pyarrow_plain_v2.parquet")).expect("read");
        assert_nullable_table(&table);
    }

    /// DataPageV2 pages with dictionary-encoded values and Snappy
    /// compression on the value section only.
    #[cfg(feature = "snappy")]
    #[test]
    fn reads_pyarrow_dictionary_data_page_v2() {
        let table = load_parquet_table(open("pyarrow_dictionary_v2.parquet")).expect("read");
        assert_nullable_table(&table);
    }

    /// Column projection over a pyarrow file selects by name and keeps
    /// the values of the selected columns intact.
    #[cfg(feature = "snappy")]
    #[test]
    fn projects_columns_from_pyarrow_file() {
        let table =
            load_parquet_table_cols(open("pyarrow_simple.parquet"), &["score", "name"])
                .expect("read");
        assert_eq!(table.n_rows, 5);
        let names: Vec<&str> = table.cols.iter().map(|c| c.field.name.as_str()).collect();
        assert_eq!(names, ["name", "score"]);
        assert_eq!(
            string_values(&table, "name"),
            ["Alice", "Bob", "Charlie", "Diana", "Eve"]
                .map(|s| Some(s.to_owned()))
        );
        assert_eq!(
            float64_values(&table, "score"),
            [Some(85.5), Some(92.0), Some(78.3), Some(95.1), Some(88.7)]
        );
    }
}
