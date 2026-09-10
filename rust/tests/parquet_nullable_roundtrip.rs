// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Parquet write-then-read over every supported column type with nulls.
//!
//! Each column spans several data pages with nulls placed on both sides of
//! every page boundary, so the value sections hold non-null entries only
//! and the reader has to scatter them back against the definition levels
//! page by page.

#[cfg(feature = "parquet")]
mod parquet_nullable_roundtrip_tests {
    use std::io::{Cursor, Seek, SeekFrom};

    use lightstream::compression::Compression;
    use lightstream::models::readers::parquet::load_parquet_table;
    use lightstream::models::writers::parquet::{PARQUET_PAGE_CHUNK_SIZE, write_parquet_table};
    use minarrow::ffi::arrow_dtype::CategoricalIndexType;
    use minarrow::{
        Array, ArrowType, Bitmask, BooleanArray, CategoricalArray, Field, FieldArray, FloatArray,
        IntegerArray, MaskedArray, NumericArray, StringArray, Table, TextArray, Vec64,
    };
    #[cfg(feature = "datetime")]
    use minarrow::{DatetimeArray, TemporalArray};

    /// Rows per column: two full pages plus a partial third page.
    const N_ROWS: usize = 2 * PARQUET_PAGE_CHUNK_SIZE + 13;

    /// Every seventh row from row three is null, plus the rows either side of
    /// each page boundary.
    fn is_valid(i: usize) -> bool {
        let boundary = [
            PARQUET_PAGE_CHUNK_SIZE - 1,
            PARQUET_PAGE_CHUNK_SIZE,
            2 * PARQUET_PAGE_CHUNK_SIZE - 1,
            2 * PARQUET_PAGE_CHUNK_SIZE,
        ];
        i % 7 != 3 && !boundary.contains(&i)
    }

    fn null_mask() -> Bitmask {
        let bits: Vec<bool> = (0..N_ROWS).map(is_valid).collect();
        Bitmask::from_bools(&bits)
    }

    fn expected<T>(value: impl Fn(usize) -> T) -> Vec<Option<T>> {
        (0..N_ROWS)
            .map(|i| is_valid(i).then(|| value(i)))
            .collect()
    }

    fn int32_value(i: usize) -> i32 {
        i as i32 - 1000
    }
    fn uint32_value(i: usize) -> u32 {
        i as u32 * 3
    }
    fn int64_value(i: usize) -> i64 {
        -(i as i64) * 7
    }
    fn uint64_value(i: usize) -> u64 {
        i as u64 + (1 << 40)
    }
    fn float32_value(i: usize) -> f32 {
        i as f32 * 0.5
    }
    fn float64_value(i: usize) -> f64 {
        i as f64 * -0.25
    }
    fn bool_value(i: usize) -> bool {
        i % 3 == 0
    }
    fn string_value(i: usize) -> String {
        format!("s{}", i % 101)
    }
    fn category_value(i: usize) -> &'static str {
        ["red", "green", "blue"][i % 3]
    }
    #[cfg(feature = "datetime")]
    fn date32_value(i: usize) -> i32 {
        i as i32 * 3
    }

    fn column(name: &str, dtype: ArrowType, array: Array) -> FieldArray {
        FieldArray::new(Field::new(name, dtype, true, None), array)
    }

    fn all_types_table() -> Table {
        let mask = null_mask();
        let strings: Vec<String> = (0..N_ROWS).map(string_value).collect();
        let categories: Vec<&str> = (0..N_ROWS).map(category_value).collect();

        let mut cols = vec![
            column(
                "int32",
                ArrowType::Int32,
                Array::from_int32(IntegerArray::from_vec64(
                    (0..N_ROWS).map(int32_value).collect::<Vec64<_>>(),
                    Some(mask.clone()),
                )),
            ),
            column(
                "uint32",
                ArrowType::UInt32,
                Array::from_uint32(IntegerArray::from_vec64(
                    (0..N_ROWS).map(uint32_value).collect::<Vec64<_>>(),
                    Some(mask.clone()),
                )),
            ),
            column(
                "int64",
                ArrowType::Int64,
                Array::from_int64(IntegerArray::from_vec64(
                    (0..N_ROWS).map(int64_value).collect::<Vec64<_>>(),
                    Some(mask.clone()),
                )),
            ),
            column(
                "uint64",
                ArrowType::UInt64,
                Array::from_uint64(IntegerArray::from_vec64(
                    (0..N_ROWS).map(uint64_value).collect::<Vec64<_>>(),
                    Some(mask.clone()),
                )),
            ),
            column(
                "float32",
                ArrowType::Float32,
                Array::from_float32(FloatArray::from_vec64(
                    (0..N_ROWS).map(float32_value).collect::<Vec64<_>>(),
                    Some(mask.clone()),
                )),
            ),
            column(
                "float64",
                ArrowType::Float64,
                Array::from_float64(FloatArray::from_vec64(
                    (0..N_ROWS).map(float64_value).collect::<Vec64<_>>(),
                    Some(mask.clone()),
                )),
            ),
            column(
                "bool",
                ArrowType::Boolean,
                Array::from_bool(BooleanArray::new(
                    Bitmask::from_bools(&(0..N_ROWS).map(bool_value).collect::<Vec<_>>()),
                    Some(mask.clone()),
                )),
            ),
            column(
                "string",
                ArrowType::String,
                Array::from_string32(StringArray::from_vec(
                    strings.iter().map(String::as_str).collect(),
                    Some(mask.clone()),
                )),
            ),
        ];

        #[cfg(feature = "default_categorical_8")]
        cols.push(column(
            "category",
            ArrowType::Dictionary(CategoricalIndexType::UInt8),
            Array::from_categorical8(CategoricalArray::<u8>::from_vec(
                categories,
                Some(mask.clone()),
            )),
        ));
        #[cfg(not(feature = "default_categorical_8"))]
        cols.push(column(
            "category",
            ArrowType::Dictionary(CategoricalIndexType::UInt32),
            Array::from_categorical32(CategoricalArray::<u32>::from_vec(
                categories,
                Some(mask.clone()),
            )),
        ));

        #[cfg(feature = "datetime")]
        {
            // Date64 is left out: Parquet has no 64-bit DATE annotation, so
            // the type mapping stores it as a plain INT64.
            cols.push(column(
                "date32",
                ArrowType::Date32,
                Array::from_datetime_i32(DatetimeArray::from_vec64(
                    (0..N_ROWS).map(date32_value).collect::<Vec64<_>>(),
                    Some(mask.clone()),
                    None,
                )),
            ));
        }

        Table::new("all_types".to_string(), Some(cols))
    }

    fn roundtrip(table: &Table, compression: Option<Compression>) -> Table {
        let mut buf = Cursor::new(Vec::new());
        write_parquet_table(table, &mut buf, compression).expect("write");
        buf.seek(SeekFrom::Start(0)).unwrap();
        load_parquet_table(&mut buf).expect("read")
    }

    fn col<'a>(table: &'a Table, name: &str) -> &'a FieldArray {
        table
            .cols
            .iter()
            .find(|c| c.field.name == name)
            .unwrap_or_else(|| panic!("column {name} missing"))
    }

    fn assert_all_types(out: &Table) {
        let expected_nulls = (0..N_ROWS).filter(|&i| !is_valid(i)).count();
        assert_eq!(out.n_rows, N_ROWS);
        for c in &out.cols {
            assert!(c.field.nullable, "{} must be nullable", c.field.name);
            assert_eq!(c.null_count, expected_nulls, "{} null count", c.field.name);
            assert_eq!(c.array.len(), N_ROWS, "{} length", c.field.name);
        }

        match &col(out, "int32").array {
            Array::NumericArray(NumericArray::Int32(a)) => {
                assert_eq!((0..N_ROWS).map(|i| a.get(i)).collect::<Vec<_>>(), expected(int32_value))
            }
            other => panic!("int32: {other:?}"),
        }
        match &col(out, "uint32").array {
            Array::NumericArray(NumericArray::UInt32(a)) => {
                assert_eq!((0..N_ROWS).map(|i| a.get(i)).collect::<Vec<_>>(), expected(uint32_value))
            }
            other => panic!("uint32: {other:?}"),
        }
        match &col(out, "int64").array {
            Array::NumericArray(NumericArray::Int64(a)) => {
                assert_eq!((0..N_ROWS).map(|i| a.get(i)).collect::<Vec<_>>(), expected(int64_value))
            }
            other => panic!("int64: {other:?}"),
        }
        match &col(out, "uint64").array {
            Array::NumericArray(NumericArray::UInt64(a)) => {
                assert_eq!((0..N_ROWS).map(|i| a.get(i)).collect::<Vec<_>>(), expected(uint64_value))
            }
            other => panic!("uint64: {other:?}"),
        }
        match &col(out, "float32").array {
            Array::NumericArray(NumericArray::Float32(a)) => {
                assert_eq!((0..N_ROWS).map(|i| a.get(i)).collect::<Vec<_>>(), expected(float32_value))
            }
            other => panic!("float32: {other:?}"),
        }
        match &col(out, "float64").array {
            Array::NumericArray(NumericArray::Float64(a)) => {
                assert_eq!((0..N_ROWS).map(|i| a.get(i)).collect::<Vec<_>>(), expected(float64_value))
            }
            other => panic!("float64: {other:?}"),
        }
        match &col(out, "bool").array {
            Array::BooleanArray(a) => {
                assert_eq!((0..N_ROWS).map(|i| a.get(i)).collect::<Vec<_>>(), expected(bool_value))
            }
            other => panic!("bool: {other:?}"),
        }
        let strings: Vec<Option<String>> = match &col(out, "string").array {
            Array::TextArray(TextArray::String32(a)) => {
                (0..N_ROWS).map(|i| a.get(i).map(str::to_owned)).collect()
            }
            #[cfg(feature = "large_string")]
            Array::TextArray(TextArray::String64(a)) => {
                (0..N_ROWS).map(|i| a.get(i).map(str::to_owned)).collect()
            }
            other => panic!("string: {other:?}"),
        };
        assert_eq!(strings, expected(string_value));

        let categories: Vec<Option<String>> = match &col(out, "category").array {
            #[cfg(feature = "default_categorical_8")]
            Array::TextArray(TextArray::Categorical8(a)) => {
                (0..N_ROWS).map(|i| a.get(i).map(str::to_owned)).collect()
            }
            #[cfg(not(feature = "default_categorical_8"))]
            Array::TextArray(TextArray::Categorical32(a)) => {
                (0..N_ROWS).map(|i| a.get(i).map(str::to_owned)).collect()
            }
            other => panic!("category: {other:?}"),
        };
        assert_eq!(categories, expected(|i| category_value(i).to_owned()));

        #[cfg(feature = "datetime")]
        {
            assert_eq!(col(out, "date32").field.dtype, ArrowType::Date32);
            match &col(out, "date32").array {
                Array::TemporalArray(TemporalArray::Datetime32(a)) => assert_eq!(
                    (0..N_ROWS).map(|i| a.get(i)).collect::<Vec<_>>(),
                    expected(date32_value)
                ),
                other => panic!("date32: {other:?}"),
            }
        }
    }

    #[test]
    fn nullable_all_types_uncompressed() {
        let table = all_types_table();
        assert_all_types(&roundtrip(&table, None));
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn nullable_all_types_snappy() {
        let table = all_types_table();
        assert_all_types(&roundtrip(&table, Some(Compression::Snappy)));
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn nullable_all_types_zstd() {
        let table = all_types_table();
        assert_all_types(&roundtrip(&table, Some(Compression::Zstd)));
    }
}
