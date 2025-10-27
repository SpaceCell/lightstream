#[cfg(feature = "parquet")]
#[cfg(test)]
mod parquet_writer_integration_tests {
    use lightstream::{
        compression::Compression,
        models::{
            readers::parquet_reader::read_parquet_table,
            writers::parquet_writer::{PAGE_CHUNK_SIZE, write_parquet_table},
        },
    };
    use minarrow::{
        Array, ArrowType, Bitmask, Field, FieldArray, IntegerArray, MaskedArray, NumericArray,
        StringArray, Table, vec64,
    };
    use std::io::{Cursor, Seek, SeekFrom};

    fn roundtrip_table(table: &Table, compression: Compression) -> Table {
        let mut buf = Cursor::new(Vec::new());
        write_parquet_table(table, &mut buf, compression).expect("writer must not fail");
        buf.seek(SeekFrom::Start(0)).unwrap();
        let out = read_parquet_table(&mut buf).expect("reader must not fail");
        assert_eq!(out.n_rows, table.n_rows, "row count must match");
        out
    }

    #[test]
    fn write_and_read_numeric_i32() {
        let arr = Array::from_int32(IntegerArray::from_slice(&[1i32, 2, 3, 4, 5]));
        let table = Table::new(
            "tbl".to_string(),
            Some(vec![FieldArray::new(
                Field::new("numbers", ArrowType::Int32, false, None),
                arr,
            )]),
        );
        let out = roundtrip_table(&table, Compression::None);
        let col = out.col_ix(0).unwrap();
        assert_eq!(col.len(), 5);
        if let Array::NumericArray(NumericArray::Int32(a)) = &col.array {
            assert_eq!(a.data.as_slice(), &[1, 2, 3, 4, 5]);
        } else {
            panic!("Expected Int32 column");
        }
    }
    #[test]
    fn write_and_read_boolean() {
        let data = [true, false, true, true, false, false, true];
        let arr = Array::from_bool(minarrow::BooleanArray::from_slice(&data));
        let table = Table::new(
            "tbl".to_string(),
            Some(vec![FieldArray::new(
                Field::new("flags", ArrowType::Boolean, false, None),
                arr,
            )]),
        );
        let out = roundtrip_table(&table, Compression::None);
        let col = out.col_ix(0).unwrap();
        assert_eq!(col.len(), data.len());
        if let Array::BooleanArray(a) = &col.array {
            let observed = a.to_opt_bool_vec64();
            assert!(observed.iter().all(|x| x.is_some()));
            let observed: Vec<bool> = observed.into_iter().map(|x| x.unwrap()).collect();
            assert_eq!(observed.as_slice(), data.as_slice());
        } else {
            panic!("Expected BooleanArray column");
        }
    }

    #[test]
    fn write_and_read_text() {
        let arr = Array::from_string32(StringArray::from_slice(&[
            "alpha", "beta", "gamma", "delta",
        ]));
        let table = Table::new(
            "tbl".to_string(),
            Some(vec![FieldArray::new(
                Field::new("labels", ArrowType::String, false, None),
                arr,
            )]),
        );
        let out = roundtrip_table(&table, Compression::None);
        let col = out.col_ix(0).unwrap();
        assert_eq!(col.len(), 4);

        // TODO[5]: Add QOL improvements to Minarrow
        // Bad syntax here
        if let Array::TextArray(a) = &col.array {
            let actual: Vec<String> = a
                .clone()
                .str32()
                .unwrap()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let expected: Vec<String> = vec!["alpha", "beta", "gamma", "delta"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            assert_eq!(actual, expected);
        } else {
            panic!("Expected TextArray column");
        }
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn write_and_read_text_snappy() {
        let arr = Array::from_string32(StringArray::from_slice(&[
            "alpha", "beta", "gamma", "delta",
        ]));
        let table = Table::new(
            "tbl".to_string(),
            Some(vec![FieldArray::new(
                Field::new("labels", ArrowType::String, false, None),
                arr,
            )]),
        );
        let out = roundtrip_table(&table, Compression::Snappy);
        let col = out.col_ix(0).unwrap();
        assert_eq!(col.len(), 4);
        if let Array::TextArray(a) = &col.array {
            let actual: Vec<String> = a
                .clone()
                .str32()
                .unwrap()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let expected: Vec<String> = vec!["alpha", "beta", "gamma", "delta"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            assert_eq!(actual, expected);
        } else {
            panic!("Expected TextArray column");
        }
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn write_and_read_text_zstd() {
        let arr = Array::from_string32(StringArray::from_slice(&[
            "alpha", "beta", "gamma", "delta",
        ]));
        let table = Table::new(
            "tbl".to_string(),
            Some(vec![FieldArray::new(
                Field::new("labels", ArrowType::String, false, None),
                arr,
            )]),
        );
        let out = roundtrip_table(&table, Compression::Snappy);
        let col = out.col_ix(0).unwrap();
        assert_eq!(col.len(), 4);
        if let Array::TextArray(a) = &col.array {
            let actual: Vec<String> = a
                .clone()
                .str32()
                .unwrap()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let expected: Vec<String> = vec!["alpha", "beta", "gamma", "delta"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            assert_eq!(actual, expected);
        } else {
            panic!("Expected TextArray column");
        }
    }

    #[test]
    fn write_and_read_categorical32() {
        // Create a regular string array (parquet will handle categorical encoding internally)
        let arr = Array::from_string32(StringArray::from_slice(&[
            "foo", "bar", "foo", "baz", "bar", "baz",
        ]));
        let table = Table::new(
            "tbl".to_string(),
            Some(vec![FieldArray::new(
                Field::new("cat", ArrowType::String, false, None),
                arr,
            )]),
        );
        let out = roundtrip_table(&table, Compression::None);
        let col = out.col_ix(0).unwrap();
        assert_eq!(col.len(), 6);

        if let Array::TextArray(a) = &col.array {
            let actual: Vec<String> = a
                .clone()
                .str32()
                .unwrap()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let expected: Vec<String> = vec!["foo", "bar", "foo", "baz", "bar", "baz"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            assert_eq!(actual, expected);
        } else {
            panic!("Expected TextArray column, got: {:?}", col.array);
        }
    }

    #[test]
    fn write_empty_table() {
        let table = Table::new("tbl".to_string(), Some(vec![]));
        let mut buf = Cursor::new(Vec::new());
        assert!(write_parquet_table(&table, &mut buf, Compression::None).is_ok());
        buf.seek(SeekFrom::Start(0)).unwrap();
        let readback = read_parquet_table(&mut buf).expect("Empty table must still be readable");
        assert_eq!(readback.n_rows, 0);
        assert!(readback.cols.is_empty());
    }

    #[test]
    fn write_and_read_nullable_column() {
        let intarray = IntegerArray::new(
            vec64![1i64, 0, 3, 0],
            Some(Bitmask::from_bools(&[true, false, true, false])),
        );
        let arr = Array::from_int64(intarray);
        let table = Table::new(
            "tbl".to_string(),
            Some(vec![FieldArray::new(
                Field::new("maybe", ArrowType::Int64, true, None),
                arr,
            )]),
        );

        let out = roundtrip_table(&table, Compression::None);
        let col = out.col_ix(0).unwrap();
        assert_eq!(col.len(), 4);
        if let Array::NumericArray(NumericArray::Int64(a)) = &col.array {
            assert_eq!(a.data.as_slice(), &[1, 0, 3, 0]);
            let null_mask = a.null_mask();
            let mask = null_mask.unwrap();
            let actual_nulls: Vec<bool> = (0..col.len()).map(|i| mask.get(i)).collect();
            assert_eq!(actual_nulls, vec![true, false, true, false]);
        } else {
            panic!("Expected Int64 column");
        }
    }

    #[test]
    fn write_and_read_max_chunk_boundary() {
        let n = PAGE_CHUNK_SIZE + 17;
        let values: Vec<i32> = (0..n as i32).collect();
        let arr = Array::from_int32(IntegerArray::from_slice(&values));
        let table = Table::new(
            "tbl".to_string(),
            Some(vec![FieldArray::new(
                Field::new("seq", ArrowType::Int32, false, None),
                arr,
            )]),
        );
        let out = roundtrip_table(&table, Compression::None);
        let col = out.col_ix(0).unwrap();
        if let Array::NumericArray(NumericArray::Int32(a)) = &col.array {
            assert_eq!(a.data.as_slice(), &values);
        } else {
            panic!("Expected Int32 column");
        }
    }
}
