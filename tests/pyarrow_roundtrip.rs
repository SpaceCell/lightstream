#[cfg(test)]
mod pyarrow_roundtrip_tests {
    use std::sync::Arc;

    use ::lightstream::enums::IPCMessageProtocol;
    use ::lightstream::models::readers::ipc::file_table_reader::FileTableReader;
    use ::lightstream::models::readers::ipc::table_stream_reader::TableStreamReader64;
    use ::lightstream::models::streams::disk::DiskByteStream;
    use ::lightstream::models::writers::ipc::table_stream_writer::write_tables_to_stream;
    use ::lightstream::models::writers::ipc::table_writer::write_tables_to_file;
    use futures_util::stream::StreamExt;
    use minarrow::ffi::arrow_dtype::ArrowType;
    use minarrow::*;
    use minarrow::{Array, TextArray, Vec64};

    /// Validate that data values match between expected and actual tables
    fn validate_table_data(expected: &Table, actual: &Table) {
        assert_eq!(expected.n_rows, actual.n_rows, "Row count mismatch");
        assert_eq!(
            expected.cols.len(),
            actual.cols.len(),
            "Column count mismatch"
        );

        for (i, (expected_col, actual_col)) in
            expected.cols.iter().zip(actual.cols.iter()).enumerate()
        {
            println!("Validating column {}: {}", i, expected_col.field.name);

            // Check field metadata
            assert_eq!(
                expected_col.field.name, actual_col.field.name,
                "Column {} name mismatch",
                i
            );
            assert_eq!(
                expected_col.field.dtype, actual_col.field.dtype,
                "Column {} type mismatch",
                i
            );

            // Check array data based on type
            match (&expected_col.array, &actual_col.array) {
                (
                    Array::NumericArray(NumericArray::Int32(exp)),
                    Array::NumericArray(NumericArray::Int32(act)),
                ) => {
                    assert_eq!(
                        exp.data.as_slice(),
                        act.data.as_slice(),
                        "Int32 data mismatch in column {}",
                        i
                    );
                }
                (
                    Array::NumericArray(NumericArray::Int64(exp)),
                    Array::NumericArray(NumericArray::Int64(act)),
                ) => {
                    assert_eq!(
                        exp.data.as_slice(),
                        act.data.as_slice(),
                        "Int64 data mismatch in column {}",
                        i
                    );
                }
                (
                    Array::NumericArray(NumericArray::UInt32(exp)),
                    Array::NumericArray(NumericArray::UInt32(act)),
                ) => {
                    assert_eq!(
                        exp.data.as_slice(),
                        act.data.as_slice(),
                        "UInt32 data mismatch in column {}",
                        i
                    );
                }
                (
                    Array::NumericArray(NumericArray::UInt64(exp)),
                    Array::NumericArray(NumericArray::UInt64(act)),
                ) => {
                    assert_eq!(
                        exp.data.as_slice(),
                        act.data.as_slice(),
                        "UInt64 data mismatch in column {}",
                        i
                    );
                }
                (
                    Array::NumericArray(NumericArray::Float32(exp)),
                    Array::NumericArray(NumericArray::Float32(act)),
                ) => {
                    for (e, a) in exp.data.iter().zip(act.data.iter()) {
                        assert!(
                            (e - a).abs() < f32::EPSILON,
                            "Float32 data mismatch in column {}: {} != {}",
                            i,
                            e,
                            a
                        );
                    }
                }
                (
                    Array::NumericArray(NumericArray::Float64(exp)),
                    Array::NumericArray(NumericArray::Float64(act)),
                ) => {
                    for (e, a) in exp.data.iter().zip(act.data.iter()) {
                        assert!(
                            (e - a).abs() < f64::EPSILON,
                            "Float64 data mismatch in column {}: {} != {}",
                            i,
                            e,
                            a
                        );
                    }
                }
                (Array::BooleanArray(exp), Array::BooleanArray(act)) => {
                    assert_eq!(
                        exp.len, act.len,
                        "Boolean array length mismatch in column {}",
                        i
                    );
                    for j in 0..exp.len {
                        assert_eq!(
                            exp.get(j),
                            act.get(j),
                            "Boolean data mismatch at index {} in column {}",
                            j,
                            i
                        );
                    }
                }
                (
                    Array::TextArray(TextArray::String32(exp)),
                    Array::TextArray(TextArray::String32(act)),
                ) => {
                    let exp_strings: Vec<String> = exp.iter().map(|s| s.to_string()).collect();
                    let act_strings: Vec<String> = act.iter().map(|s| s.to_string()).collect();
                    assert_eq!(
                        exp_strings, act_strings,
                        "String32 data mismatch in column {}",
                        i
                    );
                }
                (
                    Array::TextArray(TextArray::Categorical32(exp)),
                    Array::TextArray(TextArray::Categorical32(act)),
                ) => {
                    assert_eq!(
                        exp.data.as_slice(),
                        act.data.as_slice(),
                        "Categorical32 indices mismatch in column {}",
                        i
                    );
                    assert_eq!(
                        exp.unique_values, act.unique_values,
                        "Categorical32 values mismatch in column {}",
                        i
                    );
                }
                _ => panic!(
                    "Unsupported array type combination for column {} validation: {:?} vs {:?}",
                    i, expected_col.field.dtype, actual_col.field.dtype
                ),
            }
        }
    }

    /// Create the expected table that matches PyArrow-generated data (basic types only)
    fn create_expected_table(n_rows: usize) -> Table {
        let int32 = NumericArray::Int32(Arc::new(IntegerArray::from_vec64(
            Vec64::from_slice(&(1..=n_rows as i32).collect::<Vec<_>>()),
            None,
        )));
        let int64 = NumericArray::Int64(Arc::new(IntegerArray::from_vec64(
            Vec64::from_slice(&(100..(100 + n_rows as i64)).collect::<Vec<_>>()),
            None,
        )));
        let uint32 = NumericArray::UInt32(Arc::new(IntegerArray::from_vec64(
            Vec64::from_slice(&(0..n_rows as u32).collect::<Vec<_>>()),
            None,
        )));
        let uint64 = NumericArray::UInt64(Arc::new(IntegerArray::from_vec64(
            Vec64::from_slice(&(10..(10 + n_rows as u64)).collect::<Vec<_>>()),
            None,
        )));
        let float32 = NumericArray::Float32(Arc::new(FloatArray::from_vec64(
            Vec64::from_slice(
                &(0..n_rows)
                    .map(|i| i as f32 * 1.25 - 2.5)
                    .collect::<Vec<_>>(),
            ),
            None,
        )));
        let float64 = NumericArray::Float64(Arc::new(FloatArray::from_vec64(
            Vec64::from_slice(
                &(0..n_rows)
                    .map(|i| i as f64 * 3.5 - 1.0)
                    .collect::<Vec<_>>(),
            ),
            None,
        )));
        let bools = BooleanArray::from_slice(&(0..n_rows).map(|i| i % 2 == 0).collect::<Vec<_>>());

        let strs: Vec<String> = (0..n_rows).map(|i| format!("str{i}")).collect();
        let str_refs: Vec<&str> = strs.iter().map(|s| &**s).collect();
        let string32 = StringArray::from_vec(str_refs, None);

        let cols = vec![
            FieldArray::new(
                Field::new("int32", ArrowType::Int32, false, None),
                Array::NumericArray(int32),
            ),
            FieldArray::new(
                Field::new("int64", ArrowType::Int64, false, None),
                Array::NumericArray(int64),
            ),
            FieldArray::new(
                Field::new("uint32", ArrowType::UInt32, false, None),
                Array::NumericArray(uint32),
            ),
            FieldArray::new(
                Field::new("uint64", ArrowType::UInt64, false, None),
                Array::NumericArray(uint64),
            ),
            FieldArray::new(
                Field::new("float32", ArrowType::Float32, false, None),
                Array::NumericArray(float32),
            ),
            FieldArray::new(
                Field::new("float64", ArrowType::Float64, false, None),
                Array::NumericArray(float64),
            ),
            FieldArray::new(
                Field::new("bool", ArrowType::Boolean, false, None),
                Array::BooleanArray(Arc::new(bools)),
            ),
            FieldArray::new(
                Field::new("string", ArrowType::String, false, None),
                Array::TextArray(TextArray::String32(Arc::new(string32))),
            ),
        ];

        Table {
            cols,
            n_rows: n_rows,
            name: "test".to_owned(),
        }
    }

    #[tokio::test]
    async fn test_read_pyarrow_file_format() {
        println!("Testing PyArrow -> lightstream roundtrip (file format)");

        // Read PyArrow-generated Arrow file
        let file_path = "python/pyarrow_basic_types.arrow";
        let reader = FileTableReader::open(file_path).expect("Failed to open file");

        // Read the first (and likely only) table
        assert!(reader.num_batches() > 0, "No record batches found");
        let actual_table = reader.read_batch(0).expect("Failed to read first batch");

        println!(
            "Read table with {} rows and {} columns",
            actual_table.n_rows,
            actual_table.cols.len()
        );

        // Create expected table with matching data
        let expected_table = create_expected_table(4);

        // Validate the data matches
        validate_table_data(&expected_table, &actual_table);

        println!("✅ PyArrow file format read successfully!");
    }

    #[tokio::test]
    async fn test_read_pyarrow_stream_format() {
        println!("Testing PyArrow -> lightstream roundtrip (stream format)");

        // Read PyArrow-generated Arrow stream
        let file_path = "python/pyarrow_basic_types.stream";
        use ::lightstream::enums::BufferChunkSize;
        let stream = DiskByteStream::open(file_path, BufferChunkSize::Custom(1024))
            .await
            .expect("Failed to create stream");
        let mut reader = TableStreamReader64::new(stream, 1024, IPCMessageProtocol::Stream);

        let mut tables = vec![];
        while let Some(table) = reader.next().await {
            let table = table.expect("Failed to read table from stream");
            tables.push(table);
        }

        assert_eq!(tables.len(), 1, "Expected exactly one table");
        let actual_table = &tables[0];

        println!(
            "Read table with {} rows and {} columns",
            actual_table.n_rows,
            actual_table.cols.len()
        );

        // Create expected table with matching data
        let expected_table = create_expected_table(4);

        // Validate the data matches
        validate_table_data(&expected_table, actual_table);

        println!("✅ PyArrow stream format read successfully!");
    }

    #[tokio::test]
    async fn test_write_lightstream_file_format() {
        println!("Testing lightstream -> PyArrow roundtrip (file format)");

        // Create test data with lightstream
        let test_table = create_expected_table(4);
        let schema = test_table
            .cols
            .iter()
            .map(|col| (*col.field).clone())
            .collect::<Vec<_>>();

        // Write with lightstream to file format
        let file_path = "python/lightstream_basic_types.arrow";
        write_tables_to_file(file_path, &[test_table], schema)
            .await
            .expect("Failed to write table to file");

        println!("Wrote lightstream file to: {}", file_path);
        println!("✅ lightstream file format written successfully!");
        println!("Run `python3 python/validate_lightstream_output.py` to validate with PyArrow");
    }

    #[tokio::test]
    async fn test_write_lightstream_stream_format() {
        println!("Testing lightstream -> PyArrow roundtrip (stream format)");

        // Create test data with lightstream
        let test_table = create_expected_table(4);
        let schema = test_table
            .cols
            .iter()
            .map(|col| (*col.field).clone())
            .collect::<Vec<_>>();

        // Write with lightstream to stream format
        let file_path = "python/lightstream_basic_types.stream";
        let file = tokio::fs::File::create(file_path)
            .await
            .expect("Failed to create stream file");

        write_tables_to_stream::<_, Vec64<u8>>(
            file,
            &[test_table],
            schema,
            IPCMessageProtocol::Stream,
        )
        .await
        .expect("Failed to write table to stream");

        println!("Wrote lightstream stream to: {}", file_path);
        println!("✅ lightstream stream format written successfully!");
        println!("Run `python3 python/validate_lightstream_output.py` to validate with PyArrow");
    }
}
