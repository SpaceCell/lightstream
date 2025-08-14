use lightstream::models::writers::ipc::table_writer::write_table_to_file;
use minarrow::{
    Array, ArrowType, BooleanArray, Buffer, CategoricalArray, Field, FieldArray, IntegerArray,
    StringArray, Table, Vec64, vec64,
};
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // --- Table 1 ---
        let tbl1 = Table::new(
            "tbl1".to_string(),
            vec![
                FieldArray::new(
                    Field::new("id", ArrowType::Int32, true, None),
                    Array::from_int32(IntegerArray::<i32>::from_vec64(
                        vec64![1_i32, 2, 3, 4],
                        None,
                    )),
                ),
            ]
            .into(),
        );

        write_table_to_file(
            "t1.arrow",
            &tbl1,
            tbl1.schema().iter().map(|arc| (**arc).clone()).collect(),
        )
        .await
        .unwrap();

        // --- Table 2 ---
        let tbl2 = Table::new(
            "tbl2".to_string(),
            vec![
                FieldArray::new(
                    Field::new("name", ArrowType::String, true, None),
                    Array::from_string32(StringArray::from_vec64(
                        vec64!["alice", "bob", "cindy", "dan"],
                        None,
                    )),
                ),
                FieldArray::new(
                    Field::new("active", ArrowType::Boolean, false, None),
                    Array::from_bool(BooleanArray::from_vec64(
                        vec64![true, false, true, true],
                        None,
                    )),
                ),
            ]
            .into(),
        );
        write_table_to_file(
            "t2.arrow",
            &tbl2,
            tbl2.schema().iter().map(|arc| (**arc).clone()).collect(),
        )
        .await
        .unwrap();

        // --- Table 3 ---
        let categories = vec!["red".to_string(), "green".to_string(), "blue".to_string()];
        let cat_arr = CategoricalArray {
            data: Buffer::from(Vec64::from_slice(&[0u32, 2, 1, 1])),
            unique_values: Vec64::from(categories.clone()),
            null_mask: None,
        };
        let tbl3 = Table::new(
            "tbl3".to_string(),
            vec![FieldArray::new(
                Field::new(
                    "category",
                    ArrowType::Dictionary(minarrow::ffi::arrow_dtype::CategoricalIndexType::UInt32),
                    true,
                    None,
                ),
                Array::from_categorical32(cat_arr),
            )]
            .into(),
        );
        write_table_to_file(
            "t3.arrow",
            &tbl3,
            tbl3.schema().iter().map(|arc| (**arc).clone()).collect(),
        )
        .await
        .unwrap();
    });
}
