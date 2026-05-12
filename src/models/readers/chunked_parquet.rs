//! # Chunked Parquet reader
//!
//! Reads a directory of `<base>-NNNNNNNNNN.parquet` files emitted by
//! `ChunkedParquetWriter` and presents them as an ordered iterator of
//! `Table`s. Each chunk file is a complete, independently parseable
//! Parquet file. The reader sorts files by their numeric index so
//! consumers see batches in write order.
//!
//! ## Concurrency model
//!
//! The default [`Iterator`] path is sync and serial. Per-file Parquet
//! decode is CPU-heavy (decompression, page parsing, dictionary
//! resolution), so across files the gain from parallel reads is larger
//! than for raw IPC. [`ChunkedTableReader::par_read_all`] (inherited from
//! the trait) uses `std::thread::scope` to fan per-chunk work end-to-end
//! across worker threads and returns a `SuperTable` with batches in
//! write order.

use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};

use minarrow::Table;

use crate::error::IoError;
use crate::models::readers::parquet_reader::read_parquet_table;
use crate::traits::chunked_table_reader::ChunkedTableReader;

/// Iterator over chunk files in a directory written by
/// `ChunkedParquetWriter`. Yields one `Table` per chunk file in ascending
/// index order; the previous chunk's file handle is dropped before the
/// next is opened.
pub struct ChunkedParquetReader {
    paths: std::vec::IntoIter<PathBuf>,
}

impl ChunkedTableReader for ChunkedParquetReader {
    type Error = IoError;
    type Options = ();

    fn open<P: AsRef<Path>>(dir: P, base: &str, _options: ()) -> Result<Self, IoError> {
        let paths = Self::list_paths(dir, base)?;
        Ok(Self {
            paths: paths.into_iter(),
        })
    }

    fn list_paths<P: AsRef<Path>>(dir: P, base: &str) -> Result<Vec<PathBuf>, IoError> {
        let prefix = format!("{base}-");
        let mut indexed: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(dir.as_ref())? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
            if !name.starts_with(&prefix) || !name.ends_with(".parquet") {
                continue;
            }
            let index_str = &name[prefix.len()..name.len() - ".parquet".len()];
            let Ok(index) = index_str.parse::<u64>() else { continue };
            indexed.push((index, path));
        }
        indexed.sort_by_key(|(i, _)| *i);
        Ok(indexed.into_iter().map(|(_, p)| p).collect())
    }

    fn read_chunk(path: &Path, _options: &()) -> Result<Table, IoError> {
        let file = File::open(path).map_err(IoError::from)?;
        read_parquet_table(BufReader::new(file))
    }
}

impl Iterator for ChunkedParquetReader {
    type Item = Result<Table, IoError>;

    fn next(&mut self) -> Option<Self::Item> {
        let path = self.paths.next()?;
        Some(Self::read_chunk(&path, &()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::Compression;
    use crate::models::writers::chunked_parquet::ChunkedParquetWriter;
    use crate::traits::chunked_table_writer::ChunkedTableWriter;
    use minarrow::{Table, fa_i32};

    #[test]
    fn par_read_all_returns_batches_in_write_order() {
        let dir = std::env::temp_dir().join("lightstream_chunked_parquet_par_reader");
        let _ = fs::remove_dir_all(&dir);

        let mut w = ChunkedParquetWriter::new(&dir, "part", Compression::None).unwrap();
        for i in 0..8i32 {
            w.write_chunk(&Table::new("b".into(), Some(vec![fa_i32!("n", i, i + 100)])))
                .unwrap();
        }

        let st = ChunkedParquetReader::par_read_all(&dir, "part", (), None).unwrap();
        assert_eq!(st.batches.len(), 8);
        for (i, batch) in st.batches.iter().enumerate() {
            assert_eq!(batch.n_rows, 2);
            // First column's first row should be `i`, confirming write order
            // round-trips through parallel decode unchanged.
            let arr = &batch.cols[0].array;
            let s = format!("{arr:?}");
            assert!(
                s.contains(&format!("{i}")),
                "batch {i} first column did not contain {i}: {s}"
            );
        }

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn categorical_column_roundtrips_via_dictionary_page() {
        // Regression: the reader's DictionaryPageHeader inner parser used
        // to consume raw i32 values without the Thrift type/id prefixes
        // the writer emits. That left the cursor 3 bytes shy of the
        // payload, so the dictionary bytes were misaligned and the next
        // length prefix was parsed as garbage - causing
        // `parse_dictionary_values` to ask for ~50 MB and fail with
        // `UnexpectedEof`. This test exercises the round-trip end-to-end.
        use std::sync::Arc;
        use minarrow::{
            Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, TextArray,
            Vec64,
            ffi::arrow_dtype::CategoricalIndexType,
        };
        use crate::models::readers::parquet_reader::read_parquet_table;
        use crate::models::writers::parquet_writer::write_parquet_table;

        let n_rows = 256usize;
        let indices: Vec64<u8> = (0..n_rows).map(|i| (i % 3) as u8).collect();
        let dict_col = FieldArray::new(
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
        );
        let table = Table::new("t".into(), Some(vec![dict_col]));

        let path = std::env::temp_dir().join("ls_categorical_roundtrip.parquet");
        let _ = std::fs::remove_file(&path);
        write_parquet_table(
            &table,
            std::fs::File::create(&path).unwrap(),
            Compression::None,
        )
        .unwrap();

        let got = read_parquet_table(std::io::BufReader::new(
            std::fs::File::open(&path).unwrap(),
        ))
        .expect("categorical column must round-trip via Parquet");
        assert_eq!(got.n_rows, n_rows);
        assert_eq!(got.cols.len(), 1);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn par_read_all_handles_empty_directory() {
        let dir = std::env::temp_dir().join("lightstream_chunked_parquet_par_reader_empty");
        let _ = fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let st = ChunkedParquetReader::par_read_all(&dir, "part", (), None).unwrap();
        assert!(st.batches.is_empty());
        assert_eq!(st.n_rows, 0);

        fs::remove_dir_all(&dir).ok();
    }
}
