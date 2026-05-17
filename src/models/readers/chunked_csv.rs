//! # Chunked CSV reader
//!
//! Reads a directory of `<base>-NNNNNNNNNN.csv` files emitted by
//! `ChunkedCsvWriter` and presents them as an ordered iterator of `Table`s.
//! Each chunk file is a complete, independently parseable CSV (header +
//! rows). The reader sorts files by their numeric index so consumers see
//! batches in write order.
//!
//! Inherits [`ChunkedTableReader::par_read_all`] for sync parallel
//! decode across chunk files.

use std::fs::{self, File};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use minarrow::{Concatenate, Table};

use crate::models::decoders::csv::CsvDecodeOptions;
use crate::models::readers::csv_reader::CsvReader;
use crate::traits::chunked_table_reader::ChunkedTableReader;

/// Per-format `Options` for `ChunkedCsvReader::open`.
#[derive(Debug, Clone)]
pub struct ChunkedCsvReadOptions {
    pub decode: CsvDecodeOptions,
    pub batch_size: usize,
}

impl Default for ChunkedCsvReadOptions {
    fn default() -> Self {
        Self {
            decode: CsvDecodeOptions::default(),
            batch_size: usize::MAX,
        }
    }
}

/// Iterator over chunk files in a directory written by `ChunkedCsvWriter`.
///
/// Yields one `Table` per chunk file in ascending index order. `next` reads
/// the next file lazily; the previous file's reader is dropped before the
/// next is opened, so file handles do not accumulate.
pub struct ChunkedCsvReader {
    paths: std::vec::IntoIter<PathBuf>,
    options: ChunkedCsvReadOptions,
}

impl ChunkedTableReader for ChunkedCsvReader {
    type Error = io::Error;
    type Options = ChunkedCsvReadOptions;

    fn open<P: AsRef<Path>>(
        dir: P,
        base: &str,
        options: ChunkedCsvReadOptions,
    ) -> io::Result<Self> {
        let paths = Self::list_paths(dir, base)?;
        Ok(Self {
            paths: paths.into_iter(),
            options,
        })
    }

    fn list_paths<P: AsRef<Path>>(dir: P, base: &str) -> io::Result<Vec<PathBuf>> {
        let prefix = format!("{base}-");
        let mut indexed: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(dir.as_ref())? {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if !name.starts_with(&prefix) || !name.ends_with(".csv") {
                continue;
            }
            let index_str = &name[prefix.len()..name.len() - ".csv".len()];
            let Ok(index) = index_str.parse::<u64>() else {
                continue;
            };
            indexed.push((index, path));
        }
        indexed.sort_by_key(|(i, _)| *i);
        Ok(indexed.into_iter().map(|(_, p)| p).collect())
    }

    fn read_chunk(path: &Path, options: &ChunkedCsvReadOptions) -> io::Result<Table> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut csv = CsvReader::from_reader(reader, options.decode.clone(), options.batch_size);
        // A chunk file is one complete CSV; pull its single batch (or
        // accumulate batches if the chunk is large enough that the reader
        // splits internally) into one Table.
        let mut accumulated: Option<Table> = None;
        loop {
            match csv.next_batch()? {
                Some(batch) => {
                    accumulated = Some(match accumulated {
                        None => batch,
                        Some(existing) => existing.concat(batch).map_err(|e| {
                            io::Error::new(io::ErrorKind::InvalidData, format!("concat: {e}"))
                        })?,
                    });
                }
                None => break,
            }
        }
        Ok(accumulated.unwrap_or_default())
    }
}

impl Iterator for ChunkedCsvReader {
    type Item = io::Result<Table>;

    fn next(&mut self) -> Option<Self::Item> {
        let path = self.paths.next()?;
        Some(Self::read_chunk(&path, &self.options))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::encoders::csv::CsvEncodeOptions;
    use crate::models::writers::chunked_csv::ChunkedCsvWriter;
    use crate::traits::chunked_table_writer::ChunkedTableWriter;
    use minarrow::{Table, fa_i32};

    #[test]
    fn read_all_unifies_in_write_order() {
        let dir = std::env::temp_dir().join("lightstream_chunked_csv_test_reader");
        let _ = fs::remove_dir_all(&dir);

        let mut w = ChunkedCsvWriter::new(&dir, "part", CsvEncodeOptions::default()).unwrap();
        w.write_chunk(&Table::new("b".into(), Some(vec![fa_i32!("n", 1, 2, 3)])))
            .unwrap();
        w.write_chunk(&Table::new("b".into(), Some(vec![fa_i32!("n", 4, 5)])))
            .unwrap();
        w.write_chunk(&Table::new(
            "b".into(),
            Some(vec![fa_i32!("n", 6, 7, 8, 9)]),
        ))
        .unwrap();

        let reader = ChunkedCsvReader::open(
            &dir,
            "part",
            ChunkedCsvReadOptions {
                decode: CsvDecodeOptions::default(),
                batch_size: 1024,
            },
        )
        .unwrap();
        let combined = reader.read_all().unwrap();
        assert_eq!(combined.n_rows, 9);
        assert_eq!(combined.batches.len(), 3);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn par_read_all_returns_batches_in_write_order() {
        let dir = std::env::temp_dir().join("lightstream_chunked_csv_par_reader");
        let _ = fs::remove_dir_all(&dir);

        let mut w = ChunkedCsvWriter::new(&dir, "part", CsvEncodeOptions::default()).unwrap();
        for i in 0..12i32 {
            w.write_chunk(&Table::new(
                "b".into(),
                Some(vec![fa_i32!["n", i, i + 100]]),
            ))
            .unwrap();
        }

        let st = ChunkedCsvReader::par_read_all(
            &dir,
            "part",
            ChunkedCsvReadOptions {
                decode: CsvDecodeOptions::default(),
                batch_size: 1024,
            },
            // Force a non-default thread count so we exercise the override path.
            Some(2),
        )
        .unwrap();
        assert_eq!(st.batches.len(), 12);
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
    fn iterator_yields_each_chunk_in_order() {
        let dir = std::env::temp_dir().join("lightstream_chunked_csv_test_iter");
        let _ = fs::remove_dir_all(&dir);

        let mut w = ChunkedCsvWriter::new(&dir, "part", CsvEncodeOptions::default()).unwrap();
        w.write_chunk(&Table::new("b".into(), Some(vec![fa_i32!("n", 10)])))
            .unwrap();
        w.write_chunk(&Table::new("b".into(), Some(vec![fa_i32!("n", 20, 21)])))
            .unwrap();

        let reader = ChunkedCsvReader::open(
            &dir,
            "part",
            ChunkedCsvReadOptions {
                decode: CsvDecodeOptions::default(),
                batch_size: 1024,
            },
        )
        .unwrap();
        let lengths: Vec<usize> = reader.map(|t| t.unwrap().n_rows()).collect();
        assert_eq!(lengths, vec![1, 2]);

        fs::remove_dir_all(&dir).ok();
    }
}
