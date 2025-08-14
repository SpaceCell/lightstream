//! # CsvReader - *Read CSVs into `Table`*
//!
//! High-level API for reading CSV files or streams into Minarrow Tables.
//! - Supports chunked reading (i.e., batch iteration), schema inference or user-specified schema.
//! - Fully customisable options - delimiter, nulls, quoting, batch size, etc.
//! - No external dependencies.
//!
//! See `CsvDecodeOptions` for configuration.

use crate::models::decoders::csv::{CsvDecodeOptions, decode_csv};
use minarrow::{Field, Table};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

/// Reads CSV files into Minarrow Tables.
/// - Use `from_path`, `from_reader`, or `from_slice`.
/// - Includes `next_batch` for chunked reading.
/// - Supports schema inference and access.
pub struct CsvReader<R: BufRead> {
    reader: R,
    options: CsvDecodeOptions,
    schema: Option<Vec<Field>>,
    batch_size: usize,
    finished: bool,
}

impl CsvReader<BufReader<File>> {
    /// Open a CSV file at the given path with the given options.
    pub fn from_path<P: AsRef<Path>>(
        path: P,
        options: CsvDecodeOptions,
        batch_size: usize,
    ) -> io::Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(Self::from_reader(reader, options, batch_size))
    }
}

impl<R: BufRead> CsvReader<R> {
    /// Create from any BufRead (file, slice, etc.)
    pub fn from_reader(reader: R, options: CsvDecodeOptions, batch_size: usize) -> Self {
        CsvReader {
            reader,
            options,
            schema: None,
            batch_size,
            finished: false,
        }
    }

    /// Create from a byte slice.
    pub fn from_slice(
        slice: &[u8],
        options: CsvDecodeOptions,
        batch_size: usize,
    ) -> CsvReader<BufReader<&[u8]>> {
        let reader = BufReader::new(slice);
        CsvReader::from_reader(reader, options, batch_size)
    }

    /// Get the inferred or user-provided schema
    ///
    /// Requires reading first batch if not already done
    pub fn schema(&mut self) -> io::Result<&[Field]> {
        if self.schema.is_none() && !self.finished {
            if let Some(batch) = self.next_batch()? {
                self.schema = Some(
                    batch
                        .cols
                        .iter()
                        .map(|c| c.field.as_ref().clone())
                        .collect(),
                );
            }
        }
        Ok(self.schema.as_deref().unwrap_or(&[]))
    }

    /// Read the next batch of rows as a Table.
    /// Returns Ok(None) if end of file is reached
    pub fn next_batch(&mut self) -> io::Result<Option<Table>> {
        if self.finished {
            return Ok(None);
        }

        let mut batch_options = self.options.clone();

        // If we have a schema, all further batches should *not* treat any row as header
        if self.schema.is_some() {
            batch_options.has_header = false;
            batch_options.schema = self.schema.clone();
        }

        let mut buf = Vec::new();
        let mut rows = Vec::with_capacity(self.batch_size + 1);
        let mut saw_any = false;

        while rows.len()
            < self.batch_size
                + if self.schema.is_none() && self.options.has_header {
                    1
                } else {
                    0
                }
        {
            buf.clear();
            let n = self.reader.read_until(b'\n', &mut buf)?;
            if n == 0 {
                break;
            }
            if buf.ends_with(b"\r\n") {
                buf.truncate(buf.len() - 2);
            } else if buf.ends_with(b"\n") {
                buf.truncate(buf.len() - 1);
            }
            if buf.is_empty() && !saw_any {
                continue;
            }
            saw_any = true;
            rows.push(buf.clone());
        }

        if rows.is_empty() {
            self.finished = true;
            return Ok(None);
        }

        // Compose chunk for decode_csv
        let mut chunk = Vec::new();
        for line in &rows {
            chunk.extend_from_slice(line);
            chunk.push(b'\n');
        }

        let table = decode_csv(std::io::Cursor::new(chunk), &batch_options)?;

        // Capture schema after first batch
        if self.schema.is_none() {
            self.schema = Some(
                table
                    .cols
                    .iter()
                    .map(|c| c.field.as_ref().clone())
                    .collect(),
            );
        }

        // If fewer than batch_size data rows, mark as finished
        let effective_n_rows = table.n_rows;
        if effective_n_rows < self.batch_size {
            self.finished = true;
        }

        Ok(Some(table))
    }

    /// Consume the entire input and return a single Table - all rows
    pub fn into_table(mut self) -> io::Result<Table> {
        // Always respect has_header on first call
        decode_csv(&mut self.reader, &self.options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::decoders::csv::CsvDecodeOptions;
    use std::io::BufReader;

    #[test]
    fn test_csv_reader_full_table() {
        let csv = b"i,s,b\n1,hello,true\n2,,false\n3,world,1\n4,rust,0\n";
        let opts = CsvDecodeOptions::default();
        let reader = CsvReader::<BufReader<&[u8]>>::from_slice(csv, opts, 2);
        let table = reader.into_table().unwrap();
        assert_eq!(table.n_rows, 4);
        assert_eq!(table.cols.len(), 3);
    }

    #[test]
    fn test_csv_reader_batch_iter() {
        let csv = b"i,s,b\n1,hello,true\n2,,false\n3,world,1\n4,rust,0\n";
        let opts = CsvDecodeOptions::default();
        let mut reader = CsvReader::<BufReader<&[u8]>>::from_slice(csv, opts, 2);

        let mut total_rows = 0;
        while let Some(batch) = reader.next_batch().unwrap() {
            total_rows += batch.n_rows;
        }
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_csv_reader_schema() {
        let csv = b"i,s\n1,hello\n2,world\n";
        let opts = CsvDecodeOptions::default();
        let mut reader = CsvReader::<BufReader<&[u8]>>::from_slice(csv, opts, 1);
        let fields = reader.schema().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "i");
        assert_eq!(fields[1].name, "s");
    }
}
