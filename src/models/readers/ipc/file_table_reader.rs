//! # Arrow IPC File Reader - *Heap-backed version*
//!
//! ## Overview
//! Reads Arrow IPC file format into heap memory. Parses the footer,
//! loads schema, dictionaries, and record batch blocks, and exposes batches as
//! `Table` or a `SuperTable` aggregation.
//!
//! Consistent with the Arrow IPC file specification; expects opening/closing magic,
//! footer length, and block tables.
//!
/// # Which reader?
/// - **Speed**: Prefer the mmap variant [`MmapTableReader`] when zero-copy performance is required -
/// for e.g., the MMAP version can read millions of rows in microseconds, microseconds, and very large volumes in milliseconds.
/// - **Flexibility**: this standard reader is more flexible as it is not tied to memory-mapped shared memory.
use std::collections::HashSet;
use std::fs::File;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use minarrow::Vec64;
use minarrow::structs::shared_buffer::SharedBuffer;

/// Read exactly `buf.len()` bytes starting at `offset` without touching
/// the file's seek position. Backed by `pread(2)` on Unix and a
/// `seek_read` loop on Windows so callers can issue many positional
/// reads against the same shared `&File` without serialising through
/// `&mut self` or reopening the file. The wrapper exists only because
/// `seek_read` on Windows can return short, so callers can't share a
/// single-call shape with Unix's `read_exact_at`.
#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut total = 0;
    while total < buf.len() {
        let n = file.seek_read(&mut buf[total..], offset + total as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "file ended before requested offset/length",
            ));
        }
        total += n;
    }
    Ok(())
}

use flatbuffers::Vector;
use minarrow::{Field, SuperTable, Table};

use crate::arrow::file::org::apache::arrow::flatbuf as fbf;
use crate::arrow::message::org::apache::arrow::flatbuf as fbm;
use crate::constants::ARROW_MAGIC_NUMBER;
use crate::models::decoders::ipc::parser::{
    convert_fb_field_to_arrow, decode_record_batch, handle_dictionary_batch,
};
#[cfg(any(feature = "zstd", feature = "snappy"))]
use crate::models::decoders::ipc::parser::{decompress_sequential_body, is_body_compressed};
use crate::models::decoders::limits::DecodeLimits;

/// Footer-declared block entry (i.e., offsets/lengths) for a dictionary or record batch.
#[derive(Debug, Clone)]
struct IPCFileBlock {
    /// Absolute byte offset of the block in the file.
    offset: usize,
    /// Length of the FlatBuffers message metadata segment in bytes.
    meta_bytes: usize,
    /// Length of the data body segment in bytes.
    body_bytes: usize,
}

/// Heap-allocated Arrow file reader.
///
/// # Which reader?
/// - **Speed**: Prefer the mmap variant [`MmapTableReader`] when zero-copy performance is required -
/// for e.g., the MMAP version can read millions of rows in microseconds, and very large volumes in milliseconds.
/// - **Flexibility**: this standard reader is more flexible as it is not tied to memory-mapped
/// shared memory.
#[derive(Clone)]
pub struct FileTableReader {
    /// Open file handle shared across the reader's lifetime. All block
    /// reads go through positional reads (`pread`/`seek_read`) on this
    /// handle, so opening the file once amortises across the footer
    /// parse, every dictionary block, and every record batch read.
    file: Arc<File>,
    /// Arrow schema fields from the file footer
    schema: Vec<Arc<Field>>,
    /// Footer-declared dictionary block table
    dict_blocks: Vec<IPCFileBlock>,
    /// Footer-declared record batch block table
    record_blocks: Vec<IPCFileBlock>,
    /// Loaded dictionaries keyed by dictionary id
    dictionaries: std::collections::HashMap<i64, Vec<String>>,
}

impl FileTableReader {
    /// Open an Arrow IPC file into heap memory and parse footer/schema/block tables.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path.as_ref())?;
        let file_len = file.metadata()?.len() as usize;

        if file_len < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file too small for Arrow",
            ));
        }

        // Read only the trailing 10 bytes: footer_len (4) + closing magic (6).
        let mut tail = [0u8; 10];
        read_at(&file, &mut tail, (file_len - 10) as u64)?;

        if &tail[4..] != ARROW_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing closing magic",
            ));
        }

        let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap()) as usize;
        let footer_start = file_len - 10 - footer_len;
        if footer_start < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "footer out of bounds",
            ));
        }

        // Read just the footer
        let mut footer_buf = vec![0u8; footer_len];
        read_at(&file, &mut footer_buf, footer_start as u64)?;

        let footer_msg = flatbuffers::root::<fbf::Footer>(&footer_buf).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("bad footer: {e}"))
        })?;

        let fb_schema = footer_msg
            .schema()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "footer missing schema"))?;
        let mut fields = Vec::with_capacity(fb_schema.fields().unwrap().len());
        for i in 0..fb_schema.fields().unwrap().len() {
            let f = convert_fb_field_to_arrow(&fb_schema.fields().unwrap().get(i))?;
            fields.push(Arc::new(f));
        }

        let dict_blocks = footer_msg
            .dictionaries()
            .unwrap_or_else(|| unsafe { Vector::new(&[], 0) })
            .iter()
            .map(|b| IPCFileBlock {
                offset: b.offset() as usize,
                meta_bytes: b.metaDataLength() as usize,
                body_bytes: b.bodyLength() as usize,
            })
            .collect::<Vec<_>>();

        let record_blocks = footer_msg
            .recordBatches()
            .unwrap()
            .iter()
            .map(|b| IPCFileBlock {
                offset: b.offset() as usize,
                meta_bytes: b.metaDataLength() as usize,
                body_bytes: b.bodyLength() as usize,
            })
            .collect::<Vec<_>>();

        let mut rdr = Self {
            file: Arc::new(file),
            schema: fields,
            dict_blocks,
            record_blocks,
            dictionaries: std::collections::HashMap::new(),
        };

        rdr.load_all_dictionaries()?;
        Ok(rdr)
    }

    /// Return the parsed schema fields
    #[inline]
    pub fn schema(&self) -> &[Arc<Field>] {
        &self.schema
    }

    /// Return the number of record batches in the file
    #[inline]
    pub fn num_batches(&self) -> usize {
        self.record_blocks.len()
    }

    /// Read the `idx`th record batch as a `Table`
    pub fn read_batch(&self, idx: usize) -> io::Result<Table> {
        let blk = self
            .record_blocks
            .get(idx)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "batch idx OOB"))?;
        self.parse_batch_block(blk, None)
    }

    /// Read the `idx`th record batch, materialising only the named columns.
    ///
    /// Column names must match schema field names. Returns an error if any
    /// name is not found. The returned Table contains only the projected
    /// columns, in schema order.
    pub fn read_columns(&self, idx: usize, columns: &[&str]) -> io::Result<Table> {
        let blk = self
            .record_blocks
            .get(idx)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "batch idx OOB"))?;
        let projection = self.resolve_column_indices(columns)?;
        self.parse_batch_block(blk, Some(&projection))
    }

    /// Alias of [`read_batch`]
    #[inline]
    pub fn into_table(&self, idx: usize) -> io::Result<Table> {
        self.read_batch(idx)
    }

    /// Read all record batches and assemble them into a `SuperTable`
    ///
    /// If `name_override` is provided, that name is used for the resulting table
    pub fn into_supertable(&self, name_override: Option<String>) -> io::Result<SuperTable> {
        let mut batches = Vec::with_capacity(self.record_blocks.len());
        for blk in &self.record_blocks {
            batches.push(Arc::new(self.parse_batch_block(blk, None)?));
        }
        Ok(SuperTable::from_batches(batches, name_override))
    }

    /// Read a block from disk into 64-byte aligned memory.
    ///
    /// Uses positional I/O (`pread`/`seek_read`) on the reader's shared
    /// `Arc<File>` so concurrent block reads do not need `&mut self` and
    /// the file is opened only once per reader.
    fn read_block(&self, blk: &IPCFileBlock) -> io::Result<Vec64<u8>> {
        let total = blk.meta_bytes + blk.body_bytes;
        let mut buf = Vec64::with_capacity(total);
        // SAFETY: `total` equals `buf.capacity()` and `read_at` is the
        // read_exact_at-style wrapper above: it either fills every byte
        // we just exposed via `set_len` or returns Err, in which case
        // `buf` is dropped without anyone observing the uninitialised
        // tail. No bytes between [0..total] are read before read_at writes.
        unsafe { buf.set_len(total); }
        read_at(&self.file, &mut buf, blk.offset as u64)?;
        Ok(buf)
    }

    /// Parse the IPC frame header from a block buffer, returning the
    /// metadata slice. Validates the continuation marker.
    fn parse_frame_header<'a>(buf: &'a [u8]) -> io::Result<&'a [u8]> {
        if buf.len() < 8 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "block too short"));
        }
        let cont = u32::from_le_bytes(buf[..4].try_into().unwrap());
        if cont != 0xFFFF_FFFF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad continuation marker: {cont:#X}"),
            ));
        }
        let meta_len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let end = 8 + meta_len;
        if end > buf.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "metadata OOB"));
        }
        Ok(&buf[8..end])
    }

    /// Load and materialise all dictionary batches declared in the footer
    fn load_all_dictionaries(&mut self) -> io::Result<()> {
        let mut new_dicts = std::collections::HashMap::<i64, Vec<String>>::new();
        for blk in &self.dict_blocks {
            let buf = self.read_block(blk)?;
            let meta = Self::parse_frame_header(&buf)?;
            let fb_msg = flatbuffers::root::<fbm::Message>(meta).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad dict msg: {e}"))
            })?;
            let dict_batch = fb_msg.header_as_dictionary_batch().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "expected DictionaryBatch")
            })?;
            let body = &buf[blk.meta_bytes..blk.meta_bytes + blk.body_bytes];
            handle_dictionary_batch(&dict_batch, body, &mut new_dicts, DecodeLimits::default())?;
        }
        self.dictionaries = new_dicts;
        Ok(())
    }

    /// Resolve column names to their schema indices, erroring on unknown names.
    fn resolve_column_indices(&self, columns: &[&str]) -> io::Result<HashSet<usize>> {
        let mut indices = HashSet::with_capacity(columns.len());
        for name in columns {
            let idx = self.schema.iter().position(|f| f.name == *name)
                .ok_or_else(|| io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("column '{}' not found in schema", name),
                ))?;
            indices.insert(idx);
        }
        Ok(indices)
    }

    /// Parse a record batch block by reading it from disk on demand.
    /// When `projection` is `Some`, only the specified columns are materialised.
    fn parse_batch_block(
        &self,
        blk: &IPCFileBlock,
        projection: Option<&HashSet<usize>>,
    ) -> io::Result<Table> {
        let buf = self.read_block(blk)?;
        let body_offset = blk.meta_bytes;
        let body_len = blk.body_bytes;
        let fields: Vec<_> = self.schema.iter().map(|a| a.as_ref().clone()).collect();

        // Wrap in SharedBuffer first, then parse metadata from the slice.
        // This avoids a borrow-then-move conflict on buf.
        let shared = SharedBuffer::from_vec64(buf);
        let meta = Self::parse_frame_header(shared.as_slice())?;
        let fb_msg = flatbuffers::root::<fbm::Message>(meta).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("bad record msg: {e}"))
        })?;
        let rec = fb_msg.header_as_record_batch().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "expected RecordBatch header")
        })?;

        #[cfg(any(feature = "zstd", feature = "snappy"))]
        {
            let body_data = &shared.as_slice()[body_offset..body_offset + body_len];
            let buffers = rec
                .buffers()
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no buffers"))?;
            if is_body_compressed(&buffers, body_data) {
                let (decompressed_body, _offsets) =
                    decompress_sequential_body(&buffers, body_data)?;
                let decompressed_shared = SharedBuffer::from_vec64(Vec64::from_slice(&decompressed_body));
                let (table, _) = decode_record_batch(
                    &rec, &fields, &self.dictionaries, decompressed_shared, 0, decompressed_body.len(), projection, DecodeLimits::default(),
                )?;
                return Ok(table);
            }
        }

        let (table, _) = decode_record_batch(
            &rec, &fields, &self.dictionaries, shared.clone(), body_offset, body_len, projection, DecodeLimits::default(),
        )?;
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use minarrow::{Array, NumericArray, TextArray};
    use tracing::debug;

    use crate::{
        models::readers::ipc::file_table_reader::FileTableReader,
        test_helpers::{make_all_types_table, write_test_table_to_file},
    };

    #[tokio::test]
    async fn test_single_batch_roundtrip_heap() {
        let table = make_all_types_table();
        let temp = write_test_table_to_file(&[table.clone()]).await;
        let rdr = FileTableReader::open(&temp.path()).unwrap();
        assert_eq!(rdr.num_batches(), 1);
        let table2 = rdr.read_batch(0).unwrap();

        assert_eq!(table2.n_rows, 4);
        assert_eq!(table2.cols.len(), table.cols.len());

        println!("TABLE {:?}\n", &table2);

        // Int32 col: sum, buffer type
        match &table2.cols[0].array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                let s: i32 = arr.data.as_ref().iter().sum();
                assert_eq!(s, 10);
                // Check if buffer is shared (will be true if data is 64-byte aligned in file)
                // If not aligned, minarrow will clone for safety
                if arr.data.is_shared() {
                    debug!("Int32 buffer is shared (zero-copy)");
                } else {
                    debug!("Int32 buffer was cloned (not 64-byte aligned in file)");
                }
            }
            _ => panic!("wrong type"),
        }
        // Float64 col: value and buffer type
        match &table2.cols[5].array {
            Array::NumericArray(NumericArray::Float64(arr)) => {
                let vals: Vec<_> = arr.data.as_ref().iter().cloned().collect();
                assert_eq!(vals, vec![1.1, 2.2, 3.3, 4.4]);
                // Check if buffer is shared (will be true if data is 64-byte aligned in file)
                // If not aligned, minarrow will clone for safety
                if arr.data.is_shared() {
                    debug!("Float64 buffer is shared (zero-copy)");
                } else {
                    debug!("Float64 buffer was cloned (not 64-byte aligned in file)");
                }
            }
            _ => panic!("wrong type"),
        }
        // Check at least one string, bool, all others present
        let mut seen_string = false;
        let mut seen_bool = false;
        let mut any_shared = false;
        for arr in &table2.cols {
            match &arr.array {
                Array::TextArray(TextArray::String32(a)) => {
                    seen_string = true;
                    if a.data.is_shared() {
                        debug!("String32 data buffer is shared (zero-copy)");
                        any_shared = true;
                    } else {
                        debug!("String32 data buffer was cloned (not 64-byte aligned in file)");
                    }
                }
                Array::BooleanArray(a) => {
                    seen_bool = true;
                    if a.data.bits.is_shared() {
                        debug!("Boolean bits buffer is shared (zero-copy)");
                        any_shared = true;
                    } else {
                        debug!("Boolean bits buffer was cloned (not 64-byte aligned in file)");
                    }
                }
                _ => {}
            }
        }
        assert!(
            seen_string && seen_bool,
            "String32 and Bool must be present"
        );
        debug!("Any buffers shared: {}", any_shared);
        drop(rdr);
        drop(temp);
    }

    #[tokio::test]
    async fn test_shared_buffers_with_aligned_data() {
        // Arrow file structure:
        // 1. Magic "ARROW1\0\0"
        // 2. Schema message (aligned)
        // 3. Record batch message (aligned)
        // 4. Footer
        // 5. Footer length (4 bytes)
        // 6. Magic "ARROW1\0\0"

        // For now, just test that our reader works with the regular file
        // and report on sharing status
        let table = make_all_types_table();
        let tables = vec![table.clone()];
        let temp = write_test_table_to_file(&tables).await;

        let rdr = FileTableReader::open(&temp.path()).unwrap();
        assert_eq!(rdr.num_batches(), 1);
        let table2 = rdr.read_batch(0).unwrap();

        // Count how many buffers are shared vs cloned
        let mut shared_count = 0;
        let mut cloned_count = 0;

        for col in &table2.cols {
            match &col.array {
                Array::NumericArray(na) => match na {
                    NumericArray::Int32(arr) if arr.data.is_shared() => shared_count += 1,
                    NumericArray::Int64(arr) if arr.data.is_shared() => shared_count += 1,
                    NumericArray::UInt32(arr) if arr.data.is_shared() => shared_count += 1,
                    NumericArray::UInt64(arr) if arr.data.is_shared() => shared_count += 1,
                    NumericArray::Float32(arr) if arr.data.is_shared() => shared_count += 1,
                    NumericArray::Float64(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::Int8(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::Int16(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::UInt8(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(feature = "extended_numeric_types")]
                    NumericArray::UInt16(arr) if arr.data.is_shared() => shared_count += 1,
                    _ => cloned_count += 1,
                },
                Array::BooleanArray(arr) => {
                    if arr.data.bits.is_shared() {
                        shared_count += 1;
                    } else {
                        cloned_count += 1;
                    }
                }
                Array::TextArray(ta) => match ta {
                    TextArray::String32(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(feature = "large_string")]
                    TextArray::String64(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(any(not(feature = "default_categorical_8"), feature = "extended_categorical"))]
                    TextArray::Categorical32(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(feature = "default_categorical_8")]
                    TextArray::Categorical8(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(feature = "extended_categorical")]
                    TextArray::Categorical16(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(feature = "extended_categorical")]
                    TextArray::Categorical64(arr) if arr.data.is_shared() => shared_count += 1,
                    _ => cloned_count += 1,
                },
                _ => {}
            }
        }

        debug!(
            "Shared buffers: {}, Cloned buffers: {}",
            shared_count, cloned_count
        );
        debug!("Note: Cloning is expected when file data is not 64-byte aligned.");
        debug!("The writer currently doesn't guarantee 64-byte alignment.");

        // We don't assert on specific counts because alignment depends on the writer
        // Just verify the file was read correctly
        assert_eq!(table2.n_rows, 4);
        assert_eq!(table2.cols.len(), table.cols.len());
    }

}
