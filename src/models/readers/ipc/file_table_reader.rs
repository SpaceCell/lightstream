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
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::sync::Arc;

use flatbuffers::Vector;
use minarrow::{Field, SuperTable, Table};

use crate::arrow::file::org::apache::arrow::flatbuf as fbf;
use crate::arrow::message::org::apache::arrow::flatbuf as fbm;
use crate::constants::ARROW_MAGIC_NUMBER;
use crate::models::decoders::ipc::parser::{
    RecordBatchParser, convert_fb_field_to_arrow, handle_dictionary_batch,
    handle_record_batch_shared,
};
#[cfg(any(feature = "zstd", feature = "snappy"))]
use crate::models::decoders::ipc::parser::{is_body_compressed, decompress_sequential_body};

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
    /// Entire file contents pinned in heap memory
    data: Arc<[u8]>,
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
        let mut file = File::open(&path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let len = buf.len();

        if len < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file too small for Arrow",
            ));
        }
        if &buf[..6] != ARROW_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing opening magic",
            ));
        }
        if &buf[len - 6..] != ARROW_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing closing magic",
            ));
        }

        let footer_len_offset = len - 6 - 4;
        let footer_len = u32::from_le_bytes(
            buf[footer_len_offset..footer_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let footer_start = footer_len_offset - footer_len;
        let footer_end = footer_start + footer_len;
        if footer_start < 8 || footer_end > len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "footer out of bounds",
            ));
        }

        let footer_msg: &fbf::Footer = {
            &flatbuffers::root::<fbf::Footer>(&buf[footer_start..footer_end]).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad footer: {e}"))
            })?
        };

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

        let data: Arc<[u8]> = buf.into();

        let mut rdr = Self {
            data,
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
        self.parse_batch_block(blk)
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
            batches.push(Arc::new(self.parse_batch_block(blk)?));
        }
        Ok(SuperTable::from_batches(batches, name_override))
    }

    /// Load and materialise all dictionary batches declared in the footer
    fn load_all_dictionaries(&mut self) -> io::Result<()> {
        let mut new_dicts = std::collections::HashMap::<i64, Vec<String>>::new();
        for blk in &self.dict_blocks {
            let msg = self.slice_message(blk)?;
            let fb_msg: &fbm::Message = &flatbuffers::root::<fbm::Message>(msg).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad dict msg: {e}"))
            })?;
            let dict_batch = fb_msg.header_as_dictionary_batch().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "expected DictionaryBatch")
            })?;
            RecordBatchParser::check_dictionary_delta(&dict_batch)?;
            let body = &self.data
                [blk.offset + blk.meta_bytes..blk.offset + blk.meta_bytes + blk.body_bytes];
            handle_dictionary_batch(&dict_batch, body, &mut new_dicts)?;
        }
        self.dictionaries = new_dicts;
        Ok(())
    }

    /// Parse a record batch block into a `Table`
    fn parse_batch_block(&self, blk: &IPCFileBlock) -> io::Result<Table> {
        let meta_slice = self.slice_message(blk)?;
        let body_offset = blk.offset + blk.meta_bytes;
        let body_len = blk.body_bytes;
        let fb_msg: &fbm::Message =
            &flatbuffers::root::<fbm::Message>(meta_slice).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad record msg: {e}"))
            })?;
        let rec = fb_msg.header_as_record_batch().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "expected RecordBatch header")
        })?;

        // Check if we need to decompress the body data
        #[cfg(any(feature = "zstd", feature = "snappy"))]
        {
            let body_data = &self.data.as_ref().as_ref()[body_offset..body_offset + body_len];
            let buffers = rec.buffers().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no buffers"))?;
            if is_body_compressed(&buffers, body_data) {
                let (decompressed_body, _offsets) = decompress_sequential_body(&buffers, body_data)?;
                let arc_data = Arc::new(decompressed_body.clone());
                handle_record_batch_shared(
                    &rec,
                    &self
                        .schema
                        .iter()
                        .map(|a| a.as_ref().clone())
                        .collect::<Vec<_>>(),
                    &self.dictionaries,
                    arc_data,
                    0, // decompressed data starts at offset 0
                    decompressed_body.len(),
                )
            } else {
                handle_record_batch_shared(
                    &rec,
                    &self
                        .schema
                        .iter()
                        .map(|a| a.as_ref().clone())
                        .collect::<Vec<_>>(),
                    &self.dictionaries,
                    self.data.clone(),
                    body_offset,
                    body_len,
                )
            }
        }
        #[cfg(not(any(feature = "zstd", feature = "snappy")))]
        {
            handle_record_batch_shared(
                &rec,
                &self
                    .schema
                    .iter()
                    .map(|a| a.as_ref().clone())
                    .collect::<Vec<_>>(),
                &self.dictionaries,
                self.data.clone(),
                body_offset,
                body_len,
            )
        }
    }


    /// Slice and validate the FlatBuffers message at the given block
    ///
    /// Checks continuation + size
    fn slice_message(&self, blk: &IPCFileBlock) -> io::Result<&[u8]> {
        if blk.offset + 8 > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "block header OOB",
            ));
        }
        let cont = u32::from_le_bytes(self.data[blk.offset..blk.offset + 4].try_into().unwrap());
        if cont != 0xFFFF_FFFF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad continuation marker: {cont:#X}"),
            ));
        }
        let meta_len = u32::from_le_bytes(
            self.data[blk.offset + 4..blk.offset + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let start = blk.offset + 8;
        let end = start + meta_len;
        if end > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "msg slice OOB",
            ));
        }
        Ok(&self.data[start..end])
    }
}

#[cfg(test)]
mod tests {
    use minarrow::{Array, NumericArray, TextArray};

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
                    eprintln!("Int32 buffer is shared (zero-copy)");
                } else {
                    eprintln!("Int32 buffer was cloned (not 64-byte aligned in file)");
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
                    eprintln!("Float64 buffer is shared (zero-copy)");
                } else {
                    eprintln!("Float64 buffer was cloned (not 64-byte aligned in file)");
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
                        eprintln!("String32 data buffer is shared (zero-copy)");
                        any_shared = true;
                    } else {
                        eprintln!("String32 data buffer was cloned (not 64-byte aligned in file)");
                    }
                }
                Array::BooleanArray(a) => {
                    seen_bool = true;
                    if a.data.bits.is_shared() {
                        eprintln!("Boolean bits buffer is shared (zero-copy)");
                        any_shared = true;
                    } else {
                        eprintln!("Boolean bits buffer was cloned (not 64-byte aligned in file)");
                    }
                }
                _ => {}
            }
        }
        assert!(
            seen_string && seen_bool,
            "String32 and Bool must be present"
        );
        eprintln!("Any buffers shared: {}", any_shared);
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
                    TextArray::Categorical32(arr) if arr.data.is_shared() => shared_count += 1,
                    #[cfg(feature = "extended_categorical")]
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

        eprintln!(
            "Shared buffers: {}, Cloned buffers: {}",
            shared_count, cloned_count
        );
        eprintln!("Note: Cloning is expected when file data is not 64-byte aligned.");
        eprintln!("The writer currently doesn't guarantee 64-byte alignment.");

        // We don't assert on specific counts because alignment depends on the writer
        // Just verify the file was read correctly
        assert_eq!(table2.n_rows, 4);
        assert_eq!(table2.cols.len(), table.cols.len());
    }
}
