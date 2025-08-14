//! # Parquet Footer, Schema, and Page Metadata Serialisation
//!
//! Minimal Thrift-like writers for Parquet file structures used by the encoder:
//! file footer (`FileMetaData`), schema (`SchemaElement`), row groups, column
//! chunks, page headers (DataPage v1/v2, Dictionary), and per-column statistics.
//!
//! Writes the required `PAR1` magic at the tail; the caller is responsible for
//! writing the file body first and positioning the writer appropriately.

use std::collections::BTreeMap;
use std::io::{Seek, SeekFrom, Write};

use crate::constants::PARQUET_MAGIC;
use crate::error::IoError;
use crate::models::types::parquet::{ParquetEncoding, ParquetPhysicalType};

// --------------------- Structs ------------------------------------ //

/// Complete Parquet file metadata stored in the footer.
#[derive(Debug, Clone)]
pub(crate) struct FileMetaData {
    /// Parquet format version (e.g. 1).
    pub version: i32,
    /// Flattened schema elements (root + fields).
    pub schema: Vec<SchemaElement>,
    /// Total number of rows across all row groups.
    pub num_rows: i64,
    /// Row group descriptors with column chunk metadata.
    pub row_groups: Vec<RowGroupMeta>,
    /// Optional key–value pairs to carry producer-specific metadata.
    pub key_value_metadata: Option<BTreeMap<String, String>>,
    /// Optional producer string.
    pub created_by: Option<String>,
}

/// Schema element (Parquet `SchemaElement`) describing a node in the schema tree.
#[derive(Debug, Clone)]
pub(crate) struct SchemaElement {
    /// Column or group name.
    pub name: String,
    /// Repetition: 0=REQUIRED, 1=OPTIONAL, 2=REPEATED.
    pub repetition_type: i32,
    /// Physical type for leaf nodes (e.g. INT32, BYTE_ARRAY).
    pub type_: Option<ParquetPhysicalType>,
    /// Legacy converted type ID (if any).
    pub converted_type: Option<i32>,
    /// Type length (e.g. for FIXED_LEN_BYTE_ARRAY).
    pub type_length: Option<i32>,
    /// Decimal precision (if applicable).
    pub precision: Option<i32>,
    /// Decimal scale (if applicable).
    pub scale: Option<i32>,
    /// Field ID (optional).
    pub field_id: Option<i32>,
}

/// Row group descriptor.
#[derive(Debug, Clone)]
pub(crate) struct RowGroupMeta {
    /// Column chunks within this row group.
    pub columns: Vec<ColumnChunkMeta>,
    /// Total byte size for all columns in the row group.
    pub total_byte_size: i64,
    /// Number of rows in this row group.
    pub num_rows: i64,
}

/// Column chunk metadata.
#[derive(Debug, Clone)]
pub(crate) struct ColumnChunkMeta {
    /// File offset to the start of this column chunk.
    pub file_offset: i64,
    /// Detailed per-column metadata.
    pub meta_data: ColumnMetadata,
}

/// Column metadata for primitive/unsigned/dictionary columns.
#[derive(Debug, Clone)]
pub(crate) struct ColumnMetadata {
    /// Physical type of the column.
    pub type_: ParquetPhysicalType,
    /// Encodings used in this column chunk.
    pub encodings: Vec<ParquetEncoding>,
    /// Path in the schema (for nested columns).
    pub path_in_schema: Vec<String>,
    /// Compression codec ID.
    pub codec: i32,
    /// Total number of values in this column chunk.
    pub num_values: i64,
    /// Uncompressed byte size of this column chunk.
    pub total_uncompressed_size: i64,
    /// Compressed byte size of this column chunk.
    pub total_compressed_size: i64,
    /// Byte offset to the first data page of this column chunk.
    pub data_page_offset: i64,
    /// Optional byte offset to the dictionary page (if present).
    pub dictionary_page_offset: Option<i64>,
    /// Optional per-column statistics.
    pub statistics: Option<Statistics>,
    /// Definition level (REQUIRED/OPTIONAL/REPEATED encoded level).
    pub definition_level: i32,
}

/// Parquet statistics for a column (min/max, null/unique counts).
///
/// => Min, max, null/unique count
#[derive(Debug, Clone)]
pub(crate) struct Statistics {
    /// Number of null values (if recorded).
    pub null_count: Option<i64>,
    /// Number of distinct values (if recorded).
    pub distinct_count: Option<i64>,
    /// Minimum value as raw bytes (encoding-dependent).
    pub min: Option<Vec<u8>>,
    /// Maximum value as raw bytes (encoding-dependent).
    pub max: Option<Vec<u8>>,
}

/// Parquet DataPage v1 header.
#[derive(Debug, Clone)]
pub(crate) struct DataPageHeader {
    /// Number of values in the page (including nulls and repeats).
    pub num_values: i32,
    /// Value encoding.
    pub encoding: ParquetEncoding,
    /// Encoding for definition levels.
    pub definition_level_encoding: ParquetEncoding,
    /// Encoding for repetition levels.
    pub repetition_level_encoding: ParquetEncoding,
    /// Optional statistics for this page.
    pub statistics: Option<Statistics>,
}

/// Parquet DataPage v2 header (introduced in Parquet v2).
#[derive(Debug, Clone)]
pub(crate) struct DataPageHeaderV2 {
    /// Number of rows in the page.
    pub num_rows: i32,
    /// Number of nulls in the page.
    pub num_nulls: i32,
    /// Number of non-null values in the page.
    pub num_values: i32,
    /// Value encoding.
    pub encoding: ParquetEncoding,
    /// Byte length of encoded definition levels.
    pub definition_levels_byte_length: i32,
    /// Byte length of encoded repetition levels.
    pub repetition_levels_byte_length: i32,
    /// Whether `encoding` was applied after compression.
    pub is_compressed: bool,
    /// Optional statistics for this page.
    pub statistics: Option<Statistics>,
}

/// Union of page headers with sizes.
#[derive(Debug, Clone)]
pub(crate) struct PageHeader {
    /// Page type (data/index/dictionary/v2).
    pub type_: PageType,
    /// Uncompressed page size in bytes.
    pub uncompressed_page_size: i32,
    /// Compressed page size in bytes.
    pub compressed_page_size: i32,
    /// Optional DataPage v1 header.
    pub data_page_header: Option<DataPageHeader>,
    /// Optional DataPage v2 header.
    pub data_page_header_v2: Option<DataPageHeaderV2>,
    /// Optional dictionary page header.
    pub dictionary_page_header: Option<DictionaryPageHeader>,
}

/// Parquet Dictionary Page Header, for categorical/dictionary columns.
#[derive(Debug, Clone)]
pub(crate) struct DictionaryPageHeader {
    /// Number of dictionary entries.
    pub num_values: i32,
    /// Encoding used for the dictionary data.
    pub encoding: ParquetEncoding,
    /// Whether the dictionary is sorted.
    pub is_sorted: Option<bool>,
}

// --------------------- Enums ------------------------------------ //

/// Parquet page type identifiers (from parquet.thrift).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageType {
    /// DataPage v1.
    DataPage = 0,
    /// Index page.
    IndexPage = 1,
    /// Dictionary page.
    DictionaryPage = 2,
    /// DataPage v2.
    DataPageV2 = 3,
}

// --------------------- Implementations --------------------------- //

impl PageType {
    /// Convert the page type to its i32 representation.
    pub fn as_i32(self) -> i32 {
        self as i32
    }
    /// Parse a page type from its i32 representation.
    pub fn from_i32(v: i32) -> Option<Self> {
        Some(match v {
            0 => Self::DataPage,
            1 => Self::IndexPage,
            2 => Self::DictionaryPage,
            3 => Self::DataPageV2,
            _ => return None,
        })
    }
}
impl DataPageHeader {
    /// Write a DataPage v1 header using the minimal Thrift wire format.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] Total number of encoded values in the page
        thrift_write_field_i32(&mut w, 1, self.num_values);

        // [2] Encoding used for data values (e.g. PLAIN, RLE)
        thrift_write_field_i32(&mut w, 2, self.encoding.to_i32());

        // [3] Encoding used for definition levels
        thrift_write_field_i32(&mut w, 3, self.definition_level_encoding.to_i32());

        // [4] Encoding used for repetition levels
        thrift_write_field_i32(&mut w, 4, self.repetition_level_encoding.to_i32());

        // [5] Optional statistics block
        if let Some(ref stats) = self.statistics {
            thrift_write_field_struct_begin(&mut w, 5);
            stats.write(&mut w)?;
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl DataPageHeaderV2 {
    /// Write a DataPage v2 header using the minimal Thrift wire format.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);
        thrift_write_field_i32(&mut w, 1, self.num_rows);
        thrift_write_field_i32(&mut w, 2, self.num_nulls);
        thrift_write_field_i32(&mut w, 3, self.num_values);
        thrift_write_field_i32(&mut w, 4, self.encoding.to_i32());
        thrift_write_field_i32(&mut w, 5, self.definition_levels_byte_length);
        thrift_write_field_i32(&mut w, 6, self.repetition_levels_byte_length);
        thrift_write_field_bool(&mut w, 7, self.is_compressed);
        if let Some(ref s) = self.statistics {
            thrift_write_field_struct_begin(&mut w, 8);
            s.write(&mut w)?;
        }
        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl PageHeader {
    /// Write a page header (DataPage v1/v2 or Dictionary) via the Thrift wire format.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);
        thrift_write_field_i32(&mut w, 1, self.type_.as_i32());
        thrift_write_field_i32(&mut w, 2, self.uncompressed_page_size);
        thrift_write_field_i32(&mut w, 3, self.compressed_page_size);

        if let Some(ref h) = self.data_page_header {
            thrift_write_field_struct_begin(&mut w, 4);
            h.write(&mut w)?;
        }
        if let Some(ref h2) = self.data_page_header_v2 {
            thrift_write_field_struct_begin(&mut w, 7); // field id 7 for V2
            h2.write(&mut w)?;
        }
        if let Some(ref d) = self.dictionary_page_header {
            thrift_write_field_struct_begin(&mut w, 5);
            d.write(&mut w)?;
        }
        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl DictionaryPageHeader {
    /// Write a dictionary page header via the Thrift wire format.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] Number of dictionary entries in this page
        thrift_write_field_i32(&mut w, 1, self.num_values);

        // [2] Encoding used to serialise the dictionary values
        thrift_write_field_i32(&mut w, 2, self.encoding.to_i32());

        // [3] Optional flag indicating whether the dictionary is sorted
        if let Some(val) = self.is_sorted {
            thrift_write_field_bool(&mut w, 3, val);
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}
impl FileMetaData {
    /// Serialise the Parquet footer and write trailing metadata marker.
    ///
    /// Caller must have written the file body and positioned the writer.
    /// Returns the file position at which the footer begins.
    pub fn write<W: Write + Seek>(&self, mut w: W) -> Result<u64, IoError> {
        let start_pos = w.seek(SeekFrom::Current(0))?;

        // Serialise `FileMetaData` using Thrift encoding into a buffer.
        let mut buf = Vec::new();
        thrift_write_struct_begin(&mut buf, 0x0c);

        // [1] Version of the Parquet format
        thrift_write_field_i32(&mut buf, 1, self.version);

        // [2] Schema elements
        thrift_write_field_list_begin(&mut buf, 2, 12, self.schema.len() as i32);
        for s in &self.schema {
            s.write(&mut buf)?;
        }

        // [3] Total number of rows in the file
        thrift_write_field_i64(&mut buf, 3, self.num_rows);

        // [4] Row group metadata
        thrift_write_field_list_begin(&mut buf, 4, 12, self.row_groups.len() as i32);
        for rg in &self.row_groups {
            rg.write(&mut buf)?;
        }

        // [5] Optional key-value metadata
        if let Some(ref kv) = self.key_value_metadata {
            thrift_write_field_map_begin(&mut buf, 5, 11, 11, kv.len() as i32);
            for (k, v) in kv {
                thrift_write_string(&mut buf, k);
                thrift_write_string(&mut buf, v);
            }
        }

        // [6] Optional creator string
        if let Some(ref s) = self.created_by {
            thrift_write_field_string(&mut buf, 6, s);
        }

        thrift_write_field_stop(&mut buf);

        println!("DIAG: FileMetaData encoded len = {}", buf.len());

        // Write the encoded footer, footer length, and trailing magic marker
        w.write_all(&buf)?;
        let footer_len = buf.len() as u32;
        w.write_all(&footer_len.to_le_bytes())?;
        w.write_all(PARQUET_MAGIC)?;

        Ok(start_pos)
    }
}

// SchemaElement
impl SchemaElement {
    /// Write a single schema element using the Thrift wire format.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] Field name - required
        thrift_write_field_string(&mut w, 1, &self.name);

        // [2] Physical type - optional
        if let Some(ty) = self.type_ {
            thrift_write_field_i32(&mut w, 2, ty.as_i32());
        }

        // [3] Repetition type - 0 = REQUIRED, 1 = OPTIONAL, 2 = REPEATED
        thrift_write_field_i32(&mut w, 3, self.repetition_type);

        // [6] Converted type - optional - defaults to UTF8 for byte arrays
        match self.converted_type {
            Some(ct) => thrift_write_field_i32(&mut w, 6, ct),
            None if matches!(self.type_, Some(ParquetPhysicalType::ByteArray)) => {
                thrift_write_field_i32(&mut w, 6, 1); // 1 = UTF8
            }
            _ => {}
        }

        // [7] Type length (optional; used for fixed-length types)
        if let Some(len) = self.type_length {
            thrift_write_field_i32(&mut w, 7, len);
        }

        // [9] Decimal precision - optional
        if let Some(p) = self.precision {
            thrift_write_field_i32(&mut w, 9, p);
        }

        // [10] Decimal scale - optional
        if let Some(s) = self.scale {
            thrift_write_field_i32(&mut w, 10, s);
        }

        // [15] Field ID - optional
        if let Some(id) = self.field_id {
            thrift_write_field_i32(&mut w, 15, id);
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl RowGroupMeta {
    /// Write a row group descriptor via the Thrift wire format.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // columns
        thrift_write_field_list_begin(&mut w, 1, 12, self.columns.len() as i32);
        for col in &self.columns {
            col.write(&mut w)?;
        }

        // total_byte_size
        thrift_write_field_i64(&mut w, 2, self.total_byte_size);

        // num_rows
        thrift_write_field_i64(&mut w, 3, self.num_rows);

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl ColumnChunkMeta {
    /// Write a column chunk descriptor via the Thrift wire format.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // file_offset
        thrift_write_field_i64(&mut w, 1, self.file_offset);

        // metadata
        thrift_write_field_struct_begin(&mut w, 2);
        self.meta_data.write(&mut w)?;

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl ColumnMetadata {
    /// Write column metadata (ColumnMetaData) using minimal Thrift wire encoding.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] Physical type (e.g. INT32, BYTE_ARRAY, etc.)
        thrift_write_field_i32(&mut w, 1, self.type_.as_i32());

        // [2] Encodings used (PLAIN, RLE, DICTIONARY, etc.)
        thrift_write_field_list_begin(&mut w, 2, 8, self.encodings.len() as i32);
        for &e in &self.encodings {
            w.write_all(&e.to_i32().to_le_bytes())?;
        }

        // [3] Path in schema (e.g. ["root", "field", "nested_field"])
        thrift_write_field_list_begin(&mut w, 3, 11, self.path_in_schema.len() as i32);
        for s in &self.path_in_schema {
            thrift_write_string(&mut w, s);
        }

        // [4] Compression codec identifier
        thrift_write_field_i32(&mut w, 4, self.codec);

        // [5] Total number of values (including nulls)
        thrift_write_field_i64(&mut w, 5, self.num_values);

        // [6] Uncompressed size in bytes
        thrift_write_field_i64(&mut w, 6, self.total_uncompressed_size);

        // [7] Compressed size in bytes
        thrift_write_field_i64(&mut w, 7, self.total_compressed_size);

        // [8] Offset to the first data page
        thrift_write_field_i64(&mut w, 8, self.data_page_offset);

        // [9] Offset to dictionary page (if present)
        if let Some(dict_off) = self.dictionary_page_offset {
            thrift_write_field_i64(&mut w, 9, dict_off);
        }

        // [10] Optional statistics block
        if let Some(ref stats) = self.statistics {
            thrift_write_field_struct_begin(&mut w, 10);
            stats.write(&mut w)?;
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl Statistics {
    /// Write Parquet file statistics using the Thrift wire format.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] Null count - optional
        if let Some(n) = self.null_count {
            thrift_write_field_i64(&mut w, 1, n);
        }

        // [2] Distinct count - optional
        if let Some(d) = self.distinct_count {
            thrift_write_field_i64(&mut w, 2, d);
        }

        // [3] Minimum value - optional
        if let Some(ref min) = self.min {
            thrift_write_field_bytes(&mut w, 3, min);
        }

        // [4] Maximum value - optional
        if let Some(ref max) = self.max {
            thrift_write_field_bytes(&mut w, 4, max);
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

// --------------- Thrift serialisation helpers --------------- //

/// Begin a Thrift struct (marker only; no-op for our minimal writer).
fn thrift_write_struct_begin<W: Write>(_w: &mut W, _id: u8) {}
/// Write Thrift field stop byte.
fn thrift_write_field_stop<W: Write>(w: &mut W) {
    w.write_all(&[0]).unwrap();
}
/// Write a Thrift i32 field with `id`.
fn thrift_write_field_i32<W: Write>(w: &mut W, id: i16, v: i32) {
    w.write_all(&[8]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&v.to_le_bytes()).unwrap();
}
/// Write a Thrift i64 field with `id`.
fn thrift_write_field_i64<W: Write>(w: &mut W, id: i16, v: i64) {
    w.write_all(&[10]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&v.to_le_bytes()).unwrap();
}
/// Write a Thrift bool field with `id`.
fn thrift_write_field_bool<W: Write>(w: &mut W, id: i16, v: bool) {
    w.write_all(&[2]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&[v as u8]).unwrap();
}
/// Write a Thrift string field with `id`.
fn thrift_write_field_string<W: Write>(w: &mut W, id: i16, s: &str) {
    w.write_all(&[11]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    thrift_write_string(w, s);
}
/// Write a Thrift bytes field with `id`.
fn thrift_write_field_bytes<W: Write>(w: &mut W, id: i16, b: &[u8]) {
    w.write_all(&[11]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    let l = b.len() as i32;
    w.write_all(&l.to_le_bytes()).unwrap();
    w.write_all(b).unwrap();
}
/// Begin a Thrift list field with `id`, element type `tpe`, and `len`.
fn thrift_write_field_list_begin<W: Write>(w: &mut W, id: i16, tpe: u8, len: i32) {
    w.write_all(&[15]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&[tpe]).unwrap();
    w.write_all(&len.to_le_bytes()).unwrap();
}
/// Begin a Thrift map field with `id`, key/val types `kt`/`vt`, and `len`.
fn thrift_write_field_map_begin<W: Write>(w: &mut W, id: i16, kt: u8, vt: u8, len: i32) {
    w.write_all(&[13]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&[kt, vt]).unwrap();
    w.write_all(&len.to_le_bytes()).unwrap();
}
/// Begin a nested Thrift struct field with `id`.
fn thrift_write_field_struct_begin<W: Write>(w: &mut W, id: i16) {
    w.write_all(&[12]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
}
/// Write a Thrift length-prefixed string value.
fn thrift_write_string<W: Write>(w: &mut W, s: &str) {
    let len = s.len() as i32;
    w.write_all(&len.to_le_bytes()).unwrap();
    w.write_all(s.as_bytes()).unwrap();
}
