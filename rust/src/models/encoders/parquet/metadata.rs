// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Parquet Footer, Schema, and Page Metadata Serialisation
//!
//! Minimal Thrift-like writers for Parquet file structures used by the encoder:
//! file footer (`FileMetaData`), schema (`SchemaElement`), row groups, column
//! chunks, page headers (DataPage v1/v2, Dictionary), and per-column statistics.
//!
//! Writes the required `PAR1` magic at the tail; the caller is responsible for
//! writing the file body first and positioning the writer appropriately.

use std::collections::BTreeMap;
use std::io::{Seek, Write};

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
    /// Optional key-value pairs to carry producer-specific metadata.
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
    /// Number of child nodes for a group element. The root group carries the
    /// leaf column count. Leaf elements leave this unset.
    pub num_children: Option<i32>,
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
    /// Write a DataPage v1 header using TCompactProtocol.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        let mut last = 0i16;

        // [1] Total number of encoded values in the page
        thrift_write_field_i32(&mut w, &mut last, 1, self.num_values);

        // [2] Encoding used for data values (e.g. PLAIN, RLE)
        thrift_write_field_i32(&mut w, &mut last, 2, self.encoding.to_i32());

        // [3] Encoding used for definition levels
        thrift_write_field_i32(&mut w, &mut last, 3, self.definition_level_encoding.to_i32());

        // [4] Encoding used for repetition levels
        thrift_write_field_i32(&mut w, &mut last, 4, self.repetition_level_encoding.to_i32());

        // [5] Optional statistics block
        if let Some(ref stats) = self.statistics {
            thrift_write_field_struct_begin(&mut w, &mut last, 5);
            stats.write(&mut w)?;
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl DataPageHeaderV2 {
    /// Write a DataPage v2 header using TCompactProtocol.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        let mut last = 0i16;
        thrift_write_field_i32(&mut w, &mut last, 1, self.num_values);
        thrift_write_field_i32(&mut w, &mut last, 2, self.num_nulls);
        thrift_write_field_i32(&mut w, &mut last, 3, self.num_rows);
        thrift_write_field_i32(&mut w, &mut last, 4, self.encoding.to_i32());
        thrift_write_field_i32(&mut w, &mut last, 5, self.definition_levels_byte_length);
        thrift_write_field_i32(&mut w, &mut last, 6, self.repetition_levels_byte_length);
        thrift_write_field_bool(&mut w, &mut last, 7, self.is_compressed);
        if let Some(ref s) = self.statistics {
            thrift_write_field_struct_begin(&mut w, &mut last, 8);
            s.write(&mut w)?;
        }
        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl PageHeader {
    /// Write a page header (DataPage v1/v2 or Dictionary) via TCompactProtocol.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        let mut last = 0i16;
        thrift_write_field_i32(&mut w, &mut last, 1, self.type_.as_i32());
        thrift_write_field_i32(&mut w, &mut last, 2, self.uncompressed_page_size);
        thrift_write_field_i32(&mut w, &mut last, 3, self.compressed_page_size);

        // [5] DataPage v1 header
        if let Some(ref h) = self.data_page_header {
            thrift_write_field_struct_begin(&mut w, &mut last, 5);
            h.write(&mut w)?;
        }
        // [7] Dictionary page header
        if let Some(ref d) = self.dictionary_page_header {
            thrift_write_field_struct_begin(&mut w, &mut last, 7);
            d.write(&mut w)?;
        }
        // [8] DataPage v2 header
        if let Some(ref h2) = self.data_page_header_v2 {
            thrift_write_field_struct_begin(&mut w, &mut last, 8);
            h2.write(&mut w)?;
        }
        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl DictionaryPageHeader {
    /// Write a dictionary page header via TCompactProtocol.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        let mut last = 0i16;

        // [1] Number of dictionary entries in this page
        thrift_write_field_i32(&mut w, &mut last, 1, self.num_values);

        // [2] Encoding used to serialise the dictionary values
        thrift_write_field_i32(&mut w, &mut last, 2, self.encoding.to_i32());

        // [3] Optional flag indicating whether the dictionary is sorted
        if let Some(val) = self.is_sorted {
            thrift_write_field_bool(&mut w, &mut last, 3, val);
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
        let start_pos = w.stream_position()?;

        // Serialise `FileMetaData` using TCompactProtocol into a buffer.
        let mut buf = Vec::new();
        let mut last = 0i16;

        // [1] Version of the Parquet format
        thrift_write_field_i32(&mut buf, &mut last, 1, self.version);

        // [2] Schema elements
        thrift_write_field_list_begin(&mut buf, &mut last, 2, TC_STRUCT, self.schema.len());
        for s in &self.schema {
            s.write(&mut buf)?;
        }

        // [3] Total number of rows in the file
        thrift_write_field_i64(&mut buf, &mut last, 3, self.num_rows);

        // [4] Row group metadata
        thrift_write_field_list_begin(&mut buf, &mut last, 4, TC_STRUCT, self.row_groups.len());
        for rg in &self.row_groups {
            rg.write(&mut buf)?;
        }

        // [5] Optional key-value metadata as a list<KeyValue>, each entry a
        // struct with 1=key and 2=value.
        if let Some(ref kv) = self.key_value_metadata {
            thrift_write_field_list_begin(&mut buf, &mut last, 5, TC_STRUCT, kv.len());
            for (k, v) in kv {
                let mut kv_last = 0i16;
                thrift_write_field_string(&mut buf, &mut kv_last, 1, k);
                thrift_write_field_string(&mut buf, &mut kv_last, 2, v);
                thrift_write_field_stop(&mut buf);
            }
        }

        // [6] Optional creator string
        if let Some(ref s) = self.created_by {
            thrift_write_field_string(&mut buf, &mut last, 6, s);
        }

        thrift_write_field_stop(&mut buf);

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
    /// Write a single schema element using TCompactProtocol.
    ///
    /// Fields follow the parquet.thrift `SchemaElement` numbering: type=1,
    /// type_length=2, repetition_type=3, name=4, num_children=5,
    /// converted_type=6, scale=7, precision=8, field_id=9. Group elements
    /// (the root) carry `num_children` and omit both the physical type and
    /// the repetition type.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        let mut last = 0i16;
        let is_group = self.num_children.is_some();

        // [1] Physical type - leaf elements only
        if let Some(ty) = self.type_ {
            thrift_write_field_i32(&mut w, &mut last, 1, ty.as_i32());
        }

        // [2] Type length (optional; used for fixed-length types)
        if let Some(len) = self.type_length {
            thrift_write_field_i32(&mut w, &mut last, 2, len);
        }

        // [3] Repetition type - 0 = REQUIRED, 1 = OPTIONAL, 2 = REPEATED.
        // Group elements omit the repetition type.
        if !is_group {
            thrift_write_field_i32(&mut w, &mut last, 3, self.repetition_type);
        }

        // [4] Field name - required
        thrift_write_field_string(&mut w, &mut last, 4, &self.name);

        // [5] Number of children - group elements only
        if let Some(nc) = self.num_children {
            thrift_write_field_i32(&mut w, &mut last, 5, nc);
        }

        // [6] Converted type - optional - defaults to UTF8 for byte arrays
        match self.converted_type {
            Some(ct) => thrift_write_field_i32(&mut w, &mut last, 6, ct),
            None if matches!(self.type_, Some(ParquetPhysicalType::ByteArray)) => {
                thrift_write_field_i32(&mut w, &mut last, 6, 0); // 0 = UTF8
            }
            _ => {}
        }

        // [7] Decimal scale - optional
        if let Some(s) = self.scale {
            thrift_write_field_i32(&mut w, &mut last, 7, s);
        }

        // [8] Decimal precision - optional
        if let Some(p) = self.precision {
            thrift_write_field_i32(&mut w, &mut last, 8, p);
        }

        // [9] Field ID - optional
        if let Some(id) = self.field_id {
            thrift_write_field_i32(&mut w, &mut last, 9, id);
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl RowGroupMeta {
    /// Write a row group descriptor via TCompactProtocol.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        let mut last = 0i16;

        // [1] columns
        thrift_write_field_list_begin(&mut w, &mut last, 1, TC_STRUCT, self.columns.len());
        for col in &self.columns {
            col.write(&mut w)?;
        }

        // [2] total_byte_size
        thrift_write_field_i64(&mut w, &mut last, 2, self.total_byte_size);

        // [3] num_rows
        thrift_write_field_i64(&mut w, &mut last, 3, self.num_rows);

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl ColumnChunkMeta {
    /// Write a column chunk descriptor via TCompactProtocol.
    ///
    /// Field 1 (`file_path`) is omitted; `file_offset` is field 2 and the
    /// inline `meta_data` struct is field 3, per parquet.thrift.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        let mut last = 0i16;

        // [2] file_offset
        thrift_write_field_i64(&mut w, &mut last, 2, self.file_offset);

        // [3] metadata
        thrift_write_field_struct_begin(&mut w, &mut last, 3);
        self.meta_data.write(&mut w)?;

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl ColumnMetadata {
    /// Write column metadata (ColumnMetaData) using TCompactProtocol.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        let mut last = 0i16;

        // [1] Physical type (e.g. INT32, BYTE_ARRAY, etc.)
        thrift_write_field_i32(&mut w, &mut last, 1, self.type_.as_i32());

        // [2] Encodings used (PLAIN, RLE, DICTIONARY, etc.)
        thrift_write_field_list_begin(&mut w, &mut last, 2, TC_I32, self.encodings.len());
        for &e in &self.encodings {
            write_varint(&mut w, zigzag_i32(e.to_i32()));
        }

        // [3] Path in schema (e.g. ["root", "field", "nested_field"])
        thrift_write_field_list_begin(&mut w, &mut last, 3, TC_BINARY, self.path_in_schema.len());
        for s in &self.path_in_schema {
            thrift_write_string(&mut w, s);
        }

        // [4] Compression codec identifier
        thrift_write_field_i32(&mut w, &mut last, 4, self.codec);

        // [5] Total number of values (including nulls)
        thrift_write_field_i64(&mut w, &mut last, 5, self.num_values);

        // [6] Uncompressed size in bytes
        thrift_write_field_i64(&mut w, &mut last, 6, self.total_uncompressed_size);

        // [7] Compressed size in bytes
        thrift_write_field_i64(&mut w, &mut last, 7, self.total_compressed_size);

        // [9] Offset to the first data page
        thrift_write_field_i64(&mut w, &mut last, 9, self.data_page_offset);

        // [11] Offset to dictionary page (if present)
        if let Some(dict_off) = self.dictionary_page_offset {
            thrift_write_field_i64(&mut w, &mut last, 11, dict_off);
        }

        // [12] Optional statistics block
        if let Some(ref stats) = self.statistics {
            thrift_write_field_struct_begin(&mut w, &mut last, 12);
            stats.write(&mut w)?;
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl Statistics {
    /// Write Parquet column statistics using TCompactProtocol.
    ///
    /// `null_count` is field 3 and `distinct_count` is field 4. Min/max are
    /// emitted as the modern `max_value` (field 5) and `min_value` (field 6)
    /// byte arrays.
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        let mut last = 0i16;

        // [3] Null count - optional
        if let Some(n) = self.null_count {
            thrift_write_field_i64(&mut w, &mut last, 3, n);
        }

        // [4] Distinct count - optional
        if let Some(d) = self.distinct_count {
            thrift_write_field_i64(&mut w, &mut last, 4, d);
        }

        // [5] Maximum value - optional
        if let Some(ref max) = self.max {
            thrift_write_field_bytes(&mut w, &mut last, 5, max);
        }

        // [6] Minimum value - optional
        if let Some(ref min) = self.min {
            thrift_write_field_bytes(&mut w, &mut last, 6, min);
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

// --------------- TCompactProtocol serialisation helpers --------------- //

/// Compact-protocol element and field type identifiers from the Thrift spec.
const TC_BOOL_TRUE: u8 = 1;
const TC_BOOL_FALSE: u8 = 2;
const TC_I32: u8 = 5;
const TC_I64: u8 = 6;
const TC_BINARY: u8 = 8;
const TC_LIST: u8 = 9;
const TC_STRUCT: u8 = 12;

/// Encode an i32 as a zigzag value ready for varint serialisation.
fn zigzag_i32(v: i32) -> u64 {
    ((v << 1) ^ (v >> 31)) as u32 as u64
}
/// Encode an i64 as a zigzag value ready for varint serialisation.
fn zigzag_i64(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}
/// Encode an i16 as a zigzag value ready for varint serialisation.
fn zigzag_i16(v: i16) -> u64 {
    ((v << 1) ^ (v >> 15)) as u16 as u64
}

/// Write an unsigned LEB128 varint.
fn write_varint<W: Write>(w: &mut W, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            w.write_all(&[byte]).unwrap();
            break;
        }
        w.write_all(&[byte | 0x80]).unwrap();
    }
}

/// Write a compact field header for field `id` with compact type `ctype`,
/// advancing the struct's running field id. Uses the short form when the
/// delta from the previous field id is in 1..=15, otherwise the long form
/// with a zigzag varint field id.
fn thrift_write_field_header<W: Write>(w: &mut W, last: &mut i16, id: i16, ctype: u8) {
    let delta = id - *last;
    if (1..=15).contains(&delta) {
        w.write_all(&[((delta as u8) << 4) | ctype]).unwrap();
    } else {
        w.write_all(&[ctype]).unwrap();
        write_varint(w, zigzag_i16(id));
    }
    *last = id;
}

/// Write a compact list header - one byte `(size<<4)|elem_type` for sizes
/// below 15, otherwise the `0xF` nibble followed by a varint size.
fn thrift_write_list_header<W: Write>(w: &mut W, elem_type: u8, len: usize) {
    if len < 15 {
        w.write_all(&[((len as u8) << 4) | elem_type]).unwrap();
    } else {
        w.write_all(&[0xF0 | elem_type]).unwrap();
        write_varint(w, len as u64);
    }
}

/// Write the compact field-stop byte.
fn thrift_write_field_stop<W: Write>(w: &mut W) {
    w.write_all(&[0]).unwrap();
}
/// Write an i32 field as a zigzag varint.
fn thrift_write_field_i32<W: Write>(w: &mut W, last: &mut i16, id: i16, v: i32) {
    thrift_write_field_header(w, last, id, TC_I32);
    write_varint(w, zigzag_i32(v));
}
/// Write an i64 field as a zigzag varint.
fn thrift_write_field_i64<W: Write>(w: &mut W, last: &mut i16, id: i16, v: i64) {
    thrift_write_field_header(w, last, id, TC_I64);
    write_varint(w, zigzag_i64(v));
}
/// Write a bool field. The value rides in the compact type nibble.
fn thrift_write_field_bool<W: Write>(w: &mut W, last: &mut i16, id: i16, v: bool) {
    thrift_write_field_header(w, last, id, if v { TC_BOOL_TRUE } else { TC_BOOL_FALSE });
}
/// Write a string field as a varint length followed by the UTF-8 bytes.
fn thrift_write_field_string<W: Write>(w: &mut W, last: &mut i16, id: i16, s: &str) {
    thrift_write_field_header(w, last, id, TC_BINARY);
    thrift_write_string(w, s);
}
/// Write a bytes field as a varint length followed by the raw bytes.
fn thrift_write_field_bytes<W: Write>(w: &mut W, last: &mut i16, id: i16, b: &[u8]) {
    thrift_write_field_header(w, last, id, TC_BINARY);
    write_varint(w, b.len() as u64);
    w.write_all(b).unwrap();
}
/// Begin a list field with `id`, element type `tpe`, and element count `len`.
fn thrift_write_field_list_begin<W: Write>(w: &mut W, last: &mut i16, id: i16, tpe: u8, len: usize) {
    thrift_write_field_header(w, last, id, TC_LIST);
    thrift_write_list_header(w, tpe, len);
}
/// Begin a nested struct field with `id`. The caller writes the struct body
/// and its terminating stop byte.
fn thrift_write_field_struct_begin<W: Write>(w: &mut W, last: &mut i16, id: i16) {
    thrift_write_field_header(w, last, id, TC_STRUCT);
}
/// Write a compact string value - varint length followed by the UTF-8 bytes.
fn thrift_write_string<W: Write>(w: &mut W, s: &str) {
    write_varint(w, s.len() as u64);
    w.write_all(s.as_bytes()).unwrap();
}
