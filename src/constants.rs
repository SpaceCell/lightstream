//! # IPC Constants
//!
//! Constants used by the Arrow IPC framing and Lightstream’s framing logic.
//!
//! These cover frame sizing, magic numbers, and special markers required by the
//! Arrow IPC file and stream format. They are kept in one place for clarity and
//! to ensure consistency across encoders, decoders, readers, and writers, and are
//! consistent with the [Apache Arrow specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format).

// Default allocation size for new frame buffers (1 MB).
pub const DEFAULT_FRAME_ALLOCATION_SIZE: usize = 1024 * 1024;

/// Opening magic bytes at the start of an Arrow IPC file.
/// Always 8 bytes: `"ARROW1\0\0"`.
pub const ARROW_MAGIC_NUMBER_PADDED: &[u8] = b"ARROW1\0\0";

/// Closing magic bytes at the end of an Arrow IPC file.
/// Always 6 bytes: `"ARROW1"`.
pub const ARROW_MAGIC_NUMBER: &[u8] = b"ARROW1";

/// Length in bytes of the opening magic sequence.
pub const FILE_OPENING_MAGIC_LEN: usize = 8;

/// Length in bytes of the closing magic sequence.
pub const FILE_CLOSING_MAGIC_LEN: usize = 6;

/// Length in bytes of the EOS (end-of-stream) marker.
/// EOS marker = `0xFFFF_FFFFu32` followed by `0u32`.
pub const EOS_MARKER_LEN: usize = 8;

/// Length in bytes of the continuation marker.
/// Marker = `0xFFFF_FFFFu32`.
pub const CONTINUATION_MARKER_LEN: usize = 4;

/// Continuation marker sentinel value (0xFFFFFFFF).
pub const CONTINUATION_SENTINEL: u32 = 0xFFFF_FFFF;

/// Size of the metadata size prefix in bytes.
/// Prefix = 4-byte little-endian signed integer.
pub const METADATA_SIZE_PREFIX: usize = 4;

/// Required “PAR1” marker written at Parquet file head and tail.
pub const PARQUET_MAGIC: &[u8; 4] = b"PAR1";
