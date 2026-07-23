// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # IPC Constants
//!
//! Constants used by the Arrow IPC framing and Lightstream’s framing logic.
//!
//! These cover frame sizing, magic numbers, and special markers required by the
//! Arrow IPC file and stream format. They are kept in one place for clarity and
//! to ensure consistency across encoders, decoders, readers, and writers. They are also
//! consistent with the [Apache Arrow specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format).

// Buffer chunk size defaults. Each can be overridden via environment variable.
const DEFAULT_FILE_IO_CHUNK: usize = 1024 * 1024; // 1 MiB
const DEFAULT_HTTP_CHUNK: usize = 64 * 1024; // 64 KiB
const DEFAULT_WEBSOCKET_CHUNK: usize = 32 * 1024; // 32 KiB
const DEFAULT_WEBTRANSPORT_CHUNK: usize = 64 * 1024; // 64 KiB
const DEFAULT_INMEMORY_CHUNK: usize = 512 * 1024; // 512 KiB

fn env_or(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

use std::sync::OnceLock;

fn cached_env(cell: &OnceLock<usize>, var: &str, default: usize) -> usize {
    *cell.get_or_init(|| env_or(var, default))
}

static FILE_IO_CHUNK: OnceLock<usize> = OnceLock::new();
static HTTP_CHUNK: OnceLock<usize> = OnceLock::new();
static WEBSOCKET_CHUNK: OnceLock<usize> = OnceLock::new();
static WEBTRANSPORT_CHUNK: OnceLock<usize> = OnceLock::new();
static INMEMORY_CHUNK: OnceLock<usize> = OnceLock::new();
static ARENA_CAPACITY: OnceLock<usize> = OnceLock::new();

/// File I/O chunk size. Override with `LIGHTSTREAM_FILE_IO_CHUNK_SIZE`.
pub fn file_io_chunk_size() -> usize {
    cached_env(
        &FILE_IO_CHUNK,
        "LIGHTSTREAM_FILE_IO_CHUNK_SIZE",
        DEFAULT_FILE_IO_CHUNK,
    )
}

/// HTTP/TCP chunk size. Override with `LIGHTSTREAM_HTTP_CHUNK_SIZE`.
pub fn http_chunk_size() -> usize {
    cached_env(
        &HTTP_CHUNK,
        "LIGHTSTREAM_HTTP_CHUNK_SIZE",
        DEFAULT_HTTP_CHUNK,
    )
}

/// WebSocket chunk size. Override with `LIGHTSTREAM_WEBSOCKET_CHUNK_SIZE`.
pub fn websocket_chunk_size() -> usize {
    cached_env(
        &WEBSOCKET_CHUNK,
        "LIGHTSTREAM_WEBSOCKET_CHUNK_SIZE",
        DEFAULT_WEBSOCKET_CHUNK,
    )
}

/// WebTransport/QUIC chunk size. Override with `LIGHTSTREAM_WEBTRANSPORT_CHUNK_SIZE`.
pub fn webtransport_chunk_size() -> usize {
    cached_env(
        &WEBTRANSPORT_CHUNK,
        "LIGHTSTREAM_WEBTRANSPORT_CHUNK_SIZE",
        DEFAULT_WEBTRANSPORT_CHUNK,
    )
}

/// In-memory chunk size. Override with `LIGHTSTREAM_INMEMORY_CHUNK_SIZE`.
pub fn inmemory_chunk_size() -> usize {
    cached_env(
        &INMEMORY_CHUNK,
        "LIGHTSTREAM_INMEMORY_CHUNK_SIZE",
        DEFAULT_INMEMORY_CHUNK,
    )
}

/// Default stream arena capacity.
///
/// 2 GiB of virtual address space per arena.
/// With Vec64/MAllocPg64 backing, physical memory is committed
/// only as bytes are written, so the reservation is cheap under normal
/// Linux overcommit. Each stream decoder and, under the `arena` feature,
/// each file reader holds one arena.
pub const DEFAULT_ARENA_CAPACITY: usize = 2 * 1024 * 1024 * 1024;

/// Stream arena capacity in bytes. Override with
/// `LIGHTSTREAM_ARENA_CAPACITY` on hosts where per-stream virtual
/// address reservations must stay small, such as strict-overcommit or
/// address-space-limited deployments. Frames larger than the capacity
/// grow a dedicated generation on demand, so correctness does not
/// depend on the value - only steady-state allocation behaviour.
pub fn arena_capacity() -> usize {
    cached_env(
        &ARENA_CAPACITY,
        "LIGHTSTREAM_ARENA_CAPACITY",
        DEFAULT_ARENA_CAPACITY,
    )
}

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
