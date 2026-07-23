// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Compression codecs for Arrow IPC and Parquet.
//!
//! - Zstd via the zstd crate when the `zstd` feature is enabled.
//!   Supported by both Arrow IPC and Parquet.
//! - Snappy via the snap crate when the `snappy` feature is enabled.
//!   Supported by Parquet only. Not part of the Arrow IPC BodyCompression spec.
//!
//! The `ipc` submodule handles Arrow IPC per-buffer body decompression.
//!
//! ## Performance characteristics
//!
//! Compression introduces a per-batch allocation and decompression step that
//! breaks the zero-copy SharedBuffer path. On local transports where the wire
//! runs at multi-GiB/s (TCP ~3.3 GiB/s, UDS ~3.6 GiB/s), compression is a
//! net throughput loss because zstd decode (~3-4 GiB/s single-threaded)
//! becomes the bottleneck. Typical compressed streaming throughput is
//! ~440-540 MiB/s vs ~3.3-3.6 GiB/s uncompressed.
//!
//! For file I/O where disk is the bottleneck, compressed writes can be
//! *faster* than uncompressed (fewer bytes to flush), and reads see minimal
//! overhead (~2%) because the disk read savings offset decompression cost.
//!
//! Use compression for bandwidth-constrained or storage-bound workloads
//! (WAN, cloud cross-region, S3, disk). For local high-throughput transport,
//! the uncompressed zero-copy path is the right choice. The uncompressed
//! fast path pays no overhead for compression support existing.

/// Arrow IPC per-buffer body decompression.
#[cfg(feature = "zstd")]
pub mod ipc;

use std::io;

use crate::error::IoError;

use crate::arrow::message::org::apache::arrow::flatbuf::CompressionType;

/// Supported compression codecs.
///
/// Variants are feature-gated; with neither `zstd` nor `snappy` enabled
/// this enum is uninhabited, meaning the only `Option<Compression>` a
/// caller can construct is `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    #[cfg(feature = "snappy")]
    Snappy,
    #[cfg(feature = "zstd")]
    Zstd,
}

impl Compression {
    /// Convert to the Arrow IPC BodyCompression type. Errors for codecs
    /// outside the Arrow IPC spec.
    pub fn to_arrow_ipc_type(self) -> io::Result<CompressionType> {
        match self {
            #[cfg(feature = "zstd")]
            Compression::Zstd => Ok(CompressionType::ZSTD),
            #[cfg(feature = "snappy")]
            Compression::Snappy => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Snappy is not part of the Arrow IPC (Flatbuffers metadata) specification for BodyCompression",
            )),
        }
    }
}

/// Compress a buffer according to the requested codec.
/// Always returns a new `Vec<u8>` (per Parquet page convention).
///
/// # Arguments
/// - `input`: Slice of bytes to compress.
/// - `codec`: Compression algorithm to apply.
///
/// # Errors
/// Returns [`IoError::Compression`] if codec fails.
#[cfg_attr(
    not(any(feature = "snappy", feature = "zstd")),
    allow(unused_variables)
)]
pub fn compress(input: &[u8], codec: Compression) -> Result<Vec<u8>, IoError> {
    match codec {
        #[cfg(feature = "snappy")]
        Compression::Snappy => snappy_compress(input),
        #[cfg(feature = "zstd")]
        Compression::Zstd => zstd_compress(input),
    }
}

/// Snappy compression using the snap crate.
#[cfg(feature = "snappy")]
fn snappy_compress(input: &[u8]) -> Result<Vec<u8>, IoError> {
    use snap::raw::{Encoder, max_compress_len};
    let mut encoder = Encoder::new();
    let max_len = max_compress_len(input.len());
    let mut out = vec![0u8; max_len];
    let compressed_len = encoder
        .compress(input, &mut out)
        .map_err(|e| IoError::Compression(format!("Snappy compression failed: {:?}", e)))?;
    out.truncate(compressed_len);
    Ok(out)
}

/// Zstd compression using the Zstd crate.
#[cfg(feature = "zstd")]
fn zstd_compress(input: &[u8]) -> Result<Vec<u8>, IoError> {
    // Level 1 is fastest, with good compression.
    zstd::stream::encode_all(input, 1)
        .map_err(|e| IoError::Compression(format!("Zstd compression failed: {e}")))
}

/// Decompress a buffer according to the codec.
/// Returns a new `Vec<u8>` containing the decompressed data.
///
/// # Arguments
/// - `input`: Compressed bytes.
/// - `codec`: Compression algorithm to use (must match source).
///
/// # Errors
/// Returns [`IoError::Compression`] on failure or if codec not enabled.
#[cfg_attr(
    not(any(feature = "snappy", feature = "zstd")),
    allow(unused_variables)
)]
pub fn decompress(input: &[u8], codec: Compression) -> Result<Vec<u8>, IoError> {
    match codec {
        #[cfg(feature = "snappy")]
        Compression::Snappy => snappy_decompress(input),
        #[cfg(feature = "zstd")]
        Compression::Zstd => zstd_decompress(input),
    }
}

#[cfg(feature = "snappy")]
fn snappy_decompress(input: &[u8]) -> Result<Vec<u8>, IoError> {
    use snap::raw::Decoder;
    let mut decoder = Decoder::new();
    decoder
        .decompress_vec(input)
        .map_err(|e| IoError::Compression(format!("Snappy decompression failed: {:?}", e)))
}

#[cfg(feature = "zstd")]
fn zstd_decompress(input: &[u8]) -> Result<Vec<u8>, IoError> {
    zstd::stream::decode_all(input)
        .map_err(|e| IoError::Compression(format!("Zstd decompression failed: {e}")))
}

/// Returns the codec as a Parquet-format string identifier. `None` is
/// the uncompressed case.
pub fn parquet_codec_name(codec: Option<Compression>) -> &'static str {
    match codec {
        None => "UNCOMPRESSED",
        #[cfg(feature = "snappy")]
        Some(Compression::Snappy) => "SNAPPY",
        #[cfg(feature = "zstd")]
        Some(Compression::Zstd) => "ZSTD",
    }
}
