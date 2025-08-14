//! Compression utilities for parquet_writer.
//! - Snappy via rust-snappy crate when feature is enabled.
//! - Zstd via zstd crate when feature is enabled.

use crate::error::IoError;

/// Supported Parquet compression codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    #[cfg(feature = "snappy")]
    Snappy,
    #[cfg(feature = "zstd")]
    Zstd,
}

/// Compress a buffer according to the requested codec.
/// Always returns a new Vec<u8> (per Parquet page convention).
///
/// # Arguments
/// - `input`: Slice of bytes to compress.
/// - `codec`: Compression algorithm to apply.
///
/// # Errors
/// Returns [`IoError::Compression`] if codec fails or is not enabled.
pub fn compress(input: &[u8], codec: Compression) -> Result<Vec<u8>, IoError> {
    match codec {
        Compression::None => Ok(input.to_vec()),
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
/// Returns a new Vec<u8> containing the decompressed data.
///
/// # Arguments
/// - `input`: Compressed bytes.
/// - `codec`: Compression algorithm to use (must match source).
///
/// # Errors
/// Returns [`IoError::Compression`] on failure or if codec not enabled.
pub fn decompress(input: &[u8], codec: Compression) -> Result<Vec<u8>, IoError> {
    match codec {
        Compression::None => Ok(input.to_vec()),
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

/// Returns the codec as a Parquet-format string identifier.
pub fn parquet_codec_name(codec: Compression) -> &'static str {
    match codec {
        Compression::None => "UNCOMPRESSED",
        #[cfg(feature = "snappy")]
        Compression::Snappy => "SNAPPY",
        #[cfg(feature = "zstd")]
        Compression::Zstd => "ZSTD",
    }
}
