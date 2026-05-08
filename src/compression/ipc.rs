//! Arrow IPC per-buffer body decompression.
//!
//! Arrow IPC compression operates per-buffer: each buffer in the record batch
//! body is prefixed with an 8-byte i64 uncompressed length (-1 means the
//! buffer was stored uncompressed), followed by the compressed or raw data.
//!
//! [`decompress_ipc_body`] produces a new `Vec64<u8>` with all buffers placed
//! at `B::ALIGN` offsets, consistent with the uncompressed wire layout. A
//! corrections Vec maps each buffer index to its (offset, length) within the
//! decompressed buffer, since the flatbuffer metadata still references the
//! compressed positions.
//!
//! This is inherently not zero-copy - compressed data must be decoded into a
//! fresh allocation. The Vec64 backing ensures SIMD alignment is maintained
//! after decompression, so column construction still benefits from aligned
//! access. See the parent module docs for throughput trade-offs.
//! 
//! TLDR: Avoid compression unless you genuinely need it, or have slow network
//! speeds that are more expensive than the memory throughput trade-off.

use std::io;
use flatbuffers::Vector;
use minarrow::Vec64;
use crate::arrow::message::org::apache::arrow::flatbuf as fb;
use crate::arrow::message::org::apache::arrow::flatbuf::{BodyCompression, Buffer};
use crate::traits::stream_buffer::StreamBuffer;

/// Decompress a single Arrow IPC buffer using the specified codec.
fn decompress_buffer_data(
    data: &[u8],
    codec: fb::CompressionType,
) -> io::Result<Vec<u8>> {
    if codec == fb::CompressionType::ZSTD {
        return super::decompress(data, super::Compression::Zstd)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
    }
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("Unsupported IPC compression codec: {:?}", codec),
    ))
}

/// Decompress an Arrow IPC body with per-buffer compression into a Vec64.
///
/// Two-pass: first reads 8-byte prefixes to compute total decompressed size,
/// then decompresses each buffer into a single Vec64 allocation with
/// `B::ALIGN` gaps between buffers, consistent with the alignment the
/// encoder uses on the uncompressed wire path.
///
/// Returns the decompressed buffer and a Vec of (offset, length) per buffer
/// index for corrected buffer access.
pub fn decompress_ipc_body<B: StreamBuffer>(
    body: &[u8],
    buffers: &Vector<'_, Buffer>,
    compression: &BodyCompression,
) -> io::Result<(Vec64<u8>, Vec<(usize, usize)>)> {
    const PREFIX_LEN: usize = 8;
    let codec = compression.codec();

    // First pass: read prefixes to compute total decompressed size
    let mut total_size = 0usize;
    let mut corrections = Vec::with_capacity(buffers.len());

    for i in 0..buffers.len() {
        let buf = buffers.get(i);
        let offset = buf.offset() as usize;
        let length = buf.length() as usize;

        if length == 0 {
            corrections.push((total_size, 0));
            continue;
        }

        if offset + length > body.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Compressed buffer {} out of bounds", i),
            ));
        }

        if length < PREFIX_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Compressed buffer {} too small for length prefix", i),
            ));
        }

        let prefix = i64::from_le_bytes(body[offset..offset + PREFIX_LEN].try_into().unwrap());

        let uncompressed_len = if prefix == -1 {
            length - PREFIX_LEN
        } else if prefix < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid uncompressed length prefix for buffer {}: {}", i, prefix),
            ));
        } else {
            prefix as usize
        };

        corrections.push((total_size, uncompressed_len));
        total_size += uncompressed_len;
        // Align to B::ALIGN between buffers, consistent with the wire format
        let pad = total_size % B::ALIGN;
        if pad != 0 {
            total_size += B::ALIGN - pad;
        }
    }

    // Second pass: decompress into a single Vec64 allocation
    let mut decompressed = Vec64::<u8>::with_capacity(total_size);
    decompressed.resize(total_size, 0u8);

    for i in 0..buffers.len() {
        let buf = buffers.get(i);
        let offset = buf.offset() as usize;
        let length = buf.length() as usize;
        let (dec_offset, dec_len) = corrections[i];

        if length == 0 || dec_len == 0 {
            continue;
        }

        let prefix = i64::from_le_bytes(body[offset..offset + PREFIX_LEN].try_into().unwrap());
        let data_start = offset + PREFIX_LEN;
        let data_len = length - PREFIX_LEN;

        if prefix == -1 {
            // Not compressed - copy raw data after the prefix
            decompressed[dec_offset..dec_offset + dec_len]
                .copy_from_slice(&body[data_start..data_start + data_len]);
        } else {
            let compressed_data = &body[data_start..data_start + data_len];
            let result = decompress_buffer_data(compressed_data, codec)?;
            if result.len() != dec_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Decompressed size mismatch for buffer {}: expected {}, got {}",
                        i, dec_len, result.len()
                    ),
                ));
            }
            decompressed[dec_offset..dec_offset + dec_len].copy_from_slice(&result);
        }
    }

    Ok((decompressed, corrections))
}
