// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Decoder resource limits
//!
//! Caps the resources a single decode invocation may request from a wire format
//! before the decoder allocates or copies. The defaults are sized for genuine
//! high-throughput workloads; their job is to refuse the obvious denial-of-service
//! shape - a peer claiming a multi-gigabyte frame length, a record batch claiming
//! billions of rows, a dictionary claiming hundreds of millions of entries - while
//! staying out of the way of real data.
//!
//! Limits are checked at the point where a length or count is read from untrusted
//! bytes, before any allocation that scales with it. Each decoder accepts a
//! [`DecodeLimits`](crate::models::decoders::limits::DecodeLimits) either through its constructor or, for free functions, as a
//! trailing parameter threaded from its host reader/codec.
//!
//! Callers that need to opt out (replay tools, internal test fixtures, trusted
//! pipelines feeding the decoder from disk) construct via [`DecodeLimits::unlimited`](crate::models::decoders::limits::DecodeLimits::unlimited).

use std::io;

use crate::error::IoError;

/// Resource caps applied during decode of untrusted input.
///
/// All values are upper bounds. A request equal to the cap is allowed; one byte
/// above the cap is refused with `io::ErrorKind::InvalidData` (or the equivalent
/// [`IoError::InputDataError`] on the CSV/Parquet error channel).
///
/// The defaults are intentionally generous - they protect against the obvious
/// allocate-from-untrusted-length attack without throttling real workloads.
/// Tighten them for a specific deployment by constructing a [`DecodeLimits`]
/// literal and passing it to the relevant decoder constructor.
#[derive(Debug, Clone, Copy)]
pub struct DecodeLimits {
    /// Maximum bytes a single framed message value may declare. Applied to TLV
    /// frame length prefixes and to any per-frame body length read from a
    /// network protocol header before allocation.
    pub max_frame_bytes: usize,

    /// Maximum row count a single record batch may declare. Applied to the
    /// first `FieldNode.length` in an Arrow RecordBatch and to the explicit
    /// `len` argument passed to the Parquet plain/dictionary decoders.
    pub max_n_rows: usize,

    /// Maximum number of buffer descriptors a single record batch may declare.
    /// Buffers are the i64-offset/i64-length pairs that Arrow IPC uses to slice
    /// a record batch body into per-column regions.
    pub max_buffers: usize,

    /// Maximum number of fields a single schema or record batch may declare.
    /// Applied to both the schema field list and the record batch node list.
    pub max_fields: usize,

    /// Maximum number of entries a single dictionary batch may declare. The
    /// entry count is read from the dictionary's offset-buffer length divided
    /// by the offset element width, and from the Parquet RLE dictionary index
    /// stream's logical-length argument.
    pub max_dictionary_entries: usize,

    /// Maximum accumulated string byte total. Applied to the per-row plain
    /// string decoders, which would otherwise read an untrusted u32 length
    /// prefix and allocate a `Vec<u8>` of that size on every iteration.
    pub max_string_bytes: usize,

    /// Maximum bytes a compressed body may expand to. Applied to both the
    /// per-buffer `uncompressed_len` header read out of an Arrow IPC
    /// compressed body and to the running sum across buffers, refusing
    /// classic zip-bomb-shaped inputs before the decompressor allocates.
    pub max_decompressed_bytes: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            // 2 GiB cap on a single declared frame value. Refuses a peer that
            // claims a multi-gigabyte length to drive a matching allocation.
            max_frame_bytes: 2 * 1024 * 1024 * 1024,

            // 256M rows. Bounds row-derived offset-buffer sizing against an
            // unbounded i64 length read from untrusted metadata.
            max_n_rows: 256_000_000,

            // 64Ki buffer descriptors per record batch.
            max_buffers: 65_536,

            // 64Ki schema or per-batch fields.
            max_fields: 65_536,

            // 256M entries per dictionary batch.
            max_dictionary_entries: 256_000_000,

            // 2 GiB of accumulated string bytes in a single decode call.
            max_string_bytes: 2 * 1024 * 1024 * 1024,

            // 4 GiB total decompressed body. Larger than max_frame_bytes so a
            // legitimately compressed frame at the frame cap still has headroom
            // to expand, while refusing the unbounded-expansion zip-bomb shape.
            max_decompressed_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
}

impl DecodeLimits {
    /// Lifts every cap to `usize::MAX`. Use only for trusted input (round-trip
    /// tests, replay tools) where the source is known to be well-formed.
    pub const fn unlimited() -> Self {
        Self {
            max_frame_bytes: usize::MAX,
            max_n_rows: usize::MAX,
            max_buffers: usize::MAX,
            max_fields: usize::MAX,
            max_dictionary_entries: usize::MAX,
            max_string_bytes: usize::MAX,
            max_decompressed_bytes: usize::MAX,
        }
    }

    /// Check that `requested` does not exceed `cap`. The cap value typically
    /// comes from one of `self`'s fields; `what` names the resource for the
    /// error message.
    #[inline]
    pub fn check(&self, requested: usize, cap: usize, what: &str) -> io::Result<()> {
        if requested > cap {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "resource limit exceeded: {} requested {} (cap {})",
                    what, requested, cap
                ),
            ));
        }
        Ok(())
    }

    /// `check` for callers that propagate `IoError` rather than `io::Error`
    /// (i.e. the CSV and Parquet decoders, which keep their own error channel).
    #[inline]
    pub fn check_io(&self, requested: usize, cap: usize, what: &str) -> Result<(), IoError> {
        if requested > cap {
            return Err(IoError::InputDataError(format!(
                "resource limit exceeded: {} requested {} (cap {})",
                what, requested, cap
            )));
        }
        Ok(())
    }
}
