//! Type–Length–Value (TLV) frame definitions.
//!
//! Provides lightweight frame structs for TLV-based protocols:
//! - [`TLVFrame`] for encoding, which borrows value slices.
//! - [`TLVDecodedFrame`] for decoding, and owns the buffer via [`StreamBuffer`]).

use crate::traits::stream_buffer::StreamBuffer;

/// TLV (Type–Length–Value) frame for encoding.
///
/// The `length` field is implicit and derived from `value.len()`
/// during serialisation.
pub struct TLVFrame<'a> {
    pub t: u8,
    pub value: &'a [u8],
}

/// TLV (Type–Length–Value) frame for decoding.
///
/// The `length` field is implicit and derived from `value.len()`
/// during deserialisation.
pub struct TLVDecodedFrame<B: StreamBuffer> {
    pub t: u8,
    pub value: B,
}
