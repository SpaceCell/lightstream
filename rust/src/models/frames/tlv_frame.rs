// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Type-Length-Value (TLV) frame definitions.
//!
//! Provides lightweight frame structs for TLV-based protocols:
//! - [`TLVFrame`](crate::models::frames::tlv_frame::TLVFrame) for encoding, which borrows value slices.
//! - [`TLVDecodedFrame`](crate::models::frames::tlv_frame::TLVDecodedFrame) for decoding, and owns the buffer via [`StreamBuffer`](crate::traits::stream_buffer::StreamBuffer)).

use crate::traits::stream_buffer::StreamBuffer;

/// TLV (Type-Length-Value) frame for encoding.
///
/// The `length` field is implicit and derived from `value.len()`
/// during serialisation.
pub struct TLVFrame<'a> {
    pub t: u8,
    pub value: &'a [u8],
}

/// TLV (Type-Length-Value) frame for decoding.
///
/// The `length` field is implicit and derived from `value.len()`
/// during deserialisation.
pub struct TLVDecodedFrame<B: StreamBuffer> {
    pub t: u8,
    pub value: B,
}
