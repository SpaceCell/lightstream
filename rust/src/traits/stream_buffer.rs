// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Stream Buffer - Wire Alignment Abstraction
//!
//! Controls the alignment of Arrow IPC frame boundaries on the wire.
//!
//! ## Wire alignment parameter `B`
//!
//! Throughout the library, the generic parameter `B: StreamBuffer` determines
//! how Arrow IPC frames are padded on the wire:
//!
//! - **`Vec64<u8>` (ALIGN=64)** - 64-byte SIMD-aligned frames. Use for
//!   lightstream-to-lightstream communication where both sides are this
//!   library. Column buffers land on 64-byte boundaries, enabling zero-copy
//!   `Buffer::from_shared` without alignment fixup copies. This is the
//!   recommended default.
//!
//! - **`Vec<u8>` (ALIGN=8)** - Standard 8-byte aligned frames. Use when
//!   reading Arrow IPC data produced by external tools (incl. PyArrow, Arrow C++,
//!   etc.) that use the minimum spec alignment. Also safe for writing data
//!   consumed by external readers.
//!
//! ## Receiver side
//!
//! On the decode side, the library always reads body data into a `Vec64<u8>`
//! internally, regardless of `B`. The `B` parameter only affects frame
//! boundary calculations (where metadata padding and body padding land).
//! Column data is mapped via `SharedBuffer` for zero-copy access.
//! This allows compatibility with bytes from a different sources whilst
//! ensuring data is captured into 64-byte aligned SIMD-ready vectors.
//!
//! ## Wire padding overhead
//!
//! With `Vec64<u8>`, metadata and body sections are padded to 64-byte
//! boundaries. This adds at most 63 bytes of padding per section compared
//! to the 8-byte minimum. For a typical record batch of tens of KB or
//! more, this is well under 1% overhead on the wire. The trade-off is
//! worthwhile when targeting SIMD cache alignment because the receiver can map
//! column buffers directly from the SharedBuffer without alignment fixup copies,
//! which would otherwise cost far more than the extra padding bytes. Consequently,
//! SIMD calculations are available on the buffers permanently via `Minarrow`.
//!
//! With `Vec<u8>`, padding follows the Arrow spec minimum of 8 bytes.
//! Column buffers may not be 64-byte aligned on arrival, so
//! `Buffer::from_shared` will copy into an aligned Vec64, as this is enforced by Minarrow
//! to support its central SIMD compatibility and cache-optimal promise.
//! This means that if data was written via an 8-byte Arrow implementation there is one memory copy,
//! to resolve high-performance data buffers once at source ingest.
//!
//! ## Choosing B
//!
//! - Lightstream protocol connections: `Vec64<u8>` - both sides are controlled
//! - Arrow IPC transport for internal use: `Vec64<u8>` - best performance
//! - Arrow IPC transport for interop with external readers: `Vec<u8>` - spec minimum alignment

use minarrow::Vec64;
use minarrow::structs::shared_buffer::SharedBuffer;

/// Wire alignment buffer for Arrow IPC frame encoding and decoding.
///
/// The `ALIGN` constant controls how IPC frames are padded on the wire.
/// This is the single parameter that determines whether frames use
/// 64-byte SIMD alignment or standard 8-byte Arrow spec alignment.
///
/// Implemented for `Vec<u8>` with ALIGN=8 and `Vec64<u8>` with ALIGN=64.
/// The receiver always reads body data into a Vec64 internally -
/// `B` only affects frame boundary padding calculations.
pub trait StreamBuffer:
    AsRef<[u8]> + AsMut<[u8]> + Default + Extend<u8> + Send + Sync + 'static
{
    /// What alignment should the data buffer use?
    /// This is a data point that can be used for enforcing the alignment
    /// constraint via padding when necessary.
    const ALIGN: usize;

    /// Create with given capacity.
    fn with_capacity(n: usize) -> Self;

    /// Reserve additional capacity in the buffer without changing its length.
    fn reserve(&mut self, additional: usize);

    /// Remove the specified range from the front of the buffer.
    fn drain(&mut self, range: std::ops::Range<usize>);

    /// Current length (in bytes).
    fn len(&self) -> usize;

    /// Whether the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Append bytes from a slice.
    fn extend_from_slice(&mut self, data: &[u8]);

    /// Push a single byte to the end of the buffer.
    fn push(&mut self, byte: u8);

    /// Create a buffer from a slice (copies the bytes).
    fn from_slice(data: &[u8]) -> Self;

    /// Convert into a SharedBuffer for zero-copy Arrow decode.
    /// Both `Vec<u8>` and `Vec64<u8>` are wrapped without copying data.
    fn into_shared_buffer(self) -> SharedBuffer;
}

impl StreamBuffer for Vec<u8> {
    /// Common Arrow ecosystem alignment for metadata and non-SIMD sources.
    const ALIGN: usize = 8;

    #[inline]
    fn with_capacity(n: usize) -> Self {
        Vec::with_capacity(n)
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        Vec::<u8>::reserve(self, additional);
    }

    #[inline]
    fn drain(&mut self, range: std::ops::Range<usize>) {
        Vec::<u8>::drain(self, range);
    }

    #[inline]
    fn len(&self) -> usize {
        Vec::<u8>::len(self)
    }

    #[inline]
    fn extend_from_slice(&mut self, data: &[u8]) {
        Vec::<u8>::extend_from_slice(self, data)
    }

    #[inline]
    fn push(&mut self, byte: u8) {
        Vec::<u8>::push(self, byte)
    }

    #[inline]
    fn from_slice(data: &[u8]) -> Self {
        data.to_vec()
    }

    #[inline]
    fn into_shared_buffer(self) -> SharedBuffer {
        SharedBuffer::from_vec(self)
    }
}

impl StreamBuffer for Vec64<u8> {
    /// For this crate, `ALIGN = 64` applies only to Arrow IPC data buffers
    /// that benefit from SIMD alignment. Flatbuffers metadata and other
    /// non-payload sections continue to use 8-byte alignment.
    ///
    /// By default, `Vec64` allocates buffers with 64-byte alignment. Setting
    /// this constant communicates to the framing layer: *”pad me to 64 bytes”*.
    /// This ensures that when an Arrow buffer is allocated, its starting offset
    /// is 64-byte aligned and SIMD-ready, enabling zero-copy use
    /// with `Minarrow` or any other consumer.
    ///
    /// Metadata remains at 8-byte boundaries unless followed by an Arrow buffer,
    /// in which case the crate implementation guarantees the next buffer begins on a
    /// 64-byte boundary.
    const ALIGN: usize = 64;

    #[inline]
    fn with_capacity(n: usize) -> Self {
        Vec64::with_capacity(n)
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    #[inline]
    fn drain(&mut self, range: std::ops::Range<usize>) {
        self.0.drain(range);
    }

    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn extend_from_slice(&mut self, data: &[u8]) {
        self.0.extend_from_slice(data)
    }

    #[inline]
    fn push(&mut self, byte: u8) {
        self.0.push(byte)
    }

    #[inline]
    fn from_slice(data: &[u8]) -> Self {
        Vec64::from_slice(data)
    }

    #[inline]
    fn into_shared_buffer(self) -> SharedBuffer {
        SharedBuffer::from_vec64(self)
    }
}
