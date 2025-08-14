//! # Stream Buffer Abstraction
//!
//! Lightweight abstraction over byte buffers for frame-based streaming in Lightstream.
//!
//! ## Purpose
//! Integrate different lower-level byte abstractions (e.g. `Vec<u8>`, `minarrow::Vec64<u8>`, or
//! third-party buffers) with Minarrow/Lightstream readers and writers without coupling framing
//! logic to a concrete container.
//!
//! ## Notes
//! - Implementors must support common `Vec`-like operations: draining, appending, length queries,
//!   and slice access.
//! - `ALIGN` communicates the buffer alignment requirement so framing layers can insert padding
//!   when necessary (e.g. to maintain 64-byte SIMD alignment for Arrow buffers).
//!
//! This module provides trait bounds only; concrete buffers are supplied by callers.

use minarrow::Vec64;

/// Abstraction over a byte buffer for frame-based streaming.
///
/// This trait defines the required interface for internal buffers used by `Minarrow` stream readers
/// and the IO framing layers that support them.
///
/// It is implemented for standard `Vec<u8>` and `minarrow::Vec64<u8>`, but can also be implemented
/// for alternative byte buffer types (e.g., Tokio's `Bytes`).
///
/// This allows integration of different lower-level byte abstractions with `Minarrow`'s `TableStreamReader` and `TableStreamWriter`,
/// supporting scenarios where the byte-level IO stack is fixed or externally controlled,
/// or where an alternative buffer implementation is preferred, enabling flexibility in the underlying
/// buffer representation without requiring changes to the framing logic.
///
/// Implementors must support the following standard `Vec` operations:
/// - Draining consumed bytes
/// - Appending new bytes
/// - Querying the current buffer length
/// - Accessing the internal byte slice
pub trait StreamBuffer: AsRef<[u8]> + Default + Extend<u8> + 'static {
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
}

impl StreamBuffer for Vec<u8> {
    /// Common Arrow ecosystem alignment for metadata and non-SIMD payloads.
    const ALIGN: usize = 8;

    #[inline]
    fn with_capacity(n: usize) -> Self {
        Vec::with_capacity(n)
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        // Call inherent Vec::reserve to avoid recursion.
        Vec::<u8>::reserve(self, additional);
    }

    #[inline]
    fn drain(&mut self, range: std::ops::Range<usize>) {
        // Call Vec::<T>::drain, ignore the returned Drain iterator.
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
}

impl StreamBuffer for Vec64<u8> {
    /// For this crate, `ALIGN = 64` applies only to Arrow IPC data buffers
    /// that benefit from SIMD alignment. Flatbuffers metadata and other
    /// non-payload sections continue to use 8-byte alignment.
    ///
    /// By default, `Vec64` allocates buffers with 64-byte alignment. Setting
    /// this constant communicates to the framing layer: *“pad me to 64 bytes”*.
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
}
