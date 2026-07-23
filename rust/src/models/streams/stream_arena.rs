// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Zero-allocation stream arena for network I/O.
//!
//! Pre-allocates a single Vec64 and writes network data into it
//! incrementally. Each completed chunk is packaged as a SharedBuffer
//! window referencing its region of the backing. The write position
//! advances forward, and windows reference behind it.
//!
//! In steady state, where the consumer drops each window before the arena fills,
//! one allocation is reused forever.

use std::cell::UnsafeCell;
use std::io;
use std::mem::MaybeUninit;
use std::sync::Arc;

use minarrow::Vec64;
use minarrow::structs::shared_buffer::SharedBuffer;

use crate::constants::arena_capacity;

/// Backing allocation shared between the arena writer and all windows.
///
/// Uses UnsafeCell because the writer accesses spare capacity while
/// windows hold immutable references to earlier regions. These never
/// overlap - writes are always ahead of reads.
///
/// We use Vec64 so that it is cache and SIMD optimal via Minarrow.
struct ArenaBacking {
    data: UnsafeCell<Vec64<u8>>,
}

// Safety: mutable access is limited to regions assigned for writing. Published
// windows reference completed, disjoint regions and are never written again.
// The allocation remains stable for the lifetime of the backing.
unsafe impl Send for ArenaBacking {}
unsafe impl Sync for ArenaBacking {}

/// An immutable window into the arena backing allocation.
///
/// Passed to `SharedBuffer::from_owner` to create zero-copy views.
/// The Arc keeps the backing alive until all windows are dropped.
struct BufferWindow {
    backing: Arc<ArenaBacking>,
    offset: usize,
    len: usize,
}

impl AsRef<[u8]> for BufferWindow {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        // Safety: this window's [offset..offset+len] was fully written before
        // the window was created. No writes touch this region after creation.
        let ptr = self.backing.data.get();
        let data_ptr = unsafe { (*ptr).as_ptr() };
        unsafe { std::slice::from_raw_parts(data_ptr.add(self.offset), self.len) }
    }
}

// Safety: BufferWindow references immutable data behind an Arc.
unsafe impl Send for BufferWindow {}
unsafe impl Sync for BufferWindow {}

/// A mutable region of the arena for io_uring kernel submission.
///
/// Holds an Arc to keep the backing alive during the async kernel
/// operation. Implements `IoBuf`/`IoBufMut` so it can be submitted
/// directly to io_uring without allocating a separate buffer.
#[cfg(feature = "io_uring")]
pub struct ArenaRegion {
    backing: Arc<ArenaBacking>,
    offset: usize,
    capacity: usize,
    filled: usize,
}

#[cfg(feature = "io_uring")]
unsafe impl tokio_uring::buf::IoBuf for ArenaRegion {
    fn stable_ptr(&self) -> *const u8 {
        let ptr = self.backing.data.get();
        unsafe { (*ptr).as_ptr().add(self.offset) }
    }

    fn bytes_init(&self) -> usize {
        self.filled
    }

    fn bytes_total(&self) -> usize {
        self.capacity
    }
}

#[cfg(feature = "io_uring")]
unsafe impl tokio_uring::buf::IoBufMut for ArenaRegion {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        let ptr = self.backing.data.get();
        unsafe { (*ptr).as_mut_ptr().add(self.offset) }
    }

    unsafe fn set_init(&mut self, pos: usize) {
        if pos > self.filled {
            self.filled = pos;
        }
    }
}

/// Stream arena for zero-allocation I/O.
///
/// Write network data into the arena via `spare_uninit()` + `advance()`,
/// or `extend_from_slice()` for bytes already in hand.
/// Package completed regions as SharedBuffer windows via `window()`.
/// Recycle the backing when all windows have been dropped.
pub struct StreamArena {
    backing: Arc<ArenaBacking>,
    write_pos: usize,
    capacity: usize,
}

impl Default for StreamArena {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamArena {
    /// Create an arena with the configured capacity: 2 GiB of virtual
    /// address space by default, overridable at runtime with
    /// `LIGHTSTREAM_ARENA_CAPACITY`. Physical memory is committed only
    /// as bytes are written.
    pub fn new() -> Self {
        Self::with_capacity(arena_capacity())
    }

    /// Create an arena with the given capacity.
    ///
    /// The backing allocation is reserved but not initialised. Bytes
    /// become initialised as writes commit them through `advance`.
    pub fn with_capacity(capacity: usize) -> Self {
        let v = Vec64::with_capacity(capacity);
        Self {
            backing: Arc::new(ArenaBacking {
                data: UnsafeCell::new(v),
            }),
            write_pos: 0,
            capacity,
        }
    }

    /// Get the spare capacity for writing.
    ///
    /// Returns `[write_pos..capacity]` as uninitialised memory. The
    /// caller writes into this region, then calls `advance(n)` to commit
    /// the `n` bytes it initialised. Asynchronous read paths wrap the
    /// slice with `ReadBuf::uninit`.
    ///
    /// The returned slice is only valid until the next method call
    /// on this arena.
    #[inline]
    pub fn spare_uninit(&mut self) -> &mut [MaybeUninit<u8>] {
        // Safety: we access the spare region [write_pos..capacity] via raw
        // pointer arithmetic, never creating &mut Vec64. Existing windows
        // reference [0..write_pos] which doesn't overlap, and the region is
        // exposed as MaybeUninit so no initialisation claim is made.
        let ptr = self.backing.data.get();
        let data_ptr = unsafe { (*ptr).as_mut_ptr() };
        let spare_ptr = unsafe { data_ptr.add(self.write_pos) as *mut MaybeUninit<u8> };
        let spare_len = self.capacity - self.write_pos;
        unsafe { std::slice::from_raw_parts_mut(spare_ptr, spare_len) }
    }

    /// Copy `src` into the spare capacity and advance over it.
    ///
    /// Errors when `src` does not fit in the remaining capacity.
    pub fn extend_from_slice(&mut self, src: &[u8]) -> io::Result<()> {
        if src.len() > self.capacity - self.write_pos {
            return Err(io::Error::other(
                "StreamArena::extend_from_slice: source exceeds remaining capacity",
            ));
        }
        // Safety: the destination range [write_pos..write_pos+src.len()]
        // lies inside the backing allocation, does not overlap `src`, and
        // no window references it because windows end at or before
        // write_pos.
        let ptr = self.backing.data.get();
        unsafe {
            let dst = (*ptr).as_mut_ptr().add(self.write_pos);
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst, src.len());
        }
        self.write_pos += src.len();
        Ok(())
    }

    /// Advance the write position without alignment padding.
    ///
    /// Use this when accumulating data for a single frame across
    /// multiple reads. Call `align()` after the frame is complete
    /// to pad to the next 64-byte boundary before the next frame.
    ///
    /// The caller must have initialised the `n` bytes it advances over,
    /// and `n` must not exceed `remaining()`.
    ///
    /// # Safety
    ///
    /// Every byte in the first `n` elements returned by the preceding
    /// [`Self::spare_uninit`] call must have been initialised.
    #[inline]
    pub unsafe fn advance(&mut self, n: usize) {
        assert!(
            n <= self.capacity - self.write_pos,
            "StreamArena::advance past capacity"
        );
        self.write_pos += n;
    }

    /// Pad the write position to the next 64-byte boundary.
    ///
    /// Call this after a frame is complete so the next frame starts
    /// at a SIMD-aligned offset. Since the Vec64 base address is
    /// 64-byte aligned, this ensures every window's pointer is too.
    #[inline]
    pub fn align(&mut self) {
        let remainder = self.write_pos % 64;
        if remainder != 0 {
            let padding = (64 - remainder).min(self.capacity - self.write_pos);
            // Keep [0..write_pos) fully initialised so windows may safely
            // reference any committed region.
            let ptr = self.backing.data.get();
            unsafe {
                (*ptr).as_mut_ptr().add(self.write_pos).write_bytes(0, padding);
            }
            self.write_pos += padding;
        }
    }

    /// Package the region `[offset..offset+len]` as a SharedBuffer.
    ///
    /// The region must have been fully written (offset + len <= write_pos).
    /// The returned SharedBuffer is an independent, reference-counted view.
    #[inline]
    pub fn window(&self, offset: usize, len: usize) -> SharedBuffer {
        let end = offset
            .checked_add(len)
            .expect("StreamArena::window range overflows");
        assert!(end <= self.write_pos, "window extends past write_pos");
        SharedBuffer::from_owner(BufferWindow {
            backing: self.backing.clone(),
            offset,
            len,
        })
    }

    /// Create an io_uring-submittable region from the arena's spare capacity.
    ///
    /// Returns an `ArenaRegion` that implements `IoBuf`/`IoBufMut` and can
    /// be submitted directly to io_uring. The kernel writes into the arena's
    /// memory. After the read completes, call `advance()` with the bytes
    /// filled, then `window()` to create the SharedBuffer view.
    #[cfg(feature = "io_uring")]
    pub fn uring_region(&self, offset: usize, len: usize) -> ArenaRegion {
        let end = offset
            .checked_add(len)
            .expect("StreamArena::uring_region range overflows");
        assert!(end <= self.capacity, "uring region extends past capacity");
        ArenaRegion {
            backing: self.backing.clone(),
            offset,
            capacity: len,
            filled: 0,
        }
    }

    /// Remaining writable capacity in bytes.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.capacity - self.write_pos
    }

    /// Ensure at least `needed` bytes of writable capacity.
    ///
    /// Recycles the backing when the space exists but sits behind
    /// `write_pos`, and grows a fresh generation when `needed` exceeds
    /// the capacity itself. Outstanding windows keep the old backing
    /// alive through their own Arc.
    pub fn ensure_capacity(&mut self, needed: usize) {
        if self.capacity - self.write_pos >= needed {
            return;
        }
        if needed > self.capacity {
            self.capacity = needed.div_ceil(64) * 64;
            self.backing = Arc::new(ArenaBacking {
                data: UnsafeCell::new(Vec64::with_capacity(self.capacity)),
            });
            self.write_pos = 0;
        } else {
            self.recycle_or_reset();
        }
    }

    /// Current write position.
    #[inline]
    pub fn write_pos(&self) -> usize {
        self.write_pos
    }

    /// Recycle the arena when every window has been dropped.
    ///
    /// Resets the write position so subsequent writes reuse the pages
    /// already committed by earlier frames. When windows still reference
    /// the backing, the arena is left unchanged and writes continue to
    /// append. Callers on a sequential read path invoke this before each
    /// reservation to keep one hot region in play instead of committing
    /// fresh pages per frame.
    #[inline]
    pub fn recycle_if_free(&mut self) {
        if Arc::strong_count(&self.backing) == 1 {
            self.write_pos = 0;
        }
    }

    /// Try to recycle the arena. If all windows have been dropped
    /// (only our Arc remains), reset write_pos to reuse the same
    /// allocation. Otherwise, allocate a fresh backing.
    pub fn recycle_or_reset(&mut self) {
        if Arc::strong_count(&self.backing) == 1 {
            // All windows dropped. Reuse the same allocation.
            self.write_pos = 0;
        } else {
            // Windows still outstanding. Start a fresh generation.
            self.backing = Arc::new(ArenaBacking {
                data: UnsafeCell::new(Vec64::with_capacity(self.capacity)),
            });
            self.write_pos = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_window() {
        let mut arena = StreamArena::with_capacity(1024);
        assert_eq!(arena.remaining(), 1024);

        // Write some data
        let start = arena.write_pos();
        arena.extend_from_slice(b"hello").unwrap();

        // Create a window over the written data
        let shared = arena.window(start, 5);
        assert_eq!(shared.as_slice(), b"hello");
        assert_eq!(arena.write_pos(), 5);

        // Align for next frame
        arena.align();
        assert_eq!(arena.write_pos(), 64);
    }

    #[test]
    fn multiple_windows() {
        let mut arena = StreamArena::with_capacity(1024);

        // Write chunk 1
        let start1 = arena.write_pos();
        arena.extend_from_slice(b"abc").unwrap();
        let w1 = arena.window(start1, 3);
        arena.align();

        // Write chunk 2 - starts at 64-byte aligned offset
        let start2 = arena.write_pos();
        assert_eq!(start2 % 64, 0);
        arena.extend_from_slice(b"def").unwrap();
        let w2 = arena.window(start2, 3);
        arena.align();

        // Both windows read correctly
        assert_eq!(w1.as_slice(), b"abc");
        assert_eq!(w2.as_slice(), b"def");
    }

    #[test]
    fn alignment_padding_is_initialised() {
        let mut arena = StreamArena::with_capacity(64);
        arena.extend_from_slice(b"x").unwrap();
        arena.align();
        assert_eq!(arena.window(1, 63).as_slice(), &[0; 63]);
    }

    #[test]
    fn recycle_when_all_dropped() {
        let mut arena = StreamArena::with_capacity(256);

        arena.extend_from_slice(&[1u8; 10]).unwrap();
        let w = arena.window(0, 10);
        arena.align();
        assert_eq!(arena.write_pos(), 64);

        // Can't recycle while window exists
        arena.recycle_or_reset();
        // Window still held, so a new backing was allocated
        assert_eq!(arena.write_pos(), 0);

        // Verify old window still valid
        assert_eq!(w.as_slice(), &[1u8; 10]);
        drop(w);
    }

    #[test]
    fn recycle_reuses_allocation() {
        let mut arena = StreamArena::with_capacity(256);

        arena.extend_from_slice(&[1u8; 10]).unwrap();

        {
            let w = arena.window(0, 10);
            assert_eq!(w.as_slice(), &[1u8; 10]);
            // w is dropped here
        }

        // All windows dropped, recycle reuses the allocation
        let backing_ptr_before = Arc::as_ptr(&arena.backing);
        arena.recycle_or_reset();
        let backing_ptr_after = Arc::as_ptr(&arena.backing);
        assert_eq!(
            backing_ptr_before, backing_ptr_after,
            "should reuse same backing"
        );
        assert_eq!(arena.write_pos(), 0);
    }

    #[test]
    fn arena_fills_then_rolls_over() {
        // Use 128 bytes so we can fit at least one 64-byte-aligned window
        let mut arena = StreamArena::with_capacity(128);

        arena.extend_from_slice(&[42u8; 64]).unwrap();
        let w = arena.window(0, 64);
        arena.align();

        // Write another 64 bytes to fill the arena
        arena.extend_from_slice(&[43u8; 64]).unwrap();
        assert_eq!(arena.remaining(), 0);

        // Arena full, roll over to new generation
        arena.recycle_or_reset();
        assert_eq!(arena.write_pos(), 0);

        // Old window still valid
        assert_eq!(w.as_slice(), &[42u8; 64]);

        // Write into new generation
        let start = arena.write_pos();
        arena.extend_from_slice(b"new!").unwrap();
        let w2 = arena.window(start, 4);
        assert_eq!(w2.as_slice(), b"new!");
    }

    #[test]
    fn windows_are_64_byte_aligned() {
        let mut arena = StreamArena::with_capacity(4096);

        // Write three chunks of different sizes, align between each
        for i in 0..3 {
            let start = arena.write_pos();
            assert_eq!(start % 64, 0, "window {i} start not 64-byte aligned");
            let data = vec![(i + 1) as u8; 100];
            arena.extend_from_slice(&data).unwrap();
            let w = arena.window(start, 100);
            assert_eq!(w.as_slice(), &data);
            arena.align();
        }
    }

    #[test]
    fn multi_read_payload_is_contiguous() {
        let mut arena = StreamArena::with_capacity(4096);

        // Simulate reading a payload in three partial reads
        let start = arena.write_pos();
        arena.extend_from_slice(&[1u8; 10]).unwrap();
        arena.extend_from_slice(&[2u8; 10]).unwrap();
        arena.extend_from_slice(&[3u8; 10]).unwrap();

        // The window covers all 30 bytes contiguously
        let w = arena.window(start, 30);
        assert_eq!(w.len(), 30);
        assert_eq!(&w.as_slice()[..10], &[1u8; 10]);
        assert_eq!(&w.as_slice()[10..20], &[2u8; 10]);
        assert_eq!(&w.as_slice()[20..30], &[3u8; 10]);

        // Now align for the next frame
        arena.align();
        assert_eq!(arena.write_pos() % 64, 0);
    }

    #[test]
    #[should_panic(expected = "advance past capacity")]
    fn advance_past_capacity_panics() {
        let mut arena = StreamArena::with_capacity(64);
        unsafe { arena.advance(65) };
    }

    #[test]
    #[should_panic(expected = "window extends past write_pos")]
    fn window_past_write_pos_panics() {
        let mut arena = StreamArena::with_capacity(64);
        arena.extend_from_slice(&[1u8; 8]).unwrap();
        let _ = arena.window(0, 9);
    }

    #[test]
    fn extend_past_capacity_errors() {
        let mut arena = StreamArena::with_capacity(8);
        assert!(arena.extend_from_slice(&[0u8; 9]).is_err());
        assert_eq!(arena.write_pos(), 0);
    }

}
