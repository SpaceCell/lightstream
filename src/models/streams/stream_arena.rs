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
use std::sync::Arc;

use minarrow::Vec64;
use minarrow::structs::shared_buffer::SharedBuffer;

/// Default arena capacity: 2 GiB. With Vec64/MAllocPg64 this is virtual
/// address space reservation rather than physical memory until used.
/// Large enough that recycling is rare even for sustained streaming.
const DEFAULT_ARENA_CAPACITY: usize = 2 * 1024 * 1024 * 1024;

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

// Safety: access is non-overlapping. The writer touches [write_pos..capacity],
// windows reference [offset..offset+len] where offset+len <= write_pos.
// The stream is polled on a single task so there's no concurrent access.
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
/// Write network data into the arena via `spare_mut()` + `advance()`.
/// Package completed regions as SharedBuffer windows via `window()`.
/// Recycle the backing when all windows have been dropped.
pub struct StreamArena {
    backing: Arc<ArenaBacking>,
    write_pos: usize,
    capacity: usize,
}

impl StreamArena {
    /// Create an arena with the default capacity (1 MiB).
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_ARENA_CAPACITY)
    }

    /// Create an arena with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut v = Vec64::with_capacity(capacity);
        // Set len to capacity so the full allocation is addressable.
        // Content beyond write_pos is uninitialised but never read.
        unsafe {
            v.set_len(capacity);
        }
        Self {
            backing: Arc::new(ArenaBacking {
                data: UnsafeCell::new(v),
            }),
            write_pos: 0,
            capacity,
        }
    }

    /// Get a mutable slice of the spare capacity for writing.
    ///
    /// Returns `[write_pos..capacity]`. The caller writes into this
    /// region, then calls `advance(n)` to commit `n` bytes.
    ///
    /// The returned slice is only valid until the next method call
    /// on this arena.
    #[inline]
    pub fn spare_mut(&mut self) -> &mut [u8] {
        // Safety: we access the spare region [write_pos..capacity] via raw
        // pointer arithmetic, never creating &mut Vec64. Existing windows
        // reference [0..write_pos] which doesn't overlap.
        let ptr = self.backing.data.get();
        let data_ptr = unsafe { (*ptr).as_mut_ptr() };
        let spare_ptr = unsafe { data_ptr.add(self.write_pos) };
        let spare_len = self.capacity - self.write_pos;
        unsafe { std::slice::from_raw_parts_mut(spare_ptr, spare_len) }
    }

    /// Advance the write position without alignment padding.
    ///
    /// Use this when accumulating data for a single frame across
    /// multiple reads. Call `align()` after the frame is complete
    /// to pad to the next 64-byte boundary before the next frame.
    #[inline]
    pub fn advance(&mut self, n: usize) {
        self.write_pos += n;
        debug_assert!(self.write_pos <= self.capacity);
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
            self.write_pos += 64 - remainder;
        }
        // Clamp to capacity
        if self.write_pos > self.capacity {
            self.write_pos = self.capacity;
        }
    }

    /// Package the region `[offset..offset+len]` as a SharedBuffer.
    ///
    /// The region must have been fully written (offset + len <= write_pos).
    /// The returned SharedBuffer is an independent, reference-counted view.
    #[inline]
    pub fn window(&self, offset: usize, len: usize) -> SharedBuffer {
        debug_assert!(
            offset + len <= self.write_pos,
            "window extends past write_pos"
        );
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
        debug_assert!(offset + len <= self.capacity);
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

    /// Run an Arrow IPC encode (or any growable-buffer producer)
    /// directly into the arena's backing, advancing `write_pos` to the
    /// length the encoder grew the Vec64 to. The caller skips the
    /// encode-buf → arena memcpy entirely; the arena's window over the
    /// freshly-written region is wrapped zero-copy as `Bytes` by the
    /// transport.
    ///
    /// The closure MUST NOT cause the Vec64 to reallocate. Outstanding
    /// `SharedBuffer` windows hold raw pointers into the current
    /// allocation; a realloc moves it and the windows dangle. Callers
    /// guard this by checking `remaining()` against an upper bound on
    /// the encode size and calling `recycle_or_reset` first when short.
    /// A realloc inside the closure is surfaced as an `io::Error` and
    /// the arena is left with a sane `write_pos`.
    ///
    /// ## Status: retained but unused
    ///
    /// `stream_throughput` benchmarks show this path is slower than the
    /// conventional `TableSink64 + AsyncWrite` writer shape used by
    /// every transport in the tree (TCP, UDS, WS, QUIC, HTTP). The
    /// `Bytes::copy_from_slice` per flow-control grant in the
    /// AsyncWrite adapters runs at L1/L2 cache bandwidth and overlaps
    /// with async I/O wait; meanwhile this path pays for `Arc`
    /// bookkeeping per frame and risks fresh multi-GiB backing
    /// allocations when the transport holds outstanding `SharedBuffer`
    /// windows that block in-place recycle. Both QUIC and HTTP write
    /// paths were prototyped against this and reverted after the bench
    /// numbers came back regressed by tens of percent.
    ///
    /// Kept here in case a future transport with different lifetime
    /// properties (one where the consumer drops `Bytes` synchronously
    /// before the next encode) makes the no-memcpy shape pay off.
    #[allow(dead_code)]
    pub fn encode_in_place<F>(&mut self, f: F) -> io::Result<()>
    where
        F: FnOnce(&mut Vec64<u8>) -> io::Result<()>,
    {
        // SAFETY: we hold &mut self, so no other access path to the
        // backing is live concurrently. Outstanding SharedBuffer windows
        // hold the Arc and read [..write_pos] via raw pointer; the
        // encoder writes from `write_pos` onward, so the two ranges do
        // not overlap. The closure must respect the no-realloc invariant
        // documented above; we re-check capacity afterwards.
        let backing_ptr = self.backing.data.get();
        let vec64: &mut Vec64<u8> = unsafe { &mut *backing_ptr };

        let original_capacity = vec64.capacity();
        // Sync Vec64's len with write_pos so the encoder's Extend-style
        // appends start at the right offset within the backing. Bytes in
        // [0..write_pos) were initialised by prior writes.
        // SAFETY: re-asserts the existing initialisation invariant.
        unsafe {
            vec64.set_len(self.write_pos);
        }

        let result = f(vec64);

        let new_capacity = vec64.capacity();
        let new_len = vec64.len();

        // Restore len = capacity so spare_mut and friends keep their
        // raw-pointer addressing semantics consistent with construction.
        // SAFETY: bytes in [0..new_len) are initialised by the encoder;
        // bytes in [new_len..capacity) are uninitialised but only
        // accessed via spare_mut's raw pointer (never read).
        unsafe {
            vec64.set_len(original_capacity);
        }

        if new_capacity != original_capacity {
            // Realloc happened. Any outstanding SharedBuffer windows
            // referencing the old allocation now hold dangling pointers.
            // Surface the breach to the caller; leave write_pos sane.
            self.write_pos = new_len.min(new_capacity);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "StreamArena::encode_in_place: backing reallocated \
                     (cap {} -> {}); call recycle_or_reset before encoding \
                     when remaining() is below the max encode size",
                    original_capacity, new_capacity,
                ),
            ));
        }

        result?;
        self.write_pos = new_len;
        Ok(())
    }

    /// Current write position.
    #[inline]
    pub fn write_pos(&self) -> usize {
        self.write_pos
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
            let mut v = Vec64::with_capacity(self.capacity);
            unsafe {
                v.set_len(self.capacity);
            }
            self.backing = Arc::new(ArenaBacking {
                data: UnsafeCell::new(v),
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
        let spare = arena.spare_mut();
        spare[..5].copy_from_slice(b"hello");
        let start = arena.write_pos();
        arena.advance(5);

        // Create a window for the data portion
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
        arena.spare_mut()[..3].copy_from_slice(b"abc");
        arena.advance(3);
        let w1 = arena.window(start1, 3);
        arena.align();

        // Write chunk 2 - starts at 64-byte aligned offset
        let start2 = arena.write_pos();
        assert_eq!(start2 % 64, 0);
        arena.spare_mut()[..3].copy_from_slice(b"def");
        arena.advance(3);
        let w2 = arena.window(start2, 3);
        arena.align();

        // Both windows read correctly
        assert_eq!(w1.as_slice(), b"abc");
        assert_eq!(w2.as_slice(), b"def");
    }

    #[test]
    fn recycle_when_all_dropped() {
        let mut arena = StreamArena::with_capacity(256);

        arena.spare_mut()[..10].copy_from_slice(&[1u8; 10]);
        arena.advance(10);
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

        arena.spare_mut()[..10].copy_from_slice(&[1u8; 10]);
        arena.advance(10);

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

        arena.spare_mut()[..64].copy_from_slice(&[42u8; 64]);
        arena.advance(64);
        let w = arena.window(0, 64);
        arena.align();

        // Write another 64 bytes to fill the arena
        arena.spare_mut()[..64].copy_from_slice(&[43u8; 64]);
        arena.advance(64);
        assert_eq!(arena.remaining(), 0);

        // Arena full, roll over to new generation
        arena.recycle_or_reset();
        assert_eq!(arena.write_pos(), 0);

        // Old window still valid
        assert_eq!(w.as_slice(), &[42u8; 64]);

        // Write into new generation
        let start = arena.write_pos();
        arena.spare_mut()[..4].copy_from_slice(b"new!");
        arena.advance(4);
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
            arena.spare_mut()[..100].copy_from_slice(&data);
            arena.advance(100);
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
        arena.spare_mut()[..10].copy_from_slice(&[1u8; 10]);
        arena.advance(10);
        arena.spare_mut()[..10].copy_from_slice(&[2u8; 10]);
        arena.advance(10);
        arena.spare_mut()[..10].copy_from_slice(&[3u8; 10]);
        arena.advance(10);

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
}
