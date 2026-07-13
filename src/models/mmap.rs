// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Memory-mapped file reader with alignment guarantees.
//!
//! Lightweight wrapper over `mmap(2)` for read-only access to file
//! regions, ensuring that the mapped slice pointer is aligned to a specified
//! boundary, i.e., 64 bytes for SIMD, or as per `ALIGN`.  
//!
//! ## Overview
//! - Page-aligned mapping with automatic adjustment of offset.
//! - Safe sharing (`Send` + `Sync`) as mappings are read-only.
//! - Exposes zero-copy access via `Deref<[u8]>` and `AsRef<[u8]>`.
//! - Cleans up resources by `munmap` on drop.
//!
//! ## Errors
//! - Returns an error if the file offset is not aligned to the requested boundary.
//! - Returns an error if the mapped region is not aligned after adjustment.
//!
//! ## Typical use
//! ```ignore
//! let mmap = MemMap::<64>::open("data.arrow", 0, 4096)?;
//! let slice: &[u8] = &mmap;
//! ```

use std::fs::File;
use std::io::{self, Error, ErrorKind};
use std::os::unix::io::AsRawFd;
use std::ptr;

#[derive(Debug)]
pub struct MemMap<const ALIGN: usize> {
    pub ptr: *mut u8,
    pub len: usize,
}

// SAFETY: ptr/len describe an immutable `PROT_READ | MAP_PRIVATE` mapping
// produced inside this module; there is no shared interior mutability and no
// API hands out `&mut` access, so concurrent reads from multiple threads
// observe a stable byte sequence. The residual SIGBUS risk if the underlying
// file is truncated externally after `open` is documented on the struct
// itself, not addressable by Send/Sync.
unsafe impl<const ALIGN: usize> Send for MemMap<ALIGN> {}
unsafe impl<const ALIGN: usize> Sync for MemMap<ALIGN> {}

impl<const ALIGN: usize> MemMap<{ ALIGN }> {
    pub fn open(path: &str, offset: usize, len: usize) -> io::Result<Self> {
        if !offset.is_multiple_of(ALIGN) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "File offset must be 64-byte aligned",
            ));
        }
        let file = File::open(path)?;
        let fd = file.as_raw_fd();

        // Validate that the requested window lies within the file. Without
        // this, mmap will happily return a partial mapping, and reading the
        // unmapped tail raises SIGBUS rather than a recoverable error.
        let file_len = file.metadata()?.len();
        let requested_end = (offset as u64)
            .checked_add(len as u64)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "mmap offset+len overflow"))?;
        if requested_end > file_len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("mmap window {offset}+{len} exceeds file length {file_len}"),
            ));
        }

        // mmap must use an offset that's a multiple of page size
        // SAFETY: sysconf is FFI returning a long; no aliasing or pointer
        // arguments. A negative or zero return would be wrong on Linux but
        // is not unsound here - it would just give a degenerate map_offset.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let map_offset = offset & !(page_size - 1);
        let offset_in_page = offset - map_offset;
        let map_len = offset_in_page + len;

        // SAFETY: mmap with a null `addr`, PROT_READ, MAP_PRIVATE, a valid
        // open fd, and a page-aligned `map_offset` derived above. On success
        // mmap returns ownership of the new mapping to us; we either install
        // it in `region_ptr` for Drop to munmap, or unmap it explicitly on
        // the alignment-fail path below.
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                map_len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd,
                map_offset as libc::off_t,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }

        // Tell the kernel to use aggressive read-ahead for this mapping.
        // Without this advice the OS treats mmap'd reads conservatively
        // compared to explicit `read`/`pread` syscalls, which can leave
        // cold sequential mmap reads 2x slower than file-reader paths.
        // `madvise` is advisory; failure is ignored.
        // SAFETY: `ptr` and `map_len` describe the live mapping just
        // returned by `mmap`; both arguments are valid for `madvise`.
        unsafe {
            libc::madvise(ptr, map_len, libc::MADV_SEQUENTIAL);
        }

        // SAFETY: `ptr` is the base of a `map_len`-byte mapping and
        // `offset_in_page < page_size <= map_len`, so the resulting pointer
        // stays inside the same allocation.
        let region_ptr = unsafe { (ptr as *mut u8).add(offset_in_page) };
        // Confirm alignment
        if !(region_ptr as usize).is_multiple_of(ALIGN) {
            // SAFETY: `ptr` is the live mapping just returned by mmap and
            // we own it - munmap with the matching base+length releases it
            // before we drop the error path. The pointer is not used again.
            unsafe { libc::munmap(ptr, map_len) };
            return Err(Error::other(
                format!(
                    "MMAP region is not {ALIGN}-byte aligned (ptr = {:p})",
                    region_ptr
                ),
            ));
        }

        Ok(Self {
            ptr: region_ptr,
            len,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` points into an `mmap`-owned region of at least `len`
        // bytes (open validates this against the file size and alignment).
        // The mapping is PROT_READ and lives until Drop, so the returned
        // slice is valid for the lifetime of `&self`. If the underlying file
        // is truncated by another process between `open` and this read, the
        // OS may raise SIGBUS - documented as a residual risk on the type.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<const ALIGN: usize> AsRef<[u8]> for MemMap<{ ALIGN }> {
    /// Allow `MemMap` to be borrowed as a raw byte slice (`&[u8]`).
    ///
    /// Equivalent to calling [`MemMap::as_slice`].
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<const ALIGN: usize> std::ops::Deref for MemMap<{ ALIGN }> {
    type Target = [u8];

    /// Deref to the underlying `[u8]` slice.
    ///
    /// This allows seamless use of `&MemMap` in contexts expecting `&[u8]`.
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<const ALIGN: usize> Drop for MemMap<{ ALIGN }> {
    /// Unmap the memory region when the `MemMap` is dropped.
    ///
    /// Calculates the original page-aligned base pointer and total mapping
    /// length, then calls `munmap` to release the mapping back to the OS.
    fn drop(&mut self) {
        // Compute the base pointer for unmapping.
        // SAFETY: sysconf FFI with no aliasing risk.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let ptr_val = self.ptr as usize;
        let page_base = ptr_val & !(page_size - 1);
        let offset_in_page = ptr_val - page_base;
        let map_len = offset_in_page + self.len;
        // SAFETY: `self.ptr` was returned by mmap in `open` and adjusted
        // forward by `offset_in_page`, so `page_base = ptr - offset_in_page`
        // reconstructs the original mapping base. `map_len` reconstructs the
        // original mapping length. Ownership is unique to this `MemMap`, so
        // no other handle observes the unmap.
        unsafe {
            libc::munmap(page_base as *mut libc::c_void, map_len);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALIGN: usize = 64;

    fn pseudo_rand() -> u32 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let nanos = now.as_nanos() as u64;
        ((nanos ^ (nanos >> 32)) as u32)
            .wrapping_mul(1664525)
            .wrapping_add(1013904223)
    }

    fn create_temp_file_with_data(data: &[u8], pad: usize) -> std::path::PathBuf {
        use std::env::temp_dir;
        use std::fs::File;
        use std::io::Write;

        let mut path = temp_dir();
        path.push(format!("mmap_test_{}.bin", pseudo_rand()));

        let mut file = File::create(&path).expect("Failed to create temp file");

        if pad > 0 {
            let pad_bytes = vec![0u8; pad];
            file.write_all(&pad_bytes).expect("Pad write failed");
        }
        file.write_all(data).expect("Write failed");
        file.sync_all().unwrap();
        path
    }

    #[test]
    fn test_mmap_64_aligned() {
        let data = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let path = create_temp_file_with_data(data, 0);

        // offset 0, length = data.len()
        let map =
            MemMap::<ALIGN>::open(path.to_str().unwrap(), 0, data.len()).expect("mmap failed");
        assert_eq!(map.as_slice(), data);
        assert_eq!((map.as_slice().as_ptr() as usize) % ALIGN, 0);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_mmap_nonzero_aligned_offset() {
        let data = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let pad = 128; // ensure >1 page and 64-aligned
        let path = create_temp_file_with_data(data, pad);

        let offset = pad;
        assert_eq!(offset % ALIGN, 0, "Offset must be 64-aligned for test");

        let map =
            MemMap::<ALIGN>::open(path.to_str().unwrap(), offset, data.len()).expect("mmap failed");
        assert_eq!(map.as_slice(), data);
        assert_eq!((map.as_slice().as_ptr() as usize) % ALIGN, 0);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_mmap_unaligned_offset_error() {
        let data = b"hello world";
        let pad = 11; // not 64 aligned
        let path = create_temp_file_with_data(data, pad);

        let offset = pad;
        assert_ne!(offset % ALIGN, 0);

        let res = MemMap::<ALIGN>::open(path.to_str().unwrap(), offset, data.len());
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_mmap_rejects_window_past_eof() {
        // Map an offset+length pair that runs past the file's actual size.
        // Without the file-size guard inside `open`, mmap would return a
        // partial mapping and accessing the unmapped tail would SIGBUS at
        // runtime; with it the request is refused with InvalidInput.
        let data = b"abcdefghij"; // 10 bytes
        let path = create_temp_file_with_data(data, 0);

        let res = MemMap::<ALIGN>::open(path.to_str().unwrap(), 0, data.len() + 4096);
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("exceeds file length"));

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_mmap_alignment_is_enforced() {
        let data = b"12345678";
        let path = create_temp_file_with_data(data, 0);

        // Should succeed for 64
        let m = MemMap::<64>::open(path.to_str().unwrap(), 0, data.len()).unwrap();
        assert_eq!((m.as_slice().as_ptr() as usize) % 64, 0);

        // Should succeed for 8
        let m8 = MemMap::<8>::open(path.to_str().unwrap(), 0, data.len()).unwrap();
        assert_eq!((m8.as_slice().as_ptr() as usize) % 8, 0);

        std::fs::remove_file(path).unwrap();
    }
}
