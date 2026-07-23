// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Buffer wrapper for tokio-uring compatibility.
//!
//! Wraps `Vec64<u8>` in a newtype so we can implement IoBuf/IoBufMut
//! without hitting orphan rules. Used for both reads and writes where
//! we need 64-byte alignment and io_uring compatibility.

use minarrow::Vec64;

/// Newtype wrapper around Vec64<u8> that implements tokio-uring buffer traits.
pub(crate) struct UringBuf(pub Vec64<u8>);

// Safety: Vec64<u8> uses a heap allocation whose pointer is stable across
// moves. bytes_init = len, bytes_total = capacity.
unsafe impl tokio_uring::buf::IoBuf for UringBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.0.len()
    }

    fn bytes_total(&self) -> usize {
        self.0.capacity()
    }
}

// Safety: same pointer stability guarantee. set_init updates the length
// so kernel-written bytes are considered initialised.
unsafe impl tokio_uring::buf::IoBufMut for UringBuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        if pos > self.0.len() {
            unsafe {
                self.0.set_len(pos);
            }
        }
    }
}
