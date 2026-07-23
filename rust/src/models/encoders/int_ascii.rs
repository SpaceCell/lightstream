// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Integer-to-decimal-ASCII formatting into a stack buffer, used by the CSV
//! and JSON encoders to emit numeric cells without going through
//! `core::fmt::Formatter`.
//!
//! Vendored from `itoa` v1.0.18 by David Tolnay, dual-licensed MIT/Apache-2.0
//! (source: <https://github.com/dtolnay/itoa>, file `src/lib.rs`). Trimmed to
//! only the i8/i16/i32/i64/u8/u16/u32/u64 paths the encoders actually call;
//! u128/i128 and isize/usize support are dropped along with the `no-panic`
//! shims and the `#![no_std]` attribute (this lives in a std crate).
//!
//! Original copyright (c) David Tolnay. See the upstream repository for the
//! full MIT and Apache-2.0 licence texts.

#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::expl_impl_clone_on_copy,
    clippy::identity_op,
    clippy::items_after_statements,
    clippy::must_use_candidate,
    clippy::unreadable_literal
)]

use core::hint;
use core::mem::{self, MaybeUninit};
use core::str;

/// A correctly sized stack allocation for the formatted integer to be written
/// into.
pub(super) struct Buffer {
    bytes: [MaybeUninit<u8>; i64::MAX_STR_LEN],
}

impl Default for Buffer {
    #[inline]
    fn default() -> Buffer {
        Buffer::new()
    }
}

impl Copy for Buffer {}

#[allow(clippy::non_canonical_clone_impl)]
impl Clone for Buffer {
    #[inline]
    fn clone(&self) -> Self {
        Buffer::new()
    }
}

impl Buffer {
    /// This is a cheap operation; you don't need to worry about reusing buffers
    /// for efficiency.
    #[inline]
    pub(super) fn new() -> Buffer {
        let bytes = [MaybeUninit::<u8>::uninit(); i64::MAX_STR_LEN];
        Buffer { bytes }
    }

    /// Print an integer into this buffer and return a reference to its string
    /// representation within the buffer.
    pub(super) fn format<I: Integer>(&mut self, i: I) -> &str {
        let buf_ptr = self.bytes.as_mut_ptr().cast::<I::Buffer>();
        let string = i.write(unsafe { &mut *buf_ptr });
        if string.len() > I::MAX_STR_LEN {
            unsafe { hint::unreachable_unchecked() };
        }
        string
    }
}

/// An integer that can be written into a [`Buffer`].
///
/// This trait is sealed and cannot be implemented for types outside of this
/// module.
pub(super) trait Integer: private::Sealed {
    /// The maximum length of string that formatting an integer of this type can
    /// produce.
    const MAX_STR_LEN: usize;
}

// Seal to prevent downstream implementations of the Integer trait.
mod private {
    pub trait Sealed: Copy {
        type Buffer: 'static;
        fn write(self, buf: &mut Self::Buffer) -> &str;
    }
}

macro_rules! impl_Integer {
    ($Signed:ident, $Unsigned:ident) => {
        const _: () = {
            assert!($Signed::MIN < 0, "need signed");
            assert!($Unsigned::MIN == 0, "need unsigned");
            assert!($Signed::BITS == $Unsigned::BITS, "need counterparts");
        };

        impl Integer for $Unsigned {
            const MAX_STR_LEN: usize = $Unsigned::MAX.ilog10() as usize + 1;
        }

        impl private::Sealed for $Unsigned {
            type Buffer = [MaybeUninit<u8>; Self::MAX_STR_LEN];

            #[inline]
            fn write(self, buf: &mut Self::Buffer) -> &str {
                let offset = Unsigned::fmt(self, buf);
                // SAFETY: Starting from `offset`, all elements of the slice have been set.
                unsafe { slice_buffer_to_str(buf, offset) }
            }
        }

        impl Integer for $Signed {
            const MAX_STR_LEN: usize = $Signed::MAX.ilog10() as usize + 2;
        }

        impl private::Sealed for $Signed {
            type Buffer = [MaybeUninit<u8>; Self::MAX_STR_LEN];

            #[inline]
            fn write(self, buf: &mut Self::Buffer) -> &str {
                let mut offset = Self::MAX_STR_LEN - $Unsigned::MAX_STR_LEN;
                offset += Unsigned::fmt(
                    self.unsigned_abs(),
                    (&mut buf[offset..]).try_into().unwrap(),
                );
                if self < 0 {
                    offset -= 1;
                    buf[offset].write(b'-');
                }
                // SAFETY: Starting from `offset`, all elements of the slice have been set.
                unsafe { slice_buffer_to_str(buf, offset) }
            }
        }
    };
}

impl_Integer!(i8, u8);
impl_Integer!(i16, u16);
impl_Integer!(i32, u32);
impl_Integer!(i64, u64);

#[repr(C, align(2))]
struct DecimalPairs([u8; 200]);

// The string of all two-digit numbers in range 00..99 is used as a lookup table.
static DECIMAL_PAIRS: DecimalPairs = DecimalPairs(
    *b"0001020304050607080910111213141516171819\
       2021222324252627282930313233343536373839\
       4041424344454647484950515253545556575859\
       6061626364656667686970717273747576777879\
       8081828384858687888990919293949596979899",
);

// Returns {value / 100, value % 100} correct for values of up to 4 digits.
fn divmod100(value: u32) -> (u32, u32) {
    debug_assert!(value < 10_000);
    const EXP: u32 = 19; // 19 is faster or equal to 12 even for 3 digits.
    const SIG: u32 = (1 << EXP) / 100 + 1;
    let div = (value * SIG) >> EXP; // value / 100
    (div, value - div * 100)
}

/// This function converts a slice of ascii characters into a `&str` starting
/// from `offset`.
///
/// # Safety
///
/// `buf` content starting from `offset` index MUST BE initialized and MUST BE
/// ascii characters.
unsafe fn slice_buffer_to_str(buf: &[MaybeUninit<u8>], offset: usize) -> &str {
    // SAFETY: `offset` is always included between 0 and `buf`'s length.
    let written = unsafe { buf.get_unchecked(offset..) };
    // SAFETY: (`assume_init_ref`) All buf content since offset is set.
    // SAFETY: (`from_utf8_unchecked`) Writes use ASCII from the lookup table exclusively.
    unsafe { str::from_utf8_unchecked(&*(written as *const [MaybeUninit<u8>] as *const [u8])) }
}

trait Unsigned: Integer {
    fn fmt(self, buf: &mut Self::Buffer) -> usize;
}

macro_rules! impl_Unsigned {
    ($Unsigned:ident) => {
        impl Unsigned for $Unsigned {
            fn fmt(self, buf: &mut Self::Buffer) -> usize {
                // Count the number of bytes in buf that are not initialized.
                let mut offset = buf.len();
                // Consume the least-significant decimals from a working copy.
                let mut remain = self;

                // Format per four digits from the lookup table.
                // Four digits need a 16-bit $Unsigned or wider.
                while mem::size_of::<Self>() > 1
                    && remain
                        > 999
                            .try_into()
                            .expect("branch is not hit for types that cannot fit 999 (u8)")
                {
                    offset -= 4;

                    // pull two pairs
                    let scale: Self = 1_00_00
                        .try_into()
                        .expect("branch is not hit for types that cannot fit 1E4 (u8)");
                    let quad = remain % scale;
                    remain /= scale;
                    let (pair1, pair2) = divmod100(quad as u32);
                    unsafe {
                        buf[offset + 0]
                            .write(*DECIMAL_PAIRS.0.get_unchecked(pair1 as usize * 2 + 0));
                        buf[offset + 1]
                            .write(*DECIMAL_PAIRS.0.get_unchecked(pair1 as usize * 2 + 1));
                        buf[offset + 2]
                            .write(*DECIMAL_PAIRS.0.get_unchecked(pair2 as usize * 2 + 0));
                        buf[offset + 3]
                            .write(*DECIMAL_PAIRS.0.get_unchecked(pair2 as usize * 2 + 1));
                    }
                }

                // Format per two digits from the lookup table.
                if remain > 9 {
                    offset -= 2;

                    let (last, pair) = divmod100(remain as u32);
                    remain = last as Self;
                    unsafe {
                        buf[offset + 0]
                            .write(*DECIMAL_PAIRS.0.get_unchecked(pair as usize * 2 + 0));
                        buf[offset + 1]
                            .write(*DECIMAL_PAIRS.0.get_unchecked(pair as usize * 2 + 1));
                    }
                }

                // Format the last remaining digit, if any.
                if remain != 0 || self == 0 {
                    offset -= 1;

                    // Either the compiler sees that remain < 10, or it prevents
                    // a boundary check up next.
                    let last = remain as u8 & 15;
                    buf[offset].write(b'0' + last);
                    // not used: remain = 0;
                }

                offset
            }
        }
    };
}

impl_Unsigned!(u8);
impl_Unsigned!(u16);
impl_Unsigned!(u32);
impl_Unsigned!(u64);
