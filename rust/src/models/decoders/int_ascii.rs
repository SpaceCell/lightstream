// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! ASCII-to-integer parsing with overflow detection, used by the CSV decoder
//! to validate column types and to fill numeric column buffers without going
//! through `str::from_utf8` + `str::parse`.
//!
//! The algorithm matches the `atoi` crate's checked signed path:
//! strip an optional `+`/`-`, then accumulate digits with `checked_mul`/`checked_add`
//! (positive case) or `checked_mul`/`checked_sub` (negative case).
//! The split is what allows `i*::MIN` to parse correctly without overflowing during sign-flip.
//!
//! Accepts a leading `+` or `-` on unsigned types in line with `atoi`: `-0`
//! parses to `0`, but any other negative input returns `None`.

/// Parse a base-10 ASCII integer from `bytes`. Returns `None` if the slice is
/// empty, contains a non-digit (other than a single leading `+`/`-`), or the
/// value overflows the target type.
pub(super) fn parse_ascii_int<T: ParseAsciiInt>(bytes: &[u8]) -> Option<T> {
    T::parse_ascii_int(bytes)
}

/// Sealed trait implemented for the eight standard integer widths. The CSV
/// decoder's generic column-builder uses this as its bound.
pub(super) trait ParseAsciiInt: Sized + private::Sealed {
    fn parse_ascii_int(bytes: &[u8]) -> Option<Self>;
}

mod private {
    pub trait Sealed {}
}

macro_rules! impl_unsigned {
    ($($Unsigned:ident),+) => {
        $(
            impl private::Sealed for $Unsigned {}

            impl ParseAsciiInt for $Unsigned {
                fn parse_ascii_int(bytes: &[u8]) -> Option<Self> {
                    if bytes.is_empty() {
                        return None;
                    }
                    // Match atoi: a leading sign is consumed even on unsigned
                    // types. `-0` parses to 0; any other negative returns None.
                    let (negative, rest) = match bytes[0] {
                        b'+' => (false, &bytes[1..]),
                        b'-' => (true, &bytes[1..]),
                        _ => (false, bytes),
                    };
                    if rest.is_empty() {
                        return None;
                    }
                    let mut n: $Unsigned = 0;
                    for &b in rest {
                        let d = b.wrapping_sub(b'0');
                        if d > 9 {
                            return None;
                        }
                        n = n.checked_mul(10)?.checked_add(d as $Unsigned)?;
                    }
                    if negative && n != 0 {
                        return None;
                    }
                    Some(n)
                }
            }
        )+
    };
}

macro_rules! impl_signed {
    ($($Signed:ident),+) => {
        $(
            impl private::Sealed for $Signed {}

            impl ParseAsciiInt for $Signed {
                fn parse_ascii_int(bytes: &[u8]) -> Option<Self> {
                    if bytes.is_empty() {
                        return None;
                    }
                    let (negative, rest) = match bytes[0] {
                        b'+' => (false, &bytes[1..]),
                        b'-' => (true, &bytes[1..]),
                        _ => (false, bytes),
                    };
                    if rest.is_empty() {
                        return None;
                    }
                    // Two-loop split: positive accumulates upward, negative
                    // accumulates downward, so $Signed::MIN parses without
                    // overflowing on a sign-flip.
                    let mut n: $Signed = 0;
                    if negative {
                        for &b in rest {
                            let d = b.wrapping_sub(b'0');
                            if d > 9 {
                                return None;
                            }
                            n = n.checked_mul(10)?.checked_sub(d as $Signed)?;
                        }
                    } else {
                        for &b in rest {
                            let d = b.wrapping_sub(b'0');
                            if d > 9 {
                                return None;
                            }
                            n = n.checked_mul(10)?.checked_add(d as $Signed)?;
                        }
                    }
                    Some(n)
                }
            }
        )+
    };
}

impl_unsigned!(u8, u16, u32, u64);
impl_signed!(i8, i16, i32, i64);
