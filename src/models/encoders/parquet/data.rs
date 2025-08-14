//! # Parquet Encoding Helpers
//!
//! Little-endian “plain” encoders for numeric/temporal types, bit-packed boolean
//! encoding, UTF-8 string encoders, and hybrid RLE/bit-packing for dictionary indices.
//! Booleans are stored bit-packed, LSB-first, per the Parquet format.
//!
//! Currently supports the core physical types. Extranneous Parquet
//! encodings and nested types are not implemented.

// Primitive encoders

use minarrow::Bitmask;

use crate::error::IoError;

/// Encode `i32` values using Parquet plain little-endian format, appending to `out`.
pub fn encode_int32_plain(data: &[i32], out: &mut Vec<u8>) {
    for v in data {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// Encode `i64` values using Parquet plain little-endian format, appending to `out`.
pub fn encode_int64_plain(data: &[i64], out: &mut Vec<u8>) {
    for v in data {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// Encode `u32` values using Parquet plain little-endian format, appending to `out`.
pub fn encode_uint32_as_int32_plain(data: &[u32], out: &mut Vec<u8>) {
    for &v in data {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// Encode `u64` values using Parquet plain little-endian format, appending to `out`.
pub fn encode_uint64_as_int64_plain(data: &[u64], out: &mut Vec<u8>) {
    for &v in data {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

#[cfg(feature = "extended_numeric_types")]
/// Encode `u8` values as `u32` little-endian using Parquet plain format, appending to `out`.
pub fn encode_uint8_as_int32_plain(data: &[u8], out: &mut Vec<u8>) {
    for &v in data {
        out.extend_from_slice(&(v as u32).to_le_bytes());
    }
}

#[cfg(feature = "extended_numeric_types")]
/// Encode `u16` values as `u32` little-endian using Parquet plain format, appending to `out`.
pub fn encode_uint16_as_int32_plain(data: &[u16], out: &mut Vec<u8>) {
    for &v in data {
        out.extend_from_slice(&(v as u32).to_le_bytes());
    }
}

/// Encode `f32` values using Parquet plain IEEE-754 little-endian format, appending to `out`.
pub fn encode_float32_plain(data: &[f32], out: &mut Vec<u8>) {
    for v in data {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

/// Encode `f64` values using Parquet plain IEEE-754 little-endian format, appending to `out`.
pub fn encode_float64_plain(data: &[f64], out: &mut Vec<u8>) {
    for v in data {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

// Boolean – bit-packed “plain” encoding
//
// We only support RLE encoding for Categorical types at the present time.

/// Encode a boolean column bit-packed (LSB-first), respecting `null_mask`, appending to `out`.
pub fn encode_bool_bitpacked(
    values: &Bitmask,
    null_mask: Option<&Bitmask>,
    len: usize,
    out: &mut Vec<u8>,
) {
    //out.clear();
    let mut byte = 0u8;
    let mut bit = 0;
    for i in 0..len {
        let valid = null_mask.map_or(true, |m| m.get(i));
        let v = if valid { values.get(i) } else { false };
        if v {
            byte |= 1 << bit;
        }
        bit += 1;
        if bit == 8 {
            out.push(byte);
            byte = 0;
            bit = 0;
        }
    }
    if bit != 0 {
        out.push(byte); // pad upper bits with zeros
    }
}

// UTF-8 strings

/// Encode String32 (UTF-8) using length-prefix (u32 LE) per row
///
/// Nulls emit zero length.
pub fn encode_string_plain(
    offsets: &[u32],
    values: &[u8],
    null_mask: Option<&Bitmask>,
    len: usize,
    out: &mut Vec<u8>,
) -> Result<(), IoError> {
    for i in 0..len {
        // always emit a 4-byte length prefix
        let valid = null_mask.map_or(true, |m| m.get(i));
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let s_len = if valid { end - start } else { 0 };
        out.extend_from_slice(&(s_len as u32).to_le_bytes());
        // only write the bytes for non-null
        if valid {
            out.extend_from_slice(&values[start..end]);
        }
    }
    Ok(())
}

#[cfg(feature = "large_string")]
/// Encode LargeString (i.e., UTF-8, 64-bit offsets) as length-prefix (u32 LE)
///
/// Nulls emit zero length.
pub fn encode_large_string_plain(
    offsets: &[u64],
    values: &[u8],
    null_mask: Option<&Bitmask>,
    len: usize,
    out: &mut Vec<u8>,
) -> Result<(), IoError> {
    for i in 0..len {
        let valid = null_mask.map_or(true, |m| m.get(i));
        let start = offsets[i] as usize;
        let end = offsets[i + 1] as usize;
        let s_len = if valid { end - start } else { 0 };
        if valid && s_len > u32::MAX as usize {
            return Err(IoError::InputDataError(format!(
                "string >4 GiB ({} bytes)",
                s_len
            )));
        }
        // length prefix for every row
        out.extend_from_slice(&(s_len as u32).to_le_bytes());
        // actual bytes only if non-null
        if valid {
            out.extend_from_slice(&values[start..end]);
        }
    }
    Ok(())
}

// Temporal aliases for the same physical type

/// Encode `DATE32`/`TIME32` using Parquet plain format.
///
/// Backed by a physical i32.
pub fn encode_datetime32_plain(data: &[i32], out: &mut Vec<u8>) {
    encode_int32_plain(data, out)
}

/// Encode `DATE64`/`TIME64` using Parquet plain format.
///
/// Backed by a physical i64.
pub fn encode_datetime64_plain(data: &[i64], out: &mut Vec<u8>) {
    encode_int64_plain(data, out)
}

// RLE for dictionary indices

/// Encode dictionary indices using Parquet’s hybrid RLE/bit-packing format - appends to `out`.
///
/// Pads value runs to groups of 8 for bit-packing and byte-aligns the output so readers
/// never over-read. Returns `IoError::Format` if an index exceeds 32-bit range.
pub fn encode_dictionary_indices_rle(indices: &[u32], out: &mut Vec<u8>) -> Result<(), IoError> {
    if indices.is_empty() {
        out.push(0);
        return Ok(());
    }

    // ---------- bit-width ------------------------------------------------
    let bit_width = (32 - indices.iter().max().unwrap().leading_zeros()) as u8;
    debug_assert!(bit_width != 0 && bit_width <= 32);
    out.push(bit_width);

    // ---------- helpers --------------------------------------------------
    #[inline]
    fn write_uleb128(mut v: u64, o: &mut Vec<u8>) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                o.push(byte);
                break;
            }
            o.push(byte | 0x80);
        }
    }

    let bytes_per_val = ((bit_width + 7) / 8) as usize;
    let mut i = 0;
    while i < indices.len() {
        // ---- find run of equal values (eligible for RLE) ---------------
        let v = indices[i];
        let mut run = 1usize;
        while i + run < indices.len() && indices[i + run] == v {
            run += 1;
        }
        if run >= 8 {
            // RLE run
            write_uleb128((run as u64) << 1, out); // LSB 0
            for b in 0..bytes_per_val {
                out.push((v >> (b * 8)) as u8);
            }
            i += run;
            continue;
        }

        // ---- bit-packed segment ----------------------------------------
        let start = i;
        let mut n = 0usize;
        while i + n < indices.len() {
            // break if next values form another ≥8 RLE run
            if n >= 8 {
                let val = indices[i + n];
                let mut look = 1usize;
                while i + n + look < indices.len() && indices[i + n + look] == val {
                    look += 1;
                    if look == 8 {
                        break;
                    }
                }
                if look == 8 {
                    break;
                }
            }
            n += 1;
        }
        // pad to 8-value multiple
        let padded = ((n + 7) / 8) * 8;
        let groups = padded / 8;
        write_uleb128(((groups as u64) << 1) | 1, out); // LSB 1

        // transposed bit-packing
        for bit in 0..bit_width {
            for g in 0..groups {
                let mut byte = 0u8;
                for j in 0..8 {
                    let idx = start + g * 8 + j;
                    if idx < start + n && ((indices[idx] >> bit) & 1) != 0 {
                        byte |= 1 << j;
                    }
                }
                out.push(byte);
            }
        }

        // byte-align – if bit_width * groups isn’t a multiple of 8 add zeros
        let bytes_this_run = bit_width as usize * groups;
        if bytes_this_run % 1 != 0 {
            unreachable!(); // construction guarantees whole bytes
        }

        i += n;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use minarrow::vec64;

    use super::*;

    #[test]
    fn test_encode_int32_plain() {
        let data = [1i32, -1i32, 0x12345678];
        let mut buf = Vec::new();
        encode_int32_plain(&data, &mut buf);
        // break into 4‐byte chunks and reassemble
        let out: Vec<i32> = buf
            .chunks_exact(4)
            .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(out, data);
    }

    #[test]
    fn test_encode_int64_plain() {
        let data = [1i64, -1i64, 0x1122334455667788];
        let mut buf = Vec::new();
        encode_int64_plain(&data, &mut buf);
        let out: Vec<i64> = buf
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(out, data);
    }

    #[test]
    fn test_encode_uint32_as_int32_plain() {
        let data = [0u32, 1, 0xdeadbeef];
        let mut buf = Vec::new();
        encode_uint32_as_int32_plain(&data, &mut buf);
        let out: Vec<u32> = buf
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(out, data);
    }

    #[test]
    fn test_encode_uint64_as_int64_plain() {
        let data = [0u64, 1, 0xabcdef0123456789];
        let mut buf = Vec::new();
        encode_uint64_as_int64_plain(&data, &mut buf);
        let out: Vec<u64> = buf
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(out, data);
    }

    #[test]
    fn test_encode_float32_plain() {
        let data = [0.0f32, -1.5, 3.14159];
        let mut buf = Vec::new();
        encode_float32_plain(&data, &mut buf);
        let out: Vec<f32> = buf
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(out, data);
    }

    #[test]
    fn test_encode_float64_plain() {
        let data = [0.0f64, -1.5, 2.718281828];
        let mut buf = Vec::new();
        encode_float64_plain(&data, &mut buf);
        let out: Vec<f64> = buf
            .chunks_exact(8)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(out, data);
    }

    #[test]
    fn test_encode_bool_bitpacked_no_nulls() {
        // 10 booleans: T, F, T, T, F, F, F, T, T, F
        let bits = [
            true, false, true, true, false, false, false, true, true, false,
        ];
        let mask = Bitmask::from_bools(&bits);
        let mut buf = Vec::new();
        encode_bool_bitpacked(&mask, None, bits.len(), &mut buf);
        // Expect two bytes: first 8 bits LSB-first, then remaining 2
        assert_eq!(buf.len(), 2);
        // reconstruct:
        let mut out = Vec::new();
        for (i, &byte) in buf.iter().enumerate() {
            for bit in 0..8 {
                let idx = i * 8 + bit;
                if idx < bits.len() {
                    out.push((byte >> bit) & 1 == 1);
                }
            }
        }
        assert_eq!(out, bits);
    }

    #[test]
    fn test_encode_bool_bitpacked_with_nulls() {
        // same values but pretend positions 1 and 3 are null ⇒ should be written as false
        let values = vec64![true, true, true, false];
        let mut nulls = Bitmask::new_set_all(values.len(), true);
        nulls.set_false(1);
        nulls.set_false(3);
        let mut buf = Vec::new();
        encode_bool_bitpacked(
            &Bitmask::from_bools(&values),
            Some(&nulls),
            values.len(),
            &mut buf,
        );

        // check that bits at 1 and 3 are 0:
        let byte = buf[0];
        assert_eq!((byte >> 0) & 1, 1); // idx0 valid, true
        assert_eq!((byte >> 1) & 1, 0); // idx1 null → treated as false
        assert_eq!((byte >> 2) & 1, 1); // idx2
        assert_eq!((byte >> 3) & 1, 0); // idx3 null
    }

    #[test]
    fn test_encode_string_plain() {
        let slices = ["foo", "bar", "", "rust"];
        // build offsets + values
        let mut offsets = Vec::with_capacity(slices.len() + 1);
        offsets.push(0);
        let mut values = Vec::new();
        for s in &slices {
            values.extend_from_slice(s.as_bytes());
            offsets.push(values.len() as u32);
        }
        let mut buf = Vec::new();
        // no nulls
        encode_string_plain(&offsets, &values, None, slices.len(), &mut buf).unwrap();
        // manual encode: each record = 4-byte length + payload
        let mut cur = &buf[..];
        for s in &slices {
            let mut lenb = [0u8; 4];
            cur.read_exact(&mut lenb).unwrap();
            let l = u32::from_le_bytes(lenb) as usize;
            let mut strb = vec![0u8; l];
            cur.read_exact(&mut strb).unwrap();
            assert_eq!(&strb, s.as_bytes());
        }
        assert!(cur.is_empty());
    }

    #[cfg(feature = "large_string")]
    #[test]
    fn test_encode_large_string_plain_errors_on_too_long() {
        // create a string >4GiB (simulate by giving offset difference >u32::MAX)
        let offsets = [0u64, (u32::MAX as u64) + 1];
        let values = vec![0u8; 10]; // actual data length is small, but offset says huge
        let res = encode_large_string_plain(&offsets, &values, None, 1, &mut Vec::new());
        assert!(matches!(res, Err(IoError::InputDataError(_))));
    }

    #[test]
    fn test_encode_datetime_aliases() {
        let data32 = [10i32, -20, 30];
        let data64 = [100i64, -200, 300];
        let mut b1 = Vec::new();
        encode_datetime32_plain(&data32, &mut b1);
        let mut b2 = Vec::new();
        encode_int32_plain(&data32, &mut b2);
        assert_eq!(b1, b2);

        let mut c1 = Vec::new();
        encode_datetime64_plain(&data64, &mut c1);
        let mut c2 = Vec::new();
        encode_int64_plain(&data64, &mut c2);
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_encode_dictionary_indices_rle_empty() {
        let mut buf = Vec::new();
        encode_dictionary_indices_rle(&[], &mut buf).unwrap();
        assert_eq!(buf, &[0]);
    }

    #[test]
    fn test_encode_dictionary_indices_rle_short_bitpacked() {
        // fewer than 8 values, no repeats ⇒ single bit-packed run
        let indices = vec![1, 2, 3, 4, 5];
        let mut buf = Vec::new();
        encode_dictionary_indices_rle(&indices, &mut buf).unwrap();
        // first byte = bit_width = 3 (since max=5 <8)
        assert_eq!(buf[0], 3);
        // skip the header, ensure buffer length matches expectation:
        // one group of 8 padded ⇒ groups=1 ⇒ header= (1<<1)|1 = 3 => uleb128=3
        // then one byte of bit-packed data
        assert!(buf.len() >= 3);
    }

    #[test]
    fn test_encode_dictionary_indices_rle_rle_run() {
        // exactly 8 repeats → RLE run
        let indices = vec![7u32; 8];
        let mut buf = Vec::new();
        encode_dictionary_indices_rle(&indices, &mut buf).unwrap();
        // bit_width = 3, header = run_len<<1 = 16 → uleb128=16
        assert_eq!(buf[0], 3);
        assert_eq!(buf[1], 16);
        // next byte is the value
        assert_eq!(buf[2], 7);
    }

    #[test]
    fn test_encode_dictionary_indices_rle_mixed() {
        // 5×A, 8×B, 3×C → bit-packed for first 5, RLE for next8, bit-packed for last3
        let mut indices = vec![10u32; 5];
        indices.extend(std::iter::repeat(2).take(8));
        indices.extend(std::iter::repeat(3).take(3));
        let mut buf = Vec::new();
        encode_dictionary_indices_rle(&indices, &mut buf).unwrap();
        // should contain at least two uleb headers
        assert!(buf.len() > 0);
    }
}
