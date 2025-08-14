//! # Generic Frame Decoder
//!
//! Turn a growing byte buffer into **discrete messages** without allocating.
//!
//! **Why this is useful**
//! - Cleanly split a raw byte stream (from files/sockets) into protocol frames.
//! - Minimal overhead: the decoder only *looks* at bytes and tells you what to consume.
//! - Easy to test and reuse across transports.
//!
//! Implement `FrameDecoder` for your wire format; call `decode()` as chunks arrive.
//! On a full frame, you get `{ frame, consumed }`; on `NeedMore`, just read more bytes.
//!
//! See the IPC, TLV cases as examples.

use crate::enums::DecodeResult;
use std::io;

/// A trait for non-allocating, pull-based frame decoders.
///
/// Implement this trait for any wire format requiring message boundary detection,
/// such as Arrow IPC, protobuf, or custom binary protocols.
///
/// The decoder should never allocate or retain buffer data.
/// It must *only* inspect the provided slice and return either a complete frame
/// and how many bytes were consumed, or indicate that more data is needed.
///
/// ### Safety Contract
/// - The decoder musn't mutate or take ownership of the input buffer.
/// - It must not remove bytes itself—return `consumed`, the caller will drop them.
/// - It should always leave the buffer unchanged if returning `NeedMore`.
pub trait FrameDecoder {
    /// The type of frame yielded by this decoder.
    type Frame;

    /// Attempt to decode a complete frame from the start of `buf`.
    ///
    /// Return:
    /// - `Ok(Frame { frame, consumed })` if a full frame is present. Caller removes `consumed` bytes.
    /// - `Ok(NeedMore)` if more bytes are required; buffer remains unchanged.
    /// - `Err` if the protocol is violated, or an unrecoverable error is detected.
    fn decode(&mut self, buf: &[u8]) -> io::Result<DecodeResult<Self::Frame>>;
}
