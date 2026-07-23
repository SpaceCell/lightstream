// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Generic Frame Encoder
//!
//! Serialise protocol frames into **on-the-wire bytes** efficiently.
//!
//! **Purpose**
//! - Central place to define how a frame becomes a byte sequence i.e., length-prefix, TLV, IPC, etc..
//! - Keeps responsibility for buffer management with the caller.
//! - Plays nicely with any sink incl., files, sockets, custom transports.
//!
//! Implement `FrameEncoder` for a custom format; call `encode()` to append the wire bytes into a buffer.

use std::io;

use crate::traits::stream_buffer::StreamBuffer;

/// Implement this trait for any wire format requiring message serialisation,
/// such as Arrow IPC, protobuf, or custom binary protocols.
///
/// The encoder must only append to the provided buffer and must not retain references
/// or have side-effects to any data passed in.
///
/// ### Safety Contract
/// - The encoder must not mutate the frame being encoded.
/// - The encoder must not retain references to input data after the call.
/// - All writes must be bounded to the provided buffer.
pub trait FrameEncoder {
    /// The type of frame accepted by this encoder.
    type Frame<'a>;

    /// The type of metadata produced by this encoder.
    type Metadata;

    /// Encode a frame, producing both an output buffer and frame metadata.
    ///
    /// Returns an owned buffer containing the encoded frame and the associated metadata.
    /// Returns `Err` if encoding fails.
    ///
    /// ### Args
    /// * `global_offset`: keeps track of the pointer position across frames
    /// * `frame`: the frame being encoded
    fn encode<'a, B: StreamBuffer>(
        global_offset: &mut usize,
        frame: &Self::Frame<'a>,
    ) -> io::Result<(B, Self::Metadata)>;
}
