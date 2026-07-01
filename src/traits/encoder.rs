// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Piecewise byte encoding
//!
//! Implemented BY items in `models/encoders/` and `models/codecs/`.
//! `Encoder` is the "give me a value, give me bytes" contract: one
//! call produces a self-contained byte buffer for one input value.
//!
//! The encoder may be stateful across calls when the format requires
//! continuity (Arrow IPC schema + dictionary emission, for example),
//! so methods take `&mut self`. For one-shot use, callers construct
//! a fresh encoder, call `encode` once, and drop.
//!
//! This is distinct from [`crate::traits::frame_encoder::FrameEncoder`],
//! which deals in wire frames within a streaming session. An encoder
//! produces complete, decoder-ready bytes for one input; a frame
//! encoder produces individual frames that compose into a wire stream.

use minarrow::Vec64;

/// One-shot encoder: takes a value, returns a self-contained byte
/// buffer in the format the encoder represents.
pub trait Encoder {
    /// Value type consumed by the encoder.
    type Input;
    /// Error surfaced by `encode`.
    type Error;

    /// Encode `value` into a fresh 64-byte aligned byte buffer.
    fn encode(&mut self, value: &Self::Input) -> Result<Vec64<u8>, Self::Error>;
}
