// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Codec decoder contract
//!
//! Implemented BY format codecs (types in `models/codecs/` and
//! related decoders) that consume bytes and return a reconstructed
//! value of the codec's output type.
//!
//! Codecs may be stateful when the format requires continuity
//! (accumulated schema and dictionary state for Arrow IPC, for
//! example), so methods take `&mut self`. For one-shot use - which
//! is what [`crate::traits::serialise::Serialise`] uses - the impl
//! is expected to fully reconstruct the value from a self-contained
//! byte buffer against a freshly constructed codec.
//!
//! This contract is distinct from
//! [`crate::traits::frame_decoder::FrameDecoder`], which operates at
//! the wire-frame layer for the streaming readers.

use minarrow::Vec64;

/// One-shot decoder: takes a self-contained byte buffer, returns the
/// reconstructed value.
///
/// `decode` is the required method. `decode_owned` is an optional
/// zero-copy override that takes ownership of an aligned `Vec64<u8>`
/// and can feed it to the underlying parser without a memcpy; the
/// default forwards to `decode`.
pub trait Decoder {
    /// Value type produced by the decoder.
    type Output;
    /// Error surfaced by the decode methods.
    type Error;

    /// Decode from a borrowed byte slice.
    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, Self::Error>;

    /// Owned-bytes entry. Default forwards to `decode`. Override
    /// when the codec can take ownership of the buffer directly
    /// without a memcpy (Arrow IPC wraps the `Vec64<u8>` as a
    /// `SharedBuffer` and reads typed views in place).
    fn decode_owned(&mut self, bytes: Vec64<u8>) -> Result<Self::Output, Self::Error> {
        self.decode(&bytes)
    }
}
