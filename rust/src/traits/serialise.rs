// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Codec-based serialisation and deserialisation.
//!
//! [`Serialise`](crate::traits::serialise::Serialise) is implemented for Minarrow value types such as `Table`,
//! `Array` and `FieldArray`. The codec type `C` defines the encoded format and
//! provides the corresponding encoder and decoder implementations.
//!
//! Implementations construct the codec and delegate to its encode and decode
//! operations.

use minarrow::Vec64;

/// Encodes and decodes a value using codec `C`.
pub trait Serialise<C>: Sized {
    /// Error returned by encoding or decoding.
    type Error;

    /// Encodes this value into a self-contained byte buffer.
    fn encode(&self) -> Result<Vec64<u8>, Self::Error>;

    /// Decodes a value from a borrowed byte slice.
    fn decode(bytes: &[u8]) -> Result<Self, Self::Error>;

    /// Decodes a value from an owned byte buffer.
    ///
    /// The default implementation delegates to [`Self::decode`]. Codecs may
    /// override this method when they can consume the buffer without copying.
    fn decode_owned(bytes: Vec64<u8>) -> Result<Self, Self::Error> {
        Self::decode(&bytes)
    }
}
