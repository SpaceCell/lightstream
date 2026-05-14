//! # Type-level serialise/deserialise
//!
//! User-facing round-trip trait. Implemented ON minarrow value types
//! (`Table`, `Array`, `FieldArray`, etc.). Parametrised over a codec
//! type: the codec is whatever
//! [`crate::traits::encoder::Encoder`] +
//! [`crate::traits::decoder::Decoder`] pair the implementer wants to
//! drive. The codec knows the wire format; `Serialise` itself does
//! not.
//!
//! `Serialise<C>` is a thin wrapper around the codec's encode and
//! decode: each impl constructs the codec and forwards. Method names
//! mirror the codec on purpose - the operation is the same, the
//! receiver is what differs.

use minarrow::Vec64;

/// In-memory round-trip driven by codec `C`. A single value type can
/// implement `Serialise` once per codec it supports.
///
/// `encode` produces a self-contained `Vec64<u8>`. `decode` parses a
/// borrowed slice back into `Self`. Both are required so the
/// implementation contract is even on both sides.
///
/// `decode_owned` is an optional zero-copy override: the default
/// forwards to `decode`, but codecs whose decoder can wrap an aligned
/// `Vec64<u8>` directly can override it to skip the memcpy.
pub trait Serialise<C>: Sized {
    /// Error surfaced by the round-trip methods.
    type Error;

    /// Encode `self` to a self-contained byte buffer using codec `C`.
    fn encode(&self) -> Result<Vec64<u8>, Self::Error>;

    /// Decode bytes back into `Self` using codec `C`.
    fn decode(bytes: &[u8]) -> Result<Self, Self::Error>;

    /// Owned-bytes entry. Default forwards to `decode`. Override
    /// when the codec can take ownership of the buffer directly
    /// without a memcpy.
    fn decode_owned(bytes: Vec64<u8>) -> Result<Self, Self::Error> {
        Self::decode(&bytes)
    }
}
