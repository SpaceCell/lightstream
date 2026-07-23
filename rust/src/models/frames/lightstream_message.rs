// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Frame definitions for the Lightstream protocol.
//!
//! Each frame on the wire carries a 5-byte TLV header followed by a payload:
//!
//! ```text
//! [type_tag: u8][payload_len: u32 LE][payload: N bytes]
//! ```
//!
//! After decoding, frames are represented as [`LightstreamMessage`](crate::models::frames::lightstream_message::LightstreamMessage) variants
//! - either an opaque message or a decoded Arrow table.
//!
//! With the `protobuf` feature enabled, message variants gain typed decode
//! methods via prost: [`decode_payload`] and [`into_decoded_payload`].
//! With the `msgpack` feature enabled, [`decode_msgpack`] and
//! [`into_decoded_msgpack`] decode MessagePack payloads via serde.
//!
//! [`decode_payload`]: crate::models::frames::lightstream_message::LightstreamMessage::decode_payload
//! [`into_decoded_payload`]: crate::models::frames::lightstream_message::LightstreamMessage::into_decoded_payload
//! [`decode_msgpack`]: crate::models::frames::lightstream_message::LightstreamMessage::decode_msgpack
//! [`into_decoded_msgpack`]: crate::models::frames::lightstream_message::LightstreamMessage::into_decoded_msgpack

/// Size of a Lightstream frame header: 1 byte type tag + 4 bytes LE length.
pub const FRAME_HEADER_SIZE: usize = 5;

/// The category of a registered Lightstream type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    /// Opaque binary message payload.
    Message,
    /// Arrow IPC-encoded table.
    Table,
}

/// A decoded Lightstream message, produced after resolving a frame's type tag
/// and decoding its payload.
#[derive(Debug)]
pub enum LightstreamMessage {
    /// An opaque message payload.
    Message {
        /// The registered type's tag.
        tag: u8,
        /// The raw payload bytes.
        payload: Vec<u8>,
    },
    /// An Arrow table, carried as a view. The writer sends any row
    /// window this way and the reader wraps each decoded table as the
    /// full-width view of itself.
    Table {
        /// The registered type's tag.
        tag: u8,
        /// The table view.
        table: minarrow::TableV,
    },
}

impl LightstreamMessage {
    /// Get the type tag regardless of variant.
    pub fn tag(&self) -> u8 {
        match self {
            Self::Message { tag, .. } | Self::Table { tag, .. } => *tag,
        }
    }

    /// Get the message payload if this is a `Message` variant.
    pub fn payload(&self) -> Option<&[u8]> {
        match self {
            Self::Message { payload, .. } => Some(payload),
            _ => None,
        }
    }

    /// Consume this value and return the payload if it is a `Message` variant.
    pub fn into_payload(self) -> Option<Vec<u8>> {
        match self {
            Self::Message { payload, .. } => Some(payload),
            _ => None,
        }
    }

    /// Get the table view if this is a `Table` variant.
    pub fn table(&self) -> Option<&minarrow::TableV> {
        match self {
            Self::Table { table, .. } => Some(table),
            _ => None,
        }
    }

    /// Consume this value and return the table if it is a `Table` variant.
    /// A full-width view returns its table through a reference-count
    /// bump, so reader-decoded messages materialise without copying.
    pub fn into_table(self) -> Option<minarrow::Table> {
        match self {
            Self::Table { table, .. } => Some(table.to_table()),
            _ => None,
        }
    }

    /// Returns `true` if this is a `Message` variant.
    pub fn is_message(&self) -> bool {
        matches!(self, Self::Message { .. })
    }

    /// Returns `true` if this is a `Table` variant.
    pub fn is_table(&self) -> bool {
        matches!(self, Self::Table { .. })
    }

    /// Decode the message payload as a protobuf type.
    ///
    /// Returns `Err` if this is not a `Message` variant or if decoding fails.
    #[cfg(feature = "protobuf")]
    pub fn decode_payload<M: prost::Message + Default>(&self) -> std::io::Result<M> {
        let bytes = self.payload().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "not a message variant")
        })?;
        M::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Consume this value and decode the message payload as a protobuf type.
    ///
    /// Returns `Err` if this is not a `Message` variant or if decoding fails.
    #[cfg(feature = "protobuf")]
    pub fn into_decoded_payload<M: prost::Message + Default>(self) -> std::io::Result<M> {
        let bytes = self.into_payload().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "not a message variant")
        })?;
        M::decode(bytes.as_slice())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Decode the message payload as a MessagePack type via serde.
    ///
    /// Returns `Err` if this is not a `Message` variant or if decoding fails.
    #[cfg(feature = "msgpack")]
    pub fn decode_msgpack<M: serde::de::DeserializeOwned>(&self) -> std::io::Result<M> {
        let bytes = self.payload().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "not a message variant")
        })?;
        rmp_serde::from_slice(bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Consume this value and decode the message payload as a MessagePack type
    /// via serde.
    ///
    /// Returns `Err` if this is not a `Message` variant or if decoding fails.
    #[cfg(feature = "msgpack")]
    pub fn into_decoded_msgpack<M: serde::de::DeserializeOwned>(self) -> std::io::Result<M> {
        let bytes = self.into_payload().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "not a message variant")
        })?;
        rmp_serde::from_slice(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}
