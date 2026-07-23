// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Protocol modules
//!
//! ## Arrow IPC
//!
//! The [`ipc`](crate::models::protocol::ipc) module re-exports the Arrow IPC codec, readers, writers, and
//! sinks. Always available - no feature gate required.
//!
//! ## Lightstream
//!
//! The [`lightstream`](crate::models::protocol::lightstream) module provides multiplexed typed messages and Arrow
//! tables over a single async stream using TLV framing on top of Arrow IPC.
//! Requires the `protocol` feature.
//!
//! Both sides register named types in the same order, assigning sequential
//! `u8` tags. The outer framing is TLV - `[tag][length][payload]` - but
//! table payloads use the real Arrow IPC streaming protocol internally, not
//! per-table TLV overhead. The first table for a given type sends the full
//! IPC stream header with schema and dictionaries; subsequent tables send
//! only the record batch, as per a native Arrow IPC stream.
//!
//! ### Wire format
//!
//! ```text
//! [type_tag: u8][payload_len: u32 LE][payload: N bytes]
//! ```
//!
//! - **Message payloads** are opaque bytes. The protocol does not prescribe
//!   a serialisation format.
//! - **Table payloads** contain Arrow IPC frames. Schema and dictionary
//!   state is persistent per type, so only the first table carries the full
//!   IPC header.
//!
//! ### Protobuf support
//!
//! Enable the `protobuf` feature to get typed send/receive methods backed
//! by [prost](https://docs.rs/prost). Define your message structs with
//! `#[derive(prost::Message)]` as usual, then:
//!
//! ```rust,ignore
//! // Send a typed protobuf message
//! conn.send_proto("Trade", &trade_event).await?;
//!
//! // Receive and decode
//! let msg = conn.recv().await.unwrap()?;
//! let trade: TradeEvent = msg.decode_payload()?;
//! ```
//!
//! Without the feature, messages are sent as raw `&[u8]` via [`send`] and
//! you handle serialisation yourself.
//!
//! [`send`]: crate::models::writers::lightstream::LightstreamWriter::send

/// Arrow IPC protocol re-exports.
pub mod ipc;

/// Lightstream protocol connection with transport-specific constructors.
#[cfg(feature = "protocol")]
pub mod lightstream;

/// Bidirectional connection re-export.
#[cfg(feature = "protocol")]
pub mod connection {
    pub use super::lightstream::connection::*;
}

#[cfg(feature = "protocol")]
pub use crate::models::codecs::lightstream::LightstreamCodec;
#[cfg(feature = "protocol")]
pub use crate::models::frames::lightstream_message::{
    FRAME_HEADER_SIZE, FrameType, LightstreamMessage,
};
#[cfg(feature = "protocol")]
pub use crate::models::readers::lightstream::LightstreamReader;
#[cfg(feature = "protocol")]
pub use crate::models::writers::lightstream::LightstreamWriter;
