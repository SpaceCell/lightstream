// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Codecs for Arrow IPC and the Lightstream protocol.
//!
//! - [`ArrowIpcCodec`](crate::models::codecs::ipc::ArrowIpcCodec) - Arrow IPC streaming codec with zero-copy encode/decode
//! - [`LightstreamCodec`](crate::models::codecs::lightstream::LightstreamCodec) - Lightstream protocol codec with type registry and TLV framing

/// Arrow IPC streaming codec with zero-copy encode and decode.
pub mod ipc;

/// Lightstream protocol codec with type registry and TLV multiplexing.
#[cfg(feature = "protocol")]
pub mod lightstream;

pub use ipc::ArrowIpcCodec;

#[cfg(feature = "protocol")]
pub use lightstream::LightstreamCodec;
