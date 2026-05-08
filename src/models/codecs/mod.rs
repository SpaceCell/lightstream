//! Codecs for Arrow IPC and the Lightstream protocol.
//!
//! - [`ArrowIpcCodec`] - Arrow IPC streaming codec with zero-copy encode/decode
//! - [`LightstreamCodec`] - Lightstream protocol codec with type registry and TLV framing

/// Arrow IPC streaming codec with zero-copy encode and decode.
pub mod ipc;

/// Lightstream protocol codec with type registry and TLV multiplexing.
#[cfg(feature = "protocol")]
pub mod lightstream;

pub use ipc::ArrowIpcCodec;

#[cfg(feature = "protocol")]
pub use lightstream::LightstreamCodec;
