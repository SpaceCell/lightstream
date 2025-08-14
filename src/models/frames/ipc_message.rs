//! # IPC Frame Structures
//!
//! Core data structures for Arrow IPC frame encoding.
//!
//! - [`ArrowIPCMessage`] wraps a FlatBuffers metadata message and its associated body buffer.
//! - [`IPCFrameMetadata`] tracks byte lengths and padding for all frame sections, used to compute
//!   total frame size and to ensure compliance with Arrow IPC alignment rules.
//!
//! These are internal, low-level components used by the IPC encoders and writers.

use crate::traits::stream_buffer::StreamBuffer;

/// Arrow IPC message component of a frame.
///
/// Wraps both the FlatBuffers-encoded Arrow message and its
/// corresponding binary body payload.
#[derive(Debug)]
pub struct ArrowIPCMessage<B: StreamBuffer> {
    /// FlatBuffers-encoded Arrow message.
    pub message: B,
    /// Columnar data buffer payload.
    pub body: B,
}

/// Per-frame accounting metadata for Arrow IPC encoding.
///
/// Tracks lengths of all logical sections of an encoded frame
/// (header, metadata, body, footer, etc.) including any padding.
#[derive(Default)]
pub struct IPCFrameMetadata {
    /// Header size in bytes - continuation + metadata size prefix
    pub header_len: usize,
    /// Raw metadata length in bytes (excluding padding)
    pub meta_len: usize,
    /// Padding applied after metadata for alignment.
    pub meta_pad: usize,
    /// Raw body length in bytes (excluding padding)
    pub body_len: usize,
    /// Padding applied after body for alignment
    pub body_pad: usize,
    /// End-of-stream marker length in bytes, if present
    pub eos_len: usize,
    /// File footer length in bytes, if present
    pub footer_len: usize,
    /// Length of Arrow magic bytes - opening or closing.
    pub magic_len: usize,
}

impl IPCFrameMetadata {
    /// Return total encoded frame length.
    pub fn frame_len(&self) -> usize {
        self.header_len
            + self.metadata_total_len()
            + self.body_total_len()
            + self.footer_eos_len()
            + self.magic_len
    }

    /// Return total metadata section length including padding.
    pub fn metadata_total_len(&self) -> usize {
        self.meta_len + self.meta_pad
    }

    /// Return total body length including padding.
    pub fn body_total_len(&self) -> usize {
        self.body_len + self.body_pad
    }

    /// Return combined length of EOS marker and footer.
    pub fn footer_eos_len(&self) -> usize {
        self.eos_len + self.footer_len
    }
}
