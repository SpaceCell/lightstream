// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # IPC Frame Structures
//!
//! Core data structures for Arrow IPC frame encoding.
//!
//! - [`ArrowIPCMessage`](crate::models::frames::ipc_message::ArrowIPCMessage) wraps a FlatBuffers metadata message and its associated body buffer.
//! - [`IPCFrameMetadata`](crate::models::frames::ipc_message::IPCFrameMetadata) tracks byte lengths and padding for all frame sections, used to compute
//!   total frame size and to ensure compliance with Arrow IPC alignment rules.
//!
//! These are internal, low-level components used by the IPC encoders and writers.

use std::ops::Range;

use crate::enums::IPCMessageProtocol;
use crate::traits::stream_buffer::StreamBuffer;

/// IPC frame inputs for encoding.
///
/// Carries the flatbuffer message, body, and protocol flags needed
/// to encode a single Arrow IPC frame.
pub struct IPCFrame<'a> {
    /// Flatbuffer message bytes.
    pub meta: &'a [u8],
    /// Message body payload.
    pub body: &'a [u8],
    /// IPC protocol - file or stream.
    pub protocol: IPCMessageProtocol,
    /// Whether this is the first frame (emits opening magic in file mode).
    pub is_first: bool,
    /// Whether this is the last frame (emits EOS, and footer+closing magic in file mode).
    pub is_last: bool,
    /// File footer bytes (required when `protocol == File` and `is_last == true`).
    pub footer_bytes: Option<&'a [u8]>,
}

/// Result of parsing an IPC frame header without requiring the body.
/// Used by the zero-copy decoder to learn the body length before reading
/// the body directly into a dedicated buffer.
pub enum IPCFrameHeader {
    /// Need more data in the buffer to parse the frame header/metadata.
    NeedMore,
    /// End-of-stream or empty frame detected.
    EndOfStream { consumed: usize },
    /// A complete frame whose body is already in the buffer.
    /// Used for schema, dictionary, and small frames.
    Complete {
        frame: ArrowIPCFrameRanges,
        consumed: usize,
    },
    /// A frame whose metadata has been parsed but the body is not yet
    /// in the buffer. The caller should read `body_len` bytes directly
    /// from the transport into a dedicated buffer.
    BodyPending {
        /// Byte range of the flatbuffer metadata within the decode buffer.
        message_range: std::ops::Range<usize>,
        /// Number of bytes to consume from the buffer before reading the body.
        /// This includes the frame prefix + metadata + metadata padding.
        header_consumed: usize,
        /// Number of body bytes to read from the transport.
        body_len: usize,
        /// Padding bytes after the body that must be skipped.
        body_pad: usize,
    },
}

/// Result of decoding a single IPC frame.
pub enum IPCFrameResult {
    /// Schema learned.
    Schema,
    /// Dictionary accumulated.
    Dictionary,
    /// Record batch decoded into a Table with zero-copy column views.
    Batch(minarrow::Table),
    /// End-of-stream marker.
    EndOfStream,
}

/// Non-owning descriptor for an Arrow IPC frame within a contiguous buffer.
///
/// Instead of copying message and body bytes into owned buffers,
/// this records their byte ranges within the original decode buffer.
/// Used by the direct reader path to avoid intermediate allocations.
#[derive(Debug, Clone)]
pub struct ArrowIPCFrameRanges {
    /// Byte range of the flatbuffer message within the decode buffer.
    pub message_range: Range<usize>,
    /// Byte range of the body within the decode buffer.
    /// Empty range when the frame has no body.
    pub body_range: Range<usize>,
}

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
