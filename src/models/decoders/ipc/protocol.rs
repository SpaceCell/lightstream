//! Arrow IPC Frame Decoder
//!
//! Decodes Arrow IPC (Inter-Process Communication) frames for both *file* and *stream* protocols,
//! as defined by the official [Apache Arrow Columnar IPC specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
//!
//! # Overview
//!
//! - **File protocol**: bounded, random-access, with header magic, aligned frames, and trailing magic/footer.
//! - **Stream protocol**: unbounded, emits 8-byte continuation markers before messages (or legacy 4-byte length without marker).
//!
//! The decoder works as a state machine, consuming input buffers and producing `ArrowIPCMessage` instances as complete frames are detected.

use std::io;
use std::marker::PhantomData;

use crate::constants::{
    ARROW_MAGIC_NUMBER, ARROW_MAGIC_NUMBER_PADDED, CONTINUATION_SENTINEL, FILE_CLOSING_MAGIC_LEN,
    FILE_OPENING_MAGIC_LEN, METADATA_SIZE_PREFIX,
};
use crate::enums::{DecodeResult, DecodeState, IPCMessageProtocol};
use crate::models::frames::ipc_message::ArrowIPCMessage;
use crate::traits::frame_decoder::FrameDecoder;
use crate::traits::stream_buffer::StreamBuffer;
use crate::utils::align_8;

/// Decoder for Arrow IPC (file/stream) format state machine.
pub struct ArrowIPCFrameDecoder<B: StreamBuffer> {
    format: IPCMessageProtocol,
    state: DecodeState<B>,
    /// Bytes of prefix that precede the flatbuffer message for the current frame:
    /// - STREAM: 4 (length) or 8 (marker + length)
    /// - FILE: always 4 (length)
    pending_prefix_len: usize,
    /// True until we have accounted for the initial 8-byte file magic
    /// by including it in the `consumed` count of the first FILE frame.
    file_magic_unconsumed: bool,
    _phantom: PhantomData<B>,
}

impl<B: StreamBuffer> FrameDecoder for ArrowIPCFrameDecoder<B> {
    type Frame = ArrowIPCMessage<B>;

    fn decode(&mut self, buf: &[u8]) -> io::Result<DecodeResult<Self::Frame>> {
        loop {
            // Auto-detect File magic at head in "Stream" mode (some writers do this).
            if matches!(self.state, DecodeState::Initial)
                && matches!(self.format, IPCMessageProtocol::Stream)
                && Self::has_opening_file_magic(buf)
            {
                self.format = IPCMessageProtocol::File;
                self.file_magic_unconsumed = true;
                self.state = DecodeState::ReadingMessageLength;
                // continue within same buffer
            }

            let state = std::mem::replace(&mut self.state, DecodeState::Initial);
            let step = match state {
                DecodeState::Initial => self.decode_initial(buf)?,
                DecodeState::ReadingContinuationSize { .. } => {
                    // We don’t use this state anymore; keep compatibility by re-routing.
                    self.state = DecodeState::ReadingMessageLength;
                    None
                }
                DecodeState::ReadingMessageLength => self.decode_message_length(buf)?,
                DecodeState::ReadingMessage { msg_len } => self.decode_message(buf, msg_len)?,
                DecodeState::ReadingBody { body_len, message } => {
                    self.decode_body(buf, body_len, message)?
                }
                DecodeState::ReadingFooter {
                    footer_len,
                    footer_offset,
                } => self.decode_footer(buf, footer_len, footer_offset)?,
                DecodeState::Done => Some(DecodeResult::NeedMore),
                DecodeState::AfterMagic => {
                    // The first FILE frame must still count the 8-byte magic in `consumed`.
                    // We do not consume/emit here; but proceed to read the first length.
                    self.state = DecodeState::ReadingMessageLength;
                    None
                }
                DecodeState::AfterContMarker => {
                    // Not used; STREAM markers are handled via `pending_prefix_len`.
                    self.state = DecodeState::ReadingMessageLength;
                    None
                }
            };

            if let Some(done) = step {
                return Ok(done);
            }
            // otherwise loop and continue progressing within the same input slice
        }
    }
}

impl<B: StreamBuffer> ArrowIPCFrameDecoder<B> {
    pub fn new(format: IPCMessageProtocol) -> Self {
        Self {
            format,
            state: DecodeState::Initial,
            pending_prefix_len: 0,
            file_magic_unconsumed: matches!(format, IPCMessageProtocol::File),
            _phantom: PhantomData,
        }
    }

    /// Construct a decoder that skips file magic validation/consumption.
    pub fn new_without_header_check(format: IPCMessageProtocol) -> Self {
        Self {
            format,
            state: if format == IPCMessageProtocol::File {
                DecodeState::ReadingMessageLength
            } else {
                DecodeState::Initial
            },
            pending_prefix_len: 0,
            file_magic_unconsumed: false,
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn read_u32_le(buf: &[u8]) -> u32 {
        u32::from_le_bytes(buf[..4].try_into().unwrap())
    }

    #[inline]
    fn has_opening_file_magic(buf: &[u8]) -> bool {
        buf.len() >= FILE_OPENING_MAGIC_LEN
            && &buf[..FILE_OPENING_MAGIC_LEN] == ARROW_MAGIC_NUMBER_PADDED
    }

    #[inline]
    fn has_continuation_sentinel(buf: &[u8]) -> bool {
        buf.len() >= METADATA_SIZE_PREFIX && Self::read_u32_le(buf) == CONTINUATION_SENTINEL
    }

    #[inline]
    fn has_eos_marker(buf: &[u8]) -> bool {
        buf.len() >= 8
            && Self::read_u32_le(&buf[0..4]) == 0xFFFF_FFFF
            && Self::read_u32_le(&buf[4..8]) == 0x0000_0000
    }

    #[inline]
    fn has_file_footer_markers(buf: &[u8], len_off: usize, msg_len: usize) -> bool {
        // After the (u32) length and `msg_len` bytes, the trailing magic must fit.
        msg_len > 0
            && len_off + METADATA_SIZE_PREFIX + msg_len + FILE_OPENING_MAGIC_LEN <= buf.len()
    }

    /// For the *current* frame, return 8 only if FILE and header has not been counted yet.
    #[inline]
    fn current_base_offset(&self) -> usize {
        if self.format == IPCMessageProtocol::File && self.file_magic_unconsumed {
            FILE_OPENING_MAGIC_LEN
        } else {
            0
        }
    }

    /// Handles initial protocol-specific header detection (magic or marker).
    #[inline]
    fn decode_initial(
        &mut self,
        buf: &[u8],
    ) -> io::Result<Option<DecodeResult<ArrowIPCMessage<B>>>> {
        match self.format {
            IPCMessageProtocol::File => {
                // Validate header; do not mark it consumed yet. We include +8 in the first frame’s `consumed`.
                if buf.len() < FILE_OPENING_MAGIC_LEN {
                    return Ok(Some(DecodeResult::NeedMore));
                }
                if !Self::has_opening_file_magic(buf) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid Arrow file magic header",
                    ));
                }
                self.state = DecodeState::AfterMagic;
                // Continue within same buffer
            }
            IPCMessageProtocol::Stream => {
                // EOS at start?
                if Self::has_eos_marker(buf) {
                    return Ok(Some(DecodeResult::Frame {
                        frame: ArrowIPCMessage {
                            message: B::default(),
                            body: B::default(),
                        },
                        consumed: 8,
                    }));
                }

                // Continuation marker present?
                if Self::has_continuation_sentinel(buf) {
                    self.pending_prefix_len = 8; // marker (4) + length (4)
                } else {
                    self.pending_prefix_len = 4; // no marker - 4-byte length
                }
                self.state = DecodeState::ReadingMessageLength;
            }
        }
        Ok(None)
    }

    /// Handles parsing the length prefix for the next Arrow IPC message.
    fn decode_message_length(
        &mut self,
        buf: &[u8],
    ) -> io::Result<Option<DecodeResult<ArrowIPCMessage<B>>>> {
        let base_off = self.current_base_offset();

        // STREAM: handle EOS/marker at (base_off == 0)
        if self.format == IPCMessageProtocol::Stream {
            if buf.len() >= base_off + 8 {
                if Self::has_eos_marker(&buf[base_off..]) {
                    return Ok(Some(DecodeResult::Frame {
                        frame: ArrowIPCMessage {
                            message: B::default(),
                            body: B::default(),
                        },
                        consumed: base_off + 8, // normally 8
                    }));
                }
            }

            let has_marker =
                buf.len() >= base_off + 4 && Self::has_continuation_sentinel(&buf[base_off..]);

            let len_off = base_off + if has_marker { 4 } else { 0 };
            self.pending_prefix_len = if has_marker { 8 } else { 4 };

            if buf.len() < len_off + METADATA_SIZE_PREFIX {
                return Ok(Some(DecodeResult::NeedMore));
            }

            let msg_len = Self::read_u32_le(&buf[len_off..len_off + METADATA_SIZE_PREFIX]) as usize;

            // In stream mode, msg_len == 0 means EOS with/without marker
            if msg_len == 0 {
                return Ok(Some(DecodeResult::Frame {
                    frame: ArrowIPCMessage {
                        message: B::default(),
                        body: B::default(),
                    },
                    consumed: base_off + self.pending_prefix_len, // 4 or 8
                }));
            }

            self.state = DecodeState::ReadingMessage { msg_len };
            return Ok(None);
        }

        // FILE: read u32 length after (possibly) the 8-byte header on first frame
        let len_off = base_off;
        self.pending_prefix_len = METADATA_SIZE_PREFIX; // always 4 for FILE

        if buf.len() < len_off + METADATA_SIZE_PREFIX {
            return Ok(Some(DecodeResult::NeedMore));
        }

        let msg_len = Self::read_u32_le(&buf[len_off..len_off + METADATA_SIZE_PREFIX]) as usize;

        // Footer detection: after len+msg_len there must be trailing magic
        if Self::has_file_footer_markers(buf, len_off, msg_len) {
            let possible_magic = &buf[len_off + METADATA_SIZE_PREFIX + msg_len
                ..len_off + METADATA_SIZE_PREFIX + msg_len + FILE_OPENING_MAGIC_LEN];
            if possible_magic == ARROW_MAGIC_NUMBER {
                self.state = DecodeState::ReadingFooter {
                    footer_len: msg_len,
                    footer_offset: len_off + METADATA_SIZE_PREFIX,
                };
                return Ok(None);
            }
        }

        if msg_len == 0 {
            // In FILE mode zero-length message is invalid - not an EOS sentinel.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Zero-length message",
            ));
        }

        self.state = DecodeState::ReadingMessage { msg_len };
        Ok(None)
    }

    /// Decode the FB message and (if present) the body for the current frame.
    fn decode_message(
        &mut self,
        buf: &[u8],
        msg_len: usize,
    ) -> io::Result<Option<DecodeResult<ArrowIPCMessage<B>>>> {
        let base_off = self.current_base_offset();
        let prefix = self.pending_prefix_len; // 4/8 stream, 4 file

        let meta_start = base_off + prefix;
        let meta_end = meta_start + msg_len;

        if buf.len() < meta_end {
            return Ok(Some(DecodeResult::NeedMore));
        }

        let message = B::from_slice(&buf[meta_start..meta_end]);

        // Parse FB message for body length
        use crate::AFMessage;
        use flatbuffers::root;
        let root = root::<AFMessage>(&message.as_ref()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse message: {e}"),
            )
        })?;
        let body_len = root.bodyLength() as usize;

        let meta_pad = align_8(msg_len);
        let body_start = meta_end + meta_pad;

        if body_len > 0 {
            let body_end = body_start + body_len;
            if buf.len() < body_end {
                // Need more to finish the body later; keep the message around.
                self.state = DecodeState::ReadingBody { body_len, message };
                return Ok(Some(DecodeResult::NeedMore));
            }

            let body = B::from_slice(&buf[body_start..body_end]);
            // Account for body padding to maintain 8-byte alignment
            let consumed_before_body_pad = base_off + prefix + msg_len + meta_pad + body_len;
            let body_pad = align_8(consumed_before_body_pad);
            let consumed = consumed_before_body_pad + body_pad;

            // Prepare for next frame
            self.state = DecodeState::ReadingMessageLength;
            self.pending_prefix_len = 0;

            // First FILE frame accounts for 8-byte magic here
            if self.file_magic_unconsumed && self.format == IPCMessageProtocol::File {
                self.file_magic_unconsumed = false;
            }

            return Ok(Some(DecodeResult::Frame {
                frame: ArrowIPCMessage { message, body },
                consumed,
            }));
        } else {
            // No body
            let consumed = base_off + prefix + msg_len + meta_pad;

            self.state = DecodeState::ReadingMessageLength;
            self.pending_prefix_len = 0;

            if self.file_magic_unconsumed && self.format == IPCMessageProtocol::File {
                self.file_magic_unconsumed = false;
            }

            let frame = ArrowIPCMessage {
                message,
                body: B::default(),
            };
            return Ok(Some(DecodeResult::Frame { frame, consumed }));
        }
    }

    /// Continue reading the body if `decode_message` determined it wasn't fully available yet.
    fn decode_body(
        &mut self,
        buf: &[u8],
        body_len: usize,
        message: B,
    ) -> io::Result<Option<DecodeResult<ArrowIPCMessage<B>>>> {
        let base_off = self.current_base_offset();
        let prefix = self.pending_prefix_len; // 4/8 stream, 4 file

        let meta_pad = align_8(message.len());
        let needed = base_off + prefix + message.len() + meta_pad + body_len;

        if buf.len() < needed {
            return Ok(Some(DecodeResult::NeedMore));
        }

        let bstart = base_off + prefix + message.len() + meta_pad;
        let bend = bstart + body_len;
        let body = B::from_slice(&buf[bstart..bend]);

        self.state = DecodeState::ReadingMessageLength;
        self.pending_prefix_len = 0;

        if self.file_magic_unconsumed && self.format == IPCMessageProtocol::File {
            self.file_magic_unconsumed = false;
        }

        // Account for body padding
        let body_pad = align_8(needed);
        let consumed = needed + body_pad;

        Ok(Some(DecodeResult::Frame {
            frame: ArrowIPCMessage::<B> { message, body },
            consumed,
        }))
    }

    /// Handles decoding of file footer and trailing magic in Arrow File protocol.
    #[inline]
    fn decode_footer(
        &mut self,
        buf: &[u8],
        footer_len: usize,
        footer_offset: usize,
    ) -> io::Result<Option<DecodeResult<ArrowIPCMessage<B>>>> {
        // Wait for footer bytes
        if buf.len() < footer_offset + footer_len {
            return Ok(Some(DecodeResult::NeedMore));
        }
        // Need the size (u32 LE) and trailing magic
        if buf.len() < footer_offset + footer_len + 4 + FILE_CLOSING_MAGIC_LEN {
            return Ok(Some(DecodeResult::NeedMore));
        }

        let size_offset = footer_offset + footer_len;
        let footer_size =
            u32::from_le_bytes(buf[size_offset..size_offset + 4].try_into().unwrap()) as usize;

        if footer_size != footer_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Arrow file footer size mismatch: expected {footer_len}, found {footer_size}"
                ),
            ));
        }

        let magic = &buf[size_offset + 4..size_offset + 4 + FILE_CLOSING_MAGIC_LEN];
        if magic != ARROW_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid Arrow file trailing magic",
            ));
        }

        self.state = DecodeState::Done;
        Ok(None)
    }
}

impl<B: StreamBuffer> Default for ArrowIPCFrameDecoder<B> {
    fn default() -> Self {
        ArrowIPCFrameDecoder::new(IPCMessageProtocol::Stream)
    }
}
