use crate::traits::stream_buffer::StreamBuffer;

/// The outcome of a single frame decoder step.
///
/// Communicates whether a full frame has been detected, whether more bytes are
/// required, or whether a protocol error has occurred.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DecodeResult<F> {
    /// A complete logical frame was detected.
    ///
    /// - `frame`: The decoded frame of the target type.
    /// - `consumed`: The number of bytes from the buffer that should be discarded.
    Frame { frame: F, consumed: usize },

    /// The decoder requires more bytes to detect a full frame.
    ///
    /// No bytes should be removed from the buffer.
    NeedMore,
}

/// Arrow IPC decoding state machine.
///
/// Encodes all protocol progress and marker consumption for both file and
/// streaming modes.
#[derive(Debug, Clone)]
pub enum DecodeState<B: StreamBuffer> {
    /// Initial state before reading any bytes.
    Initial,

    /// **File Mode Only**: After reading the file magic header.
    AfterMagic,

    /// After reading a stream continuation marker.
    AfterContMarker,

    /// Currently reading continuation size.
    ReadingContinuationSize,

    /// Ready to read message length prefix.
    ReadingMessageLength,

    /// Currently reading a message body of the given length.
    ReadingMessage {
        /// Length of the message payload in bytes.
        msg_len: usize,
    },

    /// Currently reading a record body.
    ReadingBody {
        /// Length of the body in bytes.
        body_len: usize,
        /// Buffer holding the message being assembled.
        message: B,
    },

    /// Currently reading a footer section.
    ReadingFooter {
        /// Length of the footer in bytes.
        footer_len: usize,
        /// Offset into the footer (for partial reads).
        footer_offset: usize,
    },

    /// Decoding complete.
    Done,
}

/// Specifies chunk sizing strategies for `DiskByteStream` and other stream sources.
///
/// Provides domain-appropriate defaults but allows override.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BufferChunkSize {
    /// File I/O: Default is 1 MiB chunks.
    FileIO,

    /// HTTP transfers: Default is 64 KiB.
    Http,

    /// WebSocket frames: Default is 32 KiB.
    WebSocket,

    /// QUIC/WebTransport streams: Default is 64 KiB.
    WebTransport,

    /// In-memory streams: Default is 512 KiB.
    InMemory,

    /// Custom chunk size.
    Custom(usize),
}

impl BufferChunkSize {
    /// Returns the configured chunk size in bytes.
    pub fn chunk_size(self) -> usize {
        match self {
            BufferChunkSize::FileIO => 1 * 1024 * 1024, // 1 MiB
            BufferChunkSize::Http => 64 * 1024,         // 64 KiB
            BufferChunkSize::WebSocket => 32 * 1024,    // 32 KiB
            BufferChunkSize::WebTransport => 64 * 1024, // 64 KiB
            BufferChunkSize::InMemory => 512 * 1024,    // 512 KiB
            BufferChunkSize::Custom(n) => n,
        }
    }
}

/// Arrow framing protocol.
///
/// There are two variants: one for bounded files and one for unbounded streams.
/// Each defines its own termination markers, in line with the official
/// [Apache Arrow IPC specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IPCMessageProtocol {
    /// **Unbounded**: Arrow IPC stream protocol.
    Stream,

    /// **Bounded**: Arrow IPC file protocol.
    File,
}

/// Arrow message types.
///
/// Maps directly to message headers defined by the Arrow IPC specification.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    /// Schema definition message.
    Schema,

    /// Record batch payload.
    RecordBatch,

    /// Dictionary batch payload.
    DictionaryBatch,

    /// Unrecognised or unsupported message type.
    Unknown,
}

/// State machine for stream message batching.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BatchState {
    /// Schema has not yet been received.
    NeedSchema,

    /// Ready to emit batches.
    Ready,

    /// End of stream reached.
    Done,
}

/// State machine for stream writers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriterState {
    /// Fresh writer, no schema written yet.
    Fresh,

    /// Schema has been written, can emit batches.
    SchemaDone,

    /// Writer closed, no further messages may be written.
    Closed,
}
