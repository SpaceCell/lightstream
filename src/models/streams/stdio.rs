//! # Asynchronous stdin byte stream
//!
//! Type alias over [`AsyncReadByteStream`] for standard input.
//!
//! ## Use cases
//! - Receive Arrow IPC streams from Unix pipes without loading them fully into memory.
//! - Build CLI tools that compose with other Arrow-aware programs.
//! - Feed piped data directly into async Arrow decoding pipelines.

use crate::enums::BufferChunkSize;
use crate::models::streams::async_read::AsyncReadByteStream;

/// A `Stream` that reads from stdin in fixed-size byte chunks.
pub type StdinByteStream = AsyncReadByteStream<tokio::io::Stdin>;

/// Create a stdin byte stream with the given chunk size.
pub fn from_stdin(size: BufferChunkSize) -> StdinByteStream {
    StdinByteStream::new(tokio::io::stdin(), size)
}

/// Create a stdin byte stream with the default chunk size for pipes.
///
/// Uses `BufferChunkSize::Http` (64 KiB) which works well for most
/// pipe-based streaming scenarios.
pub fn from_stdin_default() -> StdinByteStream {
    from_stdin(BufferChunkSize::Http)
}
