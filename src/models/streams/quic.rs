//! # Asynchronous QUIC byte stream
//!
//! Type alias over [`AsyncReadByteStream`] for QUIC receive streams.
//!
//! ## Use cases
//! - Receive Arrow IPC streams over QUIC without loading them fully into memory.
//! - Feed QUIC I/O directly into async Arrow decoding pipelines.

use crate::models::streams::async_read::AsyncReadByteStream;

/// A `Stream` that reads a QUIC receive stream in fixed-size byte chunks.
pub type QuicByteStream = AsyncReadByteStream<quinn::RecvStream>;
