//! # Asynchronous WebTransport byte stream
//!
//! Type alias over [`AsyncReadByteStream`] for WebTransport receive streams.
//!
//! ## Use cases
//! - Receive Arrow IPC streams over WebTransport without loading them fully into memory.
//! - Feed WebTransport I/O directly into async Arrow decoding pipelines.
//! - Enable browser-to-server Arrow streaming via the WebTransport protocol.

use crate::models::streams::async_read::AsyncReadByteStream;

/// A `Stream` that reads a WebTransport receive stream in fixed-size byte chunks.
pub type WebTransportByteStream = AsyncReadByteStream<wtransport::RecvStream>;
