//! # Asynchronous WebTransport byte stream
//!
//! Type alias over [`AsyncReadByteStream`] for WebTransport receive streams.
//!
//! ## Use cases
//! - Receive Arrow IPC streams over WebTransport without loading them fully into memory.
//! - Feed WebTransport I/O directly into async Arrow decoding pipelines.
//! - Enable browser-to-server Arrow streaming via the WebTransport protocol.
//!
//! ## Stability: unstable
//!
//! WebTransport-over-HTTP/3 is not yet an IETF RFC and `wtransport` is at
//! 0.x. See [`WebTransportTableReader`](crate::models::readers::webtransport::WebTransportTableReader)
//! for the full caveat list.

use crate::models::streams::async_read::AsyncReadByteStream;

/// A `Stream` that reads a WebTransport receive stream in fixed-size byte chunks.
pub type WebTransportByteStream = AsyncReadByteStream<wtransport::RecvStream>;
