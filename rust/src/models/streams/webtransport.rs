// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Asynchronous WebTransport byte stream
//!
//! Type alias over [`AsyncReadByteStream`](crate::models::streams::async_read::AsyncReadByteStream) for WebTransport receive streams.
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
