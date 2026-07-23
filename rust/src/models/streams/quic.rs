// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Asynchronous QUIC byte stream
//!
//! Type alias over [`AsyncReadByteStream`](crate::models::streams::async_read::AsyncReadByteStream) for QUIC receive streams.
//!
//! ## Use cases
//! - Receive Arrow IPC streams over QUIC without loading them fully into memory.
//! - Feed QUIC I/O directly into async Arrow decoding pipelines.

use crate::models::streams::async_read::AsyncReadByteStream;

/// A `Stream` that reads a QUIC receive stream in fixed-size byte chunks.
pub type QuicByteStream = AsyncReadByteStream<quinn::RecvStream>;
