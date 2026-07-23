// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! io_uring-based transports for Lightstream via tokio-uring.
//!
//! Uses tokio-uring's completion-based I/O directly on the async task for
//! no ring thread, channels, or cross-thread overhead. The io_uring
//! driver is integrated into the tokio event loop.
//!
//! Generic over any [`UringStream`](crate::models::io_uring::UringStream) implementor (UDS, TCP, etc.).
//! Monomorphised at compile time for no overhead dispatch.
//!
//! Requires the `io_uring` feature and Linux. Connections must be
//! used from within a `tokio_uring::start()` runtime.
//!
//! ## Stability: unstable
//!
//! `tokio-uring` is at 0.x and its API still evolves across minor
//! versions; the `io_uring` syscall surface itself is Linux-only and its
//! best-practice patterns continue to change as the kernel adds opcodes.
//! The transport here is sound and benchmarked, but pin aggressively and
//! expect minor-version churn until `tokio-uring` hits 1.0.

mod buf;
mod connection;
mod stream;
#[cfg(feature = "websocket")]
mod websocket;

pub use connection::{IoUringConnection, IoUringTcpConnection, IoUringUdsConnection};
pub use stream::UringStream;
#[cfg(feature = "websocket")]
pub use websocket::IoUringWsConnection;
