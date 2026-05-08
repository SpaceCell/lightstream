//! io_uring-based transports for Lightstream via tokio-uring.
//!
//! Uses tokio-uring's completion-based I/O directly on the async task for
//! no ring thread, channels, or cross-thread overhead. The io_uring
//! driver is integrated into the tokio event loop.
//!
//! Generic over any [`UringStream`] implementor (UDS, TCP, etc.).
//! Monomorphised at compile time for no overhead dispatch.
//!
//! Requires the `io_uring` feature and Linux. Connections must be
//! used from within a `tokio_uring::start()` runtime.

mod buf;
mod connection;
mod stream;
#[cfg(feature = "websocket")]
mod websocket;

pub use connection::{IoUringConnection, IoUringTcpConnection, IoUringUdsConnection};
pub use stream::UringStream;
#[cfg(feature = "websocket")]
pub use websocket::IoUringWsConnection;
