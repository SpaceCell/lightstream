// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! UringStream trait for io_uring-based transports.
//!
//! Abstracts over tokio-uring's owned-buffer I/O pattern so the
//! connection logic is written once and works with any fd-backed
//! transport: UDS, TCP, or future additions.
//!
//! Implementations are monomorphised at compile time - no vtable or
//! runtime overhead.
//!
//! tokio-uring is single-threaded so futures are not Send. The trait
//! mirrors this constraint.

use std::io;

use tokio_uring::buf::{BoundedBuf, BoundedBufMut};

/// Async I/O stream using tokio-uring's owned-buffer completion model.
///
/// Each operation takes ownership of the buffer, submits it to the
/// kernel, and returns it alongside the result. This avoids the
/// lifetime issues that arise with borrow-based I/O on io_uring.
///
/// tokio-uring runs on a single-threaded runtime, so futures are
/// not Send. This is by design - the io_uring driver uses Rc internally.
pub trait UringStream {
    /// Read into a buffer slice, returning bytes read and the buffer.
    fn read<B: BoundedBuf + BoundedBufMut>(
        &self,
        buf: B,
    ) -> impl std::future::Future<Output = (io::Result<usize>, B)>;

    /// Write the entire buffer contents, returning the buffer.
    fn write_all<B: BoundedBuf>(
        &self,
        buf: B,
    ) -> impl std::future::Future<Output = (io::Result<()>, B)>;

    /// Shut down one or both halves of the connection.
    fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()>;
}

impl UringStream for tokio_uring::net::UnixStream {
    fn read<B: BoundedBuf + BoundedBufMut>(
        &self,
        buf: B,
    ) -> impl std::future::Future<Output = (io::Result<usize>, B)> {
        tokio_uring::net::UnixStream::read(self, buf)
    }

    fn write_all<B: BoundedBuf>(
        &self,
        buf: B,
    ) -> impl std::future::Future<Output = (io::Result<()>, B)> {
        tokio_uring::net::UnixStream::write_all(self, buf)
    }

    fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        tokio_uring::net::UnixStream::shutdown(self, how)
    }
}

impl UringStream for tokio_uring::net::TcpStream {
    fn read<B: BoundedBuf + BoundedBufMut>(
        &self,
        buf: B,
    ) -> impl std::future::Future<Output = (io::Result<usize>, B)> {
        tokio_uring::net::TcpStream::read(self, buf)
    }

    fn write_all<B: BoundedBuf>(
        &self,
        buf: B,
    ) -> impl std::future::Future<Output = (io::Result<()>, B)> {
        tokio_uring::net::TcpStream::write_all(self, buf)
    }

    fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        tokio_uring::net::TcpStream::shutdown(self, how)
    }
}
