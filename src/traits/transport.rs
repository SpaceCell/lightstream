// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Transport trait
//!
//! Connection establishment for the socket-backed transports i.e. TCP,
//! UDS, WebSocket, HTTP, QUIC, and WebTransport.
//!
//! Every wire offers the same two peer roles. A connecting peer reaches
//! a listening endpoint, and an accepting peer binds that endpoint and
//! takes inbound connections from it. Both roles resolve to the wire's
//! read and write halves, so the table readers, table writers, and
//! protocol connections above this layer wrap either peer the same way.

use std::future::Future;
use std::io;

use tokio::io::{AsyncRead, AsyncWrite};

/// Connection establishment contract for a socket-backed transport.
///
/// Pairs a wire with its endpoint address form and provides both peer
/// roles. [`connect`](Transport::connect) reaches a listening
/// endpoint. [`bind`](Transport::bind) claims the endpoint and
/// returns the wire's listener, and [`accept`](Transport::accept)
/// takes the next inbound connection from it. Each connection arrives
/// as the wire's read and write halves, ready for the transport
/// readers, writers, and protocol connections to wrap.
pub trait Transport {
    /// Address form the wire is reached at e.g. a socket address, path, or URL.
    type Endpoint: ?Sized;

    /// Listener held by an accepting peer across inbound connections.
    type Listener: Send;

    /// Read half of an established connection.
    type Read: AsyncRead + Send + Unpin;

    /// Write half of an established connection.
    type Write: AsyncWrite + Send + Unpin;

    /// Connect to a listening endpoint and return the connection's halves.
    fn connect(
        endpoint: &Self::Endpoint,
    ) -> impl Future<Output = io::Result<(Self::Read, Self::Write)>> + Send;

    /// Bind the endpoint and return the wire's listener.
    fn bind(endpoint: &Self::Endpoint) -> impl Future<Output = io::Result<Self::Listener>> + Send;

    /// Accept the next inbound connection and return its halves.
    fn accept(
        listener: &Self::Listener,
    ) -> impl Future<Output = io::Result<(Self::Read, Self::Write)>> + Send;
}
