// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # TCP Transport
//!
//! Connection establishment for TCP in both peer roles. A connecting
//! peer reaches a listening endpoint, and an accepting peer binds the
//! endpoint and takes inbound connections from it. Either role
//! resolves to the socket's read and write halves, ready for the byte
//! streams, table writers, and protocol connections to wrap.

use std::io;

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::traits::transport::Transport;

/// TCP implementation of [`Transport`].
///
/// Establishes plaintext connections. TLS-wrapped channels keep their
/// dedicated `connect_tls` entry points on the byte stream and writer
/// types.
pub struct TcpTransport;

impl TcpTransport {
    /// Connect to a listening TCP endpoint and return the connection's halves.
    pub async fn connect(
        addr: impl ToSocketAddrs,
    ) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
        Ok(TcpStream::connect(addr).await?.into_split())
    }

    /// Bind a TCP endpoint and return its listener.
    pub async fn bind(addr: impl ToSocketAddrs) -> io::Result<TcpListener> {
        TcpListener::bind(addr).await
    }

    /// Accept the next inbound connection and return its halves.
    pub async fn accept(listener: &TcpListener) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
        let (stream, _) = listener.accept().await?;
        Ok(stream.into_split())
    }
}

impl Transport for TcpTransport {
    type Endpoint = str;
    type Listener = TcpListener;
    type Read = OwnedReadHalf;
    type Write = OwnedWriteHalf;

    async fn connect(endpoint: &str) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
        TcpTransport::connect(endpoint).await
    }

    async fn bind(endpoint: &str) -> io::Result<TcpListener> {
        TcpTransport::bind(endpoint).await
    }

    async fn accept(listener: &TcpListener) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
        TcpTransport::accept(listener).await
    }
}
