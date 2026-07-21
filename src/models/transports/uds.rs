// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # UDS Transport
//!
//! Connection establishment for Unix domain sockets in both peer
//! roles. A connecting peer reaches a listening socket path, and an
//! accepting peer binds the path and takes inbound connections from
//! it. Either role resolves to the socket's read and write halves,
//! ready for the byte streams, table writers, and protocol
//! connections to wrap.

use std::io;
use std::path::Path;

use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};

use crate::traits::transport::Transport;

/// UDS implementation of [`Transport`].
pub struct UdsTransport;

impl UdsTransport {
    /// Connect to a listening socket path and return the connection's halves.
    pub async fn connect(
        path: impl AsRef<Path>,
    ) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
        Ok(UnixStream::connect(path).await?.into_split())
    }

    /// Bind a socket path and return its listener.
    pub fn bind(path: impl AsRef<Path>) -> io::Result<UnixListener> {
        UnixListener::bind(path)
    }

    /// Accept the next inbound connection and return its halves.
    pub async fn accept(listener: &UnixListener) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
        let (stream, _) = listener.accept().await?;
        Ok(stream.into_split())
    }
}

impl Transport for UdsTransport {
    type Endpoint = Path;
    type Listener = UnixListener;
    type Read = OwnedReadHalf;
    type Write = OwnedWriteHalf;

    async fn connect(endpoint: &Path) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
        UdsTransport::connect(endpoint).await
    }

    async fn bind(endpoint: &Path) -> io::Result<UnixListener> {
        UdsTransport::bind(endpoint)
    }

    async fn accept(listener: &UnixListener) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
        UdsTransport::accept(listener).await
    }
}
