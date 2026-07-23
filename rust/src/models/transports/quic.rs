// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # QUIC Transport
//!
//! Connection establishment for QUIC in both peer roles. QUIC requires
//! TLS, so each role carries its own quinn configuration through
//! [`QuicEndpoint`](crate::models::transports::quic::QuicEndpoint).
//! Each established connection opens one
//! bidirectional stream whose receive and send sides are the returned
//! halves.
//!
//! QUIC streams reach the peer on first use, so a connecting peer that
//! only reads should shut down its write half to open the stream.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use quinn::{ClientConfig, Endpoint, RecvStream, SendStream, ServerConfig};

use crate::traits::transport::Transport;

/// Peer-role configuration for the QUIC wire.
///
/// The connecting peer supplies the client form and the accepting peer
/// the server form. Using a form in the other role's method returns an
/// `InvalidInput` error.
pub enum QuicEndpoint {
    /// Connecting peer configuration.
    Client {
        /// Address the accepting peer listens on.
        addr: SocketAddr,
        /// Name the accepting peer's certificate must present.
        server_name: String,
        /// TLS and transport configuration for the connecting side.
        config: ClientConfig,
    },
    /// Accepting peer configuration.
    Server {
        /// Address to bind.
        addr: SocketAddr,
        /// Certificate, key, and transport configuration for the
        /// accepting side.
        config: ServerConfig,
    },
}

/// QUIC implementation of [`Transport`].
pub struct QuicTransport;

impl QuicTransport {
    /// Connect to a listening QUIC endpoint, open a bidirectional
    /// stream, and return its halves.
    pub async fn connect(
        addr: SocketAddr,
        server_name: &str,
        config: ClientConfig,
    ) -> io::Result<(RecvStream, SendStream)> {
        let bind_addr = match addr {
            SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
        };
        let endpoint = Endpoint::client(bind_addr)?;
        let conn = endpoint
            .connect_with(config, addr, server_name)
            .map_err(io::Error::other)?
            .await
            .map_err(io::Error::other)?;
        let (send, recv) = conn.open_bi().await.map_err(io::Error::other)?;
        // The connection dies when every handle drops, so a task holds
        // one until the peers close it or it idles out.
        tokio::spawn(async move {
            let _endpoint = endpoint;
            conn.closed().await;
        });
        Ok((recv, send))
    }

    /// Bind a QUIC endpoint and return its listener.
    pub fn bind(addr: SocketAddr, config: ServerConfig) -> io::Result<Endpoint> {
        Endpoint::server(config, addr)
    }

    /// Accept the next inbound connection, take its first
    /// bidirectional stream, and return the stream's halves.
    pub async fn accept(listener: &Endpoint) -> io::Result<(RecvStream, SendStream)> {
        let incoming = listener.accept().await.ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "endpoint closed while accepting")
        })?;
        let conn = incoming.await.map_err(io::Error::other)?;
        let (send, recv) = conn.accept_bi().await.map_err(io::Error::other)?;
        // The connection dies when every handle drops, so a task holds
        // one until the peers close it or it idles out.
        tokio::spawn(async move {
            conn.closed().await;
        });
        Ok((recv, send))
    }
}

impl Transport for QuicTransport {
    type Endpoint = QuicEndpoint;
    type Listener = Endpoint;
    type Read = RecvStream;
    type Write = SendStream;

    async fn connect(endpoint: &QuicEndpoint) -> io::Result<(RecvStream, SendStream)> {
        match endpoint {
            QuicEndpoint::Client {
                addr,
                server_name,
                config,
            } => QuicTransport::connect(*addr, server_name, config.clone()).await,
            QuicEndpoint::Server { .. } => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "connect requires the client endpoint form",
            )),
        }
    }

    async fn bind(endpoint: &QuicEndpoint) -> io::Result<Endpoint> {
        match endpoint {
            QuicEndpoint::Server { addr, config } => QuicTransport::bind(*addr, config.clone()),
            QuicEndpoint::Client { .. } => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bind requires the server endpoint form",
            )),
        }
    }

    async fn accept(listener: &Endpoint) -> io::Result<(RecvStream, SendStream)> {
        QuicTransport::accept(listener).await
    }
}
