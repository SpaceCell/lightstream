// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Bound listeners
//!
//! Accepting reads and writes serve from listeners held for the life
//! of the process. The first accepting call on an endpoint binds it,
//! and every later call draws from the same listener, so a serving
//! loop keeps its endpoint continuously bound and clients queue in
//! the listener's backlog while another connection is being served.
//!
//! Binds run under the registry lock, so threads opening the same
//! endpoint at once resolve to one bound listener. The QUIC and
//! WebTransport listeners carry the TLS identity given at first bind,
//! and later accepting calls on the endpoint reuse that identity.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::net;
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use pyo3::PyResult;
use tokio::net::{TcpListener, UnixListener};
use wtransport::endpoint::endpoint_side::Server;

use crate::errors::{TransportError, to_py_err};
use crate::tls;
use lightstream::error::IoError;

/// One bound endpoint per scheme and authority. The scheme stays in
/// the key so listeners for different wires on the same port remain
/// distinct bindings rather than sharing one accept queue.
#[derive(Hash, PartialEq, Eq)]
enum ListenerKey {
    Tcp(String),
    Ws(String),
    Wss(String),
    Http(String),
    Https(String),
    Uds(PathBuf),
    Quic(String),
    Wt(String),
}

enum BoundListener {
    Tcp(Arc<TcpListener>),
    Uds(Arc<UnixListener>),
    Quic(Arc<quinn::Endpoint>),
    Wt(Arc<wtransport::Endpoint<Server>>),
}

static LISTENERS: OnceLock<Mutex<HashMap<ListenerKey, BoundListener>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<ListenerKey, BoundListener>> {
    LISTENERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns the process-held TCP listener for the address, binding it
/// on the first call. Call within the runtime context.
pub fn tcp(addr: &str) -> PyResult<Arc<TcpListener>> {
    shared_tcp(ListenerKey::Tcp(addr.to_string()), addr)
}

/// Returns the process-held listener for a `ws://` URL's authority,
/// binding it on the first call. Call within the runtime context.
pub fn ws(url: &str) -> PyResult<Arc<TcpListener>> {
    let authority = authority(url, "ws://");
    shared_tcp(ListenerKey::Ws(authority.to_string()), authority)
}

/// Returns the process-held listener for a `wss://` URL's authority,
/// binding it on the first call. Call within the runtime context.
pub fn wss(url: &str) -> PyResult<Arc<TcpListener>> {
    let authority = authority(url, "wss://");
    shared_tcp(ListenerKey::Wss(authority.to_string()), authority)
}

/// Returns the process-held listener for an `http://` URL's authority,
/// binding it on the first call. Call within the runtime context.
pub fn http(url: &str) -> PyResult<Arc<TcpListener>> {
    let authority = authority(url, "http://");
    shared_tcp(ListenerKey::Http(authority.to_string()), authority)
}

/// Returns the process-held listener for an `https://` URL's
/// authority, binding it on the first call. Call within the runtime
/// context.
pub fn https(url: &str) -> PyResult<Arc<TcpListener>> {
    let authority = authority(url, "https://");
    shared_tcp(ListenerKey::Https(authority.to_string()), authority)
}

/// Returns the process-held UDS listener for the socket path, binding
/// it on the first call. A stale socket file left by an earlier
/// process is removed before the bind. Call within the runtime
/// context.
pub fn uds(path: &Path) -> PyResult<Arc<UnixListener>> {
    let mut map = registry().lock().expect("listener registry lock");
    if let Some(BoundListener::Uds(listener)) = map.get(&ListenerKey::Uds(path.to_path_buf())) {
        return Ok(listener.clone());
    }
    let _ = fs::remove_file(path);
    let listener =
        Arc::new(UnixListener::bind(path).map_err(|e| to_py_err(IoError::Io(e)))?);
    map.insert(
        ListenerKey::Uds(path.to_path_buf()),
        BoundListener::Uds(listener.clone()),
    );
    Ok(listener)
}

/// Returns the process-held QUIC endpoint for the authority, binding
/// it with the given TLS identity on the first call. Call within the
/// runtime context.
pub fn quic(authority: &str, cert: &Path, key: &Path) -> PyResult<Arc<quinn::Endpoint>> {
    let key_entry = ListenerKey::Quic(authority.to_string());
    {
        let map = registry().lock().expect("listener registry lock");
        if let Some(BoundListener::Quic(listener)) = map.get(&key_entry) {
            return Ok(listener.clone());
        }
    }
    let config = tls::quic_server(cert, key)
        .map_err(|e| TransportError::new_err(e.to_string()))?;
    let addr = socket_addr(authority)?;
    let mut map = registry().lock().expect("listener registry lock");
    if let Some(BoundListener::Quic(listener)) = map.get(&key_entry) {
        return Ok(listener.clone());
    }
    let listener = Arc::new(
        quinn::Endpoint::server(config, addr).map_err(|e| to_py_err(IoError::Io(e)))?,
    );
    map.insert(key_entry, BoundListener::Quic(listener.clone()));
    Ok(listener)
}

/// Returns the process-held WebTransport endpoint for the authority,
/// binding it with the given TLS identity on the first call. Call
/// within the runtime context.
pub async fn wt(
    authority: &str,
    cert: &Path,
    key: &Path,
) -> PyResult<Arc<wtransport::Endpoint<Server>>> {
    let key_entry = ListenerKey::Wt(authority.to_string());
    {
        let map = registry().lock().expect("listener registry lock");
        if let Some(BoundListener::Wt(listener)) = map.get(&key_entry) {
            return Ok(listener.clone());
        }
    }
    let config = tls::wt_server(socket_addr(authority)?, cert, key)
        .await
        .map_err(|e| TransportError::new_err(e.to_string()))?;
    let mut map = registry().lock().expect("listener registry lock");
    if let Some(BoundListener::Wt(listener)) = map.get(&key_entry) {
        return Ok(listener.clone());
    }
    let listener = Arc::new(
        wtransport::Endpoint::server(config).map_err(|e| to_py_err(IoError::Io(e)))?,
    );
    map.insert(key_entry, BoundListener::Wt(listener.clone()));
    Ok(listener)
}

/// Looks the key up or binds the authority, storing the listener for
/// the life of the process.
fn shared_tcp(key: ListenerKey, authority: &str) -> PyResult<Arc<TcpListener>> {
    let mut map = registry().lock().expect("listener registry lock");
    if let Some(BoundListener::Tcp(listener)) = map.get(&key) {
        return Ok(listener.clone());
    }
    let bind = || -> io::Result<TcpListener> {
        let std_listener = net::TcpListener::bind(authority)?;
        std_listener.set_nonblocking(true)?;
        TcpListener::from_std(std_listener)
    };
    let listener = Arc::new(bind().map_err(|e| to_py_err(IoError::Io(e)))?);
    map.insert(key, BoundListener::Tcp(listener.clone()));
    Ok(listener)
}

/// Resolves an authority string to its first socket address.
pub fn socket_addr(authority: &str) -> PyResult<net::SocketAddr> {
    authority
        .to_socket_addrs()
        .map_err(|e| to_py_err(IoError::Io(e)))?
        .next()
        .ok_or_else(|| {
            TransportError::new_err(format!("endpoint '{authority}' resolves to no address"))
        })
}

/// Strips the scheme and any path from a URL, leaving the authority
/// the listener binds.
fn authority<'a>(url: &'a str, scheme: &str) -> &'a str {
    let rest = url.strip_prefix(scheme).unwrap_or(url);
    rest.split('/').next().unwrap_or(rest)
}
