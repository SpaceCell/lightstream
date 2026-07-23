// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Environment arguments the run-example.py router passes to the
//! transport example servers.

#![allow(dead_code)]

use std::env;
use std::path::PathBuf;

/// The example URI from the environment, or the given default.
pub fn example_uri(default: &str) -> String {
    env::var("LIGHTSTREAM_EXAMPLE_URI").unwrap_or_else(|_| default.to_string())
}

/// The authority section of a URI, with scheme and any path stripped.
pub fn authority(uri: &str) -> String {
    let rest = uri.split_once("://").map(|(_, rest)| rest).unwrap_or(uri);
    rest.split('/').next().unwrap_or(rest).to_string()
}

/// The PEM certificate chain path for TLS servers.
pub fn tls_cert() -> PathBuf {
    PathBuf::from(env::var("LIGHTSTREAM_EXAMPLE_TLS_CERT").expect("LIGHTSTREAM_EXAMPLE_TLS_CERT is set"))
}

/// The PEM private key path for TLS servers.
pub fn tls_key() -> PathBuf {
    PathBuf::from(env::var("LIGHTSTREAM_EXAMPLE_TLS_KEY").expect("LIGHTSTREAM_EXAMPLE_TLS_KEY is set"))
}
