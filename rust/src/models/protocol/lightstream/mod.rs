// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Lightstream protocol connection with transport-specific constructors.

/// Bidirectional connection with transport-specific constructors.
pub mod connection;

pub use connection::LightstreamConnection;
