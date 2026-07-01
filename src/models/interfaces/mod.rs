// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Interfaces
//!
//! An interface adapts a vendor's wire shape to the typed columns a
//! destination expects, mapping each field onto a declared schema and
//! running that mapping frame by frame.
//!
//! Interfaces serve schemaless formats, where the bytes carry no types and
//! a connector says which wire field feeds which column. A self-describing
//! format like Arrow IPC carries its schema in the stream, and a
//! code-generated one like Protobuf defines its types in a `.proto`, so
//! both arrive already typed.

/// Maps a vendor's JSON wire shape onto a [`JsonSchema`] and runs it frame
/// by frame.
///
/// [`JsonSchema`]: json::schema::JsonSchema
#[cfg(feature = "json")]
pub mod json;
