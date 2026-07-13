// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Arrow IPC protocol surface.
//!
//! Re-exports the key types for Arrow IPC streaming and file I/O.
//! This module parallels the Lightstream protocol module, providing
//! the same role for raw Arrow IPC transport users.
//!
//! ## Codec
//!
//! [`ArrowIpcCodec`](crate::models::codecs::ipc::ArrowIpcCodec) is the central codec for Arrow IPC encode and decode.
//! It owns the encoder state machine, decoded schema, dictionary registry,
//! and SharedBuffer cache for zero-copy buffer recycling.
//!
//! ## Readers
//!
//! [`TableReader`](crate::models::readers::ipc::table::TableReader) wraps the streaming decoder and reads Arrow IPC tables
//! from any `AsyncRead` source. Transport-specific readers (TCP, UDS, etc.)
//! delegate to it internally.
//!
//! ## Writers
//!
//! - [`TableWriter`](crate::models::writers::ipc::table::TableWriter) - async writer to files or streams via the Sink trait
//! - [`TableStreamWriter`](crate::models::writers::ipc::table_stream::TableStreamWriter) - synchronous frame-by-frame writer for pipes
//!   and custom protocols
//!
//! ## Sinks
//!
//! [`TableSink`](crate::models::sinks::table_sink::TableSink) and [`TableSink64`](crate::models::sinks::table_sink::TableSink64) implement the `futures::Sink` trait
//! for streaming tables into any `AsyncWrite` destination.

pub use crate::models::codecs::ipc::ArrowIpcCodec;
pub use crate::models::encoders::ipc::IPCFrameEncoder;
pub use crate::models::frames::ipc_message::IPCFrameResult;
pub use crate::models::frames::ipc_message::{IPCFrame, IPCFrameMetadata};
pub use crate::models::readers::ipc::table::TableReader;
pub use crate::models::sinks::table_sink::GTableSink;
pub use crate::models::writers::ipc::table_stream::TableStreamWriter;
pub use crate::models::writers::ipc::table::TableWriter;
