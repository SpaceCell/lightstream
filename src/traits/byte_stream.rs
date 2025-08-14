//! # Byte Stream Traits
//!
//! Simple trait aliases that let you plug **any async stream of bytes** into this crate.
//! Choose regular `Vec<u8>` or 64-byte aligned `Vec64<u8>` when you care about SIMD.
//!
//! **Why this is useful**
//! - Works with any `futures_core::Stream<Result<_, io::Error>>` (files, sockets, in-memory).
//! - No extra layers or boxing—just trait bounds.
//! - Optional `ByteStream64` avoids re-allocations when you need aligned buffers.
//!
//! Backpressure and scheduling are handled by your underlying stream.

use futures_core::Stream;
use minarrow::Vec64;
use std::io;

/// Universal trait alias for any asynchronous, chunked byte stream.
///
/// Implemented automatically for any [`Stream`] yielding `Result<Vec<u8>, io::Error>`
/// and supporting `Send` + `Unpin`.
pub trait ByteStream: Stream<Item = Result<Vec<u8>, io::Error>> + Send + Unpin {}

impl<T> ByteStream for T where T: Stream<Item = Result<Vec<u8>, io::Error>> + Send + Unpin {}

/// Universal trait alias for any asynchronous, chunked byte stream of 64-byte aligned buffers.
///
/// This is a special case for scenarios where both the producer and consumer are under
/// your control and SIMD alignment is required end-to-end (e.g. disk or network paths that
/// preserve alignment). Using this avoids later re-allocation purely to fix alignment.
///
/// Implemented automatically for any [`Stream`] yielding `Result<Vec64<u8>, io::Error>`
/// and supporting `Send` + `Unpin`.
pub trait ByteStream64: Stream<Item = Result<Vec64<u8>, io::Error>> + Send + Unpin {}

impl<T> ByteStream64 for T where T: Stream<Item = Result<Vec64<u8>, io::Error>> + Send + Unpin {}

/// Generalised trait for any asynchronous, chunked byte stream of the given buffer type `B`.
///
/// This is a pure set of bounds that allows plugging in any compliant `Stream`
/// without dynamic dispatch.
///
/// Implemented for any `Stream<Item = Result<B, io::Error>> + Send + Unpin`.
pub trait GenByteStream<B>: Stream<Item = Result<B, io::Error>> + Send + Unpin {}

impl<T, B> GenByteStream<B> for T where T: Stream<Item = Result<B, io::Error>> + Send + Unpin {}
