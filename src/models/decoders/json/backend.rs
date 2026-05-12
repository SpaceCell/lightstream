//! # JSON row-decoder trait
//!
//! Common surface that the serde-streaming and simd-json tape backends both
//! implement. The driver feeds bytes to a backend, the backend yields one
//! row at a time and dispatches each `(key, value)` pair into the
//! appropriate [`ColumnBuilder`].

use std::collections::HashMap;
use std::io;

use crate::models::decoders::json::builder::{ColumnBuilder, TypeMismatchPolicy};

/// Borrowed reference to a JSON scalar value. Strings borrow from the input
/// buffer wherever the backend can supply them that way.
#[derive(Debug)]
pub enum JsonValueRef<'a> {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(&'a str),
}

/// Driver-facing row-decoder trait.
///
/// Implementations consume the input bytes (single buffer or chunked) and
/// invoke per-cell pushes into the supplied [`ColumnBuilder`] slice.
/// `field_map` resolves JSON keys to column indices in O(1).
pub trait JsonRowDecoder {
    /// Decode all complete rows from the supplied input into the column
    /// builders. Returns the number of rows decoded.
    ///
    /// `field_map` maps each known field name to its index in `builders`.
    /// JSON keys not present in `field_map` are silently dropped.
    /// Columns whose key was missing in a row are filled with a null.
    fn decode_rows(
        &mut self,
        input: &mut [u8],
        builders: &mut [ColumnBuilder],
        field_map: &HashMap<&str, usize>,
        policy: TypeMismatchPolicy,
    ) -> io::Result<usize>;
}

/// Apply [`TypeMismatchPolicy`] when the backend hits a value that cannot be
/// pushed into the destination builder. Returns Ok(true) if the cell was
/// recorded (as null) and the backend should continue, or an error per the
/// policy. The caller must already have decided this is a mismatch.
#[inline]
pub fn handle_type_mismatch(
    policy: TypeMismatchPolicy,
    row: usize,
    field: &str,
    detail: &str,
) -> io::Result<MismatchAction> {
    match policy {
        TypeMismatchPolicy::Error => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("type mismatch at row {row} field '{field}': {detail}"),
        )),
        TypeMismatchPolicy::Coerce => Ok(MismatchAction::Coerce),
        TypeMismatchPolicy::Null => Ok(MismatchAction::PushNull),
    }
}

/// Outcome of a [`TypeMismatchPolicy`] check that is not [`TypeMismatchPolicy::Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MismatchAction {
    /// Caller should attempt a coercion (e.g. parse string to number).
    Coerce,
    /// Caller should call `push_null` on the column and move on.
    PushNull,
}
