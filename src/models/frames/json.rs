// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # JSON frame and record
//!
//! A json frame can hold many records under one envelope, so the
//! [`JsonInterface`](crate::models::interfaces::json::JsonInterface) parses
//! it once and returns a [`JsonFrame`](crate::models::frames::json::JsonFrame) to read the records. Each
//! [`JsonRecord`](crate::models::frames::json::JsonRecord) gives its columns as [`JsonValueRef`](crate::models::decoders::json::value::JsonValueRef)s borrowed
//! from the parsed buffer for the destination to handle and/or convert.

use std::io;

use simd_json::value::tape::Node;

use crate::models::decoders::json::value::JsonValueRef;
use crate::models::decoders::json::simd::TapeOps;
use crate::models::interfaces::json::ValueSource;

/// One parsed frame, positioned on its records. Yields each record in
/// wire order via [`next_record`](Self::next_record).
pub struct JsonFrame<'f> {
    pub(crate) nodes: Vec<Node<'f>>,
    pub(crate) sources: &'f [ValueSource],
    /// Nanoseconds per tick of each column's time unit, dividing the
    /// receive clock down to a wall-clock column's unit.
    pub(crate) wall_clock_scale: &'f [i64],
    pub(crate) max_string_bytes: usize,
    /// Tape index of the next record node.
    pub(crate) next_record: usize,
    pub(crate) records_remaining: usize,
    pub(crate) now: i64,
}

impl<'f> JsonFrame<'f> {
    /// Records not yet yielded. An empty record array starts at zero -
    /// zero records is a valid frame.
    pub fn records_remaining(&self) -> usize {
        self.records_remaining
    }

    /// Yield the next record, or `None` once the frame is drained.
    pub fn next_record(&mut self) -> Option<JsonRecord<'_, 'f>> {
        if self.records_remaining == 0 {
            return None;
        }
        let record = self.next_record;
        self.records_remaining -= 1;
        self.next_record = record + self.nodes.span_at(record);
        Some(JsonRecord { frame: self, record })
    }
}

/// One record of a parsed frame. [`value`](Self::value) resolves one
/// column's [`JsonValueRef`]. The destination performs the typed
/// conversion and push.
pub struct JsonRecord<'a, 'f> {
    frame: &'a JsonFrame<'f>,
    /// Tape index of this record's node.
    record: usize,
}

impl<'a, 'f> JsonRecord<'a, 'f> {
    /// Resolve column `col`'s value on this record.
    ///
    /// A wall-clock column yields the receive clock as `I64`, scaled to the
    /// column's time unit. A missing key or path is an error.
    pub fn value(&self, col: usize) -> io::Result<JsonValueRef<'f>> {
        let frame = self.frame;
        let nodes = frame.nodes.as_slice();
        let value_idx = match &frame.sources[col] {
            ValueSource::WallClock => {
                return Ok(JsonValueRef::I64(frame.now / frame.wall_clock_scale[col]));
            }
            ValueSource::RecordKey(key) => {
                nodes.object_value_index(self.record, key).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("missing key '{key}'"))
                })?
            }
            ValueSource::JsonPath(path) => path.resolve(nodes, self.record).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, format!("path {path:?} not found"))
            })?,
            ValueSource::FramePath(path) => path.resolve(nodes, 0).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("frame path {path:?} not found"),
                )
            })?,
        };
        let value = nodes.value_at(value_idx).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "expected scalar value, found nested object/array",
            )
        })?;
        if let JsonValueRef::Str(s) = &value
            && s.len() > frame.max_string_bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "resource limit exceeded: string value of {} bytes (cap {})",
                    s.len(),
                    frame.max_string_bytes,
                ),
            ));
        }
        Ok(value)
    }
}
