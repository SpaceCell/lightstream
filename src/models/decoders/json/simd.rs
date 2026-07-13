// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # simd-json tape decoder
//!
//! Drives `simd_json` over the input bytes via reusable [`Buffers`] state,
//! then walks the parsed nodes directly into the column builders. No
//! intermediate `Value` tree; node strings borrow from the input buffer
//! and are copied straight into the [`StringArray`](minarrow::StringArray) data buffer at push time.

use std::collections::HashMap;
use std::io;

use simd_json::value::tape::Node;
use simd_json::{Buffers, StaticNode};

use crate::models::decoders::json::row_decoder::{
    JsonRowDecoder, MismatchAction, handle_type_mismatch,
};
use crate::models::decoders::json::value::JsonValueRef;
use crate::models::decoders::json::builder::{ColumnBuilder, TypeMismatchPolicy};
use crate::models::decoders::json::push::push_value_into;

/// simd-json driver. Holds reusable [`Buffers`] across batches so the
/// allocator is only touched on the first call.
///
/// Also holds a `visited` vector reused across rows that records which
/// columns have been written in the current row. It powers two things:
/// (a) detecting duplicate keys inside a row so we can fail loudly
/// instead of corrupting the table, and (b) skipping the per-row
/// scan over every column to find the ones that need a null fill -
/// only unvisited columns get nulled.
pub struct TapeDecoder {
    buffers: Buffers,
    visited: Vec<bool>,
}

impl TapeDecoder {
    pub fn new() -> Self {
        Self {
            buffers: Buffers::default(),
            visited: Vec::new(),
        }
    }
}

impl Default for TapeDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonRowDecoder for TapeDecoder {
    fn decode_rows(
        &mut self,
        input: &mut [u8],
        builders: &mut [ColumnBuilder],
        field_map: &HashMap<&str, usize>,
        policy: TypeMismatchPolicy,
    ) -> io::Result<usize> {
        let parsed = simd_json::to_tape_with_buffers(input, &mut self.buffers).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid JSON: {e}"))
        })?;
        let nodes: &[Node<'_>] = &parsed.0;
        if nodes.is_empty() {
            return Ok(0);
        }

        let (n_rows, body_count) = match nodes[0] {
            Node::Array { len, count } => (len, count),
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("expected JSON array at root, got {:?}", other.value_type()),
                ));
            }
        };

        // Make sure the visited slot exists for every builder. Resize
        // once per batch rather than per row.
        if self.visited.len() < builders.len() {
            self.visited.resize(builders.len(), false);
        }

        let row_start = 1usize;
        let row_end = 1 + body_count;
        let mut cursor = row_start;
        let starting_len = builders.first().map(|b| b.len()).unwrap_or(0);

        for row_idx in 0..n_rows {
            // Reset visited bits for this row.
            for v in self.visited[..builders.len()].iter_mut() {
                *v = false;
            }

            cursor = walk_row(
                nodes,
                cursor,
                builders,
                field_map,
                policy,
                starting_len + row_idx,
                &mut self.visited,
            )?;

            // Fill nulls only for columns that didn't appear in this row.
            for (col_idx, visited) in self.visited[..builders.len()].iter().enumerate() {
                if !visited {
                    builders[col_idx].push_null();
                }
            }
        }

        debug_assert_eq!(cursor, row_end);
        Ok(n_rows)
    }
}

/// Walk one object node starting at `cursor`. Returns the cursor position
/// immediately after the object (start of next sibling).
///
/// Marks `visited[col_idx]` as the row's keys are dispatched and errors
/// on a duplicate key for the same column, since pushing a second value
/// would misalign that column against the rest of the table.
fn walk_row(
    nodes: &[Node<'_>],
    cursor: usize,
    builders: &mut [ColumnBuilder],
    field_map: &HashMap<&str, usize>,
    policy: TypeMismatchPolicy,
    row_idx: usize,
    visited: &mut [bool],
) -> io::Result<usize> {
    let (n_keys, total_count) = match nodes.get(cursor) {
        Some(Node::Object { len, count }) => (*len, *count),
        Some(other) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected JSON object at row, got {:?}", other.value_type()),
            ));
        }
        None => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "input ended mid-row",
            ));
        }
    };
    let object_end = cursor + 1 + total_count;
    let mut p = cursor + 1;

    for _ in 0..n_keys {
        let key = match nodes.get(p) {
            Some(Node::String(s)) => *s,
            Some(other) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("expected object key string, got {:?}", other.value_type()),
                ));
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "input ended mid-object",
                ));
            }
        };
        let value_idx = p + 1;
        if value_idx >= nodes.len() {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "missing value node"));
        }
        let value_span = nodes.span_at(value_idx);

        if let Some(&col_idx) = field_map.get(key) {
            if visited[col_idx] {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("duplicate key '{key}' at row {row_idx}"),
                ));
            }
            match nodes.value_at(value_idx) {
                Some(value) => {
                    push_value_into(&mut builders[col_idx], value, policy, row_idx, key)?;
                }
                // Nested objects/arrays are not yet supported as cell values,
                // and currently result in a null.
                None => match handle_type_mismatch(policy, row_idx, key, "nested object/array")? {
                    MismatchAction::PushNull | MismatchAction::Coerce => {
                        builders[col_idx].push_null();
                    }
                },
            }
            visited[col_idx] = true;
        }
        p = value_idx + value_span;
    }

    debug_assert_eq!(p, object_end);
    Ok(object_end)
}

/// Operations on a parsed simd-json tape.
///
/// The tape is a pre-order node array where each container node carries
/// its descendant count, so siblings are reached by skipping spans.
/// Implemented on `[Node<'f>]`. Both the row walker and the
/// [`JsonInterface`](crate::models::interfaces::json::JsonInterface)
/// navigate through this surface.
pub(crate) trait TapeOps<'f> {
    /// Number of tape nodes the value at `idx` occupies, including
    /// nested children.
    fn span_at(&self, idx: usize) -> usize;

    /// Translate the node at `idx` to a [`JsonValueRef`]. `None` for object
    /// and array nodes, which are not single cell values - the caller
    /// decides whether that is a mismatch or an error.
    fn value_at(&self, idx: usize) -> Option<JsonValueRef<'f>>;

    /// Tape index of `key`'s value within the object node at `obj_idx`,
    /// walking the object's key/value pairs by span. `None` when the
    /// node is not an object or the key is absent.
    fn object_value_index(&self, obj_idx: usize, key: &str) -> Option<usize>;

    /// Tape index of the `element`-th item of the array node at
    /// `arr_idx`, skipping preceding siblings by span. `None` when the
    /// node is not an array or the index is out of range.
    fn array_element_index(&self, arr_idx: usize, element: usize) -> Option<usize>;
}

impl<'f> TapeOps<'f> for [Node<'f>] {
    #[inline]
    fn span_at(&self, idx: usize) -> usize {
        match self[idx] {
            Node::Object { count, .. } | Node::Array { count, .. } => count + 1,
            _ => 1,
        }
    }

    #[inline]
    fn value_at(&self, idx: usize) -> Option<JsonValueRef<'f>> {
        match self[idx] {
            Node::Static(StaticNode::Null) => Some(JsonValueRef::Null),
            Node::Static(StaticNode::Bool(b)) => Some(JsonValueRef::Bool(b)),
            Node::Static(StaticNode::I64(v)) => Some(JsonValueRef::I64(v)),
            Node::Static(StaticNode::U64(v)) => Some(JsonValueRef::U64(v)),
            Node::Static(StaticNode::F64(v)) => Some(JsonValueRef::F64(v)),
            Node::String(s) => Some(JsonValueRef::Str(s)),
            Node::Object { .. } | Node::Array { .. } => None,
        }
    }

    fn object_value_index(&self, obj_idx: usize, key: &str) -> Option<usize> {
        let Node::Object { len, .. } = self[obj_idx] else {
            return None;
        };
        let mut p = obj_idx + 1;
        for _ in 0..len {
            let Node::String(k) = self[p] else {
                return None;
            };
            let value_idx = p + 1;
            if k == key {
                return Some(value_idx);
            }
            p = value_idx + self.span_at(value_idx);
        }
        None
    }

    fn array_element_index(&self, arr_idx: usize, element: usize) -> Option<usize> {
        let Node::Array { len, .. } = self[arr_idx] else {
            return None;
        };
        if element >= len {
            return None;
        }
        let mut p = arr_idx + 1;
        for _ in 0..element {
            p += self.span_at(p);
        }
        Some(p)
    }
}
