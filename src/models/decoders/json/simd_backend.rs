//! # simd-json backend
//!
//! Drives `simd_json` over the input bytes via reusable [`Buffers`] state,
//! then walks the parsed nodes directly into the column builders. No
//! intermediate `Value` tree; node strings borrow from the input buffer
//! and are copied straight into the [`StringArray`] data buffer at push time.

use std::collections::HashMap;
use std::io;

use simd_json::value::tape::Node;
use simd_json::{Buffers, StaticNode};

use crate::models::decoders::json::backend::{
    JsonRowDecoder, JsonValueRef, MismatchAction, handle_type_mismatch,
};
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
pub struct SimdJsonBackend {
    buffers: Buffers,
    visited: Vec<bool>,
}

impl SimdJsonBackend {
    pub fn new() -> Self {
        Self {
            buffers: Buffers::default(),
            visited: Vec::new(),
        }
    }
}

impl Default for SimdJsonBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonRowDecoder for SimdJsonBackend {
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
        let value_node = nodes
            .get(value_idx)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "missing value node"))?;
        let value_span = value_node_span(value_node);

        if let Some(&col_idx) = field_map.get(key) {
            if visited[col_idx] {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("duplicate key '{key}' at row {row_idx}"),
                ));
            }
            dispatch_value(value_node, &mut builders[col_idx], policy, row_idx, key)?;
            visited[col_idx] = true;
        }
        p = value_idx + value_span;
    }

    debug_assert_eq!(p, object_end);
    Ok(object_end)
}

/// Number of parsed nodes consumed by a value, including nested children.
#[inline]
fn value_node_span(node: &Node<'_>) -> usize {
    match node {
        Node::Object { count, .. } | Node::Array { count, .. } => count + 1,
        _ => 1,
    }
}

/// Translate a parsed Node to JsonValueRef and dispatch into the builder.
fn dispatch_value(
    node: &Node<'_>,
    builder: &mut ColumnBuilder,
    policy: TypeMismatchPolicy,
    row_idx: usize,
    field: &str,
) -> io::Result<()> {
    let value = match node {
        Node::Static(StaticNode::Null) => JsonValueRef::Null,
        Node::Static(StaticNode::Bool(b)) => JsonValueRef::Bool(*b),
        Node::Static(StaticNode::I64(v)) => JsonValueRef::I64(*v),
        Node::Static(StaticNode::U64(v)) => JsonValueRef::U64(*v),
        Node::Static(StaticNode::F64(v)) => JsonValueRef::F64(*v),
        Node::String(s) => JsonValueRef::Str(s),
        // Nested objects/arrays are not supported as cell values; treat as
        // mismatch per the active policy.
        Node::Object { .. } | Node::Array { .. } => {
            match handle_type_mismatch(policy, row_idx, field, "nested object/array")? {
                MismatchAction::PushNull | MismatchAction::Coerce => {
                    builder.push_null();
                    return Ok(());
                }
            }
        }
    };
    push_value_into(builder, value, policy, row_idx, field)
}
