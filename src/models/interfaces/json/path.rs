// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # JSON path
//!
//! A dotted path through nested JSON arrays and objects, used by the
//! [`JsonInterface`](crate::models::interfaces::json::JsonInterface) to
//! reach values that do not live at a record's top level. Each segment
//! is either an object key or an array index.

use simd_json::value::tape::Node;

use crate::models::decoders::json::simd::TapeOps;

/// One segment of a dotted path, an object key or an array index.
#[derive(Debug, Clone)]
enum PathStep {
    Key(String),
    Index(usize),
}

/// A dotted path through nested JSON arrays and objects. A connector builds
/// one with [`parse`](Self::parse) to route a column at a value below the
/// record's top level. A segment that parses as an integer becomes an array
/// index step, and everything else an object key step.
#[derive(Debug, Clone)]
pub struct JsonPath(Vec<PathStep>);

impl JsonPath {
    /// Parse a dotted JSON path. Empty segments are dropped.
    pub fn parse(path: &str) -> Self {
        JsonPath(
            path.split('.')
                .filter(|s| !s.is_empty())
                .map(|s| match s.parse::<usize>() {
                    Ok(i) => PathStep::Index(i),
                    Err(_) => PathStep::Key(s.to_string()),
                })
                .collect(),
        )
    }

    /// True when the path has no segments.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Walk this path from the tape node at `start`, returning the tape
    /// index it lands on, or `None` when any segment is absent.
    pub(crate) fn resolve(&self, nodes: &[Node<'_>], start: usize) -> Option<usize> {
        let mut cur = start;
        for step in &self.0 {
            cur = match step {
                PathStep::Key(k) => nodes.object_value_index(cur, k)?,
                PathStep::Index(i) => nodes.array_element_index(cur, *i)?,
            };
        }
        Some(cur)
    }
}
