// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # JSON interface schema
//!
//! The mapping from a vendor's JSON wire shape to a declared schema. A
//! [`JsonSchema`] pairs each target `Field`] with a [`ValueSource`]
//! for where its value sits on the wire and a [`ReadAs`] for how that value
//! is read into the column type.
//! [`JsonInterface`](crate::models::interfaces::json::JsonInterface)
//! executes the mapping.
//!
//! Schemas apply to schemaless wire formats. A self-describing protocol
//! (Arrow IPC, schema in the stream) or a code-generated one (Protobuf,
//! types in the `.proto`) already arrives typed.

use std::borrow::Cow;

use minarrow::Field;

use crate::models::interfaces::json::ValueSource;

/// How a resolved JSON scalar is read into its column.
///
/// [`Verbatim`](Self::Verbatim) takes the JSON value as it arrives. The
/// string forms parse a quoted value, for feeds that quote their numerics
/// or timestamps. [`Dictionary`](Self::Dictionary) encodes a categorical
/// column against a fixed list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum ReadAs {
    /// Take the JSON value as it arrives, the integer, float, bool, or
    /// string already on the wire.
    #[default]
    Verbatim,
    /// Parse a quoted JSON string as the column's integer or float type.
    Number,
    /// Parse a quoted JSON string as a boolean.
    Bool,
    /// Parse a quoted JSON string as an RFC 3339 / ISO 8601 timestamp and
    /// scale it to the column's
    /// [`time_unit`](minarrow::DatetimeArray::time_unit).
    Datetime,
    /// Encode a categorical column against a fixed dictionary. Each record
    /// pushes the value's position in the list, and a value outside the
    /// list is a frame error.
    Dictionary(Vec<String>),
}

/// A target schema that maps each upstream JSON record into typed
/// columns. Built column by column with [`column`](Self::column), then
/// executed by
/// [`JsonInterface`](crate::models::interfaces::json::JsonInterface).
#[derive(Debug, Clone, Default)]
pub struct JsonSchema {
    fields: Vec<Field>,
    sources: Vec<ValueSource>,
    read_as: Vec<ReadAs>,
    record_path: Option<Cow<'static, str>>,
}

impl JsonSchema {
    /// An empty schema. Add columns with [`column`](Self::column).
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a column from its target `field`, the `source` it reads
    /// from on the wire, and the `read_as` form it parses into the column
    /// type. Columns resolve in the order they are added.
    pub fn column(mut self, field: Field, source: ValueSource, read_as: ReadAs) -> Self {
        self.fields.push(field);
        self.sources.push(source);
        self.read_as.push(read_as);
        self
    }

    /// Set the dotted path to the envelope's records, e.g. `"data"` for a
    /// feed that nests its rows under a `data` key. Field-level sources
    /// resolve relative to each record at this path. Left unset, the frame
    /// root is read as a single record.
    pub fn with_record_path(mut self, path: impl Into<Cow<'static, str>>) -> Self {
        self.record_path = Some(path.into());
        self
    }

    /// The target fields, in column order.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// The per-column value sources, aligned with [`fields`](Self::fields).
    pub fn sources(&self) -> &[ValueSource] {
        &self.sources
    }

    /// The per-column read forms, aligned with [`fields`](Self::fields).
    pub fn read_as(&self) -> &[ReadAs] {
        &self.read_as
    }

    /// The dotted path to each frame's records, or `None` when the frame
    /// root is the single record.
    pub fn record_path(&self) -> Option<&str> {
        self.record_path.as_deref()
    }
}
