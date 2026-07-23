// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Message
//!
//! One decoded Lightstream protocol frame for Python. Table frames carry
//! a decoded `minarrow.Table`, and message frames carry the raw payload
//! bytes for the caller to decode as Protobuf, MessagePack, or any other
//! registered encoding.

use lightstream::models::frames::lightstream_message::LightstreamMessage;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::input::PyBatchCapsuleStream;

/// A decoded Lightstream protocol frame, either a table or an opaque
/// message payload. The frame lives behind a `Box`, since the decoded
/// table's 64-byte alignment exceeds what the Python object allocator
/// provides for inline pyclass storage.
#[pyclass(name = "Message")]
pub struct PyMessage {
    inner: Box<LightstreamMessage>,
}

impl PyMessage {
    /// Wraps a decoded frame for handing to Python.
    pub fn new(inner: LightstreamMessage) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

#[pymethods]
impl PyMessage {
    /// The registered type's tag.
    #[getter]
    fn tag(&self) -> u8 {
        self.inner.tag()
    }

    /// True when this frame decoded as a table.
    fn is_table(&self) -> bool {
        self.inner.is_table()
    }

    /// The decoded `minarrow.Table` for table frames, or `None` for
    /// message frames.
    #[getter]
    fn table(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.table() {
            Some(table) => Ok(Some(PyBatchCapsuleStream::into_table(py, table.to_table())?)),
            None => Ok(None),
        }
    }

    /// The raw payload bytes for message frames, or `None` for table
    /// frames.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.inner.payload().map(|payload| PyBytes::new(py, payload))
    }

    fn __repr__(&self) -> String {
        match &*self.inner {
            LightstreamMessage::Table { tag, table } => {
                format!("Message(tag={}, table rows={})", tag, table.n_rows())
            }
            LightstreamMessage::Message { tag, payload } => {
                format!("Message(tag={}, payload bytes={})", tag, payload.len())
            }
        }
    }
}
