// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # Errors
//!
//! Python exception hierarchy for lightstream. `LightstreamError` is the
//! base, with `FormatError`, `TransportError`, and `ProtocolError` narrowing
//! by failure category. [`to_py_err`] translates the Rust
//! [`IoError`] into the matching exception.

use lightstream::error::IoError;
use pyo3::PyErr;
use pyo3::create_exception;
use pyo3::exceptions::PyException;

create_exception!(
    lightstream,
    LightstreamError,
    PyException,
    "Base exception for lightstream failures."
);
create_exception!(
    lightstream,
    FormatError,
    LightstreamError,
    "Encoding, decoding, schema, or compression failure."
);
create_exception!(
    lightstream,
    TransportError,
    LightstreamError,
    "Connection or wire-level failure."
);
create_exception!(
    lightstream,
    ProtocolError,
    LightstreamError,
    "Lightstream protocol violation, such as an unregistered type tag."
);

/// Translates the lightstream [`IoError`] into the exception hierarchy.
///
/// Data-shape failures raise `FormatError`. Underlying I/O and internal
/// failures raise the base `LightstreamError`, since the medium is not
/// known at this level.
pub fn to_py_err(err: IoError) -> PyErr {
    match &err {
        IoError::UnsupportedType(_)
        | IoError::NullMaskInconsistent(_)
        | IoError::InputDataError(_)
        | IoError::Metadata(_)
        | IoError::Compression(_)
        | IoError::Format(_)
        | IoError::UnsupportedEncoding(_) => FormatError::new_err(err.to_string()),
        IoError::Io(_) | IoError::Internal(_) => LightstreamError::new_err(err.to_string()),
    }
}
