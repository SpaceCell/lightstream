// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # lightstream
//!
//! Streaming Arrow I/O for Python. Reads and writes tables across files,
//! sockets, and network transports, with results as minarrow objects that
//! interoperate with the Arrow ecosystem through the PyCapsule protocol.

mod errors;
mod input;
mod listeners;
mod message;
mod output;
mod runtime;
mod tls;
mod uri;

use pyo3::prelude::*;

pub use errors::{FormatError, LightstreamError, ProtocolError, TransportError, to_py_err};
pub use input::PyDataStreamReader;
pub use message::PyMessage;
pub use output::PyDataStreamWriter;
pub use runtime::{block_on_py, runtime};

/// Opens a source for reading. The scheme picks the medium, `protocol`
/// picks the wire protocol, and the format comes from `format` or the
/// path extension. Returns a `Reader` that yields `minarrow.Table`
/// batches. `accept=True` makes the reader the accepting peer on wire
/// sources, holding the endpoint bound for the life of the process and
/// blocking until a writer connects. `parallel` takes a stream count
/// on TCP sources, where the reader always accepts that many
/// connections. The quic, wt, wss, and https schemes carry TLS,
/// taking `tls_cert` and `tls_key` PEM paths on the accepting peer
/// and a `tls_ca` PEM path on the connecting peer.
#[pyfunction]
#[pyo3(signature = (uri, *, format=None, protocol=None, parallel=0, accept=false, tls_cert=None, tls_key=None, tls_ca=None, mmap=None, out_of_core=false, base=None, delimiter=None, header=None, batch_size=None))]
fn read(
    py: Python<'_>,
    uri: &str,
    format: Option<&str>,
    protocol: Option<&str>,
    parallel: usize,
    accept: bool,
    tls_cert: Option<&str>,
    tls_key: Option<&str>,
    tls_ca: Option<&str>,
    mmap: Option<bool>,
    out_of_core: bool,
    base: Option<&str>,
    delimiter: Option<&str>,
    header: Option<bool>,
    batch_size: Option<usize>,
) -> PyResult<PyDataStreamReader> {
    let resolved = py.detach(|| {
        uri::resolve_source(
            uri,
            format,
            protocol,
            parallel,
            accept,
            tls_cert,
            tls_key,
            tls_ca,
            mmap,
            out_of_core,
            base,
            delimiter,
            header,
            batch_size,
        )
    })?;
    Ok(PyDataStreamReader::new(resolved))
}

/// Opens a target for writing. The scheme picks the medium, `protocol`
/// picks the wire protocol, and the format comes from `format` or the
/// path extension. Returns a `Writer` accepting any Arrow-compatible
/// object. `accept=True` makes the writer the accepting peer on wire
/// targets, holding the endpoint bound for the life of the process and
/// blocking until a reader connects. The quic, wt, wss, and https
/// schemes carry TLS, taking `tls_cert` and `tls_key` PEM paths on
/// the accepting peer and a `tls_ca` PEM path on the connecting peer.
#[pyfunction]
#[pyo3(signature = (uri, *, format=None, protocol=None, parallel=0, accept=false, tls_cert=None, tls_key=None, tls_ca=None, compression=None, delimiter=None, header=None))]
fn write(
    py: Python<'_>,
    uri: &str,
    format: Option<&str>,
    protocol: Option<&str>,
    parallel: usize,
    accept: bool,
    tls_cert: Option<&str>,
    tls_key: Option<&str>,
    tls_ca: Option<&str>,
    compression: Option<&str>,
    delimiter: Option<&str>,
    header: Option<bool>,
) -> PyResult<PyDataStreamWriter> {
    let resolved = py.detach(|| {
        uri::resolve_target(
            uri, format, protocol, parallel, accept, tls_cert, tls_key, tls_ca, compression,
            delimiter, header,
        )
    })?;
    Ok(PyDataStreamWriter::new(resolved))
}

/// Streaming Arrow I/O for Python.
#[pymodule]
#[pyo3(name = "lightstream")]
fn lightstream_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "__doc__",
        "Streaming Arrow I/O for Python - files, sockets, and network transports with zero-copy minarrow interop.",
    )?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(write, m)?)?;
    m.add_class::<PyDataStreamReader>()?;
    m.add_class::<PyDataStreamWriter>()?;
    m.add_class::<PyMessage>()?;

    m.add("LightstreamError", m.py().get_type::<LightstreamError>())?;
    m.add("FormatError", m.py().get_type::<FormatError>())?;
    m.add("TransportError", m.py().get_type::<TransportError>())?;
    m.add("ProtocolError", m.py().get_type::<ProtocolError>())?;

    Ok(())
}
