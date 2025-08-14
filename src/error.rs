//! # I/O Errors
//!
//! Unified error type for all reader/writer operations in Lightstream.
//!
//! Covers I/O failures, type mismatches, schema violations, compression errors,
//! and other protocol-level inconsistencies. Conversion impls are provided for
//! common error sources so that encoders/decoders can propagate errors directly.

use std::string::FromUtf8Error;
use std::{error, fmt, io};

/// Unified error type for all I/O operations.
#[derive(Debug)]
pub enum IoError {
    /// Underlying I/O failure (write error, flush error, file system error).
    Io(io::Error),

    /// Input table or column contains unsupported or mismatched types.
    UnsupportedType(String),

    /// Null mask or definition level inconsistency detected.
    NullMaskInconsistent(String),

    /// Malformed or invalid input data (UTF-8 error, bounds error, etc.).
    InputDataError(String),

    /// Metadata or schema violation.
    Metadata(String),

    /// Compression codec error.
    Compression(String),

    /// Internal logic error (should never occur).
    Internal(String),

    /// Data formatting error.
    Format(String),

    /// Unsupported encoding error.
    UnsupportedEncoding(String),
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IoError::Io(e) => write!(f, "I/O error: {}", e),
            IoError::UnsupportedType(s) => write!(f, "Unsupported type: {}", s),
            IoError::NullMaskInconsistent(s) => write!(f, "Null mask inconsistency: {}", s),
            IoError::InputDataError(s) => write!(f, "Data error: {}", s),
            IoError::Metadata(s) => write!(f, "Metadata error: {}", s),
            IoError::Compression(s) => write!(f, "Compression error: {}", s),
            IoError::Internal(s) => write!(f, "Internal error: {}", s),
            IoError::Format(s) => write!(f, "Formatting error: {}", s),
            IoError::UnsupportedEncoding(s) => write!(f, "Unsupported encoding error: {}", s),
        }
    }
}

impl error::Error for IoError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            IoError::Io(e) => Some(e),
            _ => None,
        }
    }
}

// Conversions for error handling

impl From<io::Error> for IoError {
    fn from(e: io::Error) -> Self {
        IoError::Io(e)
    }
}

impl From<FromUtf8Error> for IoError {
    fn from(e: FromUtf8Error) -> Self {
        IoError::InputDataError(e.to_string())
    }
}

// Zstd error handling is done manually in the compression.rs file

#[cfg(feature = "snappy")]
impl From<snap::Error> for IoError {
    fn from(e: snap::Error) -> Self {
        IoError::Compression(format!("Snappy: {e}"))
    }
}
