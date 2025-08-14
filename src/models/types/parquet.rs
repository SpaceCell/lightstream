//! # Arrow <-> Parquet Type Mapping
//!
//! This module provides conversions between Arrow/Minarrow types and Parquet physical/logical
//! types as defined in the Parquet specification.  
//! It's used during encoding and decoding of Parquet files to ensure type-safe
//! interoperability.

use crate::error::IoError;
#[cfg(feature = "datetime")]
use minarrow::TimeUnit;
use minarrow::{ArrowType, ffi::arrow_dtype::CategoricalIndexType};

/// Parquet physical types as defined in `parquet.thrift`.
///
/// These represent the low-level storage format for values in Parquet
/// files, independent of higher-level logical annotations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParquetPhysicalType {
    /// Bitpacked Boolean
    Boolean = 0,
    /// 32-bit signed integer.
    Int32 = 1,
    /// 64-bit signed integer.
    Int64 = 2,
    /// 32-bit IEEE floating point.
    Float = 3,
    /// 64-bit IEEE floating point.
    Double = 4,
    /// Variable-length byte array (used for strings and binary data).
    ByteArray = 6,
}

impl ParquetPhysicalType {
    /// Return the Parquet `i32` type ID corresponding to this physical type.
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// Convert from a Parquet `i32` type ID into a [`ParquetPhysicalType`].
    ///
    /// Returns `None` if the ID does not match a known type.
    pub fn from_i32(val: i32) -> Option<Self> {
        match val {
            0 => Some(Self::Boolean),
            1 => Some(Self::Int32),
            2 => Some(Self::Int64),
            3 => Some(Self::Float),
            4 => Some(Self::Double),
            6 => Some(Self::ByteArray),
            _ => None,
        }
    }
}

/// Parquet logical (or "converted") types as defined in `parquet.thrift`.
///
/// These annotate a physical type with higher-level semantics,
/// e.g. `Utf8` over a `ByteArray` or `Date32` over an `Int32`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParquetLogicalType {
    /// No logical annotation.
    NoneType,
    /// UTF-8 encoded string.
    Utf8,
    /// 32-bit date - days since epoch
    #[cfg(feature = "datetime")]
    Date32,
    /// 64-bit date - milliseconds since epoch
    #[cfg(feature = "datetime")]
    Date64,
    /// 64-bit timestamp - milliseconds since epoch
    #[cfg(feature = "datetime")]
    TimestampMillis,
    /// 64-bit timestamp - microseconds since epoch
    #[cfg(feature = "datetime")]
    TimestampMicros,
    /// 64-bit timestamp - nanoseconds since epoch
    #[cfg(feature = "datetime")]
    TimestampNanos,
    /// 32-bit time - milliseconds since midnight
    #[cfg(feature = "datetime")]
    TimeMillis,
    /// 32-bit time - microseconds since midnight
    #[cfg(feature = "datetime")]
    TimeMicros,
    /// 64-bit time - nanoseconds since midnight
    #[cfg(feature = "datetime")]
    TimeNanos,
    /// Integer type with specified bit width and sign.
    IntType {
        /// Number of bits (8, 16, 32, 64).
        bit_width: u8,
        /// Whether the type is signed (`true`) or unsigned (`false`).
        is_signed: bool,
    },
}

impl ParquetLogicalType {
    /// Convert from Parquet `ConvertedType` (legacy logical type IDs).
    ///
    /// Returns `None` if the ID is unsupported or maps to a type that is not
    /// handled (e.g. `MAP`, `DECIMAL`, `LIST`).
    pub fn from_converted_type(id: Option<i32>) -> Option<Self> {
        match id {
            None => None,
            Some(0) => None, // NONE
            Some(1) => Some(ParquetLogicalType::Utf8),
            Some(2) => None, // MAP (unsupported)
            Some(3) => None, // MAP_KEY_VALUE (unsupported)
            #[cfg(feature = "datetime")]
            Some(4) => Some(ParquetLogicalType::Date32),
            #[cfg(feature = "datetime")]
            Some(5) => Some(ParquetLogicalType::Date64),
            #[cfg(feature = "datetime")]
            Some(6) => Some(ParquetLogicalType::TimeMillis),
            #[cfg(feature = "datetime")]
            Some(7) => Some(ParquetLogicalType::TimeMicros),
            Some(8) => None, // UINT_8 (unsupported legacy alias)
            #[cfg(feature = "datetime")]
            Some(9) => Some(ParquetLogicalType::TimestampMillis),
            #[cfg(feature = "datetime")]
            Some(10) => Some(ParquetLogicalType::TimestampMicros),
            Some(11) => Some(ParquetLogicalType::IntType {
                bit_width: 8,
                is_signed: true,
            }),
            Some(12) => Some(ParquetLogicalType::IntType {
                bit_width: 16,
                is_signed: true,
            }),
            Some(13) => Some(ParquetLogicalType::IntType {
                bit_width: 32,
                is_signed: true,
            }),
            Some(14) => Some(ParquetLogicalType::IntType {
                bit_width: 64,
                is_signed: true,
            }),
            Some(15) => Some(ParquetLogicalType::IntType {
                bit_width: 8,
                is_signed: false,
            }),
            Some(16) => Some(ParquetLogicalType::IntType {
                bit_width: 16,
                is_signed: false,
            }),
            Some(17) => Some(ParquetLogicalType::IntType {
                bit_width: 32,
                is_signed: false,
            }),
            Some(18) => Some(ParquetLogicalType::IntType {
                bit_width: 64,
                is_signed: false,
            }),
            Some(19) => None, // LIST (unsupported)
            Some(20) => None, // DECIMAL (unsupported)
            Some(21) => None, // ENUM (unsupported)
            Some(22) => None, // UTF8 (duplicate, handled above)
            Some(23) => None, // BSON
            Some(24) => None, // JSON
            _ => None,
        }
    }
}

/// Parquet page encoding as per the official Parquet specification.
///
/// We only implement a small subset of these, but they are here for completeness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParquetEncoding {
    /// Basic uncompressed binary encoding.
    Plain = 0,
    /// Deprecated in favour of RLE_DICTIONARY.
    PlainDictionary = 2,
    /// Run-Length Encoding (used for levels and dictionary indices).
    Rle = 3,
    /// Packs values together into minimal bits (mainly for levels, rarely used directly now).
    BitPacked = 4,
    /// Delta encoding for integer values.
    DeltaBinaryPacked = 5,
    /// Delta encoding for byte array lengths.
    DeltaLengthByteArray = 6,
    /// Delta encoding for prefixes in byte arrays (e.g., sorted strings).
    DeltaByteArray = 7,
    /// Current standard for dictionary encoding.
    RleDictionary = 8,
    /// Can lead to better ratio and speed when a compression algorithm is used afterwards.
    ByteStreamSplit = 9,
}

impl ParquetEncoding {
    /// Convert from Parquet i32 encoding ID.
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::Plain),
            2 => Some(Self::PlainDictionary),
            3 => Some(Self::Rle),
            4 => Some(Self::BitPacked),
            5 => Some(Self::DeltaBinaryPacked),
            6 => Some(Self::DeltaLengthByteArray),
            7 => Some(Self::DeltaByteArray),
            8 => Some(Self::RleDictionary),
            9 => Some(Self::ByteStreamSplit),
            _ => None,
        }
    }

    /// Returns the canonical Parquet i32 encoding ID.
    pub fn to_i32(self) -> i32 {
        self as i32
    }
}

/// Authoritative mapping from ArrowType to Parquet physical/logical.
/// Returns IoError for any unsupported/disabled variant.
pub(crate) fn arrow_type_to_parquet(
    ty: &ArrowType,
) -> Result<(ParquetPhysicalType, ParquetLogicalType), IoError> {
    match ty {
        ArrowType::Boolean => Ok((ParquetPhysicalType::Boolean, ParquetLogicalType::NoneType)),
        #[cfg(feature = "extended_numeric_types")]
        ArrowType::Int8 => Ok((
            ParquetPhysicalType::Int32,
            ParquetLogicalType::IntType {
                bit_width: 8,
                is_signed: true,
            },
        )),
        #[cfg(feature = "extended_numeric_types")]
        ArrowType::Int16 => Ok((
            ParquetPhysicalType::Int32,
            ParquetLogicalType::IntType {
                bit_width: 16,
                is_signed: true,
            },
        )),
        ArrowType::Int32 => Ok((
            ParquetPhysicalType::Int32,
            ParquetLogicalType::IntType {
                bit_width: 32,
                is_signed: true,
            },
        )),
        ArrowType::Int64 => Ok((
            ParquetPhysicalType::Int64,
            ParquetLogicalType::IntType {
                bit_width: 64,
                is_signed: true,
            },
        )),
        #[cfg(feature = "extended_numeric_types")]
        ArrowType::UInt8 => Ok((
            ParquetPhysicalType::Int32,
            ParquetLogicalType::IntType {
                bit_width: 8,
                is_signed: false,
            },
        )),
        #[cfg(feature = "extended_numeric_types")]
        ArrowType::UInt16 => Ok((
            ParquetPhysicalType::Int32,
            ParquetLogicalType::IntType {
                bit_width: 16,
                is_signed: false,
            },
        )),
        ArrowType::UInt32 => Ok((
            ParquetPhysicalType::Int32,
            ParquetLogicalType::IntType {
                bit_width: 32,
                is_signed: false,
            },
        )),
        ArrowType::UInt64 => Ok((
            ParquetPhysicalType::Int64,
            ParquetLogicalType::IntType {
                bit_width: 64,
                is_signed: false,
            },
        )),
        ArrowType::Dictionary(CategoricalIndexType::UInt32) => {
            // TODO: Fix for non Cat32
            Ok((ParquetPhysicalType::Int32, ParquetLogicalType::NoneType))
        }
        ArrowType::Float32 => Ok((ParquetPhysicalType::Float, ParquetLogicalType::NoneType)),
        ArrowType::Float64 => Ok((ParquetPhysicalType::Double, ParquetLogicalType::NoneType)),
        ArrowType::String => Ok((ParquetPhysicalType::ByteArray, ParquetLogicalType::Utf8)),
        #[cfg(feature = "large_string")]
        ArrowType::LargeString => Ok((ParquetPhysicalType::ByteArray, ParquetLogicalType::Utf8)),
        #[cfg(feature = "datetime")]
        ArrowType::Date32 => Ok((ParquetPhysicalType::Int32, ParquetLogicalType::Date32)),
        #[cfg(feature = "datetime")]
        ArrowType::Date64 => Ok((ParquetPhysicalType::Int64, ParquetLogicalType::Date64)),
        #[cfg(feature = "datetime")]
        ArrowType::Timestamp(unit) => match unit {
            TimeUnit::Milliseconds => Ok((
                ParquetPhysicalType::Int64,
                ParquetLogicalType::TimestampMillis,
            )),
            TimeUnit::Microseconds => Ok((
                ParquetPhysicalType::Int64,
                ParquetLogicalType::TimestampMicros,
            )),
            TimeUnit::Nanoseconds => Ok((
                ParquetPhysicalType::Int64,
                ParquetLogicalType::TimestampNanos,
            )),
            TimeUnit::Seconds => Ok((
                ParquetPhysicalType::Int64,
                ParquetLogicalType::TimestampMillis,
            )), // best-effort
            TimeUnit::Days => Ok((ParquetPhysicalType::Int64, ParquetLogicalType::Date64)),
        },
        #[cfg(feature = "datetime")]
        ArrowType::Time32(unit) => match unit {
            TimeUnit::Milliseconds => {
                Ok((ParquetPhysicalType::Int32, ParquetLogicalType::TimeMillis))
            }
            TimeUnit::Microseconds => {
                Ok((ParquetPhysicalType::Int32, ParquetLogicalType::TimeMicros))
            }
            TimeUnit::Nanoseconds => {
                Ok((ParquetPhysicalType::Int32, ParquetLogicalType::TimeNanos))
            }
            TimeUnit::Seconds => Ok((ParquetPhysicalType::Int32, ParquetLogicalType::TimeMillis)), /* best-effort */
            TimeUnit::Days => Ok((ParquetPhysicalType::Int32, ParquetLogicalType::Date32)),
        },
        #[cfg(feature = "datetime")]
        ArrowType::Time64(unit) => match unit {
            TimeUnit::Milliseconds => {
                Ok((ParquetPhysicalType::Int64, ParquetLogicalType::TimeMillis))
            }
            TimeUnit::Microseconds => {
                Ok((ParquetPhysicalType::Int64, ParquetLogicalType::TimeMicros))
            }
            TimeUnit::Nanoseconds => {
                Ok((ParquetPhysicalType::Int64, ParquetLogicalType::TimeNanos))
            }
            TimeUnit::Seconds => Ok((ParquetPhysicalType::Int64, ParquetLogicalType::TimeMillis)), /* best-effort */
            TimeUnit::Days => Ok((ParquetPhysicalType::Int64, ParquetLogicalType::Date64)),
        },
        ArrowType::Null => Err(IoError::UnsupportedType(
            "Null type is not supported".into(),
        )),
        #[cfg(feature = "datetime")]
        ArrowType::Duration32(_) => panic!("Duration does not map to a parquet type."),
        #[cfg(feature = "datetime")]
        ArrowType::Duration64(_) => panic!("Duration does not map to a parquet type."),
        #[cfg(feature = "datetime")]
        ArrowType::Interval(_) => panic!("Interval does not map to a parquet type."),
        #[cfg(all(feature = "extended_categorical", feature = "extended_numeric_types"))]
        &minarrow::ArrowType::Dictionary(
            minarrow::ffi::arrow_dtype::CategoricalIndexType::UInt8,
        )
        | &minarrow::ArrowType::Dictionary(
            minarrow::ffi::arrow_dtype::CategoricalIndexType::UInt16,
        )
        | &minarrow::ArrowType::Dictionary(
            minarrow::ffi::arrow_dtype::CategoricalIndexType::UInt64,
        ) => panic!(),
    }
}

/// Parquet -> Arrow mapping
/// Bit width and sign comes from IntType logical type.
pub(crate) fn parquet_to_arrow_type(
    physical: ParquetPhysicalType,
    logical: Option<ParquetLogicalType>,
) -> Result<ArrowType, IoError> {
    match (physical, logical.clone()) {
        (ParquetPhysicalType::Boolean, _) => Ok(ArrowType::Boolean),

        // Signed/unsigned INT32 and INT64 mappings
        #[cfg(feature = "extended_numeric_types")]
        (
            ParquetPhysicalType::Int32,
            Some(ParquetLogicalType::IntType {
                bit_width: 8,
                is_signed: true,
            }),
        ) => Ok(ArrowType::Int8),
        #[cfg(feature = "extended_numeric_types")]
        (
            ParquetPhysicalType::Int32,
            Some(ParquetLogicalType::IntType {
                bit_width: 16,
                is_signed: true,
            }),
        ) => Ok(ArrowType::Int16),
        (
            ParquetPhysicalType::Int32,
            Some(ParquetLogicalType::IntType {
                bit_width: 32,
                is_signed: true,
            }),
        ) => Ok(ArrowType::Int32),
        #[cfg(feature = "extended_numeric_types")]
        (
            ParquetPhysicalType::Int32,
            Some(ParquetLogicalType::IntType {
                bit_width: 8,
                is_signed: false,
            }),
        ) => Ok(ArrowType::UInt8),
        #[cfg(feature = "extended_numeric_types")]
        (
            ParquetPhysicalType::Int32,
            Some(ParquetLogicalType::IntType {
                bit_width: 16,
                is_signed: false,
            }),
        ) => Ok(ArrowType::UInt16),
        (
            ParquetPhysicalType::Int32,
            Some(ParquetLogicalType::IntType {
                bit_width: 32,
                is_signed: false,
            }),
        ) => Ok(ArrowType::UInt32),
        (
            ParquetPhysicalType::Int64,
            Some(ParquetLogicalType::IntType {
                bit_width: 64,
                is_signed: true,
            }),
        ) => Ok(ArrowType::Int64),
        (
            ParquetPhysicalType::Int64,
            Some(ParquetLogicalType::IntType {
                bit_width: 64,
                is_signed: false,
            }),
        ) => Ok(ArrowType::UInt64),

        // Fallback for non-annotated int types
        (ParquetPhysicalType::Int32, None) => Ok(ArrowType::Int32),
        (ParquetPhysicalType::Int64, None) => Ok(ArrowType::Int64),

        // Dates, times, timestamps
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int32, Some(ParquetLogicalType::Date32)) => Ok(ArrowType::Date32),
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int64, Some(ParquetLogicalType::Date64)) => Ok(ArrowType::Date64),
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int64, Some(ParquetLogicalType::TimestampMillis)) => {
            Ok(ArrowType::Timestamp(TimeUnit::Milliseconds))
        }
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int64, Some(ParquetLogicalType::TimestampMicros)) => {
            Ok(ArrowType::Timestamp(TimeUnit::Microseconds))
        }
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int64, Some(ParquetLogicalType::TimestampNanos)) => {
            Ok(ArrowType::Timestamp(TimeUnit::Nanoseconds))
        }
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int32, Some(ParquetLogicalType::TimeMillis)) => {
            Ok(ArrowType::Time32(TimeUnit::Milliseconds))
        }
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int32, Some(ParquetLogicalType::TimeMicros)) => {
            Ok(ArrowType::Time32(TimeUnit::Microseconds))
        }
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int32, Some(ParquetLogicalType::TimeNanos)) => {
            Ok(ArrowType::Time32(TimeUnit::Nanoseconds))
        }
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int64, Some(ParquetLogicalType::TimeMillis)) => {
            Ok(ArrowType::Time64(TimeUnit::Milliseconds))
        }
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int64, Some(ParquetLogicalType::TimeMicros)) => {
            Ok(ArrowType::Time64(TimeUnit::Microseconds))
        }
        #[cfg(feature = "datetime")]
        (ParquetPhysicalType::Int64, Some(ParquetLogicalType::TimeNanos)) => {
            Ok(ArrowType::Time64(TimeUnit::Nanoseconds))
        }

        // Floats
        (ParquetPhysicalType::Float, _) => Ok(ArrowType::Float32),
        (ParquetPhysicalType::Double, _) => Ok(ArrowType::Float64),

        // Strings - always logical UTF8/Utf8
        #[cfg(not(feature = "large_string"))]
        (ParquetPhysicalType::ByteArray, Some(ParquetLogicalType::Utf8)) => Ok(ArrowType::String),
        #[cfg(feature = "large_string")]
        (ParquetPhysicalType::ByteArray, Some(ParquetLogicalType::Utf8)) => {
            Ok(ArrowType::LargeString)
        }

        // Fallback -- treat byte array without logical utf8 as unsupported
        (ParquetPhysicalType::ByteArray, None) => {
            Err(IoError::UnsupportedType("Binary not supported".into()))
        }

        _ => Err(IoError::UnsupportedType(format!(
            "Parquet type {:?} + logical {:?} not supported",
            physical, logical
        ))),
    }
}
