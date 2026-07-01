// Copyright Peter G. Bower 2025-2026.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! # JSON value reference
//!
//! A borrowed reference to one JSON scalar, yielded by the row decoder and
//! the [`JsonInterface`](crate::models::interfaces::json::JsonInterface).
//! The conversion methods read it into a column's target type, parsing a
//! quoted string when the column declares it.

#[cfg(feature = "datetime")]
use minarrow::enums::time_units::TimeUnit;
#[cfg(feature = "datetime")]
use minarrow::parse_iso8601_utc;

#[cfg(feature = "datetime")]
use crate::models::interfaces::json::schema::ReadAs;

/// Borrowed reference to a JSON scalar value. Strings borrow from the input
/// buffer wherever the decoder can supply them that way.
#[derive(Debug)]
pub enum JsonValueRef<'a> {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Str(&'a str),
}

impl<'a> JsonValueRef<'a> {
    /// Read this value as an `i64`. With `from_string`, parse it from a
    /// JSON string; otherwise accept a JSON integer.
    pub fn to_i64(&self, from_string: bool) -> Result<i64, String> {
        if from_string {
            let JsonValueRef::Str(s) = self else {
                return Err(format!("expected string for integer, got {self:?}"));
            };
            return s.parse::<i64>().map_err(|e| format!("not an integer: {e}"));
        }
        match self {
            JsonValueRef::I64(v) => Ok(*v),
            JsonValueRef::U64(v) => {
                i64::try_from(*v).map_err(|_| format!("not an integer: {self:?}"))
            }
            _ => Err(format!("not an integer: {self:?}")),
        }
    }

    /// Read this value as a `u64`. With `from_string`, parse it from a
    /// JSON string; otherwise accept a JSON integer.
    pub fn to_u64(&self, from_string: bool) -> Result<u64, String> {
        if from_string {
            let JsonValueRef::Str(s) = self else {
                return Err(format!("expected string for unsigned integer, got {self:?}"));
            };
            return s.parse::<u64>().map_err(|e| format!("not an unsigned integer: {e}"));
        }
        match self {
            JsonValueRef::U64(v) => Ok(*v),
            JsonValueRef::I64(v) => {
                u64::try_from(*v).map_err(|_| format!("not an unsigned integer: {self:?}"))
            }
            _ => Err(format!("not an unsigned integer: {self:?}")),
        }
    }

    /// Read this value as an `f64`. With `from_string`, parse it from a
    /// JSON string; otherwise accept a JSON number.
    pub fn to_f64(&self, from_string: bool) -> Result<f64, String> {
        if from_string {
            let JsonValueRef::Str(s) = self else {
                return Err(format!("expected string for float, got {self:?}"));
            };
            return s.parse::<f64>().map_err(|e| format!("not a number: {e}"));
        }
        match self {
            JsonValueRef::F64(v) => Ok(*v),
            JsonValueRef::I64(v) => Ok(*v as f64),
            JsonValueRef::U64(v) => Ok(*v as f64),
            _ => Err(format!("not a number: {self:?}")),
        }
    }

    /// Read this value as a `bool`. With `from_string`, parse the textual
    /// and `0`/`1` forms; otherwise accept a JSON boolean.
    pub fn to_bool(&self, from_string: bool) -> Result<bool, String> {
        if from_string {
            let JsonValueRef::Str(s) = self else {
                return Err(format!("expected string for bool, got {self:?}"));
            };
            return match *s {
                "true" | "True" | "TRUE" | "1" => Ok(true),
                "false" | "False" | "FALSE" | "0" => Ok(false),
                other => Err(format!("not a bool: '{other}'")),
            };
        }
        match self {
            JsonValueRef::Bool(b) => Ok(*b),
            _ => Err(format!("not a bool: {self:?}")),
        }
    }

    /// Borrow this value as a `&str`. Errors on any non-string value.
    pub fn to_str(&self) -> Result<&'a str, String> {
        match self {
            JsonValueRef::Str(s) => Ok(s),
            _ => Err(format!("expected string, got {self:?}")),
        }
    }

    /// Convert this value with a temporal column's [`ReadAs`]. `Datetime`
    /// parses an ISO 8601 string to `unit`. `Number` parses a quoted
    /// integer. Otherwise an `I64` is taken as already in the column's unit,
    /// since the receive clock arrives pre-scaled and a wire integer is read
    /// in place.
    #[cfg(feature = "datetime")]
    pub fn to_datetime(&self, unit: TimeUnit, read_as: &ReadAs) -> Result<i64, String> {
        match read_as {
            ReadAs::Datetime => match self {
                JsonValueRef::Str(s) => {
                    parse_iso8601_utc(s, unit).ok_or_else(|| format!("not a datetime: '{s}'"))
                }
                JsonValueRef::I64(v) => Ok(*v),
                _ => Err(format!("expected string for datetime, got {self:?}")),
            },
            ReadAs::Number => {
                let JsonValueRef::Str(s) = self else {
                    return Err(format!("expected string for integer, got {self:?}"));
                };
                s.parse::<i64>().map_err(|e| format!("not an integer: {e}"))
            }
            _ => self.to_i64(false),
        }
    }
}
