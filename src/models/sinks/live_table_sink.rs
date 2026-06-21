//! # Live-table sink
//!
//! The live counterpart of [`TableSink`](crate::models::sinks::table_sink).
//! Records push into a set of [`LBuffer`]-backed columns one row at a time,
//! and any number of consumers read the live [`Table`] while it is written.
//! The table wraps the buffers zero-copy via `Buffer::from_lbuffer`, and a
//! row becomes visible once its last column's atomic length advances.
//!
//! [`push_record`](LiveTableSink::push_record) takes one record of a decoded
//! frame and appends all of its columns before returning, converting each
//! value into the column type per its [`ReadAs`]. Readable rows advance only
//! over complete records. When a column write fails partway through a
//! record, [`PushRecordError::partial`] reports the row as incomplete and
//! the caller [`roll`](LiveTableSink::roll)s, so readers never see past the
//! last complete row.
//!
//! The caller owns the batch policy. It checks
//! [`is_full`](LiveTableSink::is_full) and decides when to roll. A roll seals
//! every buffer, freezing the published lengths, opens fresh ones, and
//! returns the new live table. The sealed batch lives on in whichever
//! `Arc<Table>` handle consumers hold.

use std::io;
use std::sync::Arc;

#[cfg(feature = "datetime")]
use minarrow::enums::time_units::TimeUnit;
#[cfg(feature = "datetime")]
use minarrow::DatetimeArray;
use minarrow::{
    Array, ArrowType, BooleanArray, Field, FieldArray, FloatArray, IntegerArray, LBuffer, Table,
};
use minarrow::StringArray;
use minarrow::{ByteSize, CategoricalArray, Vec64};

use crate::models::decoders::json::value::JsonValueRef;
use crate::models::frames::json::JsonRecord;
use crate::models::interfaces::json::schema::ReadAs;

/// Per-row byte reservation for the first batch's string data region. Later
/// batches size from the previous batch's measured footprint via
/// [`ByteSize::est_bytes`], so this figure only affects batch one.
const DEFAULT_STRING_BYTES_PER_ROW: usize = 32;

/// A record push failure.
///
/// `partial` is true when at least one column wrote before the failure,
/// leaving the row incomplete - the caller rolls so readers never see past
/// the last complete row. When the failure comes before the first column
/// write, the batch is intact and the caller skips the record and
/// continues.
#[derive(Debug)]
pub struct PushRecordError {
    pub partial: bool,
    pub reason: String,
}

impl std::fmt::Display for PushRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl std::error::Error for PushRecordError {}

/// A typed live column pairs the [`LBuffer`] write handles with the
/// conversion the column's [`ReadAs`] declares. It covers every array type
/// the crate's feature set builds, and each buffer wraps zero-copy into its
/// array.
enum LiveColumn {
    Int32 {
        buf: LBuffer<i32>,
        from_string: bool,
    },
    Int64 {
        buf: LBuffer<i64>,
        from_string: bool,
    },
    UInt32 {
        buf: LBuffer<u32>,
        from_string: bool,
    },
    UInt64 {
        buf: LBuffer<u64>,
        from_string: bool,
    },
    Float32 {
        buf: LBuffer<f32>,
        from_string: bool,
    },
    Float64 {
        buf: LBuffer<f64>,
        from_string: bool,
    },
    /// Stores Date32, Time32 and Duration32 as i32 offsets in `unit`,
    /// parsed from the wire according to the column's `read_as`.
    #[cfg(feature = "datetime")]
    Datetime32 {
        buf: LBuffer<i32>,
        unit: TimeUnit,
        read_as: ReadAs,
    },
    /// Stores Timestamp, Date64, Time64 and Duration64 as i64 offsets in
    /// `unit`, parsed from the wire according to the column's `read_as`.
    #[cfg(feature = "datetime")]
    Datetime64 {
        buf: LBuffer<i64>,
        unit: TimeUnit,
        read_as: ReadAs,
    },
    /// Bit-packed Boolean over the masked channel, where `push` writes a
    /// true bit and `push_null` a false bit. The validity tail atomic
    /// publishes the bit count.
    Bool {
        buf: LBuffer<u8, true>,
        from_string: bool,
    },
    /// UTF-8 rows. Each row's bytes are appended with `push_slice`, and a
    /// following offset push publishes the row.
    String32 {
        offsets: LBuffer<u32>,
        bytes: LBuffer<u8>,
    },
    #[cfg(feature = "large_string")]
    String64 {
        offsets: LBuffer<u64>,
        bytes: LBuffer<u8>,
    },
    /// Categorical column backed by a fixed dictionary declared via
    /// [`ReadAs::Dictionary`]. The codes stream live and the dictionary
    /// does not change.
    #[cfg(feature = "default_categorical_8")]
    Categorical8 {
        codes: LBuffer<u8>,
        dictionary: Vec64<String>,
    },
    #[cfg(any(not(feature = "default_categorical_8"), feature = "extended_categorical"))]
    Categorical32 {
        codes: LBuffer<u32>,
        dictionary: Vec64<String>,
    },
}

impl LiveColumn {
    fn seal(&mut self) {
        match self {
            LiveColumn::Int32 { buf, .. } => buf.seal(),
            LiveColumn::Int64 { buf, .. } => buf.seal(),
            LiveColumn::UInt32 { buf, .. } => buf.seal(),
            LiveColumn::UInt64 { buf, .. } => buf.seal(),
            LiveColumn::Float32 { buf, .. } => buf.seal(),
            LiveColumn::Float64 { buf, .. } => buf.seal(),
            #[cfg(feature = "datetime")]
            LiveColumn::Datetime32 { buf, .. } => buf.seal(),
            #[cfg(feature = "datetime")]
            LiveColumn::Datetime64 { buf, .. } => buf.seal(),
            LiveColumn::Bool { buf, .. } => buf.seal(),
            LiveColumn::String32 { offsets, bytes } => {
                bytes.seal();
                offsets.seal();
            }
            #[cfg(feature = "large_string")]
            LiveColumn::String64 { offsets, bytes } => {
                bytes.seal();
                offsets.seal();
            }
            #[cfg(feature = "default_categorical_8")]
            LiveColumn::Categorical8 { codes, .. } => codes.seal(),
            #[cfg(any(not(feature = "default_categorical_8"), feature = "extended_categorical"))]
            LiveColumn::Categorical32 { codes, .. } => codes.seal(),
        }
    }
}

/// Live LBuffer-backed table sink.
///
/// Construct with the destination schema and a per-batch row capacity,
/// hand the [`live_table`](Self::live_table) to consumers, then push
/// records as frames decode. The caller owns batch policy, checking
/// [`is_full`](Self::is_full) after each push and calling
/// [`roll`](Self::roll) at the boundary to forward the returned table to
/// consumers.
pub struct LiveTableSink {
    columns: Vec<LiveColumn>,
    fields: Vec<Field>,
    read_as: Vec<ReadAs>,
    name: String,
    rows_per_batch: usize,
    rows_in_batch: usize,
}

impl LiveTableSink {
    /// Construct a sink for the destination `fields` and their per-column
    /// `read_as` forms, sizing each batch to `rows_per_batch` rows.
    /// `rows_per_batch` controls batch memory granularity rather than
    /// latency, and tens of thousands is typical. Fails on a schema type
    /// outside the live column set or a categorical column without a
    /// [`ReadAs::Dictionary`].
    pub fn new(
        fields: &[Field],
        read_as: &[ReadAs],
        rows_per_batch: usize,
        name: impl Into<String>,
    ) -> io::Result<Self> {
        if rows_per_batch == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rows_per_batch must be non-zero",
            ));
        }
        if fields.len() != read_as.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fields and read_as differ in length",
            ));
        }
        let fields = fields.to_vec();
        let read_as = read_as.to_vec();
        // Size the first batch's string regions from the default. Later
        // batches re-size from measured bytes on each roll.
        let initial_caps: Vec<usize> = fields
            .iter()
            .map(|f| match &f.dtype {
                ArrowType::String => rows_per_batch.saturating_mul(DEFAULT_STRING_BYTES_PER_ROW),
                #[cfg(feature = "large_string")]
                ArrowType::LargeString => {
                    rows_per_batch.saturating_mul(DEFAULT_STRING_BYTES_PER_ROW)
                }
                _ => 0,
            })
            .collect();
        let columns = Self::build_live_columns(&fields, &read_as, rows_per_batch, &initial_caps)?;
        Ok(Self { columns, fields, read_as, name: name.into(), rows_per_batch, rows_in_batch: 0 })
    }

    /// Build the typed live columns for one batch from the schema fields and
    /// their read forms.
    fn build_live_columns(
        fields: &[Field],
        read_as: &[ReadAs],
        rows_per_batch: usize,
        string_byte_caps: &[usize],
    ) -> io::Result<Vec<LiveColumn>> {
        fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let read_as = &read_as[i];
                let from_string = *read_as == ReadAs::Number;
                match &f.dtype {
                    ArrowType::Int32 => Ok(LiveColumn::Int32 {
                        buf: LBuffer::with_capacity(rows_per_batch),
                        from_string,
                    }),
                    ArrowType::Int64 => Ok(LiveColumn::Int64 {
                        buf: LBuffer::with_capacity(rows_per_batch),
                        from_string,
                    }),
                    ArrowType::UInt32 => Ok(LiveColumn::UInt32 {
                        buf: LBuffer::with_capacity(rows_per_batch),
                        from_string,
                    }),
                    ArrowType::UInt64 => Ok(LiveColumn::UInt64 {
                        buf: LBuffer::with_capacity(rows_per_batch),
                        from_string,
                    }),
                    ArrowType::Float32 => Ok(LiveColumn::Float32 {
                        buf: LBuffer::with_capacity(rows_per_batch),
                        from_string,
                    }),
                    ArrowType::Float64 => Ok(LiveColumn::Float64 {
                        buf: LBuffer::with_capacity(rows_per_batch),
                        from_string,
                    }),
                    ArrowType::Boolean => Ok(LiveColumn::Bool {
                        buf: LBuffer::<u8>::with_capacity_masked(rows_per_batch),
                        from_string: *read_as == ReadAs::Bool,
                    }),
                    // i32-storage temporals. Date32 counts days. Time32 and
                    // Duration32 carry their declared unit.
                    #[cfg(feature = "datetime")]
                    ArrowType::Date32 => Ok(LiveColumn::Datetime32 {
                        buf: LBuffer::with_capacity(rows_per_batch),
                        unit: TimeUnit::Days,
                        read_as: read_as.clone(),
                    }),
                    #[cfg(feature = "datetime")]
                    ArrowType::Time32(unit) | ArrowType::Duration32(unit) => {
                        Ok(LiveColumn::Datetime32 {
                            buf: LBuffer::with_capacity(rows_per_batch),
                            unit: *unit,
                            read_as: read_as.clone(),
                        })
                    }
                    // i64-storage temporals. Date64 counts milliseconds.
                    #[cfg(feature = "datetime")]
                    ArrowType::Date64 => Ok(LiveColumn::Datetime64 {
                        buf: LBuffer::with_capacity(rows_per_batch),
                        unit: TimeUnit::Milliseconds,
                        read_as: read_as.clone(),
                    }),
                    #[cfg(feature = "datetime")]
                    ArrowType::Timestamp(unit, _) => Ok(LiveColumn::Datetime64 {
                        buf: LBuffer::with_capacity(rows_per_batch),
                        unit: *unit,
                        read_as: read_as.clone(),
                    }),
                    #[cfg(feature = "datetime")]
                    ArrowType::Time64(unit) | ArrowType::Duration64(unit) => {
                        Ok(LiveColumn::Datetime64 {
                            buf: LBuffer::with_capacity(rows_per_batch),
                            unit: *unit,
                            read_as: read_as.clone(),
                        })
                    }
                    ArrowType::String => {
                        let mut offsets = LBuffer::<u32>::with_capacity(rows_per_batch + 1);
                        offsets.push(0).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "offsets buffer rejected its base")
                        })?;
                        Ok(LiveColumn::String32 {
                            offsets,
                            bytes: LBuffer::with_capacity(string_byte_caps[i]),
                        })
                    }
                    #[cfg(feature = "large_string")]
                    ArrowType::LargeString => {
                        let mut offsets = LBuffer::<u64>::with_capacity(rows_per_batch + 1);
                        offsets.push(0).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "offsets buffer rejected its base")
                        })?;
                        Ok(LiveColumn::String64 {
                            offsets,
                            bytes: LBuffer::with_capacity(string_byte_caps[i]),
                        })
                    }
                    ArrowType::Dictionary(idx) => {
                        let ReadAs::Dictionary(values) = read_as else {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!(
                                    "categorical column '{}' needs a ReadAs::Dictionary",
                                    f.name
                                ),
                            ));
                        };
                        if values.is_empty() {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("categorical column '{}' has an empty dictionary", f.name),
                            ));
                        }
                        let mut dictionary = Vec64::new();
                        for value in values {
                            dictionary.push(value.clone());
                        }
                        use minarrow::ffi::arrow_dtype::CategoricalIndexType;
                        match idx {
                            #[cfg(feature = "default_categorical_8")]
                            CategoricalIndexType::UInt8 => Ok(LiveColumn::Categorical8 {
                                codes: LBuffer::with_capacity(rows_per_batch),
                                dictionary,
                            }),
                            #[cfg(any(
                                not(feature = "default_categorical_8"),
                                feature = "extended_categorical"
                            ))]
                            CategoricalIndexType::UInt32 => Ok(LiveColumn::Categorical32 {
                                codes: LBuffer::with_capacity(rows_per_batch),
                                dictionary,
                            }),
                            #[cfg(feature = "extended_categorical")]
                            other => Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!(
                                    "live sink does not support categorical width {other:?} for column '{}'",
                                    f.name
                                ),
                            )),
                        }
                    }
                    other => Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("live sink does not support column '{}' of type {other:?}", f.name),
                    )),
                }
            })
            .collect()
    }

    /// Wrap the current live columns as an `Arc<Table>` - the batch a
    /// consumer reads while records push into it. The declared fields
    /// ride on the table, so consumers see the exact schema dtype.
    pub fn live_table(&self) -> Arc<Table> {
        let cols: Vec<FieldArray> = self
            .columns
            .iter()
            .zip(self.fields.iter())
            .map(|(col, field)| {
                let array = match col {
                    LiveColumn::Int32 { buf, .. } => {
                        Array::from_int32(IntegerArray { data: buf.as_buffer(), null_mask: None })
                    }
                    LiveColumn::Int64 { buf, .. } => {
                        Array::from_int64(IntegerArray { data: buf.as_buffer(), null_mask: None })
                    }
                    LiveColumn::UInt32 { buf, .. } => {
                        Array::from_uint32(IntegerArray { data: buf.as_buffer(), null_mask: None })
                    }
                    LiveColumn::UInt64 { buf, .. } => {
                        Array::from_uint64(IntegerArray { data: buf.as_buffer(), null_mask: None })
                    }
                    LiveColumn::Float32 { buf, .. } => {
                        Array::from_float32(FloatArray { data: buf.as_buffer(), null_mask: None })
                    }
                    LiveColumn::Float64 { buf, .. } => {
                        Array::from_float64(FloatArray { data: buf.as_buffer(), null_mask: None })
                    }
                    #[cfg(feature = "datetime")]
                    LiveColumn::Datetime32 { buf, unit, .. } => {
                        Array::from_datetime_i32(DatetimeArray {
                            data: buf.as_buffer(),
                            null_mask: None,
                            time_unit: *unit,
                        })
                    }
                    #[cfg(feature = "datetime")]
                    LiveColumn::Datetime64 { buf, unit, .. } => {
                        Array::from_datetime_i64(DatetimeArray {
                            data: buf.as_buffer(),
                            null_mask: None,
                            time_unit: *unit,
                        })
                    }
                    LiveColumn::Bool { buf, .. } => {
                        Array::from_bool(BooleanArray::new(buf.as_bitmask(), None))
                    }
                    LiveColumn::String32 { offsets, bytes } => Array::from_string32(StringArray {
                        offsets: offsets.as_buffer(),
                        data: bytes.as_buffer(),
                        null_mask: None,
                    }),
                    #[cfg(feature = "large_string")]
                    LiveColumn::String64 { offsets, bytes } => Array::from_string64(StringArray {
                        offsets: offsets.as_buffer(),
                        data: bytes.as_buffer(),
                        null_mask: None,
                    }),
                    #[cfg(feature = "default_categorical_8")]
                    LiveColumn::Categorical8 { codes, dictionary } => {
                        Array::from_categorical8(CategoricalArray {
                            data: codes.as_buffer(),
                            unique_values: dictionary.clone(),
                            null_mask: None,
                        })
                    }
                    #[cfg(any(
                        not(feature = "default_categorical_8"),
                        feature = "extended_categorical"
                    ))]
                    LiveColumn::Categorical32 { codes, dictionary } => {
                        Array::from_categorical32(CategoricalArray {
                            data: codes.as_buffer(),
                            unique_values: dictionary.clone(),
                            null_mask: None,
                        })
                    }
                };
                FieldArray::new(field.clone(), array)
            })
            .collect();
        Arc::new(Table::new(self.name.clone(), Some(cols)))
    }

    /// Rows pushed into the current batch.
    pub fn rows_in_batch(&self) -> usize {
        self.rows_in_batch
    }

    /// `true` once the current batch has reached `rows_per_batch`. The
    /// caller rolls at this boundary.
    pub fn is_full(&self) -> bool {
        self.rows_in_batch >= self.rows_per_batch
    }

    /// Push one decoded record as a row. Every column is appended before
    /// returning, so readable rows advance only over complete records.
    pub fn push_record(&mut self, record: &JsonRecord<'_, '_>) -> Result<(), PushRecordError> {
        let mut partial = false;
        for (idx, col) in self.columns.iter_mut().enumerate() {
            let value = record
                .value(idx)
                .map_err(|e| PushRecordError { partial, reason: e.to_string() })?;
            push_value(col, value, partial)?;
            partial = true;
        }
        self.rows_in_batch += 1;
        Ok(())
    }

    /// Seal the live batch and open a fresh one, returning the new live
    /// table for the caller to forward to consumers. The sealed batch's
    /// lengths are final. Consumers holding its `Arc<Table>` read it as
    /// an immutable chunk.
    pub fn roll(&mut self) -> io::Result<Arc<Table>> {
        for col in &mut self.columns {
            col.seal();
        }
        // Size each string column's next data region from the batch just
        // sealed, so the reservation tracks the feed's real bytes per row.
        let byte_caps: Vec<usize> = self
            .columns
            .iter()
            .map(|col| match col {
                LiveColumn::String32 { bytes, .. } => bytes.as_buffer().est_bytes(),
                #[cfg(feature = "large_string")]
                LiveColumn::String64 { bytes, .. } => bytes.as_buffer().est_bytes(),
                _ => 0,
            })
            .collect();
        self.columns =
            Self::build_live_columns(&self.fields, &self.read_as, self.rows_per_batch, &byte_caps)?;
        self.rows_in_batch = 0;
        Ok(self.live_table())
    }

    /// Seal the live batch without opening a new one. For shutdown, so
    /// consumers observe the end-of-stream flag on every column.
    pub fn seal(&mut self) {
        for col in &mut self.columns {
            col.seal();
        }
    }
}

/// Convert one value per the column's declared form and push it. The
/// `partial` flag rides on any error so the caller knows whether the row is
/// left incomplete.
fn push_value(
    col: &mut LiveColumn,
    value: JsonValueRef<'_>,
    partial: bool,
) -> Result<(), PushRecordError> {
    let fail = |reason: String| PushRecordError { partial, reason };
    match col {
        LiveColumn::Int32 { buf, from_string } => {
            let v = value.to_i64(*from_string).map_err(fail)?;
            let v = i32::try_from(v)
                .map_err(|_| PushRecordError { partial, reason: format!("value {v} out of range for Int32") })?;
            buf.push(v).map_err(|_| PushRecordError { partial, reason: "int32 column buffer full".into() })?;
        }
        LiveColumn::Int64 { buf, from_string } => {
            let v = value.to_i64(*from_string).map_err(fail)?;
            buf.push(v).map_err(|_| PushRecordError { partial, reason: "int column buffer full".into() })?;
        }
        LiveColumn::UInt32 { buf, from_string } => {
            let v = value.to_u64(*from_string).map_err(fail)?;
            let v = u32::try_from(v)
                .map_err(|_| PushRecordError { partial, reason: format!("value {v} out of range for UInt32") })?;
            buf.push(v).map_err(|_| PushRecordError { partial, reason: "uint32 column buffer full".into() })?;
        }
        LiveColumn::UInt64 { buf, from_string } => {
            let v = value.to_u64(*from_string).map_err(fail)?;
            buf.push(v).map_err(|_| PushRecordError { partial, reason: "uint64 column buffer full".into() })?;
        }
        LiveColumn::Float32 { buf, from_string } => {
            let v = value.to_f64(*from_string).map_err(fail)?;
            buf.push(v as f32)
                .map_err(|_| PushRecordError { partial, reason: "float32 column buffer full".into() })?;
        }
        LiveColumn::Float64 { buf, from_string } => {
            let v = value.to_f64(*from_string).map_err(fail)?;
            buf.push(v).map_err(|_| PushRecordError { partial, reason: "float column buffer full".into() })?;
        }
        #[cfg(feature = "datetime")]
        LiveColumn::Datetime32 { buf, unit, read_as } => {
            let v = value.to_datetime(*unit, read_as).map_err(fail)?;
            let v = i32::try_from(v).map_err(|_| PushRecordError {
                partial,
                reason: format!("value {v} out of range for 32-bit temporal"),
            })?;
            buf.push(v)
                .map_err(|_| PushRecordError { partial, reason: "datetime32 column buffer full".into() })?;
        }
        #[cfg(feature = "datetime")]
        LiveColumn::Datetime64 { buf, unit, read_as } => {
            let v = value.to_datetime(*unit, read_as).map_err(fail)?;
            buf.push(v)
                .map_err(|_| PushRecordError { partial, reason: "datetime column buffer full".into() })?;
        }
        LiveColumn::Bool { buf, from_string } => {
            let v = value.to_bool(*from_string).map_err(fail)?;
            // The masked channel carries the bit: push lands true,
            // push_null lands false.
            let pushed = if v { buf.push(1).map_err(|_| ()) } else { buf.push_null() };
            pushed.map_err(|_| PushRecordError { partial, reason: "bool column buffer full".into() })?;
        }
        LiveColumn::String32 { offsets, bytes } => {
            let s = value.to_str().map_err(fail)?;
            bytes
                .push_slice(s.as_bytes())
                .map_err(|_| PushRecordError { partial, reason: "string bytes buffer full".into() })?;
            // The bytes are in but unpublished until the offset lands. A
            // failure past this point strands them, so it poisons.
            let end = u32::try_from(bytes.len()).map_err(|_| PushRecordError {
                partial: true,
                reason: "string data exceeds u32 offsets".into(),
            })?;
            offsets
                .push(end)
                .map_err(|_| PushRecordError { partial: true, reason: "string offsets buffer full".into() })?;
        }
        #[cfg(feature = "large_string")]
        LiveColumn::String64 { offsets, bytes } => {
            let s = value.to_str().map_err(fail)?;
            bytes
                .push_slice(s.as_bytes())
                .map_err(|_| PushRecordError { partial, reason: "string bytes buffer full".into() })?;
            offsets
                .push(bytes.len() as u64)
                .map_err(|_| PushRecordError { partial: true, reason: "string offsets buffer full".into() })?;
        }
        #[cfg(feature = "default_categorical_8")]
        LiveColumn::Categorical8 { codes, dictionary } => {
            let s = value.to_str().map_err(fail)?;
            let position = dictionary
                .iter()
                .position(|entry| entry == s)
                .ok_or_else(|| PushRecordError {
                    partial,
                    reason: format!("value '{s}' not in the column's dictionary"),
                })?;
            let code = u8::try_from(position).map_err(|_| PushRecordError {
                partial,
                reason: format!("dictionary code {position} overflows u8"),
            })?;
            codes
                .push(code)
                .map_err(|_| PushRecordError { partial, reason: "categorical column buffer full".into() })?;
        }
        #[cfg(any(not(feature = "default_categorical_8"), feature = "extended_categorical"))]
        LiveColumn::Categorical32 { codes, dictionary } => {
            let s = value.to_str().map_err(fail)?;
            let code = dictionary
                .iter()
                .position(|entry| entry == s)
                .ok_or_else(|| PushRecordError {
                    partial,
                    reason: format!("value '{s}' not in the column's dictionary"),
                })? as u32;
            codes
                .push(code)
                .map_err(|_| PushRecordError { partial, reason: "categorical column buffer full".into() })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use minarrow::ffi::arrow_dtype::CategoricalIndexType;
    use minarrow::{NumericArray, TemporalArray, TextArray};

    use crate::models::interfaces::json::{JsonInterface, JsonPath, JsonSchema, ValueSource};
    use crate::models::decoders::limits::DecodeLimits;

    /// The categorical index width the active feature set builds.
    #[cfg(feature = "default_categorical_8")]
    const CAT_INDEX: CategoricalIndexType = CategoricalIndexType::UInt8;
    #[cfg(not(feature = "default_categorical_8"))]
    const CAT_INDEX: CategoricalIndexType = CategoricalIndexType::UInt32;

    /// Push every record of one frame, rolling per the caller contract.
    fn push_frame(
        decoder: &mut JsonInterface,
        sink: &mut LiveTableSink,
        frame: &mut [u8],
        now: i64,
    ) -> Result<Vec<Arc<Table>>, PushRecordError> {
        let mut rolled = Vec::new();
        let Some(mut parsed) = decoder
            .parse_frame(frame, now)
            .map_err(|e| PushRecordError { partial: false, reason: e.to_string() })?
        else {
            return Ok(rolled);
        };
        while let Some(record) = parsed.next_record() {
            match sink.push_record(&record) {
                Ok(()) => {
                    if sink.is_full() {
                        rolled.push(sink.roll().unwrap());
                    }
                }
                Err(e) => {
                    if e.partial {
                        rolled.push(sink.roll().unwrap());
                    }
                    return Err(e);
                }
            }
        }
        Ok(rolled)
    }

    /// Schema exercising every live column type - integers, floats, bool
    /// from JSON bool and from string, String32/String64, a static
    /// dictionary categorical, and microsecond/date temporals.
    fn full_type_spec() -> JsonSchema {
        let schema = JsonSchema::new()
            .column(
                Field::new("event_time", ArrowType::Timestamp(TimeUnit::Microseconds, None), false, None),
                ValueSource::RecordKey("ts".into()),
                ReadAs::Datetime,
            )
            .column(Field::new("small", ArrowType::Int32, false, None), ValueSource::RecordKey("small".into()), ReadAs::Verbatim)
            .column(Field::new("index", ArrowType::UInt32, false, None), ValueSource::RecordKey("index".into()), ReadAs::Verbatim)
            .column(Field::new("wide", ArrowType::UInt64, false, None), ValueSource::RecordKey("wide".into()), ReadAs::Verbatim)
            .column(Field::new("ratio", ArrowType::Float32, false, None), ValueSource::RecordKey("ratio".into()), ReadAs::Verbatim)
            .column(Field::new("price", ArrowType::Float64, false, None), ValueSource::RecordKey("price".into()), ReadAs::Verbatim)
            .column(Field::new("live", ArrowType::Boolean, false, None), ValueSource::RecordKey("live".into()), ReadAs::Verbatim)
            .column(Field::new("maker", ArrowType::Boolean, false, None), ValueSource::RecordKey("maker".into()), ReadAs::Bool)
            .column(Field::new("note", ArrowType::String, false, None), ValueSource::RecordKey("note".into()), ReadAs::Verbatim);
        #[cfg(feature = "large_string")]
        let schema = schema.column(
            Field::new("venue", ArrowType::LargeString, false, None),
            ValueSource::RecordKey("venue".into()),
            ReadAs::Verbatim,
        );
        schema
            .column(
                Field::new("side", ArrowType::Dictionary(CAT_INDEX), false, None),
                ValueSource::RecordKey("side".into()),
                ReadAs::Dictionary(vec!["buy".into(), "sell".into()]),
            )
            .column(Field::new("settle_date", ArrowType::Date32, false, None), ValueSource::RecordKey("settle_date".into()), ReadAs::Verbatim)
            .column(
                Field::new("trade_id", ArrowType::Int64, false, None),
                ValueSource::RecordKey("trade_id".into()),
                ReadAs::Number,
            )
    }

    fn full_type_frame(side: &str) -> Vec<u8> {
        format!(
            r#"{{"ts":"2026-06-09T07:48:36.123456Z","small":-42,"index":7,"wide":9007199254740993,"ratio":1.5,"price":63245.1,"live":true,"maker":"false","note":"hello","venue":"kraken","side":"{side}","settle_date":19000,"trade_id":"000123"}}"#
        )
        .into_bytes()
    }

    #[test]
    fn pushes_every_live_column_type() {
        let spec = full_type_spec();
        let mut decoder = JsonInterface::new(&spec, DecodeLimits::default()).unwrap();
        let mut sink = LiveTableSink::new(spec.fields(), spec.read_as(), 16, "full.TEST").unwrap();
        let table = sink.live_table();

        let mut frame = full_type_frame("sell");
        push_frame(&mut decoder, &mut sink, &mut frame, 0).unwrap();

        assert_eq!(table.n_rows(), 1);
        let col = |name: &str| table.cols.iter().find(|c| c.field.name == name).unwrap();

        // 2026-06-09T07:48:36 UTC is 1_780_991_316 s since the epoch. The
        // string's microseconds survive into the us column.
        match &col("event_time").array {
            Array::TemporalArray(TemporalArray::Datetime64(a)) => {
                assert_eq!(a.data.as_slice()[0], 1_780_991_316_123_456);
                assert_eq!(a.time_unit, TimeUnit::Microseconds);
            }
            _ => panic!("event_time should be Datetime64"),
        }
        match &col("small").array {
            Array::NumericArray(NumericArray::Int32(a)) => assert_eq!(a.data.as_slice()[0], -42),
            _ => panic!("small should be Int32"),
        }
        match &col("index").array {
            Array::NumericArray(NumericArray::UInt32(a)) => assert_eq!(a.data.as_slice()[0], 7),
            _ => panic!("index should be UInt32"),
        }
        match &col("wide").array {
            Array::NumericArray(NumericArray::UInt64(a)) => {
                assert_eq!(a.data.as_slice()[0], 9_007_199_254_740_993)
            }
            _ => panic!("wide should be UInt64"),
        }
        match &col("ratio").array {
            Array::NumericArray(NumericArray::Float32(a)) => {
                assert!((a.data.as_slice()[0] - 1.5).abs() < 1e-6)
            }
            _ => panic!("ratio should be Float32"),
        }
        match &col("live").array {
            Array::BooleanArray(a) => {
                assert_eq!(a.len(), 1);
                assert!(a.data.get(0));
            }
            _ => panic!("live should be Boolean"),
        }
        match &col("maker").array {
            Array::BooleanArray(a) => assert!(!a.data.get(0)),
            _ => panic!("maker should be Boolean"),
        }
        match &col("note").array {
            Array::TextArray(TextArray::String32(a)) => assert_eq!(a.get_str(0), Some("hello")),
            _ => panic!("note should be String32"),
        }
        #[cfg(feature = "large_string")]
        match &col("venue").array {
            Array::TextArray(TextArray::String64(a)) => assert_eq!(a.get_str(0), Some("kraken")),
            _ => panic!("venue should be String64"),
        }
        match &col("side").array {
            #[cfg(feature = "default_categorical_8")]
            Array::TextArray(TextArray::Categorical8(a)) => {
                assert_eq!(a.get_str(0), Some("sell"));
                assert_eq!(a.data.as_slice()[0], 1);
            }
            #[cfg(not(feature = "default_categorical_8"))]
            Array::TextArray(TextArray::Categorical32(a)) => {
                assert_eq!(a.get_str(0), Some("sell"));
                assert_eq!(a.data.as_slice()[0], 1);
            }
            _ => panic!("side should be categorical"),
        }
        match &col("settle_date").array {
            Array::TemporalArray(TemporalArray::Datetime32(a)) => {
                assert_eq!(a.data.as_slice()[0], 19_000);
                assert_eq!(a.time_unit, TimeUnit::Days);
            }
            _ => panic!("settle_date should be Datetime32"),
        }
        // Quoted id parses straight into the Int64 column.
        match &col("trade_id").array {
            Array::NumericArray(NumericArray::Int64(a)) => assert_eq!(a.data.as_slice()[0], 123),
            _ => panic!("trade_id should be Int64"),
        }
        // The declared field rides on the table unchanged.
        assert_eq!(col("side").field.dtype, ArrowType::Dictionary(CAT_INDEX));
    }

    #[test]
    fn rows_accumulate_and_roll_at_capacity() {
        let spec = full_type_spec();
        let mut decoder = JsonInterface::new(&spec, DecodeLimits::default()).unwrap();
        let mut sink = LiveTableSink::new(spec.fields(), spec.read_as(), 2, "full.TEST").unwrap();
        let first = sink.live_table();

        let mut rolled = Vec::new();
        for side in ["buy", "sell", "buy"] {
            let mut frame = full_type_frame(side);
            rolled.extend(push_frame(&mut decoder, &mut sink, &mut frame, 0).unwrap());
        }

        // Two rows filled the first batch. The third row sits in the
        // batch opened by the roll.
        assert_eq!(rolled.len(), 1);
        assert_eq!(first.n_rows(), 2);
        assert_eq!(rolled[0].n_rows(), 1);
        let side = first.cols.iter().find(|c| c.field.name == "side").unwrap();
        match &side.array {
            #[cfg(feature = "default_categorical_8")]
            Array::TextArray(TextArray::Categorical8(a)) => {
                assert_eq!(&a.data.as_slice()[..2], &[0, 1]);
            }
            #[cfg(not(feature = "default_categorical_8"))]
            Array::TextArray(TextArray::Categorical32(a)) => {
                assert_eq!(&a.data.as_slice()[..2], &[0, 1]);
            }
            _ => panic!("side should be categorical"),
        }
    }

    #[test]
    fn unknown_dictionary_value_fails_without_poisoning() {
        let spec = JsonSchema::new().column(
            Field::new("side", ArrowType::Dictionary(CAT_INDEX), false, None),
            ValueSource::RecordKey("side".into()),
            ReadAs::Dictionary(vec!["buy".into(), "sell".into()]),
        );
        let mut decoder = JsonInterface::new(&spec, DecodeLimits::default()).unwrap();
        let mut sink = LiveTableSink::new(spec.fields(), spec.read_as(), 16, "t").unwrap();
        let mut frame = br#"{"side":"hold"}"#.to_vec();
        let err = push_frame(&mut decoder, &mut sink, &mut frame, 0).unwrap_err();
        assert!(err.reason.contains("dictionary"), "unexpected error: {err}");
        // The failure hit the first column, so no partial row exists and
        // the batch keeps accepting records.
        assert!(!err.partial);
        assert_eq!(sink.rows_in_batch(), 0);
    }

    #[test]
    fn failure_after_a_write_reports_partial() {
        let spec = JsonSchema::new()
            .column(
                Field::new("a", ArrowType::Int64, false, None),
                ValueSource::RecordKey("a".into()),
                ReadAs::Verbatim,
            )
            .column(
                Field::new("b", ArrowType::Int64, false, None),
                ValueSource::RecordKey("b".into()),
                ReadAs::Verbatim,
            );
        let mut decoder = JsonInterface::new(&spec, DecodeLimits::default()).unwrap();
        let mut sink = LiveTableSink::new(spec.fields(), spec.read_as(), 16, "t").unwrap();
        // `a` lands, then `b` is missing, leaving the row incomplete.
        let mut frame = br#"{"a":1}"#.to_vec();
        let err = push_frame(&mut decoder, &mut sink, &mut frame, 0).unwrap_err();
        assert!(err.partial);
    }

    #[test]
    fn kraken_book_levels_with_envelope_columns() {
        let spec = JsonSchema::new()
            .column(
                Field::new("event_time", ArrowType::Timestamp(TimeUnit::Microseconds, None), false, None),
                ValueSource::FramePath(JsonPath::parse("data.0.timestamp")),
                ReadAs::Datetime,
            )
            .column(
                Field::new("price", ArrowType::Float64, false, None),
                ValueSource::RecordKey("price".into()),
                ReadAs::Verbatim,
            )
            .column(
                Field::new("qty", ArrowType::Float64, false, None),
                ValueSource::RecordKey("qty".into()),
                ReadAs::Verbatim,
            )
            .column(
                Field::new("checksum", ArrowType::Int64, false, None),
                ValueSource::FramePath(JsonPath::parse("data.0.checksum")),
                ReadAs::Verbatim,
            )
            .with_record_path("data.0.bids");
        let mut decoder = JsonInterface::new(&spec, DecodeLimits::default()).unwrap();
        let mut sink = LiveTableSink::new(spec.fields(), spec.read_as(), 16, "book10_bids.BTC/USD").unwrap();
        let table = sink.live_table();

        // The snapshot's envelope timestamp lands on both levels.
        let mut snapshot = br#"{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":63245.0,"qty":0.5},{"price":63244.5,"qty":1.2}],"asks":[],"checksum":111,"timestamp":"2026-06-09T07:48:36.000000Z"}]}"#.to_vec();
        push_frame(&mut decoder, &mut sink, &mut snapshot, 0).unwrap();

        // The delta's later envelope timestamp lands on both changed levels.
        let mut delta = br#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":63245.0,"qty":1.7},{"price":63244.5,"qty":0.0}],"asks":[],"checksum":222,"timestamp":"2026-06-09T07:48:36.123456Z"}]}"#.to_vec();
        push_frame(&mut decoder, &mut sink, &mut delta, 0).unwrap();

        assert_eq!(table.n_rows(), 4);
        let col = |name: &str| table.cols.iter().find(|c| c.field.name == name).unwrap();
        match &col("event_time").array {
            Array::TemporalArray(TemporalArray::Datetime64(a)) => {
                let times = &a.data.as_slice()[..4];
                assert_eq!(times[0], 1_780_991_316_000_000);
                assert_eq!(times[0], times[1]);
                assert_eq!(times[2], 1_780_991_316_123_456);
                assert_eq!(times[3], 1_780_991_316_123_456);
            }
            _ => panic!("event_time should be Datetime64"),
        }
        match &col("checksum").array {
            Array::NumericArray(NumericArray::Int64(a)) => {
                assert_eq!(&a.data.as_slice()[..4], &[111, 111, 222, 222]);
            }
            _ => panic!("checksum should be Int64"),
        }
        match &col("qty").array {
            Array::NumericArray(NumericArray::Float64(a)) => {
                // The delta's second level is a removal: qty 0.
                assert_eq!(a.data.as_slice()[3], 0.0);
            }
            _ => panic!("qty should be Float64"),
        }
    }

    #[test]
    fn heartbeats_decode_to_no_records_and_no_rows() {
        let spec = JsonSchema::new()
            .column(
                Field::new("x", ArrowType::Int64, false, None),
                ValueSource::RecordKey("x".into()),
                ReadAs::Verbatim,
            )
            .with_record_path("data");
        let mut decoder = JsonInterface::new(&spec, DecodeLimits::default()).unwrap();
        let mut sink = LiveTableSink::new(spec.fields(), spec.read_as(), 16, "t").unwrap();
        let mut heartbeat = br#"{"channel":"heartbeat"}"#.to_vec();
        push_frame(&mut decoder, &mut sink, &mut heartbeat, 0).unwrap();
        assert_eq!(sink.rows_in_batch(), 0);
    }

    #[test]
    fn a_categorical_without_a_dictionary_fails_at_construction() {
        let fields = vec![Field::new("side", ArrowType::Dictionary(CAT_INDEX), false, None)];
        assert!(LiveTableSink::new(&fields, &[ReadAs::Verbatim], 16, "t").is_err());
    }
}
