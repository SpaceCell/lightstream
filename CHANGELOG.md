# Changelog

Notable changes are recorded from 0.5.0 onward.

## v0.7.0 19 Sep 2026

### Enhancements

- New `decimal` feature forwarding minarrow's `decimal` feature. Decimal32, Decimal64 and Decimal128 columns are supported in Arrow IPC and in Parquet, where they map to the DECIMAL logical type over INT32, INT64 and FIXED_LEN_BYTE_ARRAY.
- Parquet writer improvements:
  - The Parquet writer follows the Parquet value layout for nullable columns.
  - Several improvements in categorical, time, and date roundtripping.
  - Files with nulls written by earlier releases do not read back under this release.
  - Replaced packing for Duration and Interval columns with `UnsupportedType`

### Fixed

- The Parquet reader failed with `UnexpectedEof` on files written by pyarrow. It now reads DataPageV1 pages with compressed levels, `RLE` booleans, dictionary-encoded columns of any physical type, and files with several row groups.

### Maintenance
- Minarrow upgraded to 0.18.2 and vec64 0.5.1.
- The Python package pins minarrow and minarrow-pyo3 at 0.18.2.

## 0.6.1

### Changed

- minarrow version bump to 0.17.0, vec64 0.5.0, arrow 59.2.0 and polars 0.55.2.

## 0.6.0

### Changed

- Table writer entry points take `impl Into<TableV>`, so either owned `Table` or zero-copy `TableV`iew passes without a `.into()` conversion at the call site.

### Fixed

- `TableSink` kept only the most recent frame when several tables were admitted before a flush, corrupting streams driven through `Sink::feed` or `send_all`.

## 0.5.0

Initial public release.
