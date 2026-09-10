# Changelog

Notable changes are recorded from 0.5.0 onward.

## 0.6.2

### Changed

- minarrow 0.18.1 and vec64 0.5.1.
- New `decimal` feature forwarding minarrow's `decimal` feature. Decimal32, Decimal64 and Decimal128 columns are supported in Arrow IPC and in Parquet, where they map to the DECIMAL logical type over INT32, INT64 and FIXED_LEN_BYTE_ARRAY.
- The Python package pins minarrow and minarrow-pyo3 at 0.18.1.
- The Parquet writer follows the Parquet value layout for nullable columns. Value sections hold non-null values only and DataPageV2 headers count every row in `num_values`, so files with nulls now read in pyarrow and other Parquet readers. Files with nulls written by earlier releases do not read back under this release.

### Fixed

- The Parquet reader failed with `UnexpectedEof` on files written by pyarrow. It now reads DataPageV1 pages with compressed levels, `RLE` booleans, dictionary-encoded columns of any physical type, and files with several row groups. Dictionary pages in files without lightstream's `created_by` marker expand to the schema type rather than being read as categorical columns.

## 0.6.1

### Changed

- minarrow 0.17.0, vec64 0.5.0, arrow 59.2.0 and polars 0.55.2.

## 0.6.0

### Changed

- Table writer entry points take `impl Into<TableV>`, so either owned `Table` or zero-copy `TableV`iew passes without a `.into()` conversion at the call site.

### Fixed

- `TableSink` kept only the most recent frame when several tables were admitted before a flush, corrupting streams driven through `Sink::feed` or `send_all`.

## 0.5.0

Initial public release.
