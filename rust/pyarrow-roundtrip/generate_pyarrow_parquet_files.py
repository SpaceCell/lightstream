#!/usr/bin/env python3
"""Generate the PyArrow Parquet fixture files for tests/pyarrow_parquet.rs.

Each file exercises one combination of the page layouts pyarrow can write,
so the reader is checked against real parquet-cpp output rather than
lightstream's own encoder:

  - pyarrow_simple.parquet: three required columns written with pyarrow's
    defaults (Snappy, dictionary encoding, DataPageV1)
  - pyarrow_nullable_row_groups.parquet: nullable columns with nulls, split
    across three row groups, written with pyarrow's defaults
  - pyarrow_plain_v2.parquet: the same nullable data with dictionary
    encoding off, DataPageV2 and no compression
  - pyarrow_dictionary_v2.parquet: the same nullable data with dictionary
    encoding on, DataPageV2 and Snappy
  - pyarrow_temporal_decimal.parquet: date, time, timestamp and decimal
    columns with nulls, written with pyarrow's defaults. Nanosecond units
    and decimals exercise the LogicalType annotation

The fixtures are committed alongside this script. Run from rust/ to
regenerate them:

    python3 pyarrow-roundtrip/generate_pyarrow_parquet_files.py
"""

import os
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

OUT_DIR = os.path.dirname(os.path.abspath(__file__))


def simple_table() -> pa.Table:
    """The three-column table from the original defect report."""
    return pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie", "Diana", "Eve"], type=pa.string()),
            "score": pa.array([85.5, 92.0, 78.3, 95.1, 88.7], type=pa.float64()),
        }
    )


def nullable_table() -> pa.Table:
    """Seven rows over the basic types, matching expected_nullable_table in Rust."""
    return pa.table(
        {
            "int32": pa.array([1, None, 3, 4, None, 6, 7], type=pa.int32()),
            "int64": pa.array([100, 101, None, 103, 104, 105, None], type=pa.int64()),
            "float32": pa.array([0.5, 1.5, 2.5, None, 4.5, 5.5, 6.5], type=pa.float32()),
            "float64": pa.array([None, -1.0, -2.0, -3.0, -4.0, None, -6.0], type=pa.float64()),
            "bool": pa.array([True, False, None, True, False, True, None], type=pa.bool_()),
            "string": pa.array(["a", "b", "a", None, "c", "b", "a"], type=pa.string()),
        }
    )


def temporal_decimal_table() -> pa.Table:
    """Five rows of temporal and decimal types, matching the expectations in
    reads_pyarrow_temporal_and_decimal_columns in Rust."""
    return pa.table(
        {
            "date": pa.array([0, 1, None, 19_000, -5], type=pa.date32()),
            "time_ms": pa.array([0, 1_000, None, 43_200_000, 86_399_999], type=pa.time32("ms")),
            "time_us": pa.array([0, None, 2_000_000, 43_200_000_000, 86_399_999_999], type=pa.time64("us")),
            "ts_ms": pa.array([0, 1_700_000_000_000, None, -1, 86_400_000], type=pa.timestamp("ms")),
            "ts_us": pa.array([0, 1_700_000_000_000_000, None, -1, 1], type=pa.timestamp("us")),
            "ts_ns": pa.array([0, 1_700_000_000_000_000_000, None, -1, 1], type=pa.timestamp("ns")),
            "ts_us_utc": pa.array([0, 1_700_000_000_000_000, None, -1, 1], type=pa.timestamp("us", tz="UTC")),
            "dec32": pa.array(
                [Decimal("1.25"), None, Decimal("-3.50"), Decimal("0.01"), Decimal("99999.99")],
                type=pa.decimal32(7, 2),
            ),
            "dec64": pa.array(
                [Decimal("1.2345"), Decimal("-1.0000"), None, Decimal("0.0001"), Decimal("12345678901234.5678")],
                type=pa.decimal64(18, 4),
            ),
            "dec128": pa.array(
                [Decimal("1.000001"), None, Decimal("-1.000001"), Decimal("0.000000"), Decimal("12345678901234567890123456789012.123456")],
                type=pa.decimal128(38, 6),
            ),
        }
    )


def main() -> None:
    simple = simple_table()
    pq.write_table(simple, os.path.join(OUT_DIR, "pyarrow_simple.parquet"))

    nullable = nullable_table()
    pq.write_table(
        nullable,
        os.path.join(OUT_DIR, "pyarrow_nullable_row_groups.parquet"),
        row_group_size=3,
    )
    pq.write_table(
        nullable,
        os.path.join(OUT_DIR, "pyarrow_plain_v2.parquet"),
        use_dictionary=False,
        data_page_version="2.0",
        compression="none",
    )
    pq.write_table(
        nullable,
        os.path.join(OUT_DIR, "pyarrow_dictionary_v2.parquet"),
        data_page_version="2.0",
        compression="snappy",
    )
    pq.write_table(
        temporal_decimal_table(),
        os.path.join(OUT_DIR, "pyarrow_temporal_decimal.parquet"),
    )
    for name in sorted(os.listdir(OUT_DIR)):
        if name.startswith("pyarrow_") and name.endswith(".parquet"):
            print(f"wrote {name}")


if __name__ == "__main__":
    main()
