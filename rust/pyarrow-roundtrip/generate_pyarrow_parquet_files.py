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

The fixtures are committed alongside this script. Run from rust/ to
regenerate them:

    python3 pyarrow-roundtrip/generate_pyarrow_parquet_files.py
"""

import os

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
    for name in sorted(os.listdir(OUT_DIR)):
        if name.startswith("pyarrow_") and name.endswith(".parquet"):
            print(f"wrote {name}")


if __name__ == "__main__":
    main()
