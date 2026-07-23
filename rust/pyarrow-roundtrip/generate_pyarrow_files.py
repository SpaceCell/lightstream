#!/usr/bin/env python3
"""Generate the PyArrow fixture files for tests/pyarrow_roundtrip.rs.

Writes Arrow IPC files whose data matches `create_expected_table` in the
Rust test, in both the file and stream formats:

  - pyarrow_basic_types.arrow / .stream (4 rows) for the reader tests
  - pyarrow_basic_types_2048.arrow (2048 rows) for the mmap reader tests,
    sized so chunked reads split the batch into multiple windows

Run from the repository root:

    python3 python/generate_pyarrow_files.py
"""

import os

import pyarrow as pa
import pyarrow.ipc as ipc

OUT_DIR = os.path.dirname(os.path.abspath(__file__))


def make_table(n_rows: int) -> pa.Table:
    """Build the table matching create_expected_table(n_rows) in Rust."""
    idx = range(n_rows)
    schema = pa.schema(
        [
            pa.field("int32", pa.int32(), nullable=False),
            pa.field("int64", pa.int64(), nullable=False),
            pa.field("uint32", pa.uint32(), nullable=False),
            pa.field("uint64", pa.uint64(), nullable=False),
            pa.field("float32", pa.float32(), nullable=False),
            pa.field("float64", pa.float64(), nullable=False),
            pa.field("bool", pa.bool_(), nullable=False),
            pa.field("string", pa.string(), nullable=False),
        ]
    )
    return pa.table(
        {
            "int32": pa.array((i + 1 for i in idx), type=pa.int32()),
            "int64": pa.array((100 + i for i in idx), type=pa.int64()),
            "uint32": pa.array(idx, type=pa.uint32()),
            "uint64": pa.array((10 + i for i in idx), type=pa.uint64()),
            "float32": pa.array((i * 1.25 - 2.5 for i in idx), type=pa.float32()),
            "float64": pa.array((i * 3.5 - 1.0 for i in idx), type=pa.float64()),
            "bool": pa.array((i % 2 == 0 for i in idx), type=pa.bool_()),
            "string": pa.array((f"str{i}" for i in idx), type=pa.string()),
        },
        schema=schema,
    )


def write_file(table: pa.Table, path: str) -> None:
    with ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)
    print(f"wrote {path}")


def write_stream(table: pa.Table, path: str) -> None:
    with ipc.new_stream(path, table.schema) as writer:
        writer.write_table(table)
    print(f"wrote {path}")


def main() -> None:
    small = make_table(4)
    write_file(small, os.path.join(OUT_DIR, "pyarrow_basic_types.arrow"))
    write_stream(small, os.path.join(OUT_DIR, "pyarrow_basic_types.stream"))

    large = make_table(2048)
    write_file(large, os.path.join(OUT_DIR, "pyarrow_basic_types_2048.arrow"))


if __name__ == "__main__":
    main()
