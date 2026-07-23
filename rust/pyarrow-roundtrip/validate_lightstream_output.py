#!/usr/bin/env python3
"""Validate Lightstream-written Arrow IPC files with PyArrow.

Reads the files written by the tests/pyarrow_roundtrip.rs write tests and
checks every value against the expected data, proving the files parse
correctly in the independent Arrow implementation.

Run the Rust write tests first, then this script from the repository root:

    cargo test --test pyarrow_roundtrip
    python3 python/validate_lightstream_output.py

Exits nonzero on any mismatch.
"""

import os
import sys

import pyarrow as pa
import pyarrow.ipc as ipc

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
N_ROWS = 4

EXPECTED = {
    "int32": [i + 1 for i in range(N_ROWS)],
    "int64": [100 + i for i in range(N_ROWS)],
    "uint32": list(range(N_ROWS)),
    "uint64": [10 + i for i in range(N_ROWS)],
    "float32": [i * 1.25 - 2.5 for i in range(N_ROWS)],
    "float64": [i * 3.5 - 1.0 for i in range(N_ROWS)],
    "bool": [i % 2 == 0 for i in range(N_ROWS)],
    "string": [f"str{i}" for i in range(N_ROWS)],
}


def validate(table: pa.Table, label: str) -> bool:
    ok = True
    if table.num_rows != N_ROWS:
        print(f"{label}: expected {N_ROWS} rows, got {table.num_rows}")
        ok = False
    for name, expected in EXPECTED.items():
        if name not in table.column_names:
            print(f"{label}: missing column {name}")
            ok = False
            continue
        actual = table.column(name).to_pylist()
        if actual != expected:
            print(f"{label}: column {name} mismatch: {actual} != {expected}")
            ok = False
    if ok:
        print(f"{label}: ok")
    return ok


def main() -> None:
    file_path = os.path.join(OUT_DIR, "lightstream_basic_types.arrow")
    stream_path = os.path.join(OUT_DIR, "lightstream_basic_types.stream")

    for path in (file_path, stream_path):
        if not os.path.exists(path):
            print(f"{path} missing - run `cargo test --test pyarrow_roundtrip` first")
            sys.exit(1)

    ok = True
    with ipc.open_file(file_path) as reader:
        ok &= validate(reader.read_all(), "file format")
    with ipc.open_stream(stream_path) as reader:
        ok &= validate(reader.read_all(), "stream format")

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
