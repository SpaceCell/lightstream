# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""File-format round-trips through ls.read and ls.write.

Covers the three IPC read variants, the routing between them, the
Table / ChunkedTable output split, and the error surface.
"""

import gc

import lightstream as ls
import minarrow
import pyarrow as pa
import pytest


def sample_table(offset=0):
    return pa.table(
        {
            "sym": ["A", "B", "C"],
            "px": [1.5 + offset, 2.5 + offset, 3.5 + offset],
            "qty": [10 + offset, 20 + offset, 30 + offset],
        }
    )


def test_round_trip_single_batch(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    original = sample_table()

    with ls.write(path) as w:
        w.write(original)

    result = ls.read(path).read_all()
    assert isinstance(result, minarrow.Table)
    assert pa.table(result).equals(original)


def test_iteration_yields_minarrow_tables(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    with ls.write(path) as w:
        w.write(sample_table(0))
        w.write(sample_table(100))

    batches = list(ls.read(path))
    assert len(batches) == 2
    assert all(isinstance(b, minarrow.Table) for b in batches)
    assert pa.table(batches[0]).equals(sample_table(0))
    assert pa.table(batches[1]).equals(sample_table(100))


def test_read_all_multi_batch_returns_chunked_table(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    with ls.write(path) as w:
        w.write(sample_table(0))
        w.write(sample_table(100))

    result = ls.read(path).read_all()
    assert isinstance(result, minarrow.ChunkedTable)


def test_read_all_after_drain_returns_empty_table(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    with ls.write(path) as w:
        w.write(sample_table())

    reader = ls.read(path)
    list(reader)
    result = reader.read_all()
    assert isinstance(result, minarrow.Table)
    assert result.n_rows == 0


def test_mmap_route_matches_buffered_route(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    original = sample_table()
    with ls.write(path) as w:
        w.write(original)

    via_mmap = ls.read(path, mmap=True).read_all()
    via_buffered = ls.read(path, mmap=False).read_all()
    assert pa.table(via_mmap).equals(original)
    assert pa.table(via_buffered).equals(original)


def test_out_of_core_read(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    original = sample_table()
    with ls.write(path) as w:
        w.write(original)

    result = ls.read(path, out_of_core=True).read_all()
    assert pa.table(result).equals(original)


def test_minarrow_table_writes(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    source = minarrow.Table({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})

    with ls.write(path) as w:
        w.write(source)

    result = ls.read(path).read_all()
    assert pa.table(result).equals(pa.table(source))


def test_writer_drop_finalises_file(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    w = ls.write(path)
    w.write(sample_table())
    del w
    gc.collect()

    result = ls.read(path).read_all()
    assert pa.table(result).equals(sample_table())


def test_chunked_directory_reads_in_order(tmp_path):
    for idx in range(2):
        with ls.write(str(tmp_path / f"part-{idx:010}.arrow")) as w:
            w.write(sample_table(idx * 100))

    batches = list(ls.read(str(tmp_path)))
    assert len(batches) == 2
    assert pa.table(batches[0]).equals(sample_table(0))
    assert pa.table(batches[1]).equals(sample_table(100))


def test_chunked_directory_base_disambiguation(tmp_path):
    for base in ("left", "right"):
        with ls.write(str(tmp_path / f"{base}-0000000000.arrow")) as w:
            w.write(sample_table())

    with pytest.raises(ls.FormatError, match="multiple chunk sets"):
        ls.read(str(tmp_path))

    batches = list(ls.read(str(tmp_path), base="left"))
    assert len(batches) == 1


def test_unknown_transport_scheme_raises(tmp_path):
    with pytest.raises(ls.TransportError, match="unknown scheme"):
        ls.read("ftp://feed:9000")
    with pytest.raises(ls.TransportError, match="unknown scheme"):
        ls.write("ftp://sink:9000")


def test_unknown_protocol_raises():
    with pytest.raises(ls.ProtocolError, match="unknown protocol"):
        ls.read("feed.arrow", protocol="flight")


def test_unknown_format_raises():
    with pytest.raises(ValueError, match="unknown format"):
        ls.read("feed.dat", format="orc")


def test_uninferrable_format_raises():
    with pytest.raises(ls.FormatError, match="cannot infer a format"):
        ls.read("feed.dat")


def test_parallel_on_file_raises(tmp_path):
    with pytest.raises(ls.TransportError, match="no parallel form"):
        ls.read(str(tmp_path / "quotes.arrow"), parallel=True)


def test_out_of_core_with_mmap_false_raises():
    with pytest.raises(ValueError, match="out_of_core"):
        ls.read("quotes.arrow", mmap=False, out_of_core=True)


def test_closed_reader_raises(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    with ls.write(path) as w:
        w.write(sample_table())

    reader = ls.read(path)
    reader.close()
    with pytest.raises(ls.LightstreamError, match="closed"):
        reader.read_all()


def test_closed_writer_raises(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    writer = ls.write(path)
    writer.write(sample_table())
    writer.close()
    with pytest.raises(ls.LightstreamError, match="closed"):
        writer.write(sample_table())


def test_non_arrow_object_raises(tmp_path):
    with ls.write(str(tmp_path / "quotes.arrow")) as w:
        with pytest.raises(ls.FormatError, match="Arrow-compatible"):
            w.write({"not": "arrow"})


# --- Phase 4: Parquet, CSV, JSON -------------------------------------------


def test_parquet_round_trip(tmp_path):
    path = str(tmp_path / "quotes.parquet")
    original = sample_table()
    with ls.write(path) as w:
        w.write(original)

    result = ls.read(path).read_all()
    assert isinstance(result, minarrow.Table)
    assert pa.table(result).to_pydict() == original.to_pydict()


def test_parquet_multi_write_consolidates(tmp_path):
    path = str(tmp_path / "quotes.parquet")
    with ls.write(path) as w:
        w.write(sample_table(0))
        w.write(sample_table(100))

    result = pa.table(ls.read(path).read_all())
    assert result.num_rows == 6


@pytest.mark.parametrize("codec", ["zstd", "snappy"])
def test_parquet_compression(tmp_path, codec):
    path = str(tmp_path / f"quotes_{codec}.parquet")
    original = sample_table()
    with ls.write(path, compression=codec) as w:
        w.write(original)

    result = pa.table(ls.read(path).read_all())
    assert result.to_pydict() == original.to_pydict()


def test_ipc_compression(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    original = sample_table()
    with ls.write(path, compression="zstd") as w:
        w.write(original)

    result = pa.table(ls.read(path).read_all())
    assert result.to_pydict() == original.to_pydict()


def test_csv_round_trip(tmp_path):
    path = str(tmp_path / "quotes.csv")
    original = sample_table()
    with ls.write(path) as w:
        w.write(original)

    result = pa.table(ls.read(path).read_all())
    assert result.to_pydict() == original.to_pydict()


def test_csv_delimiter(tmp_path):
    path = str(tmp_path / "quotes.csv")
    original = sample_table()
    with ls.write(path, delimiter=";") as w:
        w.write(original)

    assert ";" in open(path).readline()
    result = pa.table(ls.read(path, delimiter=";").read_all())
    assert result.to_pydict() == original.to_pydict()


def test_csv_no_header(tmp_path):
    path = str(tmp_path / "quotes.csv")
    with ls.write(path, header=False) as w:
        w.write(sample_table())

    first_line = open(path).readline()
    assert "sym" not in first_line
    result = pa.table(ls.read(path, header=False).read_all())
    assert result.num_rows == 3


def test_csv_batch_size_splits_batches(tmp_path):
    path = str(tmp_path / "quotes.csv")
    with ls.write(path) as w:
        w.write(sample_table())

    batches = list(ls.read(path, batch_size=2))
    assert len(batches) == 2


def test_json_write_array(tmp_path):
    import json

    path = str(tmp_path / "quotes.json")
    with ls.write(path) as w:
        w.write(sample_table())

    rows = json.load(open(path))
    assert rows[0] == {"sym": "A", "px": 1.5, "qty": 10}


def test_json_write_ndjson(tmp_path):
    import json

    path = str(tmp_path / "quotes.ndjson")
    with ls.write(path) as w:
        w.write(sample_table())

    lines = [json.loads(line) for line in open(path)]
    assert len(lines) == 3
    assert lines[2] == {"sym": "C", "px": 3.5, "qty": 30}


def test_json_read_requires_schema(tmp_path):
    path = str(tmp_path / "quotes.json")
    with ls.write(path) as w:
        w.write(sample_table())

    with pytest.raises(ls.LightstreamError, match="schema"):
        ls.read(path).read_all()


def test_chunked_parquet_directory(tmp_path):
    for idx in range(2):
        with ls.write(str(tmp_path / f"part-{idx:010}.parquet")) as w:
            w.write(sample_table(idx * 100))

    batches = list(ls.read(str(tmp_path)))
    assert len(batches) == 2
    assert pa.table(batches[1]).to_pydict() == sample_table(100).to_pydict()


def test_chunked_csv_directory(tmp_path):
    for idx in range(2):
        with ls.write(str(tmp_path / f"part-{idx:010}.csv")) as w:
            w.write(sample_table(idx * 100))

    batches = list(ls.read(str(tmp_path)))
    assert len(batches) == 2
    assert pa.table(batches[0]).to_pydict() == sample_table(0).to_pydict()


def test_mixed_format_directory_narrows_by_format(tmp_path):
    with ls.write(str(tmp_path / "part-0000000000.arrow")) as w:
        w.write(sample_table(0))
    with ls.write(str(tmp_path / "part-0000000000.parquet")) as w:
        w.write(sample_table(100))

    with pytest.raises(ls.FormatError, match="multiple chunk sets"):
        ls.read(str(tmp_path))

    batches = list(ls.read(str(tmp_path), format="parquet"))
    assert pa.table(batches[0]).to_pydict() == sample_table(100).to_pydict()


def test_directory_base_spanning_formats_stays_ambiguous(tmp_path):
    with ls.write(str(tmp_path / "part-0000000000.arrow")) as w:
        w.write(sample_table(0))
    with ls.write(str(tmp_path / "part-0000000000.parquet")) as w:
        w.write(sample_table(100))

    with pytest.raises(ls.FormatError, match="multiple chunk sets"):
        ls.read(str(tmp_path), base="part")

    batches = list(ls.read(str(tmp_path), base="part", format="ipc"))
    assert pa.table(batches[0]).to_pydict() == sample_table(0).to_pydict()


def test_compression_on_csv_raises(tmp_path):
    with pytest.raises(ValueError, match="compression applies"):
        ls.write(str(tmp_path / "quotes.csv"), compression="zstd")


def test_unknown_compression_raises(tmp_path):
    with pytest.raises(ValueError, match="unknown compression"):
        ls.write(str(tmp_path / "quotes.parquet"), compression="lz9")


def test_delimiter_on_parquet_raises(tmp_path):
    with pytest.raises(ValueError, match="csv format only"):
        ls.read(str(tmp_path / "quotes.parquet"), delimiter=";")


def test_batch_size_on_ipc_raises(tmp_path):
    with pytest.raises(ValueError, match="csv and json formats only"):
        ls.read(str(tmp_path / "quotes.arrow"), batch_size=10)


def test_multichar_delimiter_raises(tmp_path):
    with pytest.raises(ValueError, match="single ASCII character"):
        ls.read(str(tmp_path / "quotes.csv"), delimiter="::")
