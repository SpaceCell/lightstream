# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Lightstream TLV protocol over the disk transport.

One connection multiplexes registered table types, decoded as Arrow
IPC, and registered message types carried as opaque payloads. Reader
and writer register the same types in the same order, since tags assign
in registration order.
"""

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


@pytest.fixture
def capture(tmp_path):
    """A TLV capture holding two quote tables around one health message."""
    path = str(tmp_path / "feed.tlv")
    w = ls.write(path, protocol="lightstream")
    w.register_table("quotes", sample_table())
    w.register_message("health")
    w.write(sample_table(0), name="quotes")
    w.write_message("health", b"\x01\x02\x03")
    w.write(sample_table(100), name="quotes")
    w.close()
    return path


def open_capture(path):
    r = ls.read(path, protocol="lightstream")
    quotes = r.register_table("quotes", sample_table())
    health = r.register_message("health")
    return r, quotes, health


def test_round_trip_mixed_frames(capture):
    r, quotes, health = open_capture(capture)
    frames = list(r)

    assert [f.is_table() for f in frames] == [True, False, True]
    assert all(isinstance(f, ls.Message) for f in frames)
    assert frames[0].tag == quotes
    assert frames[1].tag == health
    assert frames[1].payload == b"\x01\x02\x03"
    assert frames[1].table is None
    assert frames[0].payload is None

    table = frames[2].table
    assert isinstance(table, minarrow.Table)
    assert pa.table(table).to_pydict() == sample_table(100).to_pydict()


def test_read_all_skips_message_frames(capture):
    r, _, _ = open_capture(capture)
    result = r.read_all()
    assert isinstance(result, minarrow.ChunkedTable)


def test_capsule_export_skips_message_frames(capture):
    r, _, _ = open_capture(capture)
    result = pa.table(r)
    assert result.num_rows == 6
    assert result.column("qty").to_pylist() == [10, 20, 30, 110, 120, 130]


def test_registration_tags_assign_in_order(tmp_path):
    w = ls.write(str(tmp_path / "feed.tlv"), protocol="lightstream")
    first = w.register_table("quotes", sample_table())
    second = w.register_message("health")
    assert first != second
    w.close()


def test_register_on_file_source_raises(tmp_path):
    path = str(tmp_path / "quotes.arrow")
    with ls.write(path) as w:
        w.write(sample_table())

    reader = ls.read(path)
    with pytest.raises(ls.ProtocolError, match="lightstream protocol sources"):
        reader.register_table("quotes", sample_table())
    with pytest.raises(ls.ProtocolError, match="lightstream protocol sources"):
        reader.register_message("health")


def test_named_write_on_file_target_raises(tmp_path):
    with ls.write(str(tmp_path / "quotes.arrow")) as w:
        with pytest.raises(ValueError, match="lightstream protocol targets only"):
            w.write(sample_table(), name="quotes")


def test_unnamed_write_on_protocol_target_raises(tmp_path):
    w = ls.write(str(tmp_path / "feed.tlv"), protocol="lightstream")
    w.register_table("quotes", sample_table())
    with pytest.raises(ValueError, match="need name="):
        w.write(sample_table())
    w.close()


def test_write_message_on_file_target_raises(tmp_path):
    with ls.write(str(tmp_path / "quotes.arrow")) as w:
        with pytest.raises(ls.ProtocolError, match="lightstream protocol targets"):
            w.write_message("health", b"\x00")


def test_unregistered_name_raises(tmp_path):
    w = ls.write(str(tmp_path / "feed.tlv"), protocol="lightstream")
    with pytest.raises(ls.LightstreamError):
        w.write(sample_table(), name="unregistered")
    w.close()


def test_schema_object_without_columns_raises(tmp_path):
    w = ls.write(str(tmp_path / "feed.tlv"), protocol="lightstream")
    with pytest.raises(ls.FormatError, match="no columns"):
        w.register_table("quotes", pa.table({}))
    w.close()


def test_format_arguments_rejected_on_protocol_streams(tmp_path):
    with pytest.raises(ValueError, match="file-format reads"):
        ls.read(str(tmp_path / "feed.tlv"), protocol="lightstream", delimiter=";")
    with pytest.raises(ValueError, match="file-format writes"):
        ls.write(str(tmp_path / "feed.tlv"), protocol="lightstream", compression="zstd")


def test_arrow_protocol_over_disk_raises(tmp_path):
    with pytest.raises(ls.TransportError, match="does not frame the disk transport"):
        ls.read(str(tmp_path / "feed.arrow"), protocol="arrow")


def test_missing_capture_raises(tmp_path):
    with pytest.raises(ls.LightstreamError):
        ls.read(str(tmp_path / "absent.tlv"), protocol="lightstream")
