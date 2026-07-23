# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""TCP, UDS, and stdio wires under both protocols.

Reader and writer are both connecting clients, so each test runs a
relay: a listener that accepts the reader first, then the writer, and
pumps the writer's bytes to the reader until the writer closes.
"""

import socket
import subprocess
import sys
import threading

import lightstream as ls
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


def start_relay(listener):
    """Accepts the reader then the writer, pumping writer bytes to reader."""

    def pump():
        reader_side, _ = listener.accept()
        writer_side, _ = listener.accept()
        while True:
            data = writer_side.recv(65536)
            if not data:
                break
            reader_side.sendall(data)
        reader_side.close()
        writer_side.close()
        listener.close()

    thread = threading.Thread(target=pump, daemon=True)
    thread.start()
    return thread


@pytest.fixture
def tcp_relay():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(2)
    port = listener.getsockname()[1]
    thread = start_relay(listener)
    yield f"tcp://127.0.0.1:{port}"
    thread.join(timeout=5)


@pytest.fixture
def uds_relay(tmp_path):
    sock_path = str(tmp_path / "relay.sock")
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(sock_path)
    listener.listen(2)
    thread = start_relay(listener)
    yield f"uds://{sock_path}"
    thread.join(timeout=5)


def run_arrow_round_trip(uri):
    reader = ls.read(uri)
    writer = ls.write(uri)
    writer.write(sample_table(0))
    writer.write(sample_table(100))
    writer.close()

    batches = list(reader)
    assert len(batches) == 2
    assert pa.table(batches[0]).to_pydict() == sample_table(0).to_pydict()
    assert pa.table(batches[1]).to_pydict() == sample_table(100).to_pydict()


def run_lightstream_round_trip(uri):
    reader = ls.read(uri, protocol="lightstream")
    writer = ls.write(uri, protocol="lightstream")
    for end in (reader, writer):
        end.register_table("quotes", sample_table())
        end.register_message("health")
    writer.write(sample_table(0), name="quotes")
    writer.write_message("health", b"\x07")
    writer.close()

    frames = list(reader)
    assert [f.is_table() for f in frames] == [True, False]
    assert pa.table(frames[0].table).to_pydict() == sample_table(0).to_pydict()
    assert frames[1].payload == b"\x07"


def test_tcp_arrow_round_trip(tcp_relay):
    run_arrow_round_trip(tcp_relay)


def test_tcp_lightstream_round_trip(tcp_relay):
    run_lightstream_round_trip(tcp_relay)


def test_uds_arrow_round_trip(uds_relay):
    run_arrow_round_trip(uds_relay)


def test_uds_lightstream_round_trip(uds_relay):
    run_lightstream_round_trip(uds_relay)


def test_tcp_reader_feeds_arrow_ecosystem(tcp_relay):
    reader = ls.read(tcp_relay)
    writer = ls.write(tcp_relay)
    writer.write(sample_table(0))
    writer.write(sample_table(100))
    writer.close()

    result = pa.table(reader)
    assert result.num_rows == 6


def test_stdio_pipeline():
    writer_code = (
        "import lightstream as ls, pyarrow as pa\n"
        "w = ls.write('stdio:')\n"
        "w.write(pa.table({'v': [1, 2, 3]}))\n"
        "w.write(pa.table({'v': [4, 5]}))\n"
        "w.close()\n"
    )
    reader_code = (
        "import lightstream as ls\n"
        "print(sum(t.n_rows for t in ls.read('stdio:')))\n"
    )
    writer_proc = subprocess.Popen(
        [sys.executable, "-c", writer_code], stdout=subprocess.PIPE
    )
    reader_proc = subprocess.run(
        [sys.executable, "-c", reader_code],
        stdin=writer_proc.stdout,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert writer_proc.wait(timeout=60) == 0
    assert reader_proc.returncode == 0, reader_proc.stderr
    assert reader_proc.stdout.strip() == "5"


def test_stdio_csv_pipeline_through_sed():
    producer_code = (
        "import lightstream as ls, pyarrow as pa\n"
        "w = ls.write('stdio:', format='csv')\n"
        "w.write(pa.table({'sym': ['a', 'b'], 'qty': [1, 2]}))\n"
        "w.write(pa.table({'sym': ['c', 'd'], 'qty': [3, 4]}))\n"
        "w.close()\n"
    )
    consumer_code = (
        "import lightstream as ls, pyarrow as pa\n"
        "t = ls.read('stdio:', format='csv').read_all()\n"
        "print(t.n_rows, ','.join(pa.table(t)['sym'].to_pylist()))\n"
    )
    producer = subprocess.Popen(
        [sys.executable, "-c", producer_code], stdout=subprocess.PIPE
    )
    sed = subprocess.Popen(
        ["sed", "-u", "s/a/A/"], stdin=producer.stdout, stdout=subprocess.PIPE
    )
    consumer = subprocess.run(
        [sys.executable, "-c", consumer_code],
        stdin=sed.stdout,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert producer.wait(timeout=60) == 0
    assert sed.wait(timeout=60) == 0
    assert consumer.returncode == 0, consumer.stderr
    assert consumer.stdout.strip() == "4 A,b,c,d"


def test_stdio_text_streaming_formats():
    with pytest.raises(ls.FormatError, match="csv format"):
        ls.read("stdio:", format="parquet")
    with pytest.raises(ls.FormatError, match="csv format"):
        ls.read("stdio:", format="json")
    with pytest.raises(ls.FormatError, match="csv and json"):
        ls.write("stdio:", format="parquet")


def test_connection_refused_raises():
    with pytest.raises(ls.LightstreamError):
        ls.read("tcp://127.0.0.1:9")


def test_format_arguments_rejected_on_wires(tcp_relay):
    with pytest.raises(ValueError, match="not wire endpoints"):
        ls.read(tcp_relay, delimiter=";")
    with pytest.raises(ValueError, match="not wire endpoints"):
        ls.write(tcp_relay, header=False)


def test_parallel_has_no_form_on_uds(uds_relay):
    with pytest.raises(ls.TransportError, match="no parallel form"):
        ls.read(uds_relay, parallel=True)


def test_parallel_lightstream_not_available():
    with pytest.raises(ls.TransportError, match="not available in this build"):
        ls.write("tcp://127.0.0.1:9", parallel=2, protocol="lightstream")


def test_tcp_parallel_round_trip():
    import time

    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    uri = f"tcp://127.0.0.1:{port}"

    result = {}

    def read_side():
        reader = ls.read(uri, parallel=2)
        result["rows"] = sum(t.n_rows for t in reader)

    thread = threading.Thread(target=read_side, daemon=True)
    thread.start()
    time.sleep(0.5)

    writer = ls.write(uri, parallel=2)
    writer.write(sample_table(0))
    writer.write(sample_table(100))
    writer.close()

    thread.join(timeout=30)
    assert result.get("rows") == 6


def test_tls_schemes_require_roots_when_connecting():
    for scheme in ("wss", "https"):
        with pytest.raises(ls.TransportError, match="requires tls_ca"):
            ls.read(f"{scheme}://host:1")


def test_ws_and_http_reach_connection():
    for uri in ("ws://127.0.0.1:9", "http://127.0.0.1:9/feed"):
        with pytest.raises(ls.LightstreamError):
            ls.read(uri)


def test_lightstream_over_ws_and_http_reaches_connection():
    for uri in ("ws://127.0.0.1:9", "http://127.0.0.1:9/feed"):
        with pytest.raises(ls.LightstreamError):
            ls.read(uri, protocol="lightstream")
        with pytest.raises(ls.LightstreamError):
            ls.write(uri, protocol="lightstream")
