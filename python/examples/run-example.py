#!/usr/bin/env python3
# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Runs a transport or pipeline example end to end.

Every folder under transport/ holds a server.py, a server.rs, and a
client.py over the same demo table, and this router wires the chosen
pair together:

    run-example.py <transport> [--rust]

The server backend is Python by default or Rust with --rust, and the
transports are tcp, uds, ws, wss, http, https, quic, wt, and stdio.
The TLS transports get a generated private root and server
certificate. stdio pipes the server's stdout into the client.

The folders under pipes/ demonstrate composition instead: sed and jq
rewrite text-streamed batches in flight, claude watches a feed for
bad data, sql runs rolling DuckDB aggregates over a live wire, and
debug tails any feed with pretty printing.
"""

import argparse
import importlib.util
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

EXAMPLES_DIR = Path(__file__).resolve().parent
MANIFEST = EXAMPLES_DIR.parent / "Cargo.toml"

TRANSPORTS = ["tcp", "uds", "ws", "wss", "http", "https", "quic", "wt", "stdio"]
PIPES = ["sed", "claude", "jq", "sql", "debug"]
TLS_TRANSPORTS = {"wss", "https", "quic", "wt"}


def free_port():
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


def example_uri(transport, workdir):
    if transport == "uds":
        return f"uds://{workdir / 'get_table.sock'}"
    port = free_port()
    path = "/get_table" if transport in ("http", "https") else ""
    return f"{transport}://127.0.0.1:{port}{path}"


def generate_tls(workdir):
    """Generates a private root and a server certificate signed by it."""
    ca = workdir / "ca.pem"
    ca_key = workdir / "ca-key.pem"
    cert = workdir / "cert.pem"
    key = workdir / "key.pem"
    csr = workdir / "server.csr"
    ext = workdir / "server.ext"
    ec = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes"]

    subprocess.run(
        ["openssl", "req", "-x509", *ec, "-keyout", str(ca_key), "-out", str(ca)]
        + ["-days", "2", "-subj", "/CN=lightstream example root"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["openssl", "req", "-new", *ec, "-keyout", str(key), "-out", str(csr)]
        + ["-subj", "/CN=localhost"],
        check=True,
        capture_output=True,
    )
    ext.write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
        "basicConstraints=CA:FALSE\n"
        "keyUsage=digitalSignature\n"
        "extendedKeyUsage=serverAuth\n"
    )
    subprocess.run(
        ["openssl", "x509", "-req", "-in", str(csr), "-CA", str(ca), "-CAkey", str(ca_key)]
        + ["-CAcreateserial", "-out", str(cert), "-days", "2", "-extfile", str(ext)],
        check=True,
        capture_output=True,
    )
    return str(cert), str(key), str(ca)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_client(transport):
    return load_module(f"{transport}_client", EXAMPLES_DIR / "transport" / transport / "client.py")


def server_command(transport, rust):
    if rust:
        return [
            "cargo",
            "run",
            "--quiet",
            "--manifest-path",
            str(MANIFEST),
            "--example",
            f"{transport}_server",
        ]
    return [sys.executable, str(EXAMPLES_DIR / "transport" / transport / "server.py")]


def run_stdio(rust, env):
    """Pipes the stdio server's stdout into a client subprocess."""
    server = subprocess.Popen(server_command("stdio", rust), stdout=subprocess.PIPE, env=env)
    client_code = (
        "import importlib.util\n"
        f"spec = importlib.util.spec_from_file_location('client', r'{EXAMPLES_DIR / 'transport' / 'stdio' / 'client.py'}')\n"
        "client = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(client)\n"
        "print(client.fetch().n_rows)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", client_code],
        stdin=server.stdout,
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    assert server.wait(timeout=60) == 0
    assert result.returncode == 0, result.stderr
    return int(result.stdout.strip())


def fetch_with_retries(client, deadline=120.0):
    """Retries the fetch until the server has bound its endpoint."""
    import lightstream as ls

    last = None
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        try:
            return client.fetch()
        except ls.LightstreamError as exc:
            last = exc
            time.sleep(0.2)
    raise last


def run_pipeline(pipeline, intro):
    """Prints the copy-pasteable pipeline, then runs it live."""
    print("Try it yourself in a terminal:\n")
    print(f"  {pipeline}\n")
    print(f"{intro}\n", flush=True)
    result = subprocess.run(["bash", "-c", f"set -o pipefail; {pipeline}"], timeout=300)
    assert result.returncode == 0
    print("\nExample completed successfully.")


def run_sed():
    run_pipeline(
        f"{sys.executable} {EXAMPLES_DIR / 'pipes' / 'sed' / 'produce.py'}"
        " | sed -u 's/row-/ROW-/g' | "
        f"{sys.executable} {EXAMPLES_DIR / 'pipes' / 'sed' / 'consume.py'}",
        "Running it now - each batch lands as sed rewrites it:",
    )


def run_jq():
    run_pipeline(
        f"{sys.executable} {EXAMPLES_DIR / 'pipes' / 'jq' / 'produce.py'}"
        " | jq -rc --unbuffered 'select(.qty > 2) | [.label, .qty] | @csv' | "
        f"{sys.executable} {EXAMPLES_DIR / 'pipes' / 'jq' / 'consume.py'}",
        "Running it now - jq keeps rows with qty > 2 and re-emits them as CSV:",
    )


def run_claude():
    run_pipeline(
        f"{sys.executable} {EXAMPLES_DIR / 'pipes' / 'claude' / 'produce.py'}"
        " | "
        f"{sys.executable} {EXAMPLES_DIR / 'pipes' / 'claude' / 'watch.py'}",
        "Running it now - cheap checks gate the feed, bad batches escalate:",
    )


def run_feed_and(consumer_path, folder, extra_args=None):
    """Spawns a feed server and runs the consumer against it."""
    uri = f"tcp://127.0.0.1:{free_port()}"
    env = dict(os.environ)
    env["LIGHTSTREAM_EXAMPLE_URI"] = uri
    feed = subprocess.Popen(
        [sys.executable, str(EXAMPLES_DIR / "pipes" / folder / "feed.py")], env=env
    )
    try:
        args = [a.replace("{uri}", uri) for a in (extra_args or [])]
        result = subprocess.run(
            [sys.executable, str(consumer_path)] + args,
            env=env,
            timeout=300,
        )
        assert result.returncode == 0
        print("\nExample completed successfully.")
    finally:
        feed.terminate()
        try:
            feed.wait(timeout=10)
        except subprocess.TimeoutExpired:
            feed.kill()
            feed.wait()


def run_sql():
    print("Rolling DuckDB aggregates over a live TCP feed:\n", flush=True)
    run_feed_and(EXAMPLES_DIR / "pipes" / "sql" / "query.py", "sql")


def run_debug():
    tail = EXAMPLES_DIR / "pipes" / "debug" / "tail.py"
    print("tail -f for Arrow feeds, over any transport:\n")
    print(f"  {sys.executable} {tail} <uri> [--accept] [--protocol lightstream]\n")
    print("Tailing a demo TCP feed now:\n", flush=True)
    run_feed_and(tail, "debug", extra_args=["{uri}"])


def main():
    parser = argparse.ArgumentParser(description="Fetch the demo table over a transport.")
    parser.add_argument("transport", choices=TRANSPORTS + PIPES)
    parser.add_argument(
        "--rust",
        action="store_true",
        help="serve from the Rust backend in server.rs instead of server.py",
    )
    args = parser.parse_args()

    if args.transport in PIPES:
        if args.rust:
            parser.error("the pipeline demos run Python producers and consumers")
        {
            "sed": run_sed,
            "jq": run_jq,
            "claude": run_claude,
            "sql": run_sql,
            "debug": run_debug,
        }[args.transport]()
        return

    sys.path.insert(0, str(EXAMPLES_DIR / "common"))
    import datagen

    env = dict(os.environ)
    env["PYTHONPATH"] = str(EXAMPLES_DIR / "common")

    with tempfile.TemporaryDirectory(prefix="lightstream-example-") as tmp:
        workdir = Path(tmp)
        if args.transport in TLS_TRANSPORTS:
            cert, key, ca = generate_tls(workdir)
            env["LIGHTSTREAM_EXAMPLE_TLS_CERT"] = cert
            env["LIGHTSTREAM_EXAMPLE_TLS_KEY"] = key
            env["LIGHTSTREAM_EXAMPLE_TLS_CA"] = ca

        if args.rust:
            subprocess.run(
                ["cargo", "build", "--quiet", "--manifest-path", str(MANIFEST)]
                + ["--example", f"{args.transport}_server"],
                check=True,
            )

        backend = "Rust" if args.rust else "Python"
        if args.transport == "stdio":
            print(f"Piping the {backend} stdio server into the client...", flush=True)
            rows = run_stdio(args.rust, env)
            assert rows == datagen.ROWS, f"expected {datagen.ROWS} rows, got {rows}"
            print(f"Received {rows:,} rows over stdio.")
            print("Example completed successfully.")
            return

        uri = example_uri(args.transport, workdir)
        env["LIGHTSTREAM_EXAMPLE_URI"] = uri
        os.environ["LIGHTSTREAM_EXAMPLE_URI"] = uri
        for name in ("LIGHTSTREAM_EXAMPLE_TLS_CERT", "LIGHTSTREAM_EXAMPLE_TLS_KEY", "LIGHTSTREAM_EXAMPLE_TLS_CA"):
            if name in env:
                os.environ[name] = env[name]

        print(f"Starting the {backend} server on {uri}...", flush=True)
        server = subprocess.Popen(server_command(args.transport, args.rust), env=env)
        try:
            client = load_client(args.transport)
            started = time.monotonic()
            table = fetch_with_retries(client)
            elapsed = time.monotonic() - started

            assert table.n_rows == datagen.ROWS, f"expected {datagen.ROWS} rows, got {table.n_rows}"
            assert table.columns == ["id", "value", "label"], f"unexpected columns {table.columns}"
            print(f"Received {table.n_rows:,} rows x {table.n_cols} cols in {elapsed:.2f}s")
            print(f"Schema: {table.dtypes}")
            print("Example completed successfully.")
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()


if __name__ == "__main__":
    main()
