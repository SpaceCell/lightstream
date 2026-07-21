# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""QUIC server streaming the demo table as Arrow IPC.

The first accepting write binds the endpoint and it stays bound for
the life of the process, so clients queue while another connection is
served.
"""

import os
import sys
from pathlib import Path

import lightstream as ls

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
import datagen

URI = os.environ.get("LIGHTSTREAM_EXAMPLE_URI", "quic://127.0.0.1:9045")
TLS_CERT = os.environ["LIGHTSTREAM_EXAMPLE_TLS_CERT"]
TLS_KEY = os.environ["LIGHTSTREAM_EXAMPLE_TLS_KEY"]


def serve():
    table = datagen.build_table()
    print(f"Serving get_table on {URI}", flush=True)
    while True:
        writer = ls.write(URI, accept=True, tls_cert=TLS_CERT, tls_key=TLS_KEY)
        writer.write(table)
        writer.close()


if __name__ == "__main__":
    serve()
