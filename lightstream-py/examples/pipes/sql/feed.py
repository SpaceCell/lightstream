# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Serves a trickle of quote batches over TCP for live SQL."""

import os
import time

import lightstream as ls
import minarrow

URI = os.environ.get("LIGHTSTREAM_EXAMPLE_URI", "tcp://127.0.0.1:9048")
BATCHES = 30
SYMS = ["AAA", "BBB", "CCC"]


def serve():
    print(f"Serving quotes on {URI}", flush=True)
    writer = ls.write(URI, accept=True)
    for index in range(BATCHES):
        px = [100.0 + ((index * 7 + n * 3) % 50) / 10.0 for n in range(len(SYMS))]
        writer.write(minarrow.Table({"sym": SYMS, "px": px, "batch": [index] * len(SYMS)}))
        time.sleep(0.1)
    writer.close()


if __name__ == "__main__":
    serve()
