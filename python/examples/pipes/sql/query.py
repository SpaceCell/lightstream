# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Runs rolling SQL over a live feed with DuckDB.

The reader speaks the Arrow stream protocol, so batches land in
DuckDB with no copies. Every five batches, the last twenty run
through an aggregate and the result prints.
"""

import os
import time
from collections import deque

import duckdb
import lightstream as ls
import minarrow as mn

URI = os.environ.get("LIGHTSTREAM_EXAMPLE_URI", "tcp://127.0.0.1:9048")
WINDOW_BATCHES = 20
REPORT_EVERY = 5


def open_reader(deadline=60.0):
    """Connects to the feed, waiting for it to bind."""
    end = time.monotonic() + deadline
    while True:
        try:
            return ls.read(URI)
        except ls.LightstreamError:
            if time.monotonic() > end:
                raise
            time.sleep(0.1)


def query():
    window = deque(maxlen=WINDOW_BATCHES)
    for count, batch in enumerate(open_reader(), start=1):
        window.append(batch)
        if count % REPORT_EVERY == 0:
            win = mn.ChunkedTable(list(window))
            result = duckdb.sql(
                "SELECT sym, count(*) AS ticks, round(avg(px), 2) AS avg_px,"
                " round(max(px), 2) AS max_px FROM win GROUP BY sym ORDER BY sym"
            )
            print(f"after batch {count}:", flush=True)
            print(result, flush=True)


if __name__ == "__main__":
    query()
