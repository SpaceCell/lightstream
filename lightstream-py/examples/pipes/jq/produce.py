# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Streams NDJSON batches to stdout for jq pipelines.

format="json" on the stdio wire writes one JSON object per row, so jq
filters and reshapes the feed natively.
"""

import time

import lightstream as ls
import minarrow

BATCHES = 10
ROWS_PER_BATCH = 5


def produce():
    writer = ls.write("stdio:", format="json")
    for index in range(BATCHES):
        labels = [f"row-{index}-{n}" for n in range(ROWS_PER_BATCH)]
        qty = [n for n in range(ROWS_PER_BATCH)]
        writer.write(minarrow.Table({"label": labels, "qty": qty}))
        time.sleep(0.2)
    writer.close()


if __name__ == "__main__":
    produce()
