# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Streams CSV batches of string data to stdout for Unix pipelines.

format="csv" on the stdio wire writes text rather than Arrow IPC
bytes, so sed, awk, grep, and friends slot straight into the pipe.
"""

import time

import lightstream as ls
import minarrow

BATCHES = 10
ROWS_PER_BATCH = 5


def produce():
    writer = ls.write("stdio:", format="csv")
    for index in range(BATCHES):
        labels = [f"row-{index}-{n}" for n in range(ROWS_PER_BATCH)]
        writer.write(minarrow.Table({"label": labels, "batch": [index] * ROWS_PER_BATCH}))
        time.sleep(0.3)
    writer.close()


if __name__ == "__main__":
    produce()
