# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Streams CSV quote batches with occasional bad rows mixed in.

Batches 4 and 8 carry rows that fail basic data-quality checks, for
the watcher on the other side of the pipe to catch.
"""

import time

import lightstream as ls
import minarrow

BATCHES = 10
ROWS_PER_BATCH = 5


def produce():
    writer = ls.write("stdio:", format="csv")
    for index in range(BATCHES):
        syms = [f"SYM{n}" for n in range(ROWS_PER_BATCH)]
        qty = [10 * (n + 1) for n in range(ROWS_PER_BATCH)]
        px = [100.0 + index + n / 10.0 for n in range(ROWS_PER_BATCH)]
        if index in (4, 8):
            syms[2] = ""
            qty[3] = -250
        writer.write(minarrow.Table({"sym": syms, "qty": qty, "px": px}))
        time.sleep(0.2)
    writer.close()


if __name__ == "__main__":
    produce()
