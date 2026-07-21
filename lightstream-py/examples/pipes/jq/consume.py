# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Reads jq's CSV output from stdin and prints each batch.

jq re-emits selected rows with @csv, and the CSV reader turns them
back into tables. jq writes no header row, so header=False.
"""

import lightstream as ls
import pyarrow as pa


def consume():
    for batch in ls.read("stdio:", format="csv", header=False, batch_size=3):
        rows = pa.table(batch).to_pylist()
        print(f"batch of {batch.n_rows}: {rows}", flush=True)


if __name__ == "__main__":
    consume()
