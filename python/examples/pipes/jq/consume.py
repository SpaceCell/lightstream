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


def consume():
    for batch in ls.read("stdio:", format="csv", header=False, batch_size=3):
        names = batch.columns
        columns = [batch[name] for name in names]
        rows = [
            {name: column[i] for name, column in zip(names, columns)}
            for i in range(batch.n_rows)
        ]
        print(f"batch of {batch.n_rows}: {rows}", flush=True)


if __name__ == "__main__":
    consume()
