# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Reads CSV batches from stdin and prints each as it lands.

The upstream side of the pipe is any text stream, so whatever sed or
awk did to the rows arrives here as minarrow tables.
"""

import lightstream as ls
import pyarrow as pa


def consume():
    for batch in ls.read("stdio:", format="csv", batch_size=5):
        labels = pa.table(batch)["label"].to_pylist()
        print(f"batch of {batch.n_rows}: {labels}", flush=True)


if __name__ == "__main__":
    consume()
