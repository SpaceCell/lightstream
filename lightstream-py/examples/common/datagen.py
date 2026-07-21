# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Builds the demo table the example servers hold in memory.

The Rust server in server.rs generates the same rows, so the client
verifies identical results from either backend.
"""

import minarrow

ROWS = 1_000_000


def build_table():
    """Builds a one million row table with int, float, and string columns."""
    ids = list(range(ROWS))
    values = [i * 0.25 for i in range(ROWS)]
    labels = [f"row-{i % 100}" for i in range(ROWS)]
    return minarrow.Table({"id": ids, "value": values, "label": labels})
