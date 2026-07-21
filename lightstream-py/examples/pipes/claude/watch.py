# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Watches a CSV feed and escalates bad batches to Claude.

Cheap checks run on every batch at full rate, and only batches that
trip them go to the model, arriving as CSV with a diagnosis prompt.
Without the claude CLI on PATH, the watcher prints what it would have
asked instead.
"""

import shutil
import subprocess

import lightstream as ls
import pyarrow as pa

PROMPT = (
    "This CSV batch tripped a data-quality check on a quote feed. "
    "Columns are sym, qty, px. Diagnose the bad rows in one short "
    "paragraph and say whether to quarantine the batch."
)


def suspicious(frame):
    return any(s == "" for s in frame["sym"].to_pylist()) or any(
        q < 0 for q in frame["qty"].to_pylist()
    )


def to_csv(frame):
    lines = [",".join(frame.column_names)]
    for row in frame.to_pylist():
        lines.append(
            ",".join(
                "" if row[name] is None else str(row[name]) for name in frame.column_names
            )
        )
    return "\n".join(lines)


def escalate(csv_text):
    if shutil.which("claude") is None:
        print("  claude CLI not found, would have asked:", flush=True)
        print(f"  {PROMPT}", flush=True)
        return
    result = subprocess.run(
        ["claude", "-p", PROMPT], input=csv_text, capture_output=True, text=True, timeout=120
    )
    verdict = result.stdout.strip() or result.stderr.strip()
    print(f"  claude: {verdict}", flush=True)


def watch():
    for batch in ls.read("stdio:", format="csv", batch_size=5):
        frame = pa.table(batch)
        if suspicious(frame):
            print(f"batch tripped checks ({batch.n_rows} rows), escalating...", flush=True)
            escalate(to_csv(frame))
        else:
            print(f"batch ok ({batch.n_rows} rows)", flush=True)


if __name__ == "__main__":
    watch()
