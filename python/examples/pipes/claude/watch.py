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

PROMPT = (
    "This CSV batch tripped a data-quality check on a quote feed. "
    "Columns are sym, qty, px. Diagnose the bad rows in one short "
    "paragraph and say whether to quarantine the batch."
)


def suspicious(batch):
    sym = batch["sym"]
    qty = batch["qty"]
    return any(sym[i] == "" for i in range(len(sym))) or any(
        qty[i] < 0 for i in range(len(qty))
    )


def to_csv(batch):
    names = batch.columns
    columns = [batch[name] for name in names]
    lines = [",".join(names)]
    for i in range(batch.n_rows):
        lines.append(
            ",".join("" if column[i] is None else str(column[i]) for column in columns)
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
        if suspicious(batch):
            print(f"batch tripped checks ({batch.n_rows} rows), escalating...", flush=True)
            escalate(to_csv(batch))
        else:
            print(f"batch ok ({batch.n_rows} rows)", flush=True)


if __name__ == "__main__":
    watch()
