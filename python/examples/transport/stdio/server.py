# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""stdio server writing the demo table to stdout as Arrow IPC."""

import sys
from pathlib import Path

import lightstream as ls

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
import datagen


def serve():
    writer = ls.write("stdio:")
    writer.write(datagen.build_table())
    writer.close()


if __name__ == "__main__":
    serve()
