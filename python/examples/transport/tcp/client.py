# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""TCP client fetching the demo table."""

import os

import lightstream as ls

URI = os.environ.get("LIGHTSTREAM_EXAMPLE_URI", "tcp://127.0.0.1:9040")

def fetch():
    """Connects to the endpoint and reads the streamed table."""
    with ls.read(URI) as reader:
        return reader.read_all()
