# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""HTTP/2 client fetching the demo table."""

import os

import lightstream as ls

URI = os.environ.get("LIGHTSTREAM_EXAMPLE_URI", "http://127.0.0.1:9043/get_table")

def fetch():
    """Connects to the endpoint and reads the streamed table."""
    with ls.read(URI) as reader:
        return reader.read_all()
