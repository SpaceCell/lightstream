# Copyright Peter G. Bower 2025-2026.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""stdio client reading the demo table from stdin."""

import lightstream as ls


def fetch():
    """Reads the streamed table from stdin."""
    return ls.read("stdio:").read_all()
