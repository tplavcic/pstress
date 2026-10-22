#!/usr/bin/env python3
"""pstress_reducer: reduce a captured failing pstress workload while keeping concurrent sessions.

See README.md in this directory.  Requires Python 3.11+ and PyMySQL (requirements.txt).
"""

import os
import sys

if sys.version_info < (3, 10):
    sys.exit("pstress_reducer requires Python 3.11+ (3.10 works for the importer and tests)")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from reducerlib.cli import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
