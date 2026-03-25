"""
Backward-compatible shim — authoritative implementation: app.helpers.tmdb_match_helpers.

Import from ``app.helpers.tmdb_match_helpers`` in new code. This module exists so
``from tmdb_match_helpers import ...`` keeps working when the repo root is on
``sys.path`` (e.g. ``python relink_unlinked_catalog_rows.py`` from project root).
"""

from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.helpers.tmdb_match_helpers import *  # noqa: F403
