#!/usr/bin/env python3
"""
Upsert staging offers → catalog_items (preserve TMDB) — backward-compatible CLI.

Core implementation: app.services.catalog_upsert_service
"""

from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services.catalog_upsert_service import run_from_argv

if __name__ == "__main__":
    raise SystemExit(run_from_argv())
