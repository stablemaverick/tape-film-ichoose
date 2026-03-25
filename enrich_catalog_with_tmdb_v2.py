#!/usr/bin/env python3
"""
TMDB enrichment for catalog_items — backward-compatible CLI.

Core implementation: app.services.tmdb_enrichment_service

Modes:
  --daily     Cron-safe: only rows with film_id IS NULL
  (default)   Recovery: all rows with tmdb_last_refreshed_at IS NULL
"""

from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services.tmdb_enrichment_service import run_from_argv

if __name__ == "__main__":
    raise SystemExit(run_from_argv())
