#!/usr/bin/env python3
"""
Step 6 (recovery) — Enrich all unattempted catalog rows with TMDB.

Delegates to: app.services.tmdb_enrichment_service.run_from_argv
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

from app.services.tmdb_enrichment_service import run_from_argv

if __name__ == "__main__":
    raise SystemExit(run_from_argv(sys.argv[1:]))
