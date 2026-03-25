#!/usr/bin/env python3
"""
Step 7 — Build / link films from catalog_items.

Delegates to: app.services.film_builder_service.run_from_argv
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

from app.services.film_builder_service import run_from_argv

if __name__ == "__main__":
    raise SystemExit(run_from_argv())
