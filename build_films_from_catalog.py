#!/usr/bin/env python3
"""
Build / link films from catalog_items — backward-compatible CLI.

Core implementation: app.services.film_builder_service
"""

from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services.film_builder_service import run_from_argv

if __name__ == "__main__":
    raise SystemExit(run_from_argv())
