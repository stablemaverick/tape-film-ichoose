#!/usr/bin/env python3
"""
Normalize raw staging tables → staging_supplier_offers — backward-compatible CLI.

Core implementation: app.services.normalize_offers_service
"""

from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services.normalize_offers_service import run_from_argv

if __name__ == "__main__":
    raise SystemExit(run_from_argv())
