#!/usr/bin/env python3
"""
Optional entrypoint for TMDB backlog burn-down (delegates to maintenance script).

Prefer: scripts/maintenance/burn_down_tmdb_backlog.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "scripts" / "maintenance" / "burn_down_tmdb_backlog.py"

if __name__ == "__main__":
    cmd = [sys.executable, str(TARGET), *sys.argv[1:]]
    raise SystemExit(subprocess.call(cmd, cwd=str(ROOT)))
