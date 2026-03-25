#!/usr/bin/env python3
"""
Step 0 — Fetch latest supplier files (FTP + optional Lasgo SFTP mirror).

Delegates to: app.services.supplier_fetch_service.run_from_argv

Lasgo: when LASGO_SFTP_MIRROR_ENABLED=1, vendor SFTP → scan → our Lasgo FTP path, then
FTP → local (stock or catalog). Moovies: FTP → local only. Every file is scanned on disk
(SUPPLIER_FETCH_SECURITY_SCAN) before step 01 import.

CLI: `--mode stock` (inventory) or `--mode catalog` + `--strict-catalog` (catalog sync).
`run_stock_sync.sh` / `run_catalog_sync.sh` invoke this with the right flags.
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

from app.services.supplier_fetch_service import run_from_argv

if __name__ == "__main__":
    raise SystemExit(run_from_argv())
