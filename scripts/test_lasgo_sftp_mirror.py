#!/usr/bin/env python3
"""
Smoke test: Lasgo SFTP (latest matching file) → security scan → upload to Lasgo FTP inbox.

Same logic as the pipeline pre-step (no Moovies fetch, no Supabase).

Usage (from repo root):
  ./venv/bin/python scripts/test_lasgo_sftp_mirror.py --mode stock
  ./venv/bin/python scripts/test_lasgo_sftp_mirror.py --mode catalog
  ./venv/bin/python scripts/test_lasgo_sftp_mirror.py --mode stock --force

  ./venv/bin/python scripts/test_lasgo_sftp_mirror.py --mode stock --connect-only
  ./venv/bin/python scripts/test_lasgo_sftp_mirror.py --mode stock --connect-only --force

  --force  sets LASGO_SFTP_MIRROR_ENABLED=1 for this process only (if not already in .env).
  --connect-only  TCP+SSH+auth+listdir only (no download, no FTP) — use to verify credentials/timeouts.

Requires .env: LASGO_SFTP_HOST, LASGO_SFTP_USER, LASGO_SFTP_PASSWORD or LASGO_SFTP_IDENTITY,
  LASGO_SFTP_STOCK_REMOTE_DIR (stock) or LASGO_SFTP_CATALOG_REMOTE_DIR (catalog),
  plus Lasgo FTP credentials (LASGO_FTP_* or MOOVIES_FTP_* chain) for full mirror (not --connect-only).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _enabled() -> bool:
    v = os.getenv("LASGO_SFTP_MIRROR_ENABLED", "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def main() -> int:
    p = argparse.ArgumentParser(description="Test Lasgo SFTP → scan → FTP mirror")
    p.add_argument("--mode", choices=("stock", "catalog"), required=True)
    p.add_argument("--env", default=".env", help="Dotenv path")
    p.add_argument(
        "--force",
        action="store_true",
        help="Set LASGO_SFTP_MIRROR_ENABLED=1 for this run (process env only)",
    )
    p.add_argument(
        "--connect-only",
        action="store_true",
        help="Only verify SFTP connect + list remote dir (no file transfer, no FTP)",
    )
    args = p.parse_args()

    from dotenv import load_dotenv

    env_path = Path(args.env)
    if not env_path.is_absolute():
        env_path = ROOT / env_path
    load_dotenv(env_path)

    if args.force:
        os.environ["LASGO_SFTP_MIRROR_ENABLED"] = "1"

    if not _enabled():
        print(
            "LASGO_SFTP_MIRROR_ENABLED is not on. Add it to .env or pass --force.",
            file=sys.stderr,
        )
        return 2

    scan = (os.getenv("SUPPLIER_FETCH_SECURITY_SCAN") or "basic").strip().lower()
    print(
        f"test_lasgo_sftp_mirror: mode={args.mode!r} scan={scan!r} "
        f"connect_only={args.connect_only} env={env_path}",
        file=sys.stderr,
    )

    from app.services.lasgo_sftp_mirror import mirror_lasgo_sftp_to_ftp_if_enabled, probe_lasgo_sftp

    if args.connect_only:
        probe_lasgo_sftp(args.mode, env_file=str(env_path), require_mirror_enabled=True)
    else:
        mirror_lasgo_sftp_to_ftp_if_enabled(args.mode, env_file=str(env_path))
    print("test_lasgo_sftp_mirror: done.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
