#!/usr/bin/env python3
"""
Shopify store → ``shopify_listings`` sync (no supplier import, no catalog upsert).

Usage::

    ./venv/bin/python -m jobs.shopify_store_sync
    ./venv/bin/python -m jobs.shopify_store_sync --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _setup_logging(log_dir: Path, stem: str) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    log_path = log_dir / f"job_{stem}_{stamp}.log"
    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    root.addHandler(fh)
    root.addHandler(sh)
    return log_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync Shopify variants into shopify_listings")
    parser.add_argument("--dry-run", action="store_true", help="Fetch + match only; no DB writes")
    parser.add_argument("--env-file", default=".env", help="Path to .env (default: .env under repo root)")
    args = parser.parse_args(argv)

    repo = _repo_root()
    os.chdir(repo)
    log_path = _setup_logging(repo / "logs", "shopify_store_sync")
    log = logging.getLogger("jobs.shopify_store_sync")
    log.info("Starting shopify_store_sync repo_root=%s log_file=%s", repo, log_path)

    from dotenv import load_dotenv

    env_path = repo / args.env_file if not os.path.isabs(args.env_file) else Path(args.env_file)
    if env_path.is_file():
        load_dotenv(env_path)
        log.info("Loaded environment from %s", env_path)

    try:
        from app.services.shopify_store_sync_service import run_shopify_store_sync

        result = run_shopify_store_sync(env_file=str(env_path), dry_run=args.dry_run)
        log.info("shopify_store_sync finished: %s", result)
        print(f"[jobs.shopify_store_sync] SUCCESS — {result}")
        return 0
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        log.error("Exited with code %s", code)
        return code
    except Exception:
        log.error("shopify_store_sync failed:\n%s", traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
