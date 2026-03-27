#!/usr/bin/env python3
"""
Reconcile Shopify on-hand quantities with ``catalog_items``; optional push via Admin API.

Set ``SHOPIFY_INVENTORY_APPLY=1`` and ``SHOPIFY_INVENTORY_LOCATION_ID`` to write to Shopify.

Usage::

    ./venv/bin/python -m jobs.shopify_inventory_sync
    ./venv/bin/python -m jobs.shopify_inventory_sync --dry-run
    ./venv/bin/python -m jobs.shopify_inventory_sync --dry-run --drift-report logs/shopify_inventory_drift.json
"""

from __future__ import annotations

import argparse
import json
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
    parser = argparse.ArgumentParser(description="Reconcile Shopify inventory vs catalog_items")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compare only; no Supabase timestamps or Shopify mutations",
    )
    parser.add_argument(
        "--drift-report",
        metavar="PATH",
        default=None,
        help="Write drift_details JSON array to this file (use with --dry-run to inspect before apply)",
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env (default: .env under repo root)")
    args = parser.parse_args(argv)

    repo = _repo_root()
    os.chdir(repo)
    log_path = _setup_logging(repo / "logs", "shopify_inventory_sync")
    log = logging.getLogger("jobs.shopify_inventory_sync")
    log.info("Starting shopify_inventory_sync repo_root=%s log_file=%s", repo, log_path)

    from dotenv import load_dotenv

    env_path = repo / args.env_file if not os.path.isabs(args.env_file) else Path(args.env_file)
    if env_path.is_file():
        load_dotenv(env_path)
        log.info("Loaded environment from %s", env_path)

    try:
        from app.services.shopify_inventory_sync_service import run_shopify_inventory_sync

        result = run_shopify_inventory_sync(env_file=str(env_path), dry_run=args.dry_run)
        drift_details = result.get("drift_details")
        summary = {k: v for k, v in result.items() if k != "drift_details"}
        log.info("shopify_inventory_sync finished: %s", summary if drift_details else result)
        print(f"[jobs.shopify_inventory_sync] SUCCESS — {summary}")
        if drift_details is not None:
            if args.drift_report:
                out_path = Path(args.drift_report)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(
                    json.dumps(drift_details, indent=2, default=str) + "\n",
                    encoding="utf-8",
                )
                log.info("Wrote %s drift row(s) to %s", len(drift_details), out_path.resolve())
                print(
                    f"[jobs.shopify_inventory_sync] drift report: {len(drift_details)} row(s) → {out_path.resolve()}"
                )
            elif args.dry_run:
                print("[jobs.shopify_inventory_sync] drift_details (JSON):\n")
                print(json.dumps(drift_details, indent=2, default=str))
        return 0
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        log.error("Exited with code %s", code)
        return code
    except Exception:
        log.error("shopify_inventory_sync failed:\n%s", traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
