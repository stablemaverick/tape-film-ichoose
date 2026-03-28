#!/usr/bin/env python3
"""
Ad hoc publish: barcodes from ``catalog_items`` → new Shopify products (separate from store/inventory sync).

Usage::

    ./venv/bin/python -m jobs.publish_catalog_to_shopify --barcodes 123,456
    ./venv/bin/python -m jobs.publish_catalog_to_shopify --barcodes-file path.txt --dry-run
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


def _parse_barcodes_from_args(args: argparse.Namespace) -> list[str]:
    from app.services.catalog_shopify_publish_service import normalize_barcodes

    raw: list[str] = []
    if args.barcodes:
        raw.extend([b.strip() for b in args.barcodes.split(",") if b.strip()])
    if args.barcodes_file:
        p = Path(args.barcodes_file)
        text = p.read_text(encoding="utf-8")
        raw.extend(text.splitlines())
    return normalize_barcodes(raw)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish selected catalog barcodes to Shopify (ad hoc flow; not store sync)."
    )
    parser.add_argument("--barcodes", default=None, help="Comma-separated barcodes")
    parser.add_argument("--barcodes-file", default=None, help="Newline-separated barcodes file")
    parser.add_argument(
        "--supplier",
        default="best_offer",
        help="best_offer (default) or supplier name (e.g. moovies, lasgo, Tape Film)",
    )
    parser.add_argument(
        "--status",
        choices=["draft", "active", "archived"],
        default="draft",
        help="Shopify product status for created products",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env (default .env under repo root)",
    )
    parser.add_argument(
        "--json-summary",
        action="store_true",
        help="Print per-barcode results as JSON to stdout after the run",
    )
    parser.add_argument(
        "--no-publish-flags",
        action="store_true",
        help="Only write shopify_product_id / shopify_variant_id; skip published_to_shopify / shopify_published_at",
    )
    args = parser.parse_args(argv)

    repo = _repo_root()
    os.chdir(repo)
    log_path = _setup_logging(repo / "logs", "publish_catalog_to_shopify")
    log = logging.getLogger("jobs.publish_catalog_to_shopify")
    log.info("Starting publish_catalog_to_shopify repo_root=%s log_file=%s", repo, log_path)

    barcodes = _parse_barcodes_from_args(args)
    if not barcodes:
        log.error("Provide --barcodes and/or --barcodes-file")
        return 1

    try:
        from app.services.catalog_shopify_publish_service import run_catalog_shopify_publish

        result = run_catalog_shopify_publish(
            barcodes=barcodes,
            supplier_mode=args.supplier,
            shopify_status=args.status,
            dry_run=args.dry_run,
            env_file=args.env_file,
            api_version=args.api_version,
            set_publish_flags=not args.no_publish_flags,
        )
        for row in result["results"]:
            log.info("%s", row)
        log.info("summary=%s", result["summary"])
        print(f"[jobs.publish_catalog_to_shopify] SUCCESS — {result['summary']}")
        if args.json_summary:
            print(json.dumps(result["results"], indent=2, default=str))
        return 0 if result["summary"].get("failed", 0) == 0 else 2
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        log.error("Exited with code %s", code)
        return code
    except Exception:
        log.error("publish_catalog_to_shopify failed:\n%s", traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
