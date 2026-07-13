#!/usr/bin/env python3
"""
Publish selected catalog barcodes to Shopify (VM-friendly CLI).

All create logic lives in ``app.services.catalog_shopify_publish_service`` —
inventory policy, location seeding, and post-create snapshot included.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish selected barcodes from catalog_items to Shopify as draft products."
    )
    parser.add_argument("--barcodes", default=None, help="Comma-separated list of barcodes")
    parser.add_argument("--barcodes-file", default=None, help="Path to newline-separated barcodes file")
    parser.add_argument(
        "--supplier",
        default="best_offer",
        help=(
            "Supplier selection mode: 'best_offer' (default) or a supplier name "
            "(e.g. 'moovies', 'lasgo', 'Tape Film')."
        ),
    )
    parser.add_argument(
        "--status",
        choices=["active", "draft", "archived"],
        default="draft",
        help="Shopify product status for created products",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument(
        "--env",
        default=".env",
        help="Path to env file (default: .env, use .env.prod for production)",
    )
    args = parser.parse_args()

    from app.services.catalog_shopify_publish_service import (
        normalize_barcodes,
        print_publish_cli_summary,
        run_catalog_shopify_publish,
    )

    raw: list[str] = []
    if args.barcodes:
        raw.extend(b.strip() for b in args.barcodes.split(",") if b.strip())
    if args.barcodes_file:
        raw.extend(
            line.strip()
            for line in Path(args.barcodes_file).read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    barcodes = normalize_barcodes(raw)
    if not barcodes:
        raise SystemExit("Provide --barcodes and/or --barcodes-file")

    result = run_catalog_shopify_publish(
        barcodes=barcodes,
        supplier_mode=args.supplier,
        shopify_status=args.status,
        dry_run=args.dry_run,
        env_file=args.env,
        api_version=args.api_version,
    )
    raise SystemExit(print_publish_cli_summary(result, supplier_mode=args.supplier))


if __name__ == "__main__":
    main()
