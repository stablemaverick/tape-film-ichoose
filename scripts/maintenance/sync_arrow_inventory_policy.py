#!/usr/bin/env python3
"""Arrow-only daily inventoryPolicy sync (DENY↔CONTINUE) based on supplier stock."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.services.arrow_inventory_policy_sync_service import run_arrow_inventory_policy_sync


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Sync Arrow inventoryPolicy for zero Shopify stock: "
            "DENY→CONTINUE when supplier available; CONTINUE→DENY when not."
        )
    )
    parser.add_argument("--env", default=".env", help="Env file path")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply Shopify inventoryPolicy mutations (dry-run by default)",
    )
    parser.add_argument("--product-query", default="status:active")
    parser.add_argument(
        "--csv",
        default="",
        help="Optional CSV output path (default: tmp/arrow_inventory_policy_sync_*.csv)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _decisions, summary = run_arrow_inventory_policy_sync(
        env_file=args.env,
        api_version=args.api_version,
        apply=args.apply,
        product_query=args.product_query,
        csv_path=args.csv or None,
    )

    print("=== Arrow Inventory Policy Sync ===")
    print(f"dry_run: {summary.dry_run}")
    print(f"products_scanned: {summary.products_scanned}")
    print(f"arrow_products: {summary.arrow_products}")
    print(f"variants_examined: {summary.variants_examined}")
    print(f"zero_stock_variants: {summary.zero_stock_variants}")
    print(f"set_continue: {summary.set_continue}")
    print(f"set_deny: {summary.set_deny}")
    print(f"no_change: {summary.no_change}")
    print(f"skipped: {summary.skipped}")
    print(f"applied_ok: {summary.applied_ok}")
    print(f"applied_failed: {summary.applied_failed}")
    print(f"csv_path: {summary.csv_path}")
    print("ARROW_INVENTORY_POLICY_SYNC_STATUS="
          f"{'failed' if summary.applied_failed else 'success'} "
          f"dry_run={1 if summary.dry_run else 0} "
          f"set_continue={summary.set_continue} set_deny={summary.set_deny} "
          f"applied_ok={summary.applied_ok} applied_failed={summary.applied_failed}")
    return 2 if summary.applied_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
