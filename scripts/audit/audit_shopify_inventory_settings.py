#!/usr/bin/env python3
"""
Audit Shopify inventory tracking + inventoryPolicy vs custom.pre_order / custom.backorder.

Read-only by default. Writes CSV to tmp/shopify_inventory_audit.csv.

Usage::

    ./venv/bin/python scripts/audit/audit_shopify_inventory_settings.py
    ./venv/bin/python scripts/audit/audit_shopify_inventory_settings.py --issues-only
    ./venv/bin/python scripts/audit/audit_shopify_inventory_settings.py --fix --dry-run
    ./venv/bin/python scripts/audit/audit_shopify_inventory_settings.py --fix
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit Shopify variant inventory tracking/policy against "
            "custom.pre_order and custom.backorder metafields."
        )
    )
    parser.add_argument(
        "--issues-only",
        action="store_true",
        help="Limit terminal detail to variants with detected issues (CSV still full).",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Propose (and optionally apply) deterministic tracking/policy repairs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --fix: show proposed mutations without calling Shopify.",
    )
    parser.add_argument(
        "--env",
        default=".env",
        help="Env file path (default: .env under repo root).",
    )
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument(
        "--csv",
        default=None,
        help="CSV output path (default: tmp/shopify_inventory_audit.csv)",
    )
    parser.add_argument(
        "--include-gift-cards",
        action="store_true",
        help="Include gift-card product types (skipped by default).",
    )
    parser.add_argument(
        "--product-query",
        default="status:active OR status:draft",
        help='Shopify products search query (default: "status:active OR status:draft").',
    )
    args = parser.parse_args(argv)

    if args.dry_run and not args.fix:
        print("NOTE: --dry-run only applies with --fix; running read-only audit.", file=sys.stderr)

    from app.services.shopify_inventory_settings_audit import run_audit

    csv_path = Path(args.csv) if args.csv else (_REPO / "tmp" / "shopify_inventory_audit.csv")

    confirm: str | None = None
    if args.fix and not args.dry_run:
        print(
            "\nWARNING: --fix will mutate Shopify inventory tracking / inventoryPolicy.\n"
            "It will NOT change quantities, metafields, or product status.\n"
            'Type FIX and press Enter to continue, or anything else to abort.'
        )
        try:
            confirm = input("> ").strip()
        except EOFError:
            confirm = ""
        if confirm != "FIX":
            print("Aborted (confirmation was not FIX).")
            return 1

    try:
        _rows, summary = run_audit(
            env_file=args.env,
            api_version=args.api_version,
            csv_path=csv_path,
            issues_only=args.issues_only,
            fix=args.fix,
            dry_run=args.dry_run,
            confirm_token=confirm,
            include_gift_cards=args.include_gift_cards,
            product_query=args.product_query,
        )
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 1
        return code
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if summary.repair_failures:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
