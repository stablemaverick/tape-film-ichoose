#!/usr/bin/env python3
"""Daily supplier replacement-cost monitoring (existing catalogue is read-only by default)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.services.supplier_margin_protection_service import (
    format_status_line,
    parse_apply_allowlist,
    run_supplier_margin_protection,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Monitor supplier GBP cost movements and 28% replacement-cost margin exposure. "
            "Default: monitoring only — zero Shopify mutations on existing catalogue. "
            "Scoped apply requires --apply plus an explicit allowlist."
        )
    )
    p.add_argument("--env", default=".env", help="Env file path")
    p.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Apply price increases ONLY for allowlisted variants/barcodes "
            "(ignored without allowlist)"
        ),
    )
    p.add_argument(
        "--allowlist-barcodes",
        default="",
        help="Comma-separated barcodes permitted for scoped apply",
    )
    p.add_argument(
        "--allowlist-variants",
        default="",
        help="Comma-separated Shopify variant GIDs permitted for scoped apply",
    )
    p.add_argument(
        "--allowlist-csv",
        default="",
        help="CSV of barcodes (barcode column or first column) for scoped apply",
    )
    p.add_argument(
        "--movement-csv",
        default="",
        help="Significant movement CSV (default tmp/supplier_cost_movement_<stamp>.csv)",
    )
    p.add_argument(
        "--monitor-csv",
        default="",
        help="Full monitor CSV (default tmp/supplier_margin_monitor_<stamp>.csv)",
    )
    p.add_argument(
        "--no-reconcile",
        action="store_true",
        help="Skip inbound Shopify store sync after successful scoped price apply",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    barcodes = [b.strip() for b in args.allowlist_barcodes.split(",") if b.strip()]
    variants = [v.strip() for v in args.allowlist_variants.split(",") if v.strip()]
    allowlist = parse_apply_allowlist(
        variant_ids=variants,
        barcodes=barcodes,
        barcodes_csv=args.allowlist_csv or None,
    )
    _rows, summary = run_supplier_margin_protection(
        env_file=args.env,
        apply=args.apply,
        allowlist=allowlist,
        movement_csv=args.movement_csv or None,
        monitor_csv=args.monitor_csv or None,
        reconcile_after_apply=not args.no_reconcile,
    )
    print(format_status_line(summary))
    print(f"movement_csv={summary.movement_csv}")
    print(f"monitor_csv={summary.monitor_csv}")
    for alert in summary.alerts:
        print(alert)
    if summary.ii_reconcile:
        print(f"ii_reconcile={summary.ii_reconcile}")
    return 1 if summary.errors > 0 and not summary.monitoring_only else 0


if __name__ == "__main__":
    raise SystemExit(main())
