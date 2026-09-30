#!/usr/bin/env python3
"""D-5 retention for supplier_orders FTP report series (dry-run by default)."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.services.supplier_orders_report_retention import (
    apply_retention,
    format_retention_status,
    sydney_today,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Retain today + previous 5 calendar days of supplier_orders_* dated CSVs "
            "(Australia/Sydney). Dry-run by default."
        )
    )
    p.add_argument(
        "--dir",
        default="/srv/ftps/data/tapester/reports/supplier_orders",
        help="Exact report directory only",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete files older than D-5 (default: dry-run)",
    )
    p.add_argument(
        "--as-of",
        default="",
        help="YYYY-MM-DD override for Sydney calendar date (tests)",
    )
    p.add_argument("--retain-days", type=int, default=5)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    as_of = None
    if args.as_of.strip():
        as_of = datetime.strptime(args.as_of.strip(), "%Y-%m-%d").date()
    summary = apply_retention(
        args.dir,
        as_of=as_of or sydney_today(),
        retain_days=args.retain_days,
        dry_run=not args.apply,
    )
    print(format_retention_status(summary))
    for d in summary.decisions:
        if d.action in {"DELETE", "UNCLASSIFIED"}:
            print(
                f"{d.action}\t{d.filename}\tdate={d.parsed_report_date}\t"
                f"age={d.age_days}\treason={d.reason}"
            )
    for err in summary.errors:
        print(f"ERROR\t{err}")
    return 1 if summary.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
