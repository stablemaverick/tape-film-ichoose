#!/usr/bin/env python3
"""Read-only Region B film pricing-health evaluation for production observability.

This entry point has NO --apply flag and cannot mutate Shopify prices,
inventoryPolicy, inventory quantity, cost, or metafields.

Manual allowlisted repricing remains a separate command:
  scripts/maintenance/sync_region_b_film_repricing.py
"""

from __future__ import annotations

import argparse
import inspect
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.services.region_b_film_repricing_service import (
    DEFAULT_COMMERCIAL_REVIEW_MAX,
    DEFAULT_MAX_AUTO_INCREASE,
    apply_price_updates,
    format_pricing_health_status_line,
    run_region_b_pricing_health,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "READ-ONLY Region B film pricing-health evaluation. "
            "Writes deterministic latest artifacts under var/pricing_health/. "
            "Does not accept --apply and cannot mutate Shopify."
        )
    )
    p.add_argument("--env", default=".env.prod")
    p.add_argument("--api-version", default="2026-04")
    p.add_argument(
        "--studios",
        default="",
        help="Optional comma-separated studio labels (default: all eligible films)",
    )
    p.add_argument(
        "--max-auto-increase",
        type=float,
        default=DEFAULT_MAX_AUTO_INCREASE,
    )
    p.add_argument(
        "--commercial-review-max",
        type=float,
        default=DEFAULT_COMMERCIAL_REVIEW_MAX,
    )
    return p


def main(argv: list[str] | None = None) -> int:
    # Hard refusal: this module must never expose an apply path.
    if any(a in {"--apply", "--mutate", "--write-prices"} for a in (argv or sys.argv[1:])):
        print(
            "ERROR: run_region_b_pricing_health.py is read-only and rejects --apply",
            file=sys.stderr,
        )
        return 2
    # Defense: ensure apply helper still raises under the health readonly env flag.
    assert "REGION_B_PRICING_HEALTH_READONLY" in inspect.getsource(apply_price_updates)

    args = build_parser().parse_args(argv)
    labels = [x.strip() for x in args.studios.split(",") if x.strip()] or None
    rows, summary, _payload = run_region_b_pricing_health(
        env_file=args.env,
        api_version=args.api_version,
        labels=labels,
        max_auto_increase=float(args.max_auto_increase),
        commercial_review_max=float(args.commercial_review_max),
    )
    print(format_pricing_health_status_line(summary), flush=True)
    print(
        f"rows={len(rows)} latest_json={summary.json_path} latest_csv={summary.csv_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
