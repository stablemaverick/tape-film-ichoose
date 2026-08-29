#!/usr/bin/env python3
"""Controlled Region B existing-film catalogue repricing (dry-run by default).

--apply alone never mutates Shopify. An explicit barcode/variant allowlist is required.
Film-only eligibility is enforced even for allowlisted variants.

Auto-eligible increases are $1–$5. $6–$10 and >$10 require commercial/manual review.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.services.region_b_film_repricing_service import (
    ACTION_PRICE_INCREASE_AUTO_ELIGIBLE,
    DEFAULT_COMMERCIAL_REVIEW_MAX,
    DEFAULT_MAX_AUTO_INCREASE,
    run_region_b_film_repricing,
)
from app.services.supplier_margin_protection_service import parse_apply_allowlist


FIRST_COHORT_LABELS = (
    "Criterion Collection",
    "Radiance Films",
    "Second Sight",
    "88 Films",
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Region B film-only catalogue repricing using the canonical GBP 28% floor. "
            "Dry-run by default. Mutations require --apply AND an explicit allowlist. "
            "Auto-eligible: ≤$5; commercial review: $6–$10; large review: >$10."
        )
    )
    p.add_argument("--env", default=".env.prod")
    p.add_argument("--api-version", default="2026-04")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Apply price increases ONLY for allowlisted AUTO_ELIGIBLE film variants",
    )
    p.add_argument(
        "--studios",
        default="",
        help=(
            "Comma-separated studio labels to include "
            "(default: all films). First cohort example: "
            "'Criterion Collection,Radiance Films,Second Sight,88 Films'"
        ),
    )
    p.add_argument(
        "--first-cohort",
        action="store_true",
        help="Shortcut: Criterion Collection + Radiance + Second Sight + 88 Films",
    )
    p.add_argument(
        "--max-auto-increase",
        type=float,
        default=DEFAULT_MAX_AUTO_INCREASE,
        help="Max $ increase for PRICE_INCREASE_AUTO_ELIGIBLE (default 5)",
    )
    p.add_argument(
        "--commercial-review-max",
        type=float,
        default=DEFAULT_COMMERCIAL_REVIEW_MAX,
        help="Max $ increase for REVIEW_PRICE_INCREASE band (default 10)",
    )
    p.add_argument("--allowlist-barcodes", default="")
    p.add_argument("--allowlist-variants", default="")
    p.add_argument("--allowlist-csv", default="", help="Reviewed candidates CSV (barcode/variant_id)")
    p.add_argument("--csv", default="")
    p.add_argument("--json", default="")
    p.add_argument("--candidates-csv", default="")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.first_cohort:
        labels = list(FIRST_COHORT_LABELS)
    else:
        labels = [x.strip() for x in args.studios.split(",") if x.strip()] or None

    barcodes = [b.strip() for b in args.allowlist_barcodes.split(",") if b.strip()]
    variants = [v.strip() for v in args.allowlist_variants.split(",") if v.strip()]
    allowlist = parse_apply_allowlist(
        variant_ids=variants,
        barcodes=barcodes,
        barcodes_csv=args.allowlist_csv or None,
    )

    rows, summary = run_region_b_film_repricing(
        env_file=args.env,
        api_version=args.api_version,
        apply=args.apply,
        allowlist=allowlist,
        labels=labels,
        max_auto_increase=float(args.max_auto_increase),
        commercial_review_max=float(args.commercial_review_max),
        reviewed_artifact_csv=args.allowlist_csv or None,
        csv_path=args.csv or None,
        json_path=args.json or None,
        candidates_csv_path=args.candidates_csv or None,
    )

    increases = [r for r in rows if r.action == ACTION_PRICE_INCREASE_AUTO_ELIGIBLE]
    by_label: dict[str, list] = {}
    for r in increases:
        by_label.setdefault(r.studio_norm, []).append(r)

    print("=== Region B Film Repricing ===")
    print(f"dry_run: {summary.dry_run}")
    print(f"apply: {args.apply}")
    print(f"allowlist_empty: {allowlist.empty}")
    print(f"labels: {summary.labels or ['(all films)']}")
    print(f"products_scanned: {summary.products_scanned}")
    print(f"variants_examined: {summary.variants_examined}")
    print(f"films_assessed: {summary.films_assessed}")
    print(f"non_film_excluded: {summary.non_film_excluded}")
    print(f"ambiguous_skipped: {summary.ambiguous_skipped}")
    print(f"region_a_blocked: {summary.region_a_blocked}")
    print(f"price_increase_auto_eligible: {summary.price_increase}")
    print(f"review_price_increase: {summary.review_price_increase}")
    print(f"review_large_increase: {summary.review_large_increase}")
    print(f"keep_current: {summary.keep_current}")
    print(f"no_change: {summary.no_change}")
    print(f"review_anomaly: {summary.review_anomaly}")
    print(f"applied_ok: {summary.applied_ok}")
    print(f"applied_failed: {summary.applied_failed}")
    print(f"apply_skipped: {summary.apply_skipped}")
    print(f"csv_path: {summary.csv_path}")
    print(f"candidates_csv: {summary.candidates_csv_path}")
    print(f"json_path: {summary.json_path}")
    print(
        "AUTO_ELIGIBLE_INCREASES="
        + json.dumps(
            {
                lab: [
                    {
                        "title": r.title,
                        "barcode": r.barcode,
                        "variant_id": r.variant_id,
                        "current": r.current_retail,
                        "proposed": r.proposed_retail,
                        "change": r.dollar_change,
                        "gbp": r.source_cost_gbp,
                        "supplier": r.preferred_supplier,
                    }
                    for r in sorted(items, key=lambda x: -(x.dollar_change or 0))
                ]
                for lab, items in sorted(by_label.items())
            },
            indent=2,
        )
    )
    print(
        "REGION_B_FILM_REPRICING_STATUS="
        f"{'failed' if summary.applied_failed else 'success'} "
        f"dry_run={1 if summary.dry_run else 0} "
        f"auto_eligible={summary.price_increase} "
        f"review_price={summary.review_price_increase} "
        f"review_large={summary.review_large_increase} "
        f"applied_ok={summary.applied_ok} applied_failed={summary.applied_failed}"
    )
    return 2 if summary.applied_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
