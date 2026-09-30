#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.services.shopify_catalog_hygiene_service import (
    apply_deactivations_from_csv,
    run_catalog_hygiene_audit,
    write_json_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit Shopify catalog hygiene (dry-run by default)")
    parser.add_argument("--env", default=".env", help="Env file path")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument("--product-query", default="status:active")
    parser.add_argument(
        "--apply-cleanup",
        action="store_true",
        help="Apply cleanup mutations (remove New tags, clear invalid pre_order)",
    )
    parser.add_argument(
        "--apply-deactivations-csv",
        default=None,
        help="Archive product_id rows from approved candidate CSV (explicit mode only)",
    )
    parser.add_argument(
        "--confirm-deactivate",
        default="",
        help='Required token for deactivation apply mode: "DEACTIVATE"',
    )
    parser.add_argument(
        "--report-json",
        default=str(_REPO / "tmp" / "shopify_catalog_hygiene_audit.json"),
        help="Path to JSON report output",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.apply_cleanup and args.apply_deactivations_csv:
        print("ERROR: choose either --apply-cleanup or --apply-deactivations-csv, not both", file=sys.stderr)
        return 1

    if args.apply_deactivations_csv:
        if args.confirm_deactivate != "DEACTIVATE":
            print('ERROR: --confirm-deactivate must be exactly "DEACTIVATE"', file=sys.stderr)
            return 1
        summary = apply_deactivations_from_csv(
            csv_path=args.apply_deactivations_csv,
            env_file=args.env,
            api_version=args.api_version,
            apply=True,
        )
        print("=== Shopify Catalog Hygiene Deactivations ===")
        print(f"rows_read: {summary.rows_read}")
        print(f"attempted: {summary.attempted}")
        print(f"succeeded: {summary.succeeded}")
        print(f"failed: {summary.failed}")
        print(f"dry_run: {summary.dry_run}")
        return 2 if summary.failed else 0

    records, summary = run_catalog_hygiene_audit(
        env_file=args.env,
        api_version=args.api_version,
        apply_cleanup=args.apply_cleanup,
        product_query=args.product_query,
    )

    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    write_json_report(str(report_path), records=records, summary=summary)

    print("=== Shopify Catalog Hygiene Audit ===")
    print(f"dry_run: {not args.apply_cleanup}")
    print(f"products_examined: {summary.products_examined}")
    print(f"active_products_examined: {summary.active_products_examined}")
    print(f"variants_examined: {summary.variants_examined}")
    print(f"positive_stock_products: {summary.positive_stock_products}")
    print(f"negative_stock_products_protected: {summary.negative_stock_products_protected}")
    print(f"valid_future_preorders_protected: {summary.valid_future_preorders_protected}")
    print(f"zero_stock_candidate_products: {summary.zero_stock_candidate_products}")
    print(f"products_skipped_unknown_stock: {summary.products_skipped_unknown_stock}")
    print(f"stale_preorders: {summary.stale_preorders}")
    print(f"preorder_missing_media_release_date: {summary.preorder_missing_media_release_date}")
    print(f"future_release_without_preorder: {summary.future_release_without_preorder}")
    print(f"products_with_new: {summary.products_with_new}")
    print(f"mutated_preorder: {summary.mutated_preorder}")
    print(f"mutated_new_metafields: {summary.mutated_new_metafields}")
    print(f"mutated_tags: {summary.mutated_tags}")
    print(f"failures: {summary.failures}")
    print(f"observed_preorder_fields: {','.join(summary.observed_preorder_fields) or '(none)'}")
    print(f"observed_release_fields: {','.join(summary.observed_release_fields) or '(none)'}")
    print(f"observed_new_mechanisms: {','.join(summary.observed_new_mechanisms) or '(none)'}")
    print(f"report_json: {report_path}")

    return 2 if summary.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
