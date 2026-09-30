#!/usr/bin/env python3
"""
Shopify store cleanup: export archive candidates, archive from CSV, clear metafields.

Usage::

    # Phase 1 — candidate list (no mutations)
    ./venv/bin/python scripts/maintenance/shopify_store_cleanup.py export --env .env.prod

    # Phase 2 — archive from approved CSV (dry-run default)
    ./venv/bin/python scripts/maintenance/shopify_store_cleanup.py archive \\
        --csv tmp/store_cleanup/archive_candidates_YYYYMMDD.csv --env .env.prod
    ./venv/bin/python scripts/maintenance/shopify_store_cleanup.py archive \\
        --csv tmp/store_cleanup/archive_candidates_YYYYMMDD.csv --env .env.prod --apply

    # Phase 3 — clear metafields (dry-run default)
    ./venv/bin/python scripts/maintenance/shopify_store_cleanup.py clear-metafields \\
        --csv path/to/products.csv --keys custom.new --env .env.prod
    ./venv/bin/python scripts/maintenance/shopify_store_cleanup.py clear-metafields \\
        --csv path/to/products.csv --keys custom.new --env .env.prod --apply
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))


def _cmd_export(args: argparse.Namespace) -> int:
    from app.services.shopify_store_cleanup_service import (
        export_archive_candidates,
        print_export_summary,
    )

    out_dir = Path(args.out_dir) if args.out_dir else (_REPO / "tmp" / "store_cleanup")
    try:
        _cands, _excl, summary = export_archive_candidates(
            env_file=args.env,
            api_version=args.api_version,
            out_dir=out_dir,
            release_lookback_days=args.release_lookback_days,
            include_gift_cards=args.include_gift_cards,
            product_query=args.product_query,
        )
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print_export_summary(summary)
    return 0


def _cmd_archive(args: argparse.Namespace) -> int:
    from app.services.shopify_store_cleanup_service import archive_products_from_csv

    csv_path = Path(args.csv)
    if not csv_path.is_file():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        return 1

    if args.apply:
        print(
            "\nWARNING: --apply will set product status to ARCHIVED for every "
            "product_id in the CSV.\n"
            "Type ARCHIVE and press Enter to continue, or anything else to abort."
        )
        try:
            confirm = input("> ").strip()
        except EOFError:
            confirm = ""
        if confirm != "ARCHIVE":
            print("Aborted (confirmation was not ARCHIVE).")
            return 1

    try:
        summary = archive_products_from_csv(
            csv_path=csv_path,
            env_file=args.env,
            api_version=args.api_version,
            apply=args.apply,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(
        f"\nArchive done: read={summary.rows_read} attempted={summary.attempted} "
        f"ok={summary.succeeded} failed={summary.failed} dry_run={summary.dry_run}"
    )
    return 2 if summary.failed else 0


def _cmd_clear_metafields(args: argparse.Namespace) -> int:
    from app.services.shopify_store_cleanup_service import clear_metafields_from_csv

    csv_path = Path(args.csv)
    if not csv_path.is_file():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        return 1
    keys = [k.strip() for k in args.keys.split(",") if k.strip()]
    if not keys:
        print("ERROR: --keys required (comma-separated namespace.key)", file=sys.stderr)
        return 1

    if args.apply:
        print(
            f"\nWARNING: --apply will delete metafields {keys} on every product_id "
            "in the CSV.\n"
            "Type CLEAR and press Enter to continue, or anything else to abort."
        )
        try:
            confirm = input("> ").strip()
        except EOFError:
            confirm = ""
        if confirm != "CLEAR":
            print("Aborted (confirmation was not CLEAR).")
            return 1

    try:
        summary = clear_metafields_from_csv(
            csv_path=csv_path,
            metafield_keys=keys,
            env_file=args.env,
            api_version=args.api_version,
            apply=args.apply,
            delete=not args.set_empty,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(
        f"\nClear metafields done: read={summary.rows_read} attempted={summary.attempted} "
        f"ok={summary.succeeded} failed={summary.failed} dry_run={summary.dry_run}"
    )
    return 2 if summary.failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Shopify store cleanup tools")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--env", default=".env", help="Env file (default: .env)")
        p.add_argument("--api-version", default="2026-04")

    p_export = sub.add_parser("export", help="Export archive candidate + excluded CSVs")
    add_common(p_export)
    p_export.add_argument(
        "--out-dir",
        default=None,
        help="Output directory (default: tmp/store_cleanup)",
    )
    p_export.add_argument(
        "--release-lookback-days",
        type=int,
        default=60,
        help="Exclude products released within this many days (default: 60)",
    )
    p_export.add_argument(
        "--include-gift-cards",
        action="store_true",
        help="Include gift-card product types",
    )
    p_export.add_argument(
        "--product-query",
        default="status:active",
        help='Shopify products search query (default: "status:active")',
    )
    p_export.set_defaults(func=_cmd_export)

    p_archive = sub.add_parser("archive", help="Archive products listed in CSV")
    add_common(p_archive)
    p_archive.add_argument("--csv", required=True, help="CSV with product_id column")
    p_archive.add_argument(
        "--apply",
        action="store_true",
        help="Actually archive (default: dry-run)",
    )
    p_archive.set_defaults(func=_cmd_archive)

    p_clear = sub.add_parser("clear-metafields", help="Clear metafields on CSV products")
    add_common(p_clear)
    p_clear.add_argument("--csv", required=True, help="CSV with product_id column")
    p_clear.add_argument(
        "--keys",
        required=True,
        help="Comma-separated namespace.key list (e.g. custom.new,custom.po_flag)",
    )
    p_clear.add_argument(
        "--apply",
        action="store_true",
        help="Actually clear (default: dry-run)",
    )
    p_clear.add_argument(
        "--set-empty",
        action="store_true",
        help="Set empty/false instead of deleting the metafield",
    )
    p_clear.set_defaults(func=_cmd_clear_metafields)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
