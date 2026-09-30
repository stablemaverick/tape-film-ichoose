#!/usr/bin/env python3
"""CLI / agent-tool entry for Stock Availability V1."""

from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client

from app.services.stock_availability_service import (
    StockAvailabilityError,
    StockAvailabilityService,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    sub = parser.add_subparsers(dest="cmd", required=True)

    get_p = sub.add_parser("get", help="get_inventory")
    get_p.add_argument("--release-variant-id")
    get_p.add_argument("--barcode")
    get_p.add_argument("--shopify-variant-id")
    get_p.add_argument("--supplier-id")
    get_p.add_argument("--supplier-sku")
    get_p.add_argument("--hide-costs", action="store_true")

    search_p = sub.add_parser("search", help="search_inventory")
    search_p.add_argument("query")
    search_p.add_argument("--limit", type=int, default=20)

    hist_p = sub.add_parser("history", help="get_inventory_history")
    hist_p.add_argument("--release-variant-id", required=True)
    hist_p.add_argument("--limit", type=int, default=50)

    args = parser.parse_args(argv)
    load_dotenv(args.env_file, override=True)
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print(json.dumps({"error": "MISSING_ENV", "message": "SUPABASE_URL/SERVICE_KEY required"}))
        return 2

    sb = create_client(url, key)
    svc = StockAvailabilityService(sb)
    try:
        if args.cmd == "get":
            out = svc.get_stock_availability(
                release_variant_id=args.release_variant_id,
                barcode=args.barcode,
                shopify_variant_id=args.shopify_variant_id,
                supplier_id=args.supplier_id,
                supplier_sku=args.supplier_sku,
                include_costs=not args.hide_costs,
            )
        elif args.cmd == "search":
            out = svc.search_inventory(args.query, limit=args.limit)
        else:
            out = svc.get_inventory_history(
                release_variant_id=args.release_variant_id, limit=args.limit
            )
    except StockAvailabilityError as exc:
        print(json.dumps(exc.to_dict(), default=str))
        return 1

    print(json.dumps(out, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
