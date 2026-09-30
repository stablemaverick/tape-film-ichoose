#!/usr/bin/env python3
"""Dry-run Shopify → release_variant mapping coverage (read-only; no dual-write)."""

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

from app.services.shopify_release_mapping import (
    classify_shopify_listing_mapping,
    summarize_mapping_results,
)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--env-file", default=".env")
    p.add_argument("--shop", default=None, help="Filter shopify_listings.shop")
    p.add_argument("--limit", type=int, default=500)
    p.add_argument("--out", default=None)
    args = p.parse_args()
    load_dotenv(args.env_file, override=True)
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    q = sb.table("shopify_listings").select(
        "shop,shopify_variant_id,barcode,catalog_item_id,product_title,price_amount"
    )
    if args.shop:
        q = q.eq("shop", args.shop)
    rows = q.limit(max(1, min(args.limit, 5000))).execute().data or []

    results = []
    for r in rows:
        results.append(
            classify_shopify_listing_mapping(
                sb,
                shop=r.get("shop") or "",
                shopify_variant_id=r.get("shopify_variant_id") or "",
                barcode=r.get("barcode"),
                catalog_item_id=r.get("catalog_item_id"),
            )
        )
    summary = summarize_mapping_results(results)
    summary["sample_shop"] = args.shop
    summary["note"] = (
        "Dry-run only. Flags not enabled. Does not write release_shopify_listings "
        "or tape_inventory_levels."
    )
    text = json.dumps(summary, indent=2)
    print(text)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
