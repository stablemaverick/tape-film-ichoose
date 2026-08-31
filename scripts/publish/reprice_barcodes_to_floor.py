#!/usr/bin/env python3
"""Reprice existing Shopify listings to the 28% margin floor (new-listing policy)."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client
from app.services.catalog_shopify_publish_service import (
    fetch_catalog_rows_for_barcodes,
    normalize_barcodes,
    pick_best_row,
    resolve_new_listing_price,
)

VARIANT_PRICE_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id price }
    userErrors { field message }
  }
}
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Reprice barcodes to resolve_new_listing_price floor")
    parser.add_argument("--env", default=".env.prod")
    parser.add_argument("--barcodes-file", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--api-version", default="2026-04")
    args = parser.parse_args()

    load_dotenv(args.env, override=True)
    barcodes = normalize_barcodes(
        Path(args.barcodes_file).read_text(encoding="utf-8").splitlines()
    )
    if not barcodes:
        raise SystemExit("No barcodes")

    sb = create_fresh_client(args.env)
    shopify = ShopifyClient(api_version=args.api_version)
    by_barcode = fetch_catalog_rows_for_barcodes(sb, barcodes)

    id_rows = (
        sb.table("catalog_items")
        .select("barcode,shopify_variant_id,shopify_product_id")
        .in_("barcode", barcodes)
        .execute()
        .data
        or []
    )
    ids_by_bc: dict[str, tuple[str, str]] = {}
    for row in id_rows:
        bc = row.get("barcode")
        if bc and row.get("shopify_variant_id") and row.get("shopify_product_id"):
            ids_by_bc[bc] = (row["shopify_product_id"], row["shopify_variant_id"])

    ok = skipped = failed = 0
    for barcode in barcodes:
        best = pick_best_row(by_barcode.get(barcode) or [], supplier_preference="best_offer")
        if not best:
            print(f"FAIL no catalog {barcode}")
            failed += 1
            continue

        pid_vid = ids_by_bc.get(barcode)
        if not pid_vid:
            existing = shopify.variant_exists_by_barcode(barcode)
            if not existing:
                print(f"FAIL no shopify {barcode}")
                failed += 1
                continue
            pid_vid = (
                (existing.get("product") or {}).get("id"),
                existing["id"],
            )
        product_id, variant_id = pid_vid
        if not product_id or not variant_id:
            print(f"FAIL missing ids {barcode}")
            failed += 1
            continue

        new_price = resolve_new_listing_price(row=best)
        old_price = (
            float(best["calculated_sale_price"])
            if best.get("calculated_sale_price") is not None
            else None
        )
        title = (best.get("title") or "")[:45]

        if old_price is not None and abs(old_price - new_price) < 0.005:
            print(f"SKIP unchanged {barcode} {old_price:.2f}")
            skipped += 1
            continue

        new_price_s = f"{new_price:.2f}"
        if args.dry_run:
            print(f"DRY-RUN {barcode} {old_price} -> {new_price_s} | {title}")
            ok += 1
            continue

        try:
            data = shopify.graphql(
                VARIANT_PRICE_UPDATE,
                {
                    "productId": product_id,
                    "variants": [{"id": variant_id, "price": new_price_s}],
                },
            )
            block = data.get("productVariantsBulkUpdate") or {}
            errs = block.get("userErrors") or []
            if errs:
                raise RuntimeError(str(errs))
            sb.table("catalog_items").update(
                {
                    "calculated_sale_price": new_price,
                    "shopify_product_id": product_id,
                    "shopify_variant_id": variant_id,
                }
            ).eq("id", best["id"]).execute()
            print(f"OK {barcode} {old_price} -> {new_price_s} | {title}")
            ok += 1
            time.sleep(0.1)
        except Exception as exc:
            print(f"FAIL {barcode} {exc}")
            failed += 1
            time.sleep(0.2)

    print(f"\nDone ok={ok} skipped={skipped} failed={failed}")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
