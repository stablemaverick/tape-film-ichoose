#!/usr/bin/env python3
"""
Bulk publish all staging_supplier_offers → catalog_items (insert-only legacy path).

Mapping helpers: app.services.catalog_offer_mapping
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict

from dotenv import load_dotenv
from supabase import create_client

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.helpers.text_helpers import chunked
from app.services.catalog_offer_mapping import make_offer_key, map_offer_to_catalog_row


def fetch_all_rows(supabase, table: str, select_cols: str, page_size: int = 1000) -> list[Dict[str, Any]]:
    all_rows: list[Dict[str, Any]] = []
    offset = 0
    while True:
        resp = (
            supabase.table(table)
            .select(select_cols)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = resp.data or []
        if not page:
            break
        all_rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return all_rows


def main() -> None:
    load_dotenv(".env")
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_key:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")

    supabase = create_client(supabase_url, supabase_key)

    offers = fetch_all_rows(
        supabase,
        "staging_supplier_offers",
        "supplier,source_type,title,harmonized_title,edition_title,format,harmonized_format,director,harmonized_director,studio,harmonized_studio,media_release_date,barcode,supplier_sku,supplier_currency,cost_price,calculated_sale_price,availability_status,supplier_stock_status,source_priority,active,media_type,shopify_product_id,shopify_variant_id",
    )

    deduped: Dict[str, Dict[str, Any]] = {}
    for offer in offers:
        key = make_offer_key(offer)
        deduped[key] = offer

    rows = [map_offer_to_catalog_row(offer) for offer in deduped.values()]
    inserted = 0
    for batch in chunked(rows, 1000):
        supabase.table("catalog_items").insert(batch).execute()
        inserted += len(batch)

    print(f"Published {inserted} catalog_items rows from {len(offers)} staging_supplier_offers rows")


if __name__ == "__main__":
    main()
