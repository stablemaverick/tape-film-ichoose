import argparse
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from supabase import create_client

from publish_supplier_offers_to_catalog import clean_text, map_offer_to_catalog_row


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_all_offers(supabase, table: str, page_size: int = 1000) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    while True:
        page = (
            supabase.table(table)
            .select(
                "supplier,barcode,shopify_variant_id,cost_price,calculated_sale_price,availability_status,supplier_stock_status,active,media_type,source_priority,media_release_date,format,studio,director,title,harmonized_title,harmonized_format,harmonized_director,harmonized_studio,shopify_product_id"
            )
            .range(offset, offset + page_size - 1)
            .execute()
        ).data or []
        if not page:
            break
        all_rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return all_rows


def pricing_source_for_supplier(supplier: str) -> str:
    return "shopify_live" if supplier == "Tape Film" else "gbp_formula_v1"


ALLOWED_EXISTING_UPDATE_FIELDS = {
    "supplier_stock_status",
    "availability_status",
    "cost_price",
    "calculated_sale_price",
    "media_release_date",
    "supplier_last_seen_at",
}


def filter_existing_update_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    # Hard guard: existing-row updates must be strictly commercial/operational fields only.
    return {k: v for k, v in payload.items() if k in ALLOWED_EXISTING_UPDATE_FIELDS}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offers-table", default="staging_supplier_offers")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--existing-only",
        action="store_true",
        help="Only update existing catalog_items rows; never insert new rows.",
    )
    args = parser.parse_args()

    load_dotenv(".env")
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

    if not supabase_url or not supabase_key:
        raise SystemExit("Missing Supabase env")

    supabase = create_client(supabase_url, supabase_key)

    # Pull only fields needed for operational sync (price + stock).
    offers = fetch_all_offers(supabase, args.offers_table)

    if not offers:
        print("No supplier offers found.")
        return

    # Build keys and group by supplier for efficient existing-row lookup.
    existing_ids: Dict[Tuple[str, str], str] = {}

    # supplier -> list of barcode
    barcodes_by_supplier: Dict[str, List[str]] = defaultdict(list)
    # for Tape Film specifically
    variants_by_supplier: Dict[str, List[str]] = defaultdict(list)

    offers_for_keys: list[tuple[Dict[str, Any], Optional[Tuple[str, str]]]] = []
    for offer in offers:
        supplier = clean_text(offer.get("supplier")) or "Unknown"
        if supplier == "Tape Film":
            variant = clean_text(offer.get("shopify_variant_id"))
            key = ("Tape Film", f"variant:{variant}") if variant else None
            if variant:
                variants_by_supplier[supplier].append(variant)
        else:
            barcode = clean_text(offer.get("barcode"))
            key = (supplier, f"barcode:{barcode}") if barcode else None
            if barcode:
                barcodes_by_supplier[supplier].append(barcode)
        offers_for_keys.append((offer, key))

    # Fetch existing catalog_items ids.
    # (supplier, "barcode:{barcode}") -> id
    for supplier, barcodes in barcodes_by_supplier.items():
        for batch in chunked(list(set(barcodes)), 200):
            resp = (
                supabase.table("catalog_items")
                .select("id,barcode")
                .eq("supplier", supplier)
                .in_("barcode", batch)
                .execute()
            ).data or []
            for r in resp:
                bc = clean_text(r.get("barcode"))
                if bc:
                    existing_ids[(supplier, f"barcode:{bc}")] = r["id"]

    for supplier, variants in variants_by_supplier.items():
        for batch in chunked(list(set(variants)), 200):
            resp = (
                supabase.table("catalog_items")
                .select("id,shopify_variant_id")
                .eq("supplier", supplier)
                .in_("shopify_variant_id", batch)
                .execute()
            ).data or []
            for r in resp:
                v = clean_text(r.get("shopify_variant_id"))
                if v:
                    existing_ids[(supplier, f"variant:{v}")] = r["id"]

    inserts: List[Dict[str, Any]] = []
    updates: List[Tuple[str, Dict[str, Any]]] = []

    # Operational-only update payloads. Important: NO tmdb_* fields here.
    for offer, key in offers_for_keys:
        supplier = clean_text(offer.get("supplier")) or "Unknown"
        if not key:
            # Can't key it reliably; skip for safety.
            continue

        catalog_id = existing_ids.get(key)

        update_payload = {
            "cost_price": offer.get("cost_price"),
            "calculated_sale_price": offer.get("calculated_sale_price"),
            "supplier_stock_status": offer.get("supplier_stock_status") or 0,
            "availability_status": clean_text(offer.get("availability_status")),
            "media_release_date": offer.get("media_release_date"),
            "supplier_last_seen_at": now_iso(),
        }
        update_payload = filter_existing_update_payload(update_payload)

        if catalog_id:
            updates.append((catalog_id, update_payload))
        else:
            if args.existing_only:
                continue
            # New catalog item: create full row so later enrichment can enrich tmdb fields.
            inserts.append(map_offer_to_catalog_row(offer))

    inserted = 0
    updated = 0

    # Insert in batches.
    for batch in chunked(inserts, args.batch_size):
        if not batch:
            continue
        supabase.table("catalog_items").insert(batch).execute()
        inserted += len(batch)

    # Update in batches (per-row updates because payload differs; ids list is handled by row).
    for catalog_id, payload in updates:
        supabase.table("catalog_items").update(payload).eq("id", catalog_id).execute()
        updated += 1
        time.sleep(0.001)  # tiny pacing to avoid bursts

    print(f"Operational sync complete. inserted={inserted} updated={updated} offers_total={len(offers)}")


if __name__ == "__main__":
    main()

