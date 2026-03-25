"""
Normalize staging_*_raw rows into staging_supplier_offers.

CLI shim: normalize_supplier_products.py
Pipeline: pipeline/03_normalize_supplier_products.py -> run_from_argv()
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from dotenv import load_dotenv
from supabase import create_client

from app.helpers.catalog_match_helpers import normalize_title
from app.helpers.text_helpers import chunked, clean_text, parse_date, parse_int, parse_price_gbp, now_iso
from app.rules.pricing_rules import calculate_sale_price


def fetch_all_rows_for_batch(
    supabase, table: str, select_cols: str, batch_id: str, page_size: int = 1000
) -> list[Dict[str, Any]]:
    all_rows: list[Dict[str, Any]] = []
    offset = 0
    while True:
        resp = (
            supabase.table(table)
            .select(select_cols)
            .eq("import_batch_id", batch_id)
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


def normalize_from_moovies(supabase, offers_table: str, batch_id: str) -> int:
    rows = fetch_all_rows_for_batch(
        supabase,
        "staging_moovies_raw",
        "id,raw_title,raw_barcode,raw_format,raw_category,raw_release,raw_studio,raw_country_of_origin,raw_sku,raw_price,raw_qty,source_filename,row_number",
        batch_id,
    )
    deduped_by_barcode: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        barcode = clean_text(r.get("raw_barcode"))
        if not barcode:
            continue

        title = clean_text(r.get("raw_title")) or barcode
        cost_gbp = parse_price_gbp(r.get("raw_price"))
        qty = parse_int(r.get("raw_qty"))
        deduped_by_barcode[barcode] = {
            "import_batch_id": batch_id,
            "supplier": "moovies",
            "source_filename": clean_text(r.get("source_filename")),
            "source_row_number": r.get("row_number"),
            "supplier_sku": clean_text(r.get("raw_sku")),
            "barcode": barcode,
            "title": title,
            "normalized_title": normalize_title(title or ""),
            "edition_title": None,
            "format": clean_text(r.get("raw_format")),
            "media_type": "film",
            "director": None,
            "studio": clean_text(r.get("raw_studio")),
            "media_release_date": parse_date(r.get("raw_release")),
            "supplier_stock_status": qty,
            "availability_status": "supplier_stock" if qty > 0 else "supplier_out",
            "supplier_currency": "GBP",
            "cost_price": cost_gbp,
            "calculated_sale_price": calculate_sale_price(cost_gbp),
            "source_priority": 1,
            "source_type": "catalog",
            "active": True,
            "harmonized_title": title,
            "harmonized_format": clean_text(r.get("raw_format")),
            "harmonized_director": None,
            "harmonized_studio": clean_text(r.get("raw_studio")),
            "harmonized_from_supplier": "moovies",
            "harmonized_at": now_iso(),
            "raw_source_id": r.get("id"),
            "raw_source_table": "staging_moovies_raw",
        }

    out = list(deduped_by_barcode.values())

    for batch in chunked(out, 1000):
        supabase.table(offers_table).upsert(batch, on_conflict="supplier,barcode").execute()

    print(f"Upserted {len(out)} staging_supplier_offers rows from Moovies batch {batch_id}")
    return len(out)


def normalize_from_lasgo(supabase, offers_table: str, batch_id: str) -> int:
    rows = fetch_all_rows_for_batch(
        supabase,
        "staging_lasgo_raw",
        "id,raw_title,raw_ean,raw_format_l2,raw_release_date,raw_label,raw_artist,raw_selling_price_sterling,raw_free_stock,source_filename,row_number",
        batch_id,
    )

    deduped_by_barcode: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        barcode = clean_text(r.get("raw_ean"))
        if not barcode:
            continue

        title = clean_text(r.get("raw_title")) or barcode
        norm_title = normalize_title(title or "")
        lasgo_release = parse_date(r.get("raw_release_date"))
        cost_gbp = parse_price_gbp(r.get("raw_selling_price_sterling"))
        qty = parse_int(r.get("raw_free_stock"))
        fmt = clean_text(r.get("raw_format_l2"))
        studio = clean_text(r.get("raw_label"))

        deduped_by_barcode[barcode] = {
            "import_batch_id": batch_id,
            "supplier": "lasgo",
            "source_filename": clean_text(r.get("source_filename")),
            "source_row_number": r.get("row_number"),
            "supplier_sku": None,
            "barcode": barcode,
            "title": title,
            "normalized_title": norm_title,
            "edition_title": None,
            "format": fmt,
            "media_type": "film",
            "director": None,
            "studio": studio,
            "media_release_date": lasgo_release,
            "supplier_stock_status": qty,
            "availability_status": "supplier_stock" if qty > 0 else "supplier_out",
            "supplier_currency": "GBP",
            "cost_price": cost_gbp,
            "calculated_sale_price": calculate_sale_price(cost_gbp),
            "source_priority": 2,
            "source_type": "catalog",
            "active": True,
            "harmonized_title": title,
            "harmonized_format": fmt,
            "harmonized_director": None,
            "harmonized_studio": studio,
            "harmonized_from_supplier": "lasgo",
            "harmonized_at": now_iso(),
            "raw_source_id": r.get("id"),
            "raw_source_table": "staging_lasgo_raw",
        }

    out = list(deduped_by_barcode.values())

    for batch in chunked(out, 1000):
        supabase.table(offers_table).upsert(batch, on_conflict="supplier,barcode").execute()

    print(f"Upserted {len(out)} staging_supplier_offers rows from Lasgo batch {batch_id}")
    return len(out)


def parse_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    text = str(value).replace("£", "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_int_default_zero(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def is_future_release(date_str: Optional[str]) -> bool:
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(str(date_str))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt > datetime.now(timezone.utc)
    except Exception:
        return False


def normalize_from_shopify(supabase, offers_table: str, batch_id: str) -> int:
    rows = fetch_all_rows_for_batch(
        supabase,
        "staging_shopify_raw",
        "id,shopify_product_id,shopify_variant_id,raw_title,raw_variant_title,raw_barcode,raw_sku,raw_price,raw_inventory_qty,raw_inventory_policy,raw_vendor,raw_director,raw_studio,raw_film_released,raw_media_release_date,raw_unit_cost_amount,raw_unit_cost_currency,source_filename,row_number",
        batch_id,
    )

    deduped_by_variant: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        variant_id = clean_text(r.get("shopify_variant_id"))
        if not variant_id:
            continue

        barcode = clean_text(r.get("raw_barcode"))
        product_title = clean_text(r.get("raw_title"))
        variant_title = clean_text(r.get("raw_variant_title"))

        title = product_title
        if variant_title and variant_title.lower() not in {"default title", "default"}:
            title = f"{product_title} — {variant_title}" if product_title else variant_title

        qty = parse_int_default_zero(r.get("raw_inventory_qty"))
        media_release_date = parse_date(r.get("raw_media_release_date"))
        if is_future_release(media_release_date):
            availability_status = "preorder"
        elif qty > 0:
            availability_status = "store_stock"
        else:
            availability_status = "store_out"

        fmt = variant_title if variant_title and variant_title.lower() not in {"default title", "default"} else None
        director = clean_text(r.get("raw_director"))
        studio = clean_text(r.get("raw_studio")) or clean_text(r.get("raw_vendor"))

        deduped_by_variant[variant_id] = {
            "import_batch_id": batch_id,
            "supplier": "Tape Film",
            "source_filename": clean_text(r.get("source_filename")),
            "source_row_number": r.get("row_number"),
            "supplier_sku": clean_text(r.get("raw_sku")),
            "barcode": barcode,
            "title": title,
            "normalized_title": normalize_title(title or ""),
            "edition_title": None,
            "format": fmt,
            "media_type": "film",
            "director": director,
            "studio": studio,
            "media_release_date": media_release_date,
            "supplier_stock_status": qty,
            "availability_status": availability_status,
            "supplier_currency": clean_text(r.get("raw_unit_cost_currency")) or "AUD",
            "cost_price": parse_float(r.get("raw_unit_cost_amount")),
            "calculated_sale_price": parse_float(r.get("raw_price")),
            "source_priority": 0,
            "source_type": "shopify",
            "active": True,
            "harmonized_title": title,
            "harmonized_format": fmt,
            "harmonized_director": director,
            "harmonized_studio": studio,
            "harmonized_from_supplier": "Tape Film",
            "harmonized_at": now_iso(),
            "raw_source_id": r.get("id"),
            "raw_source_table": "staging_shopify_raw",
            "shopify_product_id": clean_text(r.get("shopify_product_id")),
            "shopify_variant_id": variant_id,
        }

    out = list(deduped_by_variant.values())

    variant_ids = [r.get("shopify_variant_id") for r in out if r.get("shopify_variant_id")]
    existing_by_variant: Dict[str, str] = {}
    for batch in chunked(variant_ids, 200):
        resp = (
            supabase.table(offers_table)
            .select("id,shopify_variant_id")
            .eq("supplier", "Tape Film")
            .in_("shopify_variant_id", batch)
            .execute()
        )
        for row in resp.data or []:
            vid = row.get("shopify_variant_id")
            if vid:
                existing_by_variant[vid] = row.get("id")

    to_insert: list[Dict[str, Any]] = []
    to_update: list[Dict[str, Any]] = []
    for row in out:
        vid = row.get("shopify_variant_id")
        if vid and vid in existing_by_variant:
            to_update.append(row)
        else:
            to_insert.append(row)

    barcodes = [r.get("barcode") for r in to_insert if r.get("barcode")]
    existing_barcodes: set[str] = set()
    for batch in chunked(barcodes, 200):
        resp = (
            supabase.table(offers_table)
            .select("barcode")
            .eq("supplier", "Tape Film")
            .in_("barcode", batch)
            .execute()
        )
        for row in resp.data or []:
            bc = row.get("barcode")
            if bc:
                existing_barcodes.add(bc)

    still_to_insert: list[Dict[str, Any]] = []
    for row in to_insert:
        bc = row.get("barcode")
        if bc and bc in existing_barcodes:
            to_update.append(row)
        else:
            still_to_insert.append(row)
    to_insert = still_to_insert

    deduped_insert_by_key: Dict[str, Dict[str, Any]] = {}
    for row in to_insert:
        bc = row.get("barcode")
        vid = row.get("shopify_variant_id")
        key = f"barcode:{bc}" if bc else f"variant:{vid}"
        deduped_insert_by_key[key] = row
    to_insert = list(deduped_insert_by_key.values())

    for row in to_update:
        q = supabase.table(offers_table).update(row).eq("supplier", "Tape Film")
        if row.get("shopify_variant_id") in existing_by_variant:
            q = q.eq("shopify_variant_id", row["shopify_variant_id"])
        else:
            q = q.eq("barcode", row.get("barcode"))
        q.execute()

    for batch in chunked(to_insert, 1000):
        supabase.table(offers_table).insert(batch).execute()

    print(
        f"Synced {len(out)} staging_supplier_offers rows from Shopify batch {batch_id} "
        f"(updated: {len(to_update)}, inserted: {len(to_insert)})"
    )
    return len(out)


def run_normalize(
    *,
    offers_table: str,
    moovies_batch: Optional[str],
    lasgo_batch: Optional[str],
    shopify_batch: Optional[str],
    env_file: str = ".env",
) -> None:
    if not moovies_batch and not lasgo_batch and not shopify_batch:
        print("Provide --moovies-batch and/or --lasgo-batch and/or --shopify-batch", file=sys.stderr)
        raise SystemExit(1)

    load_dotenv(env_file)
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url:
        print("Missing SUPABASE_URL in .env", file=sys.stderr)
        raise SystemExit(1)
    if not key:
        print("Missing SUPABASE_SERVICE_KEY in .env", file=sys.stderr)
        raise SystemExit(1)

    supabase = create_client(url, key)

    if moovies_batch:
        normalize_from_moovies(supabase, offers_table, moovies_batch)
    if lasgo_batch:
        normalize_from_lasgo(supabase, offers_table, lasgo_batch)
    if shopify_batch:
        normalize_from_shopify(supabase, offers_table, shopify_batch)


def run_from_argv(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offers-table", default="staging_supplier_offers")
    parser.add_argument("--moovies-batch", default=None)
    parser.add_argument("--lasgo-batch", default=None)
    parser.add_argument("--shopify-batch", default=None)
    args = parser.parse_args(argv)
    try:
        run_normalize(
            offers_table=args.offers_table,
            moovies_batch=args.moovies_batch,
            lasgo_batch=args.lasgo_batch,
            shopify_batch=args.shopify_batch,
        )
    except SystemExit as e:
        code = e.code
        return int(code) if isinstance(code, int) else 1
    return 0


def main() -> None:
    raise SystemExit(run_from_argv())
