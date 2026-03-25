import os
import re
import sys
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import uuid

load_dotenv(dotenv_path=".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

from catalog_match_helpers import (
    fetch_all_films,
    resolve_existing_film_metadata,
)

def round_up_to_99(value):
    rounded = round(value, 2)
    whole = int(rounded)

    if rounded <= whole + 0.99:
        return round(whole + 0.99, 2)

    return round((whole + 1) + 0.99, 2)


def get_margin(cost_gbp):
    if cost_gbp <= 15:
        return 0.32
    elif cost_gbp <= 30:
        return 0.28
    elif cost_gbp <= 40:
        return 0.24
    else:
        return 0.20


def calculate_sale_price(cost_gbp):
    aud_base = cost_gbp * 2
    total_cost = aud_base * 1.12
    margin = get_margin(cost_gbp)
    pre_gst_sale = total_cost * (1 + margin)
    final_sale = pre_gst_sale * 1.10
    return round_up_to_99(final_sale)


def parse_stock(value):
    if pd.isna(value):
        return 0

    text = str(value).strip()

    if text == "":
        return 0

    text = text.replace("+", "").replace(",", "").strip()

    try:
        return int(float(text))
    except ValueError:
        return 0

def parse_price_gbp(value):
    if pd.isna(value):
        return 0.0

    text = str(value).strip().replace("£", "").replace(",", "")
    if text == "":
        return 0.0

    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_availability_text(value):
    if pd.isna(value):
        return 0

    text = str(value).strip().lower()
    if text == "":
        return 0

    if "250+" in text:
        return 250

    match = re.search(r"(\d+)", text)
    if match:
        return int(match.group(1))

    return 0


def parse_date_ddmmyyyy(value):
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text == "":
        return None

    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue

    return None

def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_int_or_none(value):
    if pd.isna(value):
        return None

    text = str(value).strip().replace(",", "")
    if text == "":
        return None

    try:
        return int(float(text))
    except ValueError:
        return None


def fetch_existing_rows(supplier, barcodes):
    existing = {}
    batch_size = 200

    clean_barcodes = [b for b in barcodes if b]

    for i in range(0, len(clean_barcodes), batch_size):
        batch = clean_barcodes[i:i + batch_size]

        response = (
            supabase.table("catalog_items")
            .select("""
                id,
                supplier,
                barcode,
                title,
                edition_title,
                format,
                director,
                studio,
                film_released,
                media_release_date,
                supplier_sku,
                supplier_currency,
                cost_price,
                pricing_source,
                calculated_sale_price,
                availability_status,
                supplier_stock_status,
                supplier_priority,
                no_of_discs,
                region_code,
                country_of_origin,
                tmdb_id,
                tmdb_title,
                tmdb_match_status,
                top_cast,
                genres,
                source_type,
                active,
                supplier_last_seen_at,
                film_id,
                film_link_status,
                film_link_method,
                tmdb_poster_path,
                tmdb_backdrop_path,
                tmdb_vote_average,
                tmdb_vote_count,
                tmdb_popularity,
                media_type
            """)
            .eq("supplier", supplier)
            .in_("barcode", batch)
            .execute()
        )

        for row in response.data or []:
            existing[row["barcode"]] = row

    return existing

def merge_catalog_row(existing, incoming):
    if not existing:
        return incoming

    locked_film_id = existing.get("film_id")
    has_locked_match = bool(locked_film_id)

    return {
        # identity
        "id": existing.get("id"),
        "supplier": incoming["supplier"],
        "barcode": incoming["barcode"],

        # latest supplier / commercial values
        "title": incoming.get("title") or existing.get("title"),
        "edition_title": existing.get("edition_title"),
        "format": incoming.get("format") or existing.get("format"),
        "supplier_sku": incoming.get("supplier_sku") or existing.get("supplier_sku"),
        "supplier_currency": incoming.get("supplier_currency") or existing.get("supplier_currency"),
        "cost_price": incoming.get("cost_price"),
        "pricing_source": incoming.get("pricing_source") or existing.get("pricing_source"),
        "calculated_sale_price": incoming.get("calculated_sale_price"),
        "availability_status": incoming.get("availability_status"),
        "supplier_stock_status": incoming.get("supplier_stock_status"),
        "supplier_priority": incoming.get("supplier_priority") or existing.get("supplier_priority"),
        "no_of_discs": incoming.get("no_of_discs") or existing.get("no_of_discs"),
        "region_code": incoming.get("region_code") or existing.get("region_code"),
        "source_type": incoming.get("source_type") or existing.get("source_type"),
        "active": True,
        "supplier_last_seen_at": incoming.get("supplier_last_seen_at"),
        "media_type": existing.get("media_type") or incoming.get("media_type") or "film",

        # preserve / refresh film metadata snapshot
        # Once a film link exists, do not rematch/relink on subsequent imports.
        "film_id": locked_film_id or incoming.get("film_id"),
        "film_link_status": existing.get("film_link_status") if has_locked_match else incoming.get("film_link_status") or existing.get("film_link_status"),
        "film_link_method": existing.get("film_link_method") if has_locked_match else incoming.get("film_link_method") or existing.get("film_link_method"),

        # Keep the existing metadata snapshot when the link is locked.
        "tmdb_id": existing.get("tmdb_id") if has_locked_match else incoming.get("tmdb_id") or existing.get("tmdb_id"),
        "tmdb_title": existing.get("tmdb_title") if has_locked_match else incoming.get("tmdb_title") or existing.get("tmdb_title"),
        "tmdb_match_status": existing.get("tmdb_match_status") if has_locked_match else incoming.get("tmdb_match_status") or existing.get("tmdb_match_status"),
        "director": existing.get("director") if has_locked_match else incoming.get("director") or existing.get("director"),
        "studio": incoming.get("studio") or existing.get("studio"),
        "film_released": existing.get("film_released") if has_locked_match else incoming.get("film_released") or existing.get("film_released"),
        "media_release_date": incoming.get("media_release_date") or existing.get("media_release_date"),
        "country_of_origin": incoming.get("country_of_origin") or existing.get("country_of_origin"),
        "top_cast": existing.get("top_cast") if has_locked_match else incoming.get("top_cast") or existing.get("top_cast"),
        "genres": existing.get("genres") if has_locked_match else incoming.get("genres") or existing.get("genres"),
        "tmdb_poster_path": existing.get("tmdb_poster_path") if has_locked_match else incoming.get("tmdb_poster_path") or existing.get("tmdb_poster_path"),
        "tmdb_backdrop_path": existing.get("tmdb_backdrop_path") if has_locked_match else incoming.get("tmdb_backdrop_path") or existing.get("tmdb_backdrop_path"),
        "tmdb_vote_average": existing.get("tmdb_vote_average") if has_locked_match else incoming.get("tmdb_vote_average") or existing.get("tmdb_vote_average"),
        "tmdb_vote_count": existing.get("tmdb_vote_count") if has_locked_match else incoming.get("tmdb_vote_count") or existing.get("tmdb_vote_count"),
        "tmdb_popularity": existing.get("tmdb_popularity") if has_locked_match else incoming.get("tmdb_popularity") or existing.get("tmdb_popularity"),
    }


def import_catalog(filepath):
    df = pd.read_excel(filepath)
    print("Columns detected:", list(df.columns))

    df = df.rename(columns={
        "EAN/Barcode": "barcode",
        "CATALOGUE": "supplier_sku",
        "TITLE": "title",
        "RELEASE": "media_release_date",
        "DIRECTOR/ARTIST": "director",
        "STUDIO/BRAND": "studio",
        "FORMAT": "format",
        "AVAILABILITY": "supplier_stock_text",
        "Your Price ex VAT": "cost_price",
    })

    films_cache = fetch_all_films(supabase)
    rows = []

    for _, r in df.iterrows():
        if pd.isna(r["barcode"]):
            continue

        cost = parse_price_gbp(r["cost_price"])
        sale_price = calculate_sale_price(cost)
        stock = parse_availability_text(r["supplier_stock_text"])
        availability = "supplier_stock" if stock > 0 else "supplier_out"

        base_row = {
            "id": str(uuid.uuid4()),
            "title": clean_text(r["title"]),
            "edition_title": None,
            "format": clean_text(r["format"]),
            "barcode": clean_text(r["barcode"]),
            "supplier": "Lasgo",
            "supplier_sku": clean_text(r["supplier_sku"]),
            "supplier_currency": "GBP",
            "cost_price": cost,
            "pricing_source": "gbp_formula_v1",
            "calculated_sale_price": sale_price,
            "supplier_stock_status": stock,
            "availability_status": availability,
            "supplier_priority": 2,
            "no_of_discs": None,
            "region_code": None,
            "source_type": "catalog",
            "active": True,
            "supplier_last_seen_at": now_iso(),
            "media_type": "film",
            "director": clean_text(r["director"]),
            "studio": clean_text(r["studio"]),
            "media_release_date": parse_date_ddmmyyyy(r["media_release_date"]),
        }

        linked_metadata = resolve_existing_film_metadata(supabase, base_row, films_cache)
        row = {**base_row, **linked_metadata}
        rows.append(row)

    print(f"Processing {len(rows)} Lasgo items")

    existing_rows = fetch_existing_rows(
        "Lasgo",
        [row["barcode"] for row in rows if row.get("barcode")]
    )

    merged_rows = [
        merge_catalog_row(existing_rows.get(row["barcode"]), row)
        for row in rows
    ]

    batch_size = 500

    for i in range(0, len(merged_rows), batch_size):
        batch = merged_rows[i:i + batch_size]

        (
            supabase.table("catalog_items")
            .upsert(batch, on_conflict="supplier,barcode")
            .execute()
        )

        print(f"Upserted batch {i} - {i + len(batch)}")


if __name__ == "__main__":
    import_catalog(sys.argv[1])
