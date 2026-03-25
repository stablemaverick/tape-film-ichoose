import os
import sys
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


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
        


def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def now_iso():
    return datetime.now(timezone.utc).isoformat()


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
                supplier_last_seen_at
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

    return {
        # identity / core supplier row
        "id": existing.get("id"),
        "supplier": incoming["supplier"],
        "barcode": incoming["barcode"],

        # keep latest supplier/commercial values
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

        # preserve enriched / curated metadata
        "director": existing.get("director") or incoming.get("director"),
        "studio": existing.get("studio") or incoming.get("studio"),
        "film_released": existing.get("film_released") or incoming.get("film_released"),
        "media_release_date": existing.get("media_release_date") or incoming.get("media_release_date"),
        "country_of_origin": existing.get("country_of_origin") or incoming.get("country_of_origin"),
        "tmdb_id": existing.get("tmdb_id"),
        "tmdb_title": existing.get("tmdb_title"),
        "tmdb_match_status": existing.get("tmdb_match_status"),
        "top_cast": existing.get("top_cast"),
        "genres": existing.get("genres"),
    }        


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

def import_catalog(filepath):

    df = pd.read_excel(filepath, header=1)

    df = df.rename(columns={
        "Title": "title",
        "Cat No": "supplier_sku",
        "Barcode": "barcode",
        "Format": "format",
        "No of Discs": "no_of_discs",
        "Qty Free": "supplier_stock",
        "Selling Price £": "cost_price",
        "Region Code": "region_code",
    })

    rows = []

    for _, r in df.iterrows():

        if pd.isna(r["barcode"]):
            continue

        cost = float(r["cost_price"]) if not pd.isna(r["cost_price"]) else 0
        sale_price = calculate_sale_price(cost)

        stock = parse_stock(r["supplier_stock"])

        availability = "supplier_stock" if stock > 0 else "supplier_out"

        row = {
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
            "no_of_discs": parse_int_or_none(r["no_of_discs"]),
            "region_code": clean_text(r["region_code"]),
            "source_type": "catalog",
            "active": True,
            "supplier_last_seen_at": now_iso(),
        }

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
    
        batch = merged_rows[i:i+batch_size]
    
        response = (
            supabase.table("catalog_items")
            .upsert(batch, on_conflict="supplier,barcode")
            .execute()
        )
    
        print(f"Upserted batch {i} - {i+len(batch)}")


if __name__ == "__main__":
    import_catalog(sys.argv[1])