import os
import sys
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
from datetime import datetime, timezone

load_dotenv(dotenv_path=".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL:
    raise ValueError("Missing SUPABASE_URL in .env")

if not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_SERVICE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def round_up_to_99(price: float) -> float:
    whole = int(price)
    target = whole + 0.99

    if price <= target:
        return round(target, 2)

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

def parse_date(date_string):
    if pd.isna(date_string) or not str(date_string).strip():
        return None

    date_string = str(date_string).strip()

    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_string, fmt).date().isoformat()
        except ValueError:
            continue

    return None


def parse_float(value):
    if pd.isna(value) or str(value).strip() == "":
        return None

    try:
        cleaned = str(value).replace("£", "").replace(",", "").strip()
        return float(cleaned)
    except ValueError:
        return None


def parse_int(value):
    if pd.isna(value) or str(value).strip() == "":
        return 0

    try:
        cleaned = str(value).replace(",", "").strip()
        return int(float(cleaned))
    except ValueError:
        return 0


def map_availability(status, stock_available):
    status_str = str(status).strip().lower() if status is not None else ""
    stock_int = parse_int(stock_available)

    if status_str in {"deleted", "discontinued", "inactive"}:
        return "archived"

    if stock_int > 0:
        return "supplier_stock"

    return "supplier_out"

def clean_text(value):
    if pd.isna(value):
        return None

    text = str(value).strip()
    return text if text else None
    
def now_iso():
    return datetime.now(timezone.utc).isoformat()
    

def map_availability(status, stock_available):
    status_str = str(status).strip().lower() if status is not None else ""
    stock_int = parse_int(stock_available)

    if status_str in {"deleted", "discontinued", "inactive"}:
        return "archived"

    if stock_int > 0:
        return "supplier_stock"

    return "supplier_out"


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
                country_of_origin,
                category,
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
        "id": existing.get("id"),
        "supplier": incoming["supplier"],
        "barcode": incoming["barcode"],

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
        "country_of_origin": incoming.get("country_of_origin") or existing.get("country_of_origin"),
        "category": incoming.get("category") or existing.get("category"),
        "source_type": incoming.get("source_type") or existing.get("source_type"),
        "active": True,
        "supplier_last_seen_at": incoming.get("supplier_last_seen_at"),

        "director": existing.get("director") or incoming.get("director"),
        "studio": existing.get("studio") or incoming.get("studio"),
        "film_released": existing.get("film_released") or incoming.get("film_released"),
        "media_release_date": existing.get("media_release_date") or incoming.get("media_release_date"),
        "tmdb_id": existing.get("tmdb_id"),
        "tmdb_title": existing.get("tmdb_title"),
        "tmdb_match_status": existing.get("tmdb_match_status"),
        "top_cast": existing.get("top_cast"),
        "genres": existing.get("genres"),
    }


def map_row(row):
    cost = parse_float(row.get("Your Price"))
    sale_price = calculate_sale_price(cost) if cost is not None else None
    stock = parse_int(row.get("Stock Available"))

    return {
        "title": clean_text(row.get("Description")),
        "edition_title": None,
        "format": clean_text(row.get("Format")),
        "director": None,
        "studio": clean_text(row.get("Label")),
        "film_released": None,
        "media_release_date": parse_date(row.get("Release Date")),
        "barcode": clean_text(row.get("Barcode")),
        "sku": None,
        "supplier": "Moovies",
        "supplier_sku": clean_text(row.get("Product Code")),
        "supplier_currency": "GBP",
        "cost_price": cost,
        "pricing_source": "gbp_formula_v1",
        "calculated_sale_price": sale_price,
        "availability_status": map_availability(
            row.get("Status"),
            row.get("Stock Available"),
        ),
        "supplier_stock_status": stock,
        "supplier_priority": 1,
        "country_of_origin": clean_text(row.get("Country of Origin")),
        "category": clean_text(row.get("Category")),
        "source_type": "catalog",
        "active": True,
        "supplier_last_seen_at": now_iso(),
    }


def import_catalog(file_path):
    print(f"Loading file: {file_path}")

    if file_path.lower().endswith(".xlsx"):
        df = pd.read_excel(file_path)
    else:
        df = pd.read_csv(file_path)

    print(f"Rows found: {len(df)}")
    print("Columns detected:", list(df.columns))

    rows = []
    skipped = 0

    for _, row in df.iterrows():
        mapped = map_row(row)

        if not mapped["title"]:
            skipped += 1
            continue

        if not mapped["barcode"]:
            skipped += 1
            continue

        rows.append(mapped)

    print(f"Prepared {len(rows)} valid Moovies rows, skipped {skipped}")

    existing_rows = fetch_existing_rows(
        "Moovies",
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

    print(f"Import complete. Upserted: {len(merged_rows)}, Skipped: {skipped}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_moovies_catalog.py <file.csv|file.xlsx>")
        raise SystemExit(1)

    import_catalog(sys.argv[1])