import os
import re
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


def normalize_title(value):
    if not value:
        return ""

    text = str(value).lower().strip()

    text = re.sub(r"\b4k\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\buhd\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bblu[\s-]?ray\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdvd\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\blimited edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bcollector'?s edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsteelbook\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bbox set\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdeluxe edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdeluxe\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bslipcase\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bslipcover\b", "", text, flags=re.IGNORECASE)

    text = re.sub(r"\(.*?\)|\[.*?\]", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


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

def fetch_film_by_id(film_id):
    if not film_id:
        return None

    response = (
        supabase.table("films")
        .select("""
            id,
            title,
            tmdb_id,
            tmdb_title,
            director,
            film_released,
            country_of_origin,
            genres,
            top_cast,
            tmdb_poster_path,
            tmdb_backdrop_path,
            tmdb_vote_average,
            tmdb_vote_count,
            tmdb_popularity
        """)
        .eq("id", film_id)
        .limit(1)
        .execute()
    )

    if response.data:
        return response.data[0]

    return None

def find_existing_film_match_by_barcode(barcode, current_supplier=None):
    barcode = clean_text(barcode)
    if not barcode:
        return None, None, None

    query = (
        supabase.table("catalog_items")
        .select("""
            supplier,
            film_id,
            tmdb_id,
            tmdb_title,
            tmdb_match_status,
            director,
            film_released,
            country_of_origin,
            genres,
            top_cast,
            tmdb_poster_path,
            tmdb_backdrop_path,
            tmdb_vote_average,
            tmdb_vote_count,
            tmdb_popularity
        """)
        .eq("barcode", barcode)
        .not_.is_("film_id", "null")
    )

    if current_supplier:
        query = query.neq("supplier", current_supplier)

    response = query.execute()
    rows = response.data or []

    if not rows:
        return None, None, None

    def donor_score(row):
        score = 0
        if row.get("tmdb_id"): score += 10
        if row.get("tmdb_title"): score += 5
        if row.get("genres"): score += 4
        if row.get("top_cast"): score += 4
        if row.get("country_of_origin"): score += 3
        if row.get("director"): score += 2
        if row.get("film_released"): score += 2
        if row.get("tmdb_poster_path"): score += 1
        return score

    best_row = sorted(rows, key=donor_score, reverse=True)[0]
    return best_row, best_row.get("film_id"), "barcode"


def fetch_all_films():
    response = (
        supabase.table("films")
        .select("""
            id,
            title,
            tmdb_id,
            tmdb_title,
            director,
            film_released,
            country_of_origin,
            genres,
            top_cast,
            tmdb_poster_path,
            tmdb_backdrop_path,
            tmdb_vote_average,
            tmdb_vote_count,
            tmdb_popularity
        """)
        .execute()
    )
    return response.data or []


def find_existing_film_by_clean_title(cleaned_title, films_cache):
    if not cleaned_title:
        return None, None

    for film in films_cache:
        film_title = normalize_title(film.get("title"))
        tmdb_title = normalize_title(film.get("tmdb_title"))

        if cleaned_title == film_title or cleaned_title == tmdb_title:
            return film, "local_tmdb_title"

    return None, None


def build_linked_metadata_from_film(film, method):
    if not film:
        return {}

    return {
        "film_id": film.get("id"),
        "film_link_status": "linked",
        "film_link_method": method,
        "tmdb_id": film.get("tmdb_id"),
        "tmdb_title": film.get("tmdb_title") or film.get("title"),
        "tmdb_match_status": "matched" if film.get("tmdb_id") else None,
        "director": film.get("director"),
        "film_released": film.get("film_released"),
        "country_of_origin": film.get("country_of_origin"),
        "genres": film.get("genres"),
        "top_cast": film.get("top_cast"),
        "tmdb_poster_path": film.get("tmdb_poster_path"),
        "tmdb_backdrop_path": film.get("tmdb_backdrop_path"),
        "tmdb_vote_average": film.get("tmdb_vote_average"),
        "tmdb_vote_count": film.get("tmdb_vote_count"),
        "tmdb_popularity": film.get("tmdb_popularity"),
    }


def build_linked_metadata_from_catalog_row(row, method):
    if not row:
        return {}

    return {
        "film_id": row.get("film_id"),
        "film_link_status": "linked",
        "film_link_method": method,
        "tmdb_id": row.get("tmdb_id"),
        "tmdb_title": row.get("tmdb_title"),
        "tmdb_match_status": row.get("tmdb_match_status"),
        "director": row.get("director"),
        "film_released": row.get("film_released"),
        "country_of_origin": row.get("country_of_origin"),
        "genres": row.get("genres"),
        "top_cast": row.get("top_cast"),
        "tmdb_poster_path": row.get("tmdb_poster_path"),
        "tmdb_backdrop_path": row.get("tmdb_backdrop_path"),
        "tmdb_vote_average": row.get("tmdb_vote_average"),
        "tmdb_vote_count": row.get("tmdb_vote_count"),
        "tmdb_popularity": row.get("tmdb_popularity"),
    }


def resolve_existing_film_metadata(row, films_cache):
    barcode = clean_text(row.get("barcode"))
    title = row.get("title") or ""
    cleaned_title = normalize_title(title)
    current_supplier = row.get("supplier")

    # 1. Barcode match from existing catalog row
    matched_catalog_row, film_id, method = find_existing_film_match_by_barcode(
        barcode,
        current_supplier=current_supplier
    )
    if matched_catalog_row:
        return build_linked_metadata_from_catalog_row(matched_catalog_row, method)

    # 2. Existing films match by clean title / tmdb_title
    film, method = find_existing_film_by_clean_title(cleaned_title, films_cache)
    if film:
        return build_linked_metadata_from_film(film, method)

    # 3. No local match found
    return {
        "film_id": None,
        "film_link_status": None,
        "film_link_method": None,
        "tmdb_id": None,
        "tmdb_title": None,
        "tmdb_match_status": None,
        "director": None,
        "film_released": None,
        "country_of_origin": None,
        "genres": None,
        "top_cast": None,
        "tmdb_poster_path": None,
        "tmdb_backdrop_path": None,
        "tmdb_vote_average": None,
        "tmdb_vote_count": None,
        "tmdb_popularity": None,
    }

def merge_catalog_row(existing, incoming):
    if not existing:
        return incoming

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
        "film_id": incoming.get("film_id") or existing.get("film_id"),
        "film_link_status": incoming.get("film_link_status") or existing.get("film_link_status"),
        "film_link_method": incoming.get("film_link_method") or existing.get("film_link_method"),
        "tmdb_id": incoming.get("tmdb_id") or existing.get("tmdb_id"),
        "tmdb_title": incoming.get("tmdb_title") or existing.get("tmdb_title"),
        "tmdb_match_status": incoming.get("tmdb_match_status") or existing.get("tmdb_match_status"),
        "director": incoming.get("director") or existing.get("director"),
        "studio": incoming.get("studio") or existing.get("studio"),
        "film_released": incoming.get("film_released") or existing.get("film_released"),
        "media_release_date": existing.get("media_release_date") or incoming.get("media_release_date"),
        "country_of_origin": incoming.get("country_of_origin") or existing.get("country_of_origin"),
        "top_cast": incoming.get("top_cast") or existing.get("top_cast"),
        "genres": incoming.get("genres") or existing.get("genres"),
        "tmdb_poster_path": incoming.get("tmdb_poster_path") or existing.get("tmdb_poster_path"),
        "tmdb_backdrop_path": incoming.get("tmdb_backdrop_path") or existing.get("tmdb_backdrop_path"),
        "tmdb_vote_average": incoming.get("tmdb_vote_average") or existing.get("tmdb_vote_average"),
        "tmdb_vote_count": incoming.get("tmdb_vote_count") or existing.get("tmdb_vote_count"),
        "tmdb_popularity": incoming.get("tmdb_popularity") or existing.get("tmdb_popularity"),
    }


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

    films_cache = fetch_all_films()
    rows = []

    for _, r in df.iterrows():
        if pd.isna(r["barcode"]):
            continue

        cost = float(r["cost_price"]) if not pd.isna(r["cost_price"]) else 0
        sale_price = calculate_sale_price(cost)
        stock = parse_stock(r["supplier_stock"])
        availability = "supplier_stock" if stock > 0 else "supplier_out"

        base_row = {
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
            "media_type": "film",
            "studio": None,
            "media_release_date": None,
        }

        linked_metadata = resolve_existing_film_metadata(base_row, films_cache)
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
