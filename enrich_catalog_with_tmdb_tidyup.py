import os
import re
import sys
import time
import unicodedata
import requests
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from app.helpers.tmdb_match_helpers import extract_year, search_tmdb_movie_safe

load_dotenv(dotenv_path=".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_API_URL = "https://api.themoviedb.org/3"

if not SUPABASE_URL:
    raise ValueError("Missing SUPABASE_URL in .env")

if not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_SERVICE_KEY in .env")

if not TMDB_API_KEY:
    raise ValueError("Missing TMDB_API_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def clean_movie_title(title):
    if not title:
        return ""

    patterns_to_remove = [
        r"\b4K\b", r"\bUHD\b", r"\bBlu[- ]?ray\b", r"\bUltra HD\b", r"\bDVD\b",
        r"\bLimited Edition\b", r"\bLimited\b", r"\bDeluxe Edition\b", r"\bDeluxe\b",
        r"\bCollectors?'? Edition\b", r"\bSpecial Edition\b",
        r"\bSteelbook\b", r"\bCombo Pack\b", r"\bSlipcover\b", r"\bBox Set\b",
        r"\bRegion\s?\w\b", r"\bHDR\b", r"\bDolby Vision\b", r"\bAtmos\b",
        r"\bFrom the World of.*", r"\bThe Complete.*", r"\bMovie Collection\b",
        r"\+\s*Blu[- ]?ray", r"\bDigital HD\b", r"\bDigital Copy\b",
    ]

    pattern = re.compile("|".join(patterns_to_remove), flags=re.IGNORECASE)
    cleaned = pattern.sub("", str(title))
    cleaned = re.sub(r"\(.*?\)|\[.*?\]", "", cleaned)
    cleaned = re.sub(r"[-:]+\s*$", "", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" -–:,")
    return cleaned.strip()


def normalize_match_title(value):
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
    text = re.sub(r"\b3d\s*\+\s*2d\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bseason\s+\d+\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bseries\s+\d+\b", "", text, flags=re.IGNORECASE)

    text = re.sub(r"\s*&\s*", " and ", text, flags=re.IGNORECASE)
    text = re.sub(r"\band\b", " and ", text, flags=re.IGNORECASE)

    text = re.sub(r"\(.*?\)|\[.*?\]", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def title_tokens(value):
    return [t for t in normalize_match_title(value).split() if t]


def is_safe_tmdb_match(source_title, candidate_title):
    source_norm = normalize_match_title(source_title)
    candidate_norm = normalize_match_title(candidate_title)

    if not source_norm or not candidate_norm:
        return False

    # safest path: exact normalized title
    if source_norm == candidate_norm:
        return True

    # allow common subtitle cases only when the base title is clearly the same
    if candidate_norm.startswith(source_norm + " "):
        return True

    if source_norm.startswith(candidate_norm + " "):
        return True

    # conservative token overlap gate
    source_tokens = set(title_tokens(source_title))
    candidate_tokens = set(title_tokens(candidate_title))

    if not source_tokens or not candidate_tokens:
        return False

    overlap = source_tokens & candidate_tokens

    # require nearly all source tokens to be present, and reject weak overlaps
    if len(source_tokens) <= 2:
        return False

    return len(overlap) >= len(source_tokens) - 0


def search_tmdb_movie(title, source_year=None):
    return search_tmdb_movie_safe(title, TMDB_API_KEY, TMDB_API_URL, source_year=source_year)



def get_tmdb_details(movie_id):
    details = requests.get(
        f"{TMDB_API_URL}/movie/{movie_id}",
        params={"api_key": TMDB_API_KEY},
        timeout=30,
    )
    details.raise_for_status()

    credits = requests.get(
        f"{TMDB_API_URL}/movie/{movie_id}/credits",
        params={"api_key": TMDB_API_KEY},
        timeout=30,
    )
    credits.raise_for_status()

    return details.json(), credits.json()

def clean_text(value):
    if value is None:
        return None

    text = str(value).strip()
    return text if text else None


def clean_date(value):
    if value is None:
        return None

from datetime import datetime, timezone

def now_iso():
    return datetime.now(timezone.utc).isoformat()

    text = str(value).strip()
    return text if text else None


def clean_update_data(data):
    cleaned = {}

    for key, value in data.items():
        if key == "film_released":
            cleaned[key] = clean_date(value)
        elif isinstance(value, str):
            cleaned[key] = clean_text(value)
        else:
            cleaned[key] = value

    return cleaned


BATCH_SIZE = 500


def group_rows_by_barcode(rows):
    groups = {}
    no_barcode_rows = []

    for row in rows:
        barcode = (row.get("barcode") or "").strip()

        if barcode:
            groups.setdefault(barcode, []).append(row)
        else:
            no_barcode_rows.append(row)

    return groups, no_barcode_rows


def update_rows_by_ids(row_ids, update_data):
    if not row_ids:
        return

    (
        supabase.table("catalog_items")
        .update(update_data)
        .in_("id", row_ids)
        .execute()
    )


def fetch_rows_needing_enrichment():
    response = (
        supabase.table("catalog_items")
        .select("""
            id,
            title,
            barcode,
            director,
            film_released,
            tmdb_id,
            tmdb_title,
            tmdb_match_status,
            tmdb_last_refreshed_at,
            media_type,
            film_id
        """)
        .eq("active", True)
        .eq("media_type", "film")
        .is_("film_id", "null")
        .is_("tmdb_match_status", "null")
        .limit(BATCH_SIZE)
        .execute()
    )
    return response.data or []

def enrich_row(row):
    title = row.get("title") or ""
    cleaned = clean_movie_title(title)

    if not cleaned:
        return {
            "tmdb_match_status": "no_clean_title",
        }

    print(f"Searching TMDb for: {cleaned}")
    source_year = extract_year(row.get("film_released"))
    movie = search_tmdb_movie(title, source_year=source_year)

    if not movie:
        return {
            "tmdb_match_status": "not_found",
        }

    details, credits = get_tmdb_details(movie["id"])

    directors = [p["name"] for p in credits.get("crew", []) if p.get("job") == "Director"]
    cast = [p["name"] for p in credits.get("cast", [])][:5]
    genres = [g["name"] for g in details.get("genres", [])][:3]
    poster_path = details.get("poster_path")
    backdrop_path = details.get("backdrop_path")
    vote_average = details.get("vote_average")
    vote_count = details.get("vote_count")
    popularity = details.get("popularity")

    production_countries = details.get("production_countries", []) or []
    country_of_origin = (
        production_countries[0].get("name")
        if production_countries and production_countries[0].get("name")
        else None
    )


    return {
        "director": row.get("director") or (directors[0] if directors else None),
        "film_released": details.get("release_date"),
        "tmdb_id": movie.get("id"),
        "tmdb_title": movie.get("title"),
        "tmdb_match_status": "matched",
        "top_cast": ", ".join(cast) if cast else None,
        "genres": ", ".join(genres) if genres else None,
        "country_of_origin": country_of_origin,
        "tmdb_poster_path": poster_path,
        "tmdb_backdrop_path": backdrop_path,
        "tmdb_vote_average": vote_average,
        "tmdb_vote_count": vote_count,
        "tmdb_popularity": popularity,
        "tmdb_last_refreshed_at": now_iso(),
    }

def main():
    rows = fetch_rows_needing_enrichment()

    if not rows:
        print("No rows need enrichment.")
        return

    print(f"Fetched {len(rows)} rows needing enrichment")

    barcode_groups, no_barcode_rows = group_rows_by_barcode(rows)

    processed_groups = 0
    processed_rows = 0

    # 1. Enrich grouped barcode rows once per barcode
    for barcode, grouped_rows in barcode_groups.items():
        representative = grouped_rows[0]

        try:
            update_data = enrich_row(representative)
            update_data = clean_update_data(update_data)

            row_ids = [r["id"] for r in grouped_rows]
            update_rows_by_ids(row_ids, update_data)

            processed_groups += 1
            processed_rows += len(grouped_rows)

            print(
                f"[barcode group {processed_groups}] "
                f"Updated {len(grouped_rows)} row(s) for barcode {barcode}: "
                f"{representative.get('title')}"
            )

            time.sleep(0.25)

        except Exception as e:
            print(
                f"Error enriching barcode group {barcode} "
                f"for title '{representative.get('title')}': {e}"
            )

    # 2. Enrich barcode-less rows individually
    for i, row in enumerate(no_barcode_rows, start=1):
        try:
            update_data = enrich_row(row)
            update_data = clean_update_data(update_data)

            (
                supabase.table("catalog_items")
                .update(update_data)
                .eq("id", row["id"])
                .execute()
            )
            processed_rows += 1

            print(
                f"[no-barcode {i}/{len(no_barcode_rows)}] "
                f"Updated: {row.get('title')}"
            )

            time.sleep(0.25)

        except Exception as e:
            print(f"Error enriching '{row.get('title')}': {e}")

    print(f"Done. Processed {processed_rows} row(s) in this batch.")

if __name__ == "__main__":
    main()
