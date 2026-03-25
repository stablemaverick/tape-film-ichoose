"""
MAINTENANCE: Relink suspicious catalog rows to correct films.

Purpose:
  Identifies catalog_items rows where the film link looks suspicious
  (e.g. title mismatch between catalog and film) and attempts to
  re-link them using TMDB search with the catalog title.

Tables/fields mutated:
  catalog_items: film_id, film_link_status, film_link_method, film_linked_at,
                 tmdb_id, tmdb_title, tmdb_match_status, tmdb_last_refreshed_at,
                 director, film_released, genres, top_cast, country_of_origin,
                 tmdb_poster_path, tmdb_backdrop_path

Safe mode: Preview logging before writes
Cron-safe: NO — manual/one-off use only
"""

import os
import re
import sys
import time
import requests
from dotenv import load_dotenv
from supabase import create_client

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
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

BATCH_SIZE = 250
SLEEP_SECONDS = 0.25


def clean_text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


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
    text = re.sub(r"\bthe final cut\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdirector'?s cut\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bultimate edition\b", "", text, flags=re.IGNORECASE)

    text = re.sub(r"\(.*?\)|\[.*?\]", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    text = re.sub(r"\s-\s", " ", text)
    text = re.sub(r"\s:\s", " ", text)
    text = re.sub(r"\bthe fantastic four\b", "fantastic four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bfantastic 4\b", "fantastic four", text, flags=re.IGNORECASE)

    return text


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


def fetch_suspicious_rows():
    all_rows = []
    offset = 0
    page_size = 1000

    while True:
        response = (
            supabase.table("catalog_items")
            .select("""
                id,
                supplier,
                barcode,
                title,
                format,
                director,
                studio,
                film_released,
                media_release_date,
                supplier_sku,
                supplier_currency,
                cost_price,
                calculated_sale_price,
                availability_status,
                supplier_stock_status,
                supplier_priority,
                country_of_origin,
                top_cast,
                genres,
                tmdb_id,
                tmdb_title,
                tmdb_match_status,
                film_id,
                film_link_status,
                film_link_method,
                tmdb_poster_path,
                tmdb_backdrop_path,
                tmdb_vote_average,
                tmdb_vote_count,
                tmdb_popularity,
                media_type,
                active
            """)
            .eq("active", True)
            .eq("media_type", "film")
            .not_.is_("film_id", "null")
            # .ilike("title", "%Cat's Eye%")
            .range(offset, offset + page_size - 1)
            .execute()
        )

        rows = response.data or []
        if not rows:
            break

        all_rows.extend(rows)
        offset += page_size

    suspicious = []

    film_ids = list({r["film_id"] for r in all_rows if r.get("film_id")})
    film_map = fetch_films_by_ids(film_ids)

    for row in all_rows:
        film = film_map.get(row.get("film_id"))
        if not film:
            suspicious.append(row)
            continue

        catalog_title = normalize_title(row.get("title"))
        film_title = normalize_title(film.get("title"))
        tmdb_title = normalize_title(film.get("tmdb_title"))

        if not catalog_title:
            suspicious.append(row)
            continue

        if catalog_title == film_title or catalog_title == tmdb_title:
            continue

        if film_title and catalog_title.startswith(film_title + " "):
            continue

        if tmdb_title and catalog_title.startswith(tmdb_title + " "):
            continue

        suspicious.append(row)

    return suspicious



def fetch_films_by_ids(film_ids):
    film_map = {}
    clean_ids = [fid for fid in film_ids if fid]
    batch_size = 500

    for i in range(0, len(clean_ids), batch_size):
        batch = clean_ids[i:i + batch_size]

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
            .in_("id", batch)
            .execute()
        )

        for film in response.data or []:
            film_map[film["id"]] = film

    return film_map


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


def find_existing_film_match_by_barcode(barcode, current_row_id=None, current_supplier=None):
    barcode = clean_text(barcode)
    if not barcode:
        return None, None, None

    query = (
        supabase.table("catalog_items")
        .select("""
            id,
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

    if current_row_id:
        query = query.neq("id", current_row_id)

    response = query.execute()
    rows = response.data or []

    if not rows:
        return None, None, None

    def donor_score(row):
        score = 0
        if current_supplier and row.get("supplier") != current_supplier:
            score += 3
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


def upsert_tmdb_film(details, credits):
    directors = [p["name"] for p in credits.get("crew", []) if p.get("job") == "Director"]
    cast = [p["name"] for p in credits.get("cast", [])][:5]
    genres = [g["name"] for g in details.get("genres", [])][:3]

    production_countries = details.get("production_countries", []) or []
    country_of_origin = (
        production_countries[0].get("name")
        if production_countries and production_countries[0].get("name")
        else None
    )

    payload = {
        "title": clean_text(details.get("title")),
        "original_title": clean_text(details.get("original_title")),
        "film_released": clean_text(details.get("release_date")),
        "director": clean_text(directors[0] if directors else None),
        "tmdb_id": details.get("id"),
        "tmdb_title": clean_text(details.get("title")),
        "genres": clean_text(", ".join(genres) if genres else None),
        "top_cast": clean_text(", ".join(cast) if cast else None),
        "country_of_origin": clean_text(country_of_origin),
        "tmdb_poster_path": clean_text(details.get("poster_path")),
        "tmdb_backdrop_path": clean_text(details.get("backdrop_path")),
        "tmdb_vote_average": details.get("vote_average"),
        "tmdb_vote_count": details.get("vote_count"),
        "tmdb_popularity": details.get("popularity"),
        "metadata_source": "tmdb",
    }

    response = (
        supabase.table("films")
        .upsert(payload, on_conflict="tmdb_id")
        .execute()
    )

    if response.data:
        return response.data[0]

    lookup = (
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
        .eq("tmdb_id", details.get("id"))
        .limit(1)
        .execute()
    )

    return lookup.data[0] if lookup.data else None


def clear_bad_link(row_id):
    (
        supabase.table("catalog_items")
        .update({
            "film_id": None,
            "film_link_status": None,
            "film_link_method": None,
            "tmdb_id": None,
            "tmdb_title": None,
            "tmdb_match_status": None,
            "director": None,
            "film_released": None,
            "top_cast": None,
            "genres": None,
            "country_of_origin": None,
            "tmdb_poster_path": None,
            "tmdb_backdrop_path": None,
            "tmdb_vote_average": None,
            "tmdb_vote_count": None,
            "tmdb_popularity": None,
        })
        .eq("id", row_id)
        .execute()
    )


def apply_relink(row_id, metadata):
    (
        supabase.table("catalog_items")
        .update(metadata)
        .eq("id", row_id)
        .execute()
    )


def resolve_link_for_row(row, films_cache):
    title = row.get("title") or ""
    cleaned_title = normalize_title(title)

    # 1. exact normalized title against existing films
    film, method = find_existing_film_by_clean_title(cleaned_title, films_cache)
    if film:
        return build_linked_metadata_from_film(film, method), method

    # 2. TMDb fallback using safe matcher
    source_year = extract_year(row.get("film_released"))
    movie = search_tmdb_movie(title, source_year=source_year)
    if movie:
        details, credits = get_tmdb_details(movie["id"])
        film = upsert_tmdb_film(details, credits)
        if film:
            return build_linked_metadata_from_film(film, "tmdb"), "tmdb"

    # 3. leave unresolved if no safe match
    return {
        "film_id": None,
        "film_link_status": None,
        "film_link_method": None,
        "tmdb_id": None,
        "tmdb_title": None,
        "tmdb_match_status": "not_found",
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
    }, "unresolved"


def main():
    suspicious_rows = fetch_suspicious_rows()[:20]

    if not suspicious_rows:
        print("No suspicious legacy linked rows found.")
        return

    print(f"Found {len(suspicious_rows)} suspicious rows to relink")

    films_cache = fetch_all_films()

    cleared = 0
    relinked_barcode = 0
    relinked_local = 0
    relinked_tmdb = 0
    unresolved = 0

    for i, row in enumerate(suspicious_rows, start=1):
        row_id = row["id"]
        title = row.get("title")

        try:
            clear_bad_link(row_id)
            cleared += 1

            metadata, method = resolve_link_for_row(row, films_cache)

            apply_relink(row_id, metadata)

            if method == "barcode":
                relinked_barcode += 1
                print(f"[{i}/{len(suspicious_rows)}] RELINKED (barcode): {title}")
            elif method == "local_tmdb_title":
                relinked_local += 1
                print(f"[{i}/{len(suspicious_rows)}] RELINKED (local_tmdb_title): {title}")
            elif method == "tmdb":
                relinked_tmdb += 1
                print(f"[{i}/{len(suspicious_rows)}] RELINKED (tmdb): {title}")
            else:
                unresolved += 1
                print(f"[{i}/{len(suspicious_rows)}] UNRESOLVED: {title}")

            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            unresolved += 1
            print(f"[{i}/{len(suspicious_rows)}] ERROR: {title} -> {e}")

    print(
        f"Done. Cleared {cleared}. "
        f"Relinked via barcode: {relinked_barcode}, "
        f"local title: {relinked_local}, "
        f"tmdb: {relinked_tmdb}, "
        f"unresolved: {unresolved}."
    )


if __name__ == "__main__":
    main()
