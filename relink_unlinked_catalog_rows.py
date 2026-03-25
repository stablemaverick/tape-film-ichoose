import os
import re
import sys
import time
import requests
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

BATCH_SIZE = 500
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

    text = re.sub(r"\b3d\s*\+\s*2d\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bseason\s+\d+\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bseries\s+\d+\b", "", text, flags=re.IGNORECASE)

    # normalise "&" and "and" consistently
    text = re.sub(r"\s*&\s*", " and ", text, flags=re.IGNORECASE)
    text = re.sub(r"\band\b", " and ", text, flags=re.IGNORECASE)

    text = re.sub(r"\(.*?\)|\[.*?\]", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def build_search_variants(title):
    variants = []
    base = clean_text(title) or ""

    if not base:
        return variants

    variants.append(base)

    v = base

    # common cleanup
    v = re.sub(r"\b4K\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\bUHD\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\bBlu[\s-]?Ray\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\bDVD\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\bUltra HD\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\bLimited Edition\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\bComplete Legacy Collection\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\bCollection\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\bSeason\s+\d+\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\bSeries\s+\d+\b", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\b3D\s*\+\s*2D\b", "", v, flags=re.IGNORECASE)

    # special normalisations
    v2 = v
    v2 = re.sub(r"\bVolume\s+1\b", "Vol. 1", v2, flags=re.IGNORECASE)
    v2 = re.sub(r"\bVolume\s+2\b", "Vol. 2", v2, flags=re.IGNORECASE)
    v2 = re.sub(r"\bFantastic 4\b", "Fantastic Four", v2, flags=re.IGNORECASE)
    v2 = re.sub(r"\bET\b", "E.T.", v2, flags=re.IGNORECASE)
    v2 = re.sub(r"\bMonty Pythons\b", "Monty Python's", v2, flags=re.IGNORECASE)
    v2 = re.sub(r"\bX-Men\s*-\s*", "X-Men: ", v2, flags=re.IGNORECASE)
    v2 = re.sub(r"\s+", " ", v2).strip(" -:")

    if v2 and v2 not in variants:
        variants.append(v2)

    v3 = re.sub(r"[^\w\s:&.'-]", " ", v2)
    v3 = re.sub(r"\s+", " ", v3).strip()
    if v3 and v3 not in variants:
        variants.append(v3)

    return variants

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


def fetch_unlinked_rows():
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
                edition_title,
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
            .is_("film_id", "null")
            .is_("tmdb_id", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )

        rows = response.data or []
        if not rows:
            break

        all_rows.extend(rows)
        offset += page_size
        print(f"Fetched {len(all_rows)} candidate unlinked rows so far...")

    return all_rows


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


def resolve_link_for_row(row, films_cache):
    current_row_id = row.get("id")
    current_supplier = row.get("supplier")
    barcode = clean_text(row.get("barcode"))
    title = row.get("title") or ""
    cleaned_title = normalize_title(title)

    # 1. barcode donor
    matched_catalog_row, film_id, method = find_existing_film_match_by_barcode(
        barcode,
        current_row_id=current_row_id,
        current_supplier=current_supplier,
    )
    if matched_catalog_row:
        return build_linked_metadata_from_catalog_row(matched_catalog_row, method), "barcode"

    # 2. exact normalized title against films
    film, method = find_existing_film_by_clean_title(cleaned_title, films_cache)
    if film:
        return build_linked_metadata_from_film(film, method), "local_tmdb_title"

    # 3. TMDb fallback
    source_year = extract_year(row.get("film_released"))
    movie = search_tmdb_movie(title, source_year=source_year)
    if movie:
        details, credits = get_tmdb_details(movie["id"])
        film = upsert_tmdb_film(details, credits)
        if film:
            return build_linked_metadata_from_film(film, "tmdb"), "tmdb"

    return {}, "unresolved"


def apply_relink(row_id, metadata):
    (
        supabase.table("catalog_items")
        .update(metadata)
        .eq("id", row_id)
        .execute()
    )


def main():
    rows = fetch_unlinked_rows()

    if not rows:
        print("No unlinked film rows found.")
        return

    print(f"Found {len(rows)} unlinked film rows to process")

    films_cache = fetch_all_films()

    relinked_barcode = 0
    relinked_local = 0
    relinked_tmdb = 0
    unresolved = 0

    for i, row in enumerate(rows, start=1):
        title = row.get("title")

        try:
            metadata, method = resolve_link_for_row(row, films_cache)

            if metadata:
                apply_relink(row["id"], metadata)

                if method == "barcode":
                    relinked_barcode += 1
                elif method == "local_tmdb_title":
                    relinked_local += 1
                elif method == "tmdb":
                    relinked_tmdb += 1

                print(f"[{i}/{len(rows)}] RELINKED ({method}): {title}")
            else:
                unresolved += 1
                print(f"[{i}/{len(rows)}] UNRESOLVED: {title}")

            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            unresolved += 1
            print(f"[{i}/{len(rows)}] ERROR: {title} -> {e}")

    print(
        f"Done. Relinked via barcode: {relinked_barcode}, "
        f"local title: {relinked_local}, "
        f"tmdb: {relinked_tmdb}, "
        f"unresolved: {unresolved}."
    )


if __name__ == "__main__":
    main()
