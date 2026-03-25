import os
import time
import requests
from dotenv import load_dotenv
from supabase import create_client

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


def clean_date(value):
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None

from datetime import datetime, timezone

def now_iso():
    return datetime.now(timezone.utc).isoformat()


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


def drop_none_values(data):
    return {k: v for k, v in data.items() if v is not None}


def fetch_rows_to_refresh():
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
            media_type,
            genres,
            top_cast,
            country_of_origin,
            tmdb_poster_path,
            tmdb_backdrop_path,
            tmdb_vote_average,
            tmdb_vote_count,
            tmdb_popularity,
            tmdb_last_refreshed_at
        """)
        .eq("active", True)
        .eq("media_type", "film")
        .eq("tmdb_match_status", "matched")
        .not_.is_("tmdb_id", "null")
        .order("tmdb_last_refreshed_at", desc=False, nullsfirst=True)
        .limit(BATCH_SIZE)
        .execute()
    )
    return response.data or []

def group_rows_by_tmdb_id(rows):
    groups = {}
    no_tmdb_rows = []

    for row in rows:
        tmdb_id = row.get("tmdb_id")

        if tmdb_id:
            groups.setdefault(str(tmdb_id), []).append(row)
        else:
            no_tmdb_rows.append(row)

    return groups, no_tmdb_rows


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


def build_refresh_payload(row, details, credits):
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
        "director": row.get("director") or (directors[0] if directors else None),
        "film_released": details.get("release_date"),
        "tmdb_title": details.get("title") or row.get("tmdb_title"),
        "tmdb_match_status": "matched",
        "top_cast": ", ".join(cast) if cast else None,
        "genres": ", ".join(genres) if genres else None,
        "country_of_origin": country_of_origin,
        "tmdb_poster_path": details.get("poster_path"),
        "tmdb_backdrop_path": details.get("backdrop_path"),
        "tmdb_vote_average": details.get("vote_average"),
        "tmdb_vote_count": details.get("vote_count"),
        "tmdb_popularity": details.get("popularity"),
        "tmdb_last_refreshed_at": now_iso(),
    }

    payload = clean_update_data(payload)
    payload = drop_none_values(payload)
    return payload


def update_rows_by_ids(row_ids, update_data):
    if not row_ids:
        return

    (
        supabase.table("catalog_items")
        .update(update_data)
        .in_("id", row_ids)
        .execute()
    )


def main():
    rows = fetch_rows_to_refresh()

    if not rows:
        print("No matched film rows need refresh.")
        return

    print(f"Fetched {len(rows)} matched film row(s) to refresh")

    never_refreshed = sum(1 for r in rows if not r.get("tmdb_last_refreshed_at"))
    print(f"Rows never refreshed before in this batch: {never_refreshed}")

    tmdb_groups, no_tmdb_rows = group_rows_by_tmdb_id(rows)

    processed_groups = 0
    processed_rows = 0

    # Refresh grouped rows once per TMDb ID
    for tmdb_id, grouped_rows in tmdb_groups.items():
        representative = grouped_rows[0]

        try:
            details, credits = get_tmdb_details(tmdb_id)
            update_data = build_refresh_payload(representative, details, credits)

            row_ids = [r["id"] for r in grouped_rows]
            update_rows_by_ids(row_ids, update_data)

            processed_groups += 1
            processed_rows += len(grouped_rows)

            print(
                f"[tmdb group {processed_groups}] "
                f"Updated {len(grouped_rows)} row(s) for tmdb_id {tmdb_id}: "
                f"{representative.get('title')}"
            )

            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print(
                f"Error refreshing tmdb_id {tmdb_id} "
                f"for title '{representative.get('title')}': {e}"
            )

    # These should normally be zero because we filtered for tmdb_id not null
    if no_tmdb_rows:
        print(f"Skipped {len(no_tmdb_rows)} row(s) with no tmdb_id")

    print(f"Done. Refreshed {processed_rows} catalog row(s) in this batch.")


if __name__ == "__main__":
    main()
