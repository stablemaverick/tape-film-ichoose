import os
import re
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=".env")

from datetime import datetime, timezone, timedelta



SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL:
    raise ValueError("Missing SUPABASE_URL in .env")

if not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_SERVICE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BATCH_SIZE = 2500
RELINK_ALL_MATCHED = True
######CHANGE ABOVE TO FALSE TO PROCESS Only unlinked rows######

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
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def drop_none_values(payload):
    return {k: v for k, v in payload.items() if v is not None}


def fetch_catalog_rows(batch_size=2500):
    sixty_days_ago = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()

    query = (
        supabase.table("catalog_items")
        .select("""
            id,
            title,
            barcode,
            director,
            film_released,
            tmdb_id,
            tmdb_title,
            genres,
            top_cast,
            country_of_origin,
            tmdb_poster_path,
            tmdb_backdrop_path,
            tmdb_vote_average,
            tmdb_vote_count,
            tmdb_popularity,
            film_id,
            film_link_status,
            film_linked_at
        """)
        .eq("active", True)
        .eq("tmdb_match_status", "matched")
    )

    if not RELINK_ALL_MATCHED:
        query = query.is_("film_id", "null")
    else:
        query = query.or_(
            f"film_linked_at.is.null,film_linked_at.lt.{sixty_days_ago}"
        )

    response = query.limit(batch_size).execute()
    return response.data or []

def upsert_tmdb_film_from_row(row):
    tmdb_id = row.get("tmdb_id")
    if not tmdb_id:
        return None

    payload = drop_none_values({
        "title": clean_text(row.get("tmdb_title")) or clean_text(row.get("title")),
        "original_title": None,
        "film_released": row.get("film_released"),
        "director": clean_text(row.get("director")),
        "tmdb_id": tmdb_id,
        "tmdb_title": clean_text(row.get("tmdb_title")),
        "genres": clean_text(row.get("genres")),
        "top_cast": clean_text(row.get("top_cast")),
        "country_of_origin": clean_text(row.get("country_of_origin")),
        "tmdb_poster_path": clean_text(row.get("tmdb_poster_path")),
        "tmdb_backdrop_path": clean_text(row.get("tmdb_backdrop_path")),
        "tmdb_vote_average": row.get("tmdb_vote_average"),
        "tmdb_vote_count": row.get("tmdb_vote_count"),
        "tmdb_popularity": row.get("tmdb_popularity"),
        "metadata_source": "tmdb",
    })

    response = (
        supabase.table("films")
        .upsert(payload, on_conflict="tmdb_id")
        .execute()
    )

    film_row = response.data[0] if response.data else None

    if film_row:
        return film_row["id"]

    lookup = (
        supabase.table("films")
        .select("id")
        .eq("tmdb_id", tmdb_id)
        .limit(1)
        .execute()
    )

    if lookup.data:
        return lookup.data[0]["id"]

    return None


def find_existing_fallback_film(row):
    title = clean_text(row.get("title"))
    if not title:
        return None

    normalized = normalize_title(title)
    if not normalized:
        return None

    response = (
        supabase.table("films")
        .select("id,title,tmdb_id,metadata_source")
        .is_("tmdb_id", "null")
        .limit(1000)
        .execute()
    )

    for film in response.data or []:
        if normalize_title(film.get("title")) == normalized:
            return film["id"]

    return None


def create_fallback_film(row):
    payload = drop_none_values({
        "title": clean_text(row.get("title")),
        "original_title": None,
        "film_released": row.get("film_released"),
        "director": clean_text(row.get("director")),
        "tmdb_id": None,
        "tmdb_title": None,
        "genres": clean_text(row.get("genres")),
        "top_cast": clean_text(row.get("top_cast")),
        "country_of_origin": clean_text(row.get("country_of_origin")),
        "tmdb_poster_path": clean_text(row.get("tmdb_poster_path")),
        "tmdb_backdrop_path": clean_text(row.get("tmdb_backdrop_path")),
        "tmdb_vote_average": row.get("tmdb_vote_average"),
        "tmdb_vote_count": row.get("tmdb_vote_count"),
        "tmdb_popularity": row.get("tmdb_popularity"),
        "metadata_source": "fallback_catalog",
    })

    response = (
        supabase.table("films")
        .insert(payload)
        .execute()
    )

    film_row = response.data[0] if response.data else None
    return film_row["id"] if film_row else None


def get_or_create_fallback_film_id(row):
    existing_id = find_existing_fallback_film(row)
    if existing_id:
        return existing_id

    return create_fallback_film(row)


def update_catalog_film_id(catalog_id, film_id):
    (
        supabase.table("catalog_items")
        .update({
            "film_id": film_id,
            "film_link_status": "linked",
            "film_linked_at": datetime.now(timezone.utc).isoformat(),
        })
        .eq("id", catalog_id)
        .execute()
    )

def main():
    rows = fetch_catalog_rows()

    if not rows:
        print("No matching catalog rows found.")
        return

    linked = 0
    linked_tmdb = 0
    linked_fallback = 0

    for row in rows:
        try:
            film_id = None

            if row.get("tmdb_id"):
                film_id = upsert_tmdb_film_from_row(row)
                if film_id:
                    linked_tmdb += 1
            else:
                film_id = get_or_create_fallback_film_id(row)
                if film_id:
                    linked_fallback += 1

            if film_id:
                update_catalog_film_id(row["id"], film_id)
                linked += 1
                print(f"Linked catalog row {row['id']} to film {film_id}: {row.get('title')}")
            else:
                print(f"Could not link row: {row.get('title')}")

        except Exception as e:
            print(f"Error processing '{row.get('title')}': {e}")

    print(
        f"Done. Linked {linked} catalog rows "
        f"({linked_tmdb} via TMDb, {linked_fallback} via fallback)."
    )


if __name__ == "__main__":
    main()