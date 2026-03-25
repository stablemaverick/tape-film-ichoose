"""
MAINTENANCE: Clear bad film links from catalog_items.

Purpose:
  Removes incorrect film_id / tmdb_id links from catalog_items rows
  that have been identified as wrongly matched. Resets the row so it
  can be re-enriched and re-linked.

Tables/fields mutated:
  catalog_items: film_id, film_link_status, film_link_method, film_linked_at,
                 tmdb_id, tmdb_title, tmdb_match_status, tmdb_last_refreshed_at

Safe mode: Review output before committing changes
Cron-safe: NO — manual/one-off use only
"""

import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def clear_bad_links_for_overlord():
    response = (
        supabase.table("catalog_items")
        .select("id,title,film_id,tmdb_id,tmdb_title, studio")
        ##update title name##
        .ilike("title", "%mirror%")
        .ilike("studio", "%criterion%")
        .execute()
    )

    rows = response.data or []

    print(f"Found {len(rows)} rows matching 'Overlord'")

    for row in rows:
        print(f"Clearing: {row['title']} (id={row['id']})")

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
                "genres": None,
                "top_cast": None,
                "country_of_origin": None,
                "tmdb_poster_path": None,
                "tmdb_backdrop_path": None,
                "tmdb_vote_average": None,
                "tmdb_vote_count": None,
                "tmdb_popularity": None,
            })
            .eq("id", row["id"])
            .execute()
        )

    print("Done.")


if __name__ == "__main__":
    clear_bad_links_for_overlord()
