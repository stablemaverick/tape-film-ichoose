"""
TMDB enrichment rules and constraints.

Core rule: "Match once, never rematch."
Once tmdb_last_refreshed_at is set (whether matched or not_found),
the row is permanently locked and will never be re-attempted.

Two enrichment modes:
  Daily   (--daily)   -> tmdb_last_refreshed_at IS NULL AND film_id IS NULL
                         Safe for cron. Skips already-linked rows.
  Recovery (default)  -> tmdb_last_refreshed_at IS NULL only
                         For one-off rebuilds or major recovery.

Rate limiting:
  - 350ms between barcode groups (configurable via --sleep-ms)
  - Exponential backoff on 429 / 5xx / timeouts (up to 8 retries, 60s read timeout)
  - In-run caching: same cleaned title + year reuses prior search result
"""

from typing import Any, Dict, Optional


TMDB_FIELDS_WRITTEN = frozenset({
    "director",
    "film_released",
    "tmdb_id",
    "tmdb_title",
    "tmdb_match_status",
    "top_cast",
    "genres",
    "country_of_origin",
    "tmdb_poster_path",
    "tmdb_backdrop_path",
    "tmdb_vote_average",
    "tmdb_vote_count",
    "tmdb_popularity",
    "tmdb_last_refreshed_at",
})

MATCH_STATUSES = frozenset({"matched", "not_found", "no_clean_title"})

DEFAULT_SLEEP_MS = 250
DEFAULT_MAX_ROWS = 3000
DEFAULT_MAX_GROUPS = 1000
DEFAULT_PAGE_SIZE = 500
MAX_RETRIES = 8
READ_TIMEOUT_SECONDS = 60


def is_row_locked(row: Dict[str, Any]) -> bool:
    """A row is locked once tmdb_last_refreshed_at has been set."""
    return row.get("tmdb_last_refreshed_at") is not None


def should_enrich_row(row: Dict[str, Any], *, daily: bool = False) -> bool:
    """
    Determine if a row qualifies for enrichment.
    Both modes: active=true, tmdb_last_refreshed_at IS NULL.
    Daily mode also requires: film_id IS NULL.
    """
    if not row.get("active", True):
        return False
    if is_row_locked(row):
        return False
    if daily and row.get("film_id") is not None:
        return False
    return True


def build_not_found_update(timestamp: str) -> Dict[str, Any]:
    """Stamp a row as searched but not matched."""
    return {
        "tmdb_match_status": "not_found",
        "tmdb_last_refreshed_at": timestamp,
    }


def build_no_clean_title_update(timestamp: str) -> Dict[str, Any]:
    """Stamp a row whose title couldn't produce a usable search query."""
    return {
        "tmdb_match_status": "no_clean_title",
        "tmdb_last_refreshed_at": timestamp,
    }


def build_matched_update(
    tmdb_match: Dict[str, Any],
    details: Dict[str, Any],
    credits: Dict[str, Any],
    media_type: str,
    existing_director: Optional[str],
    timestamp: str,
) -> Dict[str, Any]:
    """Build the full TMDB update payload from API results."""
    cast = [p.get("name") for p in (credits.get("cast") or []) if p.get("name")][:5]
    genres = [g.get("name") for g in (details.get("genres") or []) if g.get("name")][:4]

    if media_type == "tv":
        release_date = details.get("first_air_date")
        tmdb_title = tmdb_match.get("name") or tmdb_match.get("title")
        director = existing_director
        countries = details.get("origin_country") or []
        country_of_origin = countries[0] if countries else None
    else:
        release_date = details.get("release_date")
        tmdb_title = tmdb_match.get("title") or tmdb_match.get("name")
        directors = [
            c.get("name")
            for c in (credits.get("crew") or [])
            if c.get("job") == "Director" and c.get("name")
        ]
        director = existing_director or (directors[0] if directors else None)
        countries = details.get("production_countries") or []
        country_of_origin = (
            countries[0].get("name") if countries and countries[0].get("name") else None
        )

    if release_date == "":
        release_date = None

    return {
        "director": director,
        "film_released": release_date,
        "tmdb_id": tmdb_match.get("id"),
        "tmdb_title": tmdb_title,
        "tmdb_match_status": "matched",
        "top_cast": ", ".join(cast) if cast else None,
        "genres": ", ".join(genres) if genres else None,
        "country_of_origin": country_of_origin,
        "tmdb_poster_path": details.get("poster_path"),
        "tmdb_backdrop_path": details.get("backdrop_path"),
        "tmdb_vote_average": details.get("vote_average"),
        "tmdb_vote_count": details.get("vote_count"),
        "tmdb_popularity": details.get("popularity"),
        "tmdb_last_refreshed_at": timestamp,
    }
