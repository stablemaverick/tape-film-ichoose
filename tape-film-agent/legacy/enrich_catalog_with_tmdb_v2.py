# FULL RECOVERY ENRICHMENT SCRIPT
# Intended for one-off rebuilds or major recovery only.
# Not for daily scheduled execution.

import argparse
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from supabase import create_client

from tmdb_match_helpers import detect_tmdb_search_type, extract_year, normalize_match_title, search_tmdb_movie_safe


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def chunked(items: Iterable[Any], size: int) -> Iterable[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


CATALOG_SELECT = (
    "id,title,barcode,director,film_released,media_release_date,tmdb_id,tmdb_match_status,"
    "tmdb_last_refreshed_at,media_type"
)


def fetch_rows_needing_enrichment(supabase, page_size: int, max_rows: int) -> List[Dict[str, Any]]:
    """
    Full enrichment pass for initial matching only.
    Select only rows that have never had a TMDB attempt:
    tmdb_last_refreshed_at IS NULL.
    """
    rows: List[Dict[str, Any]] = []
    offset = 0
    while len(rows) < max_rows:
        remaining = max_rows - len(rows)
        take = min(page_size, remaining)
        resp = (
            supabase.table("catalog_items")
            .select(CATALOG_SELECT)
            .eq("active", True)
            .is_("tmdb_last_refreshed_at", "null")
            .range(offset, offset + take - 1)
            .execute()
        )
        page = resp.data or []
        if not page:
            break
        rows.extend(page)
        if len(page) < take:
            break
        offset += take
    return rows


def group_rows_by_barcode(rows: List[Dict[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    no_barcode: List[Dict[str, Any]] = []
    for row in rows:
        barcode = clean_text(row.get("barcode"))
        if barcode:
            groups.setdefault(barcode, []).append(row)
        else:
            no_barcode.append(row)
    return groups, no_barcode


def request_with_backoff(url: str, params: Dict[str, Any], max_retries: int = 5) -> Optional[Dict[str, Any]]:
    delay = 0.5
    for attempt in range(max_retries):
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 404:
            return None
        if resp.status_code == 429:
            time.sleep(delay)
            delay *= 2
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"TMDB request failed after retries: {url}")


def fetch_tmdb_details_and_credits(
    tmdb_api_key: str, tmdb_api_url: str, tmdb_id: int, media_type: str
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    endpoint = "tv" if media_type == "tv" else "movie"
    details = request_with_backoff(
        f"{tmdb_api_url}/{endpoint}/{tmdb_id}",
        {"api_key": tmdb_api_key},
    )
    if not details:
        return None
    credits = request_with_backoff(
        f"{tmdb_api_url}/{endpoint}/{tmdb_id}/credits",
        {"api_key": tmdb_api_key},
    )
    if not credits:
        return None
    return details, credits


def build_tmdb_update(
    row: Dict[str, Any],
    tmdb_match: Optional[Dict[str, Any]],
    details: Optional[Dict[str, Any]],
    credits: Optional[Dict[str, Any]],
    media_type: str,
) -> Dict[str, Any]:
    if not tmdb_match or not details or not credits:
        return {
            "tmdb_match_status": "not_found",
            "tmdb_last_refreshed_at": now_iso(),
        }

    cast = [p.get("name") for p in (credits.get("cast") or []) if p.get("name")]
    cast = cast[:5]
    genres = [g.get("name") for g in (details.get("genres") or []) if g.get("name")]
    genres = genres[:4]

    if media_type == "tv":
        release_date = details.get("first_air_date")
        tmdb_title = tmdb_match.get("name") or tmdb_match.get("title")
        director = row.get("director")
        countries = details.get("origin_country") or []
        country_of_origin = countries[0] if countries else None
    else:
        release_date = details.get("release_date")
        tmdb_title = tmdb_match.get("title") or tmdb_match.get("name")
        directors = [c.get("name") for c in (credits.get("crew") or []) if c.get("job") == "Director" and c.get("name")]
        director = row.get("director") or (directors[0] if directors else None)
        countries = details.get("production_countries") or []
        country_of_origin = countries[0].get("name") if countries and countries[0].get("name") else None

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
        "tmdb_last_refreshed_at": now_iso(),
    }


def update_rows_by_ids(supabase, row_ids: List[str], update_data: Dict[str, Any]) -> None:
    safe_update = dict(update_data)
    if safe_update.get("film_released") == "":
        safe_update["film_released"] = None
    for batch in chunked(row_ids, 500):
        delay = 0.5
        updated = False
        for _ in range(5):
            try:
                supabase.table("catalog_items").update(safe_update).in_("id", batch).execute()
                updated = True
                break
            except Exception:
                time.sleep(delay)
                delay *= 2
        if not updated:
            print(f"WARN: failed to update batch of {len(batch)} rows after retries")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-rows", type=int, default=3000)
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-groups", type=int, default=1000)
    parser.add_argument("--sleep-ms", type=int, default=250)
    args = parser.parse_args()

    load_dotenv(".env")
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    tmdb_api_key = os.getenv("TMDB_API_KEY")
    tmdb_api_url = "https://api.themoviedb.org/3"

    if not supabase_url or not supabase_key:
        raise SystemExit("Missing Supabase env")
    if not tmdb_api_key:
        raise SystemExit("Missing TMDB_API_KEY")

    supabase = create_client(supabase_url, supabase_key)
    rows = fetch_rows_needing_enrichment(supabase, args.page_size, args.max_rows)
    if not rows:
        print("No catalog rows need enrichment.")
        return

    barcode_groups, no_barcode_rows = group_rows_by_barcode(rows)

    # In-run caches to avoid repeat TMDB calls.
    query_cache: Dict[str, Dict[str, Any] | None] = {}
    details_cache: Dict[str, Tuple[Dict[str, Any], Dict[str, Any]]] = {}

    processed_groups = 0
    processed_rows = 0

    for barcode, grouped in barcode_groups.items():
        if processed_groups >= args.max_groups:
            break
        rep = grouped[0]
        source_title = clean_text(rep.get("title")) or ""
        source_year = extract_year(rep.get("film_released"))
        cleaned = normalize_match_title(source_title)

        if not cleaned:
            update_rows_by_ids(
                supabase,
                [r["id"] for r in grouped],
                {"tmdb_match_status": "no_clean_title", "tmdb_last_refreshed_at": now_iso()},
            )
            processed_groups += 1
            processed_rows += len(grouped)
            continue

        search_type = detect_tmdb_search_type(source_title)
        cache_key = f"{search_type}|{cleaned}|{source_year or ''}"
        tmdb_match = query_cache.get(cache_key)
        if cache_key not in query_cache:
            tmdb_match = search_tmdb_movie_safe(cleaned, tmdb_api_key, tmdb_api_url, source_year=source_year)
            query_cache[cache_key] = tmdb_match

        if tmdb_match:
            det_key = f"{search_type}|{tmdb_match.get('id')}"
            details_tuple = details_cache.get(det_key)
            if not details_tuple:
                details_tuple = fetch_tmdb_details_and_credits(
                    tmdb_api_key, tmdb_api_url, int(tmdb_match["id"]), search_type
                )
                if details_tuple:
                    details_cache[det_key] = details_tuple
            if not details_tuple:
                update_data = build_tmdb_update(rep, None, None, None, search_type)
            else:
                details, credits = details_tuple
                update_data = build_tmdb_update(rep, tmdb_match, details, credits, search_type)
        else:
            update_data = build_tmdb_update(rep, None, None, None, search_type)

        update_rows_by_ids(supabase, [r["id"] for r in grouped], update_data)
        processed_groups += 1
        processed_rows += len(grouped)
        print(
            f"[{processed_groups}] barcode={barcode} rows={len(grouped)} title='{source_title}' status={update_data.get('tmdb_match_status')}"
        )
        time.sleep(args.sleep_ms / 1000.0)

    # barcode-less rows (one by one, still cached by cleaned title)
    for idx, row in enumerate(no_barcode_rows, start=1):
        source_title = clean_text(row.get("title")) or ""
        source_year = extract_year(row.get("film_released"))
        cleaned = normalize_match_title(source_title)
        if not cleaned:
            update_rows_by_ids(
                supabase,
                [row["id"]],
                {"tmdb_match_status": "no_clean_title", "tmdb_last_refreshed_at": now_iso()},
            )
            continue

        search_type = detect_tmdb_search_type(source_title)
        cache_key = f"{search_type}|{cleaned}|{source_year or ''}"
        tmdb_match = query_cache.get(cache_key)
        if cache_key not in query_cache:
            tmdb_match = search_tmdb_movie_safe(cleaned, tmdb_api_key, tmdb_api_url, source_year=source_year)
            query_cache[cache_key] = tmdb_match

        if tmdb_match:
            det_key = f"{search_type}|{tmdb_match.get('id')}"
            details_tuple = details_cache.get(det_key)
            if not details_tuple:
                details_tuple = fetch_tmdb_details_and_credits(
                    tmdb_api_key, tmdb_api_url, int(tmdb_match["id"]), search_type
                )
                if details_tuple:
                    details_cache[det_key] = details_tuple
            if not details_tuple:
                update_data = build_tmdb_update(row, None, None, None, search_type)
            else:
                details, credits = details_tuple
                update_data = build_tmdb_update(row, tmdb_match, details, credits, search_type)
        else:
            update_data = build_tmdb_update(row, None, None, None, search_type)

        update_rows_by_ids(supabase, [row["id"]], update_data)
        processed_rows += 1
        print(
            f"[no-barcode {idx}/{len(no_barcode_rows)}] title='{source_title}' status={update_data.get('tmdb_match_status')}"
        )
        time.sleep(args.sleep_ms / 1000.0)

    print(f"Done. Processed rows={processed_rows}, barcode_groups={processed_groups}")


if __name__ == "__main__":
    main()

