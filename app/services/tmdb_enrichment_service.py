"""
TMDB enrichment for catalog_items (daily vs recovery modes).

CLI shim: enrich_catalog_with_tmdb_v2.py
Pipeline: 06_enrich_catalog_with_tmdb_daily.py / recovery -> run_from_argv()
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.exceptions import ReadTimeout, RequestException
from dotenv import load_dotenv
from supabase import create_client

from app.helpers.tmdb_match_helpers import (
    detect_tmdb_search_type,
    extract_year,
    normalize_match_title,
    search_tmdb_movie_safe,
)
from app.helpers.text_helpers import chunked, clean_text


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class EnrichmentPassStats:
    """Per-invocation counts from run_enrich (optional; see return_stats)."""

    processed_rows: int = 0
    processed_barcode_groups: int = 0
    rows_matched: int = 0
    rows_not_found: int = 0
    rows_no_clean_title: int = 0
    rows_other_status: int = 0
    no_barcode_rows_processed: int = 0


def _enrichment_bump_stats(stats: Optional[EnrichmentPassStats], row_count: int, status: Optional[str]) -> None:
    if stats is None or row_count <= 0:
        return
    stats.processed_rows += row_count
    s = status or ""
    if s == "matched":
        stats.rows_matched += row_count
    elif s == "not_found":
        stats.rows_not_found += row_count
    elif s == "no_clean_title":
        stats.rows_no_clean_title += row_count
    else:
        stats.rows_other_status += row_count


CATALOG_SELECT = (
    "id,title,barcode,director,film_released,media_release_date,tmdb_id,tmdb_match_status,"
    "tmdb_last_refreshed_at,media_type"
)


def fetch_rows_needing_enrichment(
    supabase, page_size: int, max_rows: int, *, daily: bool = False
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    while len(rows) < max_rows:
        remaining = max_rows - len(rows)
        take = min(page_size, remaining)
        q = (
            supabase.table("catalog_items")
            .select(CATALOG_SELECT)
            .eq("active", True)
            .is_("tmdb_last_refreshed_at", "null")
        )
        if daily:
            q = q.is_("film_id", "null")
        resp = q.range(offset, offset + take - 1).execute()
        page = resp.data or []
        if not page:
            break
        rows.extend(page)
        if len(page) < take:
            break
        offset += take
    return rows


def group_rows_by_barcode(
    rows: List[Dict[str, Any]],
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    no_barcode: List[Dict[str, Any]] = []
    for row in rows:
        barcode = clean_text(row.get("barcode"))
        if barcode:
            groups.setdefault(barcode, []).append(row)
        else:
            no_barcode.append(row)
    return groups, no_barcode


def request_with_backoff(url: str, params: Dict[str, Any], max_retries: int = 8) -> Optional[Dict[str, Any]]:
    delay = 1.0
    timeout_s = 60
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout_s)
        except (ReadTimeout, RequestException) as exc:
            if attempt == max_retries - 1:
                raise RuntimeError(f"TMDB request failed after retries: {url} ({exc})") from exc
            time.sleep(delay)
            delay = min(delay * 2, 120.0)
            continue
        if resp.status_code == 404:
            return None
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            if attempt == max_retries - 1:
                resp.raise_for_status()
            time.sleep(delay)
            delay = min(delay * 2, 120.0)
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
        directors = [
            c.get("name")
            for c in (credits.get("crew") or [])
            if c.get("job") == "Director" and c.get("name")
        ]
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


def run_enrich(
    *,
    max_rows: int = 3000,
    page_size: int = 500,
    max_groups: int = 1000,
    sleep_ms: int = 250,
    daily: bool = False,
    env_file: str = ".env",
    return_stats: bool = False,
) -> Optional[EnrichmentPassStats]:
    mode_label = "DAILY" if daily else "RECOVERY"
    print(f"Enrichment mode: {mode_label}")

    stats = EnrichmentPassStats() if return_stats else None

    load_dotenv(env_file)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    tmdb_api_key = os.getenv("TMDB_API_KEY")
    tmdb_api_url = "https://api.themoviedb.org/3"

    if not supabase_url or not supabase_key:
        print("Missing Supabase env", file=sys.stderr)
        raise SystemExit(1)
    if not tmdb_api_key:
        print("Missing TMDB_API_KEY", file=sys.stderr)
        raise SystemExit(1)

    supabase = create_client(supabase_url, supabase_key)
    rows = fetch_rows_needing_enrichment(supabase, page_size, max_rows, daily=daily)
    if not rows:
        print("No catalog rows need enrichment.")
        return stats

    barcode_groups, no_barcode_rows = group_rows_by_barcode(rows)

    query_cache: Dict[str, Dict[str, Any] | None] = {}
    details_cache: Dict[str, Tuple[Dict[str, Any], Dict[str, Any]]] = {}

    processed_groups = 0
    processed_rows = 0

    for barcode, grouped in barcode_groups.items():
        if processed_groups >= max_groups:
            break
        rep = grouped[0]
        source_title = clean_text(rep.get("title")) or ""
        source_year = extract_year(rep.get("film_released"))
        cleaned = normalize_match_title(source_title)

        if not cleaned:
            st = "no_clean_title"
            update_rows_by_ids(
                supabase,
                [r["id"] for r in grouped],
                {"tmdb_match_status": st, "tmdb_last_refreshed_at": now_iso()},
            )
            _enrichment_bump_stats(stats, len(grouped), st)
            processed_groups += 1
            processed_rows += len(grouped)
            if stats is not None:
                stats.processed_barcode_groups += 1
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
        if stats is not None:
            stats.processed_barcode_groups += 1
        _enrichment_bump_stats(stats, len(grouped), str(update_data.get("tmdb_match_status") or ""))
        print(
            f"[{processed_groups}] barcode={barcode} rows={len(grouped)} title='{source_title}' "
            f"status={update_data.get('tmdb_match_status')}"
        )
        time.sleep(sleep_ms / 1000.0)

    for idx, row in enumerate(no_barcode_rows, start=1):
        source_title = clean_text(row.get("title")) or ""
        source_year = extract_year(row.get("film_released"))
        cleaned = normalize_match_title(source_title)
        if not cleaned:
            st = "no_clean_title"
            update_rows_by_ids(
                supabase,
                [row["id"]],
                {"tmdb_match_status": st, "tmdb_last_refreshed_at": now_iso()},
            )
            _enrichment_bump_stats(stats, 1, st)
            processed_rows += 1
            if stats is not None:
                stats.no_barcode_rows_processed += 1
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
        if stats is not None:
            stats.no_barcode_rows_processed += 1
        _enrichment_bump_stats(stats, 1, str(update_data.get("tmdb_match_status") or ""))
        print(
            f"[no-barcode {idx}/{len(no_barcode_rows)}] title='{source_title}' "
            f"status={update_data.get('tmdb_match_status')}"
        )
        time.sleep(sleep_ms / 1000.0)

    print(f"Done. Processed rows={processed_rows}, barcode_groups={processed_groups}")
    return stats


def run_from_argv(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-rows", type=int, default=3000)
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-groups", type=int, default=1000)
    parser.add_argument("--sleep-ms", type=int, default=250)
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Daily mode: only enrich rows where film_id IS NULL (new, unlinked rows).",
    )
    args = parser.parse_args(argv)
    try:
        run_enrich(
            max_rows=args.max_rows,
            page_size=args.page_size,
            max_groups=args.max_groups,
            sleep_ms=args.sleep_ms,
            daily=args.daily,
        )
    except SystemExit as e:
        code = e.code
        return int(code) if isinstance(code, int) else 1
    return 0


def main() -> None:
    raise SystemExit(run_from_argv())
