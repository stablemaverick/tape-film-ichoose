import argparse
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from supabase import create_client

from tmdb_match_helpers import detect_tmdb_search_type, extract_year, normalize_match_title, search_tmdb_movie_safe
from enrich_catalog_with_tmdb_v2 import build_tmdb_update


TMDB_API_URL = "https://api.themoviedb.org/3"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def request_with_backoff(url: str, params: Dict[str, Any], max_retries: int = 5) -> Optional[Dict[str, Any]]:
    delay = 0.5
    for _ in range(max_retries):
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


def fetch_tmdb_details_and_credits(tmdb_api_key: str, tmdb_id: int, media_type: str) -> Optional[tuple[Dict[str, Any], Dict[str, Any]]]:
    endpoint = "tv" if media_type == "tv" else "movie"
    details = request_with_backoff(f"{TMDB_API_URL}/{endpoint}/{tmdb_id}", {"api_key": tmdb_api_key})
    if not details:
        return None
    credits = request_with_backoff(f"{TMDB_API_URL}/{endpoint}/{tmdb_id}/credits", {"api_key": tmdb_api_key})
    if not credits:
        return None
    return details, credits


def fetch_catalog_row(supabase, catalog_id: str) -> Dict[str, Any]:
    resp = (
        supabase.table("catalog_items")
        .select("id,title,director,film_released,media_release_date,barcode,media_type")
        .eq("id", catalog_id)
        .limit(1)
        .execute()
    )
    if not resp.data:
        raise ValueError(f"catalog_items row not found: {catalog_id}")
    return resp.data[0]


@dataclass
class RematchSpec:
    catalog_id: str
    manual_clean_title: Optional[str]
    manual_tmdb_id: Optional[int]
    manual_media_type: Optional[str]
    notes: Optional[str]


def parse_rematch_spec(row: pd.Series) -> Optional[RematchSpec]:
    retry = clean_text(row.get("retry")) or ""
    if retry.lower() not in ("yes", "true", "1"):
        return None

    catalog_id = clean_text(row.get("id"))
    if not catalog_id:
        return None

    manual_clean_title = clean_text(row.get("manual_clean_title"))
    manual_tmdb_id_raw = clean_text(row.get("manual_tmdb_id"))
    manual_tmdb_id = int(manual_tmdb_id_raw) if manual_tmdb_id_raw and manual_tmdb_id_raw.isdigit() else None
    manual_media_type = clean_text(row.get("manual_media_type"))
    notes = clean_text(row.get("notes"))

    # Must provide either a TMDB id or a cleaned title to retry.
    if not manual_tmdb_id and not manual_clean_title:
        return None

    return RematchSpec(
        catalog_id=catalog_id,
        manual_clean_title=manual_clean_title,
        manual_tmdb_id=manual_tmdb_id,
        manual_media_type=manual_media_type,
        notes=notes,
    )


def update_catalog_tmdb_fields(supabase, catalog_id: str, update_data: Dict[str, Any]) -> None:
    supabase.table("catalog_items").update(update_data).eq("id", catalog_id).execute()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Edited not_found_review_*.csv file")
    parser.add_argument("--sleep-ms", type=int, default=350)
    parser.add_argument("--max-items", type=int, default=2000)
    args = parser.parse_args()

    load_dotenv(".env")
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    tmdb_api_key = os.getenv("TMDB_API_KEY")

    if not supabase_url or not supabase_key:
        raise SystemExit("Missing Supabase env")
    if not tmdb_api_key:
        raise SystemExit("Missing TMDB_API_KEY")

    supabase = create_client(supabase_url, supabase_key)

    df = pd.read_csv(args.csv, dtype=str).fillna("")
    specs: list[RematchSpec] = []
    for _, r in df.iterrows():
        spec = parse_rematch_spec(r)
        if spec:
            specs.append(spec)
        if len(specs) >= args.max_items:
            break

    if not specs:
        print("No rows marked for retry with a manual_clean_title or manual_tmdb_id.")
        return

    ok = 0
    failed = 0

    tmdb_details_cache: dict[tuple[str, int], tuple[Dict[str, Any], Dict[str, Any]]] = {}

    for idx, spec in enumerate(specs, start=1):
        try:
            catalog_row = fetch_catalog_row(supabase, spec.catalog_id)
            title_for_search = spec.manual_clean_title or catalog_row.get("title") or ""
            source_year = extract_year(catalog_row.get("film_released"))

            media_type = (
                spec.manual_media_type
                or catalog_row.get("media_type")
                or detect_tmdb_search_type(title_for_search)
            )
            media_type = media_type if media_type in ("tv", "movie") else detect_tmdb_search_type(title_for_search)

            tmdb_match: Optional[Dict[str, Any]] = None
            details_credits: Optional[tuple[Dict[str, Any], Dict[str, Any]]] = None

            if spec.manual_tmdb_id:
                tmdb_match = {"id": spec.manual_tmdb_id, "title": None, "name": None}
                cache_key = (media_type, spec.manual_tmdb_id)
                if cache_key in tmdb_details_cache:
                    details_credits = tmdb_details_cache[cache_key]
                else:
                    details_credits = fetch_tmdb_details_and_credits(tmdb_api_key, spec.manual_tmdb_id, media_type)
                    if details_credits:
                        tmdb_details_cache[cache_key] = details_credits
            else:
                cleaned = normalize_match_title(title_for_search)
                if not cleaned:
                    raise ValueError("manual_clean_title cleaned to empty")
                tmdb_match = search_tmdb_movie_safe(cleaned, tmdb_api_key, TMDB_API_URL, source_year=source_year)
                if tmdb_match and tmdb_match.get("id"):
                    tmdb_id = int(tmdb_match["id"])
                    cache_key = (media_type, tmdb_id)
                    if cache_key in tmdb_details_cache:
                        details_credits = tmdb_details_cache[cache_key]
                    else:
                        details_credits = fetch_tmdb_details_and_credits(tmdb_api_key, tmdb_id, media_type)
                        if details_credits:
                            tmdb_details_cache[cache_key] = details_credits

            if not tmdb_match or not details_credits:
                update_data = {
                    "tmdb_match_status": "not_found",
                    "tmdb_last_refreshed_at": now_iso(),
                }
                update_catalog_tmdb_fields(supabase, spec.catalog_id, update_data)
                failed += 1
                print(f"[{idx}] {spec.catalog_id} => not_found (manual rematch)")
                time.sleep(args.sleep_ms / 1000.0)
                continue

            details, credits = details_credits

            # Fill title/name so build_tmdb_update can set tmdb_title correctly.
            if media_type == "tv":
                tmdb_match = {**tmdb_match}
                tmdb_match["name"] = details.get("name") or tmdb_match.get("name")
            else:
                tmdb_match = {**tmdb_match}
                tmdb_match["title"] = details.get("title") or tmdb_match.get("title")

            update_data = build_tmdb_update(catalog_row, tmdb_match, details, credits, media_type)
            update_catalog_tmdb_fields(supabase, spec.catalog_id, update_data)
            ok += 1
            print(f"[{idx}] {spec.catalog_id} => matched tmdb_id={update_data.get('tmdb_id')}")

            time.sleep(args.sleep_ms / 1000.0)
        except Exception as e:
            failed += 1
            print(f"[{idx}] ERROR id={spec.catalog_id}: {e}")

    print(f"Maintenance rematch done. ok={ok} failed={failed} total_attempted={len(specs)}")


if __name__ == "__main__":
    main()

