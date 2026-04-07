#!/usr/bin/env python3
"""
One-off maintenance: fix bad TMDB matches on ``catalog_items``.

**Not for scheduled runs.** Does not change daily enrichment or catalog sync.

Modes:
  * ``clear`` — null out TMDB match payload written by enrichment (see docstring below);
    sets ``tmdb_match_status`` to ``not_found`` and ``tmdb_last_refreshed_at`` to NULL so the
    row can be picked up again by enrichment. Optionally clears ``film_id``.
  * ``apply`` — force a known TMDB id: fetch details + credits once, then update each target row
    with ``build_tmdb_update`` (same fields as automatic enrichment).

Fields cleared by ``clear`` (aligned with what ``build_tmdb_update`` sets on match):
  ``tmdb_id``, ``tmdb_title``, ``tmdb_poster_path``, ``tmdb_backdrop_path``,
  ``tmdb_vote_average``, ``tmdb_vote_count``, ``tmdb_popularity``,
  ``top_cast``, ``genres``, ``country_of_origin``,
  plus ``tmdb_match_status`` → ``not_found``, ``tmdb_last_refreshed_at`` → NULL.

  ``director`` and ``film_released`` are **not** cleared by default (they may still reflect
  supplier data). Optional ``--clear-director-and-release`` nulls them as well.

``apply`` updates the same columns as a successful enrichment match (including ``director`` /
``film_released`` where ``build_tmdb_update`` sets them).

Examples (repo root, venv):

  # 1) Dry-run clear
  ./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py clear --id <UUID> --dry-run

  # 2) Clear a bad match (single row)
  ./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py clear --id <UUID>

  # 3) Force-apply a movie id
  ./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py apply --id <UUID> \\
      --tmdb-id 27205 --media-type movie

  # 4) Force-apply a TV id
  ./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py apply --id <UUID> \\
      --tmdb-id 1396 --media-type tv

  # 5) Apply to every active row sharing the seed row’s barcode
  ./venv/bin/python scripts/maintenance/correct_catalog_tmdb_match.py apply --id <UUID> \\
      --tmdb-id 1396 --media-type tv --apply-to-barcode-group
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from supabase import create_client

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.helpers.text_helpers import clean_text  # noqa: E402
from app.services.tmdb_enrichment_service import (  # noqa: E402
    build_tmdb_update,
    fetch_tmdb_details_and_credits,
)

CORRECT_SELECT = (
    "id,title,barcode,active,director,film_released,media_release_date,"
    "tmdb_id,tmdb_title,tmdb_match_status,tmdb_last_refreshed_at,media_type,film_id"
)


def _normalize_media_type(raw: str) -> str:
    s = (raw or "").strip().lower()
    if s in ("film", "movie"):
        return "movie"
    if s == "tv":
        return "tv"
    raise ValueError(f"Unsupported media type {raw!r}; use movie or tv.")


def _clear_tmdb_match_payload(
    *, clear_director_and_release: bool, clear_film_id: bool
) -> Dict[str, Any]:
    patch: Dict[str, Any] = {
        "tmdb_id": None,
        "tmdb_title": None,
        "tmdb_poster_path": None,
        "tmdb_backdrop_path": None,
        "tmdb_vote_average": None,
        "tmdb_vote_count": None,
        "tmdb_popularity": None,
        "top_cast": None,
        "genres": None,
        "country_of_origin": None,
        "tmdb_match_status": "not_found",
        "tmdb_last_refreshed_at": None,
    }
    if clear_director_and_release:
        patch["director"] = None
        patch["film_released"] = None
    if clear_film_id:
        patch["film_id"] = None
    return patch


def _synthetic_tmdb_match(details: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": details.get("id"),
        "title": details.get("title"),
        "name": details.get("name"),
    }


def _update_one_row(supabase, row_id: str, patch: Dict[str, Any]) -> None:
    """Same null/empty handling as ``update_rows_by_ids``; raises if all retries fail."""
    safe = dict(patch)
    if safe.get("film_released") == "":
        safe["film_released"] = None
    delay = 0.5
    last_exc: Optional[BaseException] = None
    for _ in range(5):
        try:
            supabase.table("catalog_items").update(safe).eq("id", row_id).execute()
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Supabase update failed after retries (id={row_id})") from last_exc


def _fetch_seed_row(supabase, catalog_id: str) -> Optional[Dict[str, Any]]:
    resp = (
        supabase.table("catalog_items")
        .select(CORRECT_SELECT)
        .eq("id", catalog_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    return rows[0] if rows else None


def _resolve_targets(
    supabase,
    seed: Dict[str, Any],
    *,
    apply_to_barcode_group: bool,
    barcode_check: Optional[str],
) -> List[Dict[str, Any]]:
    seed_bc = clean_text(seed.get("barcode"))
    if barcode_check is not None:
        want = clean_text(barcode_check)
        if want != seed_bc:
            raise ValueError(
                f"--barcode {barcode_check!r} does not match row barcode {seed_bc!r} "
                f"(catalog id={seed.get('id')})."
            )
    if not apply_to_barcode_group:
        return [seed]
    if not seed_bc:
        raise ValueError("Seed row has no barcode; cannot use --apply-to-barcode-group.")
    resp = (
        supabase.table("catalog_items")
        .select(CORRECT_SELECT)
        .eq("barcode", seed_bc)
        .eq("active", True)
        .execute()
    )
    return list(resp.data or [])


def _log_line(
    *,
    row: Dict[str, Any],
    old_tmdb_id: Any,
    new_tmdb_id: Any,
    media_type: str,
    action: str,
    outcome: str,
    detail: str = "",
) -> None:
    extra = f" {detail}" if detail else ""
    print(
        f"id={row.get('id')} title={row.get('title')!r} "
        f"existing_tmdb_id={old_tmdb_id!r} new_tmdb_id={new_tmdb_id!r} "
        f"media_type={media_type} action={action} outcome={outcome}{extra}"
    )


def _run_clear(
    supabase,
    targets: List[Dict[str, Any]],
    *,
    dry_run: bool,
    clear_film_id: bool,
    clear_director_and_release: bool,
) -> int:
    patch = _clear_tmdb_match_payload(
        clear_director_and_release=clear_director_and_release,
        clear_film_id=clear_film_id,
    )
    failures = 0
    for row in targets:
        old_id = row.get("tmdb_id")
        action = "clear_tmdb_match" + ("+clear_film_id" if clear_film_id else "")
        if dry_run:
            _log_line(
                row=row,
                old_tmdb_id=old_id,
                new_tmdb_id=None,
                media_type=str(row.get("media_type") or ""),
                action=action,
                outcome="dry_run",
                detail=f"would_patch_keys={sorted(patch.keys())}",
            )
            continue
        try:
            _update_one_row(supabase, str(row["id"]), patch)
            _log_line(
                row=row,
                old_tmdb_id=old_id,
                new_tmdb_id=None,
                media_type=str(row.get("media_type") or ""),
                action=action,
                outcome="success",
            )
        except Exception as exc:  # noqa: BLE001 — maintenance CLI
            failures += 1
            _log_line(
                row=row,
                old_tmdb_id=old_id,
                new_tmdb_id=None,
                media_type=str(row.get("media_type") or ""),
                action=action,
                outcome="failure",
                detail=str(exc),
            )
    return failures


def _run_apply(
    supabase,
    targets: List[Dict[str, Any]],
    *,
    tmdb_id: int,
    media_type: str,
    tmdb_api_key: str,
    tmdb_api_url: str,
    dry_run: bool,
) -> int:
    details_tuple = fetch_tmdb_details_and_credits(
        tmdb_api_key, tmdb_api_url, tmdb_id, media_type
    )
    if not details_tuple:
        for row in targets:
            _log_line(
                row=row,
                old_tmdb_id=row.get("tmdb_id"),
                new_tmdb_id=tmdb_id,
                media_type=media_type,
                action="apply_tmdb_id",
                outcome="failure",
                detail="TMDB details or credits fetch returned nothing",
            )
        return len(targets)

    details, credits = details_tuple
    synthetic = _synthetic_tmdb_match(details)
    failures = 0
    for row in targets:
        old_id = row.get("tmdb_id")
        update_data = build_tmdb_update(row, synthetic, details, credits, media_type)
        new_id = update_data.get("tmdb_id")
        if dry_run:
            _log_line(
                row=row,
                old_tmdb_id=old_id,
                new_tmdb_id=new_id,
                media_type=media_type,
                action="apply_tmdb_id",
                outcome="dry_run",
                detail=f"would_set_status={update_data.get('tmdb_match_status')!r}",
            )
            continue
        try:
            _update_one_row(supabase, str(row["id"]), update_data)
            _log_line(
                row=row,
                old_tmdb_id=old_id,
                new_tmdb_id=new_id,
                media_type=media_type,
                action="apply_tmdb_id",
                outcome="success",
            )
        except Exception as exc:  # noqa: BLE001
            failures += 1
            _log_line(
                row=row,
                old_tmdb_id=old_id,
                new_tmdb_id=new_id,
                media_type=media_type,
                action="apply_tmdb_id",
                outcome="failure",
                detail=str(exc),
            )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Maintenance: clear or force-apply TMDB data on catalog_items (explicit, id-first).",
    )
    parser.add_argument("--env", default=str(ROOT / ".env"), help="Dotenv path")
    sub = parser.add_subparsers(dest="mode", required=True)

    p_clear = sub.add_parser("clear", help="Clear TMDB match fields; optionally film_id")
    p_clear.add_argument("--id", required=True, help="catalog_items.id (seed row)")
    p_clear.add_argument("--barcode", help="If set, must match the seed row’s barcode")
    p_clear.add_argument(
        "--apply-to-barcode-group",
        action="store_true",
        help="Also clear every other active row with the same barcode as the seed row",
    )
    p_clear.add_argument(
        "--clear-film-id",
        action="store_true",
        help="Set film_id to NULL (full relink)",
    )
    p_clear.add_argument(
        "--clear-director-and-release",
        action="store_true",
        help="Also null director and film_released (destructive)",
    )
    p_clear.add_argument("--dry-run", action="store_true")

    p_apply = sub.add_parser("apply", help="Fetch TMDB by id and write enrichment-shaped row")
    p_apply.add_argument("--id", required=True, help="catalog_items.id (seed row)")
    p_apply.add_argument("--barcode", help="If set, must match the seed row’s barcode")
    p_apply.add_argument(
        "--apply-to-barcode-group",
        action="store_true",
        help="Apply the same TMDB id to every active row with the seed row’s barcode",
    )
    p_apply.add_argument("--tmdb-id", type=int, required=True)
    p_apply.add_argument(
        "--media-type",
        required=True,
        choices=("movie", "film", "tv"),
        help="TMDB API kind: movie (or film) or tv",
    )
    p_apply.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    load_dotenv(args.env)

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    tmdb_api_key = os.getenv("TMDB_API_KEY")
    tmdb_api_url = "https://api.themoviedb.org/3"

    if not supabase_url or not supabase_key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY", file=sys.stderr)
        return 1

    supabase = create_client(supabase_url, supabase_key)
    seed = _fetch_seed_row(supabase, args.id)
    if not seed:
        print(f"No catalog_items row for id={args.id!r}", file=sys.stderr)
        return 1

    try:
        targets = _resolve_targets(
            supabase,
            seed,
            apply_to_barcode_group=args.apply_to_barcode_group,
            barcode_check=getattr(args, "barcode", None),
        )
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 1

    print(
        f"mode={args.mode} targets={len(targets)} seed_id={seed.get('id')!r} "
        f"dry_run={getattr(args, 'dry_run', False)}"
    )

    if args.mode == "clear":
        failures = _run_clear(
            supabase,
            targets,
            dry_run=args.dry_run,
            clear_film_id=args.clear_film_id,
            clear_director_and_release=args.clear_director_and_release,
        )
        return 1 if failures else 0

    if args.mode == "apply":
        if not tmdb_api_key:
            print("Missing TMDB_API_KEY (required for apply)", file=sys.stderr)
            return 1
        try:
            mt = _normalize_media_type(args.media_type)
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 1
        failures = _run_apply(
            supabase,
            targets,
            tmdb_id=args.tmdb_id,
            media_type=mt,
            tmdb_api_key=tmdb_api_key,
            tmdb_api_url=tmdb_api_url,
            dry_run=args.dry_run,
        )
        return 1 if failures else 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
