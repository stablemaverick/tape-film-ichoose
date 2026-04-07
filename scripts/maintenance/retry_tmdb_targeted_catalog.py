#!/usr/bin/env python3
"""
One-off maintenance: TMDB retry for a *narrow* subset of catalog_items.

**Not for scheduled or daily runs.** Use after intentionally requeueing rows (e.g. clearing
``tmdb_last_refreshed_at``). Refuses to scan the whole “null refresh” queue unless
``--allow-wide-query`` is passed.

Uses the same enrichment pipeline as production recovery (``run_enrichment_for_rows``):
``detect_tmdb_search_type(source_title)`` and ``search_tmdb_movie_safe(source_title, …)``
with the raw supplier title string — aligned with promoted TV routing.

Differs from ``enrich_catalog_with_tmdb_v2.py`` / ``run_enrich``:
  - ``run_enrich`` pulls the next N rows from the global “needs enrichment” queue (daily or
    recovery). This script only loads rows matching *your* filters (TV, title patterns, ids,
    barcodes, etc.).

Examples — from repo root, with venv:

  # 1) All active, never-refreshed TV rows (typical “requeued TV-like” batch)
  ./venv/bin/python scripts/maintenance/retry_tmdb_targeted_catalog.py \\
      --media-type tv --limit 500

  # 2) Small sample by row ids (still requires NULL tmdb_last_refreshed_at + active)
  ./venv/bin/python scripts/maintenance/retry_tmdb_targeted_catalog.py \\
      --ids "uuid-a,uuid-b"

  # 3) Dry-run: log what would happen, no Supabase updates
  ./venv/bin/python scripts/maintenance/retry_tmdb_targeted_catalog.py \\
      --media-type tv --limit 20 --dry-run

  # Optional title narrowing (PostgREST ilike; include %% for wildcards if needed)
  ./venv/bin/python scripts/maintenance/retry_tmdb_targeted_catalog.py \\
      --media-type tv --title-contains "Season" --limit 100
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.helpers.text_helpers import chunked  # noqa: E402
from app.services.tmdb_enrichment_service import (  # noqa: E402
    CATALOG_SELECT,
    EnrichmentPassStats,
    run_enrichment_for_rows,
)


def _split_csv(s: str | None) -> list[str]:
    if not s or not str(s).strip():
        return []
    return [p.strip() for p in str(s).split(",") if p.strip()]


def _scoped_select_base(supabase, args: argparse.Namespace):
    q = (
        supabase.table("catalog_items")
        .select(CATALOG_SELECT)
        .eq("active", True)
        .is_("tmdb_last_refreshed_at", "null")
    )
    if args.media_type:
        q = q.eq("media_type", args.media_type)
    if args.title_contains:
        q = q.ilike("title", f"%{args.title_contains}%")
    if args.title_ilike:
        q = q.ilike("title", args.title_ilike)
    return q


def fetch_target_rows(supabase, args: argparse.Namespace) -> list[dict]:
    limit = max(1, int(args.limit))
    page_size = max(1, min(int(args.page_size), limit))

    ids = _split_csv(args.ids)
    barcodes = _split_csv(args.barcodes)

    if ids:
        out: list[dict] = []
        for batch in chunked(ids, 200):
            resp = _scoped_select_base(supabase, args).in_("id", list(batch)).execute()
            out.extend(resp.data or [])
            if len(out) >= limit:
                break
        return out[:limit]

    if barcodes:
        out = []
        for batch in chunked(barcodes, 200):
            resp = _scoped_select_base(supabase, args).in_("barcode", list(batch)).execute()
            out.extend(resp.data or [])
            if len(out) >= limit:
                break
        return out[:limit]

    rows: list[dict] = []
    offset = 0
    while len(rows) < limit:
        take = min(page_size, limit - len(rows))
        resp = (
            _scoped_select_base(supabase, args)
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Targeted TMDB retry for catalog_items (maintenance only). "
            "Same field updates as recovery enrichment; scoped SELECT only."
        ),
    )
    parser.add_argument("--env", default=str(ROOT / ".env"), help="Dotenv path")
    parser.add_argument("--limit", type=int, default=500, help="Max rows to load and process")
    parser.add_argument("--page-size", type=int, default=500, help="Page size for broad queries")
    parser.add_argument("--max-groups", type=int, default=10_000, help="Cap barcode groups")
    parser.add_argument("--sleep-ms", type=int, default=250, help="Delay between TMDB calls")
    parser.add_argument(
        "--media-type",
        metavar="TYPE",
        help="e.g. tv or film — passed to .eq('media_type', …)",
    )
    parser.add_argument(
        "--title-contains",
        help="Substring match on title (SQL ilike %%value%%)",
    )
    parser.add_argument(
        "--title-ilike",
        help="Raw PostgREST ilike pattern on title (you supply %% wildcards)",
    )
    parser.add_argument(
        "--ids",
        help="Comma-separated catalog_items.id (must still be active + null tmdb_last_refreshed_at)",
    )
    parser.add_argument(
        "--barcodes",
        help="Comma-separated barcode values (same guards as --ids)",
    )
    parser.add_argument(
        "--allow-wide-query",
        action="store_true",
        help="Allow query with only active + tmdb_last_refreshed_at IS NULL (no other narrowing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log enrichment path only; do not write to Supabase",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print aggregate matched / not_found counts at the end",
    )
    args = parser.parse_args()

    narrowed = bool(
        args.ids
        or args.barcodes
        or args.media_type
        or args.title_contains
        or args.title_ilike
    )
    if not narrowed and not args.allow_wide_query:
        print(
            "Refusing to run without a scope filter. Pass --media-type, --title-contains, "
            "--title-ilike, --ids, --barcodes, or --allow-wide-query.",
            file=sys.stderr,
        )
        return 2

    load_dotenv(args.env)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    tmdb_api_key = os.getenv("TMDB_API_KEY")
    tmdb_api_url = "https://api.themoviedb.org/3"

    if not supabase_url or not supabase_key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY", file=sys.stderr)
        return 1
    if not tmdb_api_key:
        print("Missing TMDB_API_KEY", file=sys.stderr)
        return 1

    supabase = create_client(supabase_url, supabase_key)
    rows = fetch_target_rows(supabase, args)
    if not rows:
        print("No rows matched filters (check active, tmdb_last_refreshed_at NULL, and scope).")
        return 0

    print(
        f"Loaded {len(rows)} row(s). dry_run={args.dry_run} "
        f"media_type={args.media_type!r} title_contains={args.title_contains!r}"
    )

    stats = EnrichmentPassStats() if args.stats else None
    run_enrichment_for_rows(
        supabase,
        rows,
        tmdb_api_key=tmdb_api_key,
        tmdb_api_url=tmdb_api_url,
        max_groups=args.max_groups,
        sleep_ms=args.sleep_ms,
        stats=stats,
        dry_run=args.dry_run,
        log_style="maintenance",
    )
    if stats is not None:
        print(
            f"Stats: matched={stats.rows_matched} not_found={stats.rows_not_found} "
            f"no_clean_title={stats.rows_no_clean_title} other={stats.rows_other_status} "
            f"rows={stats.processed_rows}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
