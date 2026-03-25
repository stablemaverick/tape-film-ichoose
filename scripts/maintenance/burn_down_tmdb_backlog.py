#!/usr/bin/env python3
"""
Burn down the TMDB enrichment backlog without running catalog import / normalize / harmonize / upsert.

This is for draining the queue after a large catalog sync. It does **not** replace normal
daily catalog_sync / stock_sync jobs — use those for routine operations.

Uses the same matching, TMDB HTTP backoff, and Supabase updates as
``app.services.tmdb_enrichment_service.run_enrich`` (no duplicate TMDB logic).

Default scope matches **daily** enrichment: active rows with ``tmdb_last_refreshed_at`` NULL
and ``film_id`` NULL. Pass ``--include-linked`` to process every never-refreshed active row
(recovery-style), including rows already linked to a film.

Examples:
  ./venv/bin/python scripts/maintenance/burn_down_tmdb_backlog.py --dry-run
  ./venv/bin/python scripts/maintenance/burn_down_tmdb_backlog.py --max-rows 8000 --max-groups 8000
  ./venv/bin/python scripts/maintenance/burn_down_tmdb_backlog.py --loop --pause-seconds 10
  ./venv/bin/python scripts/maintenance/burn_down_tmdb_backlog.py --include-linked --loop
  ./venv/bin/python scripts/maintenance/burn_down_tmdb_backlog.py --loop --run-film-build
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.tmdb_enrichment_service import (  # noqa: E402
    EnrichmentPassStats,
    run_enrich,
)


def count_pending(supabase, *, only_unlinked: bool) -> int:
    q = (
        supabase.table("catalog_items")
        .select("id", count="exact")
        .eq("active", True)
        .is_("tmdb_last_refreshed_at", "null")
    )
    if only_unlinked:
        q = q.is_("film_id", "null")
    return int(q.limit(1).execute().count or 0)


def _fmt_stats(s: EnrichmentPassStats | None) -> str:
    if s is None:
        return "(no stats)"
    return (
        f"rows={s.processed_rows} groups={s.processed_barcode_groups} "
        f"no_barcode_rows={s.no_barcode_rows_processed} | "
        f"matched={s.rows_matched} not_found={s.rows_not_found} "
        f"no_clean_title={s.rows_no_clean_title} other={s.rows_other_status}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Controlled TMDB backlog burner: only updates catalog_items via run_enrich. "
            "Does not run import, normalize, harmonize, or catalog upsert."
        ),
        epilog=(
            "Note: Intended after a large catalog sync to clear TMDB work. "
            "Routine operation should remain catalog_sync / stock_sync on schedule."
        ),
    )
    parser.add_argument("--env", default=str(ROOT / ".env"), help="Dotenv path")
    parser.add_argument("--max-rows", type=int, default=8000, help="Rows fetched per pass")
    parser.add_argument("--max-groups", type=int, default=8000, help="Barcode groups per pass")
    parser.add_argument("--sleep-ms", type=int, default=250, help="Pause between groups (TMDB rate limit)")
    parser.add_argument(
        "--include-linked",
        action="store_true",
        help="Include rows with film_id set (all never-refreshed actives). Default: film_id IS NULL only.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Repeat until pending is zero or a pass touches zero rows while work remained",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=5.0,
        help="Sleep between passes when --loop (default 5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print pending count and configuration only; no TMDB or DB updates",
    )
    parser.add_argument(
        "--run-film-build",
        action="store_true",
        help="After each pass with row updates, run build_films_from_catalog.py (root helper)",
    )
    args = parser.parse_args()

    os.chdir(ROOT)
    load_dotenv(args.env)

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY required", file=sys.stderr)
        return 1

    only_unlinked = not args.include_linked
    daily = only_unlinked

    sb = create_client(url, key)
    pending_start = count_pending(sb, only_unlinked=only_unlinked)

    scope = "film_id IS NULL + never refreshed" if only_unlinked else "all active never refreshed"
    print(f"Scope: {scope}")
    print(f"Pending (eligible): {pending_start}")
    print(f"Pass limits: max_rows={args.max_rows} max_groups={args.max_groups} sleep_ms={args.sleep_ms}")

    if args.dry_run:
        print("Dry-run: no enrichment executed.")
        return 0

    if not os.getenv("TMDB_API_KEY"):
        print("ERROR: TMDB_API_KEY required for enrichment", file=sys.stderr)
        return 1

    films_script = ROOT / "build_films_from_catalog.py"
    py = sys.executable

    total_passes = 0
    total_matched = 0
    total_not_found = 0
    total_no_clean_title = 0
    pending_before_all = pending_start
    pending_after = pending_start
    exit_code = 0

    try:
        while True:
            pending_before_pass = count_pending(sb, only_unlinked=only_unlinked)
            if pending_before_pass == 0:
                print("No pending rows; nothing to do.")
                break

            total_passes += 1
            t0 = time.perf_counter()
            print(f"\n--- pass {total_passes} (pending before: {pending_before_pass}) ---")

            stats = run_enrich(
                max_rows=args.max_rows,
                page_size=500,
                max_groups=args.max_groups,
                sleep_ms=args.sleep_ms,
                daily=daily,
                env_file=args.env,
                return_stats=True,
            )
            elapsed = time.perf_counter() - t0
            pending_after_pass = count_pending(sb, only_unlinked=only_unlinked)

            if stats:
                total_matched += stats.rows_matched
                total_not_found += stats.rows_not_found
                total_no_clean_title += stats.rows_no_clean_title

            print(f"Pass elapsed: {elapsed:.1f}s")
            print(f"Processed: {_fmt_stats(stats)}")
            print(f"Still pending (eligible): {pending_after_pass}")

            pending_after = pending_after_pass

            if args.run_film_build and stats and stats.processed_rows > 0:
                if not films_script.is_file():
                    print(f"ERROR: missing {films_script}", file=sys.stderr)
                    exit_code = 1
                    break
                print("Running build_films_from_catalog.py …")
                proc = subprocess.run([py, str(films_script)], cwd=str(ROOT))
                if proc.returncode != 0:
                    print(f"ERROR: build_films_from_catalog exited {proc.returncode}", file=sys.stderr)
                    exit_code = proc.returncode
                    break

            if not args.loop:
                break

            if pending_after_pass == 0:
                print("Backlog cleared.")
                break

            if stats is None or stats.processed_rows == 0:
                print(
                    "Stopping: pass made no row updates while pending > 0 "
                    "(check filters, limits, or errors above).",
                    file=sys.stderr,
                )
                exit_code = 2
                break

            if args.pause_seconds > 0:
                time.sleep(args.pause_seconds)
    finally:
        print("\n======== SUMMARY ========")
        print(f"Total passes:           {total_passes}")
        print(f"Total rows matched:       {total_matched}")
        print(f"Total rows not_found:     {total_not_found}")
        print(f"Total rows no_clean_title: {total_no_clean_title}")
        print(f"Pending before (start): {pending_before_all}")
        print(f"Pending after (end):    {pending_after}")
        print("=========================")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
