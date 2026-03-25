#!/usr/bin/env python3
"""
Run TMDB enrichment until no catalog_items rows remain with tmdb_last_refreshed_at IS NULL,
then optionally run build_films_from_catalog.py.

Designed for unattended runs (e.g. laptop plugged in + caffeinate).

TMDB-only (no film build):
  ./venv/bin/python scripts/run_enrichment_then_build_films.py --tmdb-only

Recovery mode (default): all active rows never TMDB-attempted, including those already linked to a film.

Env overrides:
  ENRICH_MAX_ROWS      default 50000
  ENRICH_MAX_GROUPS    default 25000
  ENRICH_SLEEP_MS      default 350
  ENRICH_MAX_BATCHES   safety cap on enrichment invocations (default 500)
  LOG_PATH             optional log file (append mode)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


ROOT = Path(__file__).resolve().parents[1]


def log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}"
    print(line, flush=True)
    path = os.getenv("LOG_PATH")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def count_pending(supabase, *, daily: bool = False) -> int:
    q = (
        supabase.table("catalog_items")
        .select("id", count="exact")
        .eq("active", True)
        .is_("tmdb_last_refreshed_at", "null")
    )
    if daily:
        q = q.is_("film_id", "null")
    r = q.execute()
    return int(r.count or 0)


def count_matched_ready(supabase) -> int:
    r = (
        supabase.table("catalog_items")
        .select("id", count="exact")
        .eq("active", True)
        .eq("tmdb_match_status", "matched")
        .not_.is_("tmdb_id", "null")
        .execute()
    )
    return int(r.count or 0)


def count_linked(supabase) -> int:
    r = (
        supabase.table("catalog_items")
        .select("id", count="exact")
        .eq("active", True)
        .not_.is_("film_id", "null")
        .execute()
    )
    return int(r.count or 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Loop TMDB recovery enrichment until queue empty, then optionally build films.",
    )
    parser.add_argument(
        "--tmdb-only",
        action="store_true",
        help="Stop when enrichment queue is empty; do not run build_films_from_catalog.py.",
    )
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Pass --daily to enrichment (only film_id IS NULL); default is full recovery queue.",
    )
    args = parser.parse_args()

    os.chdir(ROOT)
    load_dotenv(ROOT / ".env")

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        log("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")
        return 1

    supabase = create_client(url, key)

    max_rows = int(os.getenv("ENRICH_MAX_ROWS", "50000"))
    max_groups = int(os.getenv("ENRICH_MAX_GROUPS", "25000"))
    sleep_ms = int(os.getenv("ENRICH_SLEEP_MS", "350"))
    max_batches = int(os.getenv("ENRICH_MAX_BATCHES", "500"))

    py = sys.executable
    enrich_script = ROOT / "enrich_catalog_with_tmdb_v2.py"
    films_script = ROOT / "build_films_from_catalog.py"

    if not enrich_script.is_file():
        log(f"ERROR: Missing {enrich_script}")
        return 1
    if not args.tmdb_only and not films_script.is_file():
        log(f"ERROR: Missing {films_script}")
        return 1

    pending = count_pending(supabase, daily=args.daily)
    log(
        f"Start: pending_enrichment={pending} matched_with_tmdb={count_matched_ready(supabase)} "
        f"already_linked={count_linked(supabase)}"
    )

    batch = 0
    while pending > 0 and batch < max_batches:
        batch += 1
        log(
            f"Enrichment batch {batch}/{max_batches} (pending={pending}) "
            f"--max-rows {max_rows} --max-groups {max_groups}"
            + (" --daily" if args.daily else " (recovery)")
        )
        cmd = [
            py,
            str(enrich_script),
            "--max-rows",
            str(max_rows),
            "--max-groups",
            str(max_groups),
            "--sleep-ms",
            str(sleep_ms),
        ]
        if args.daily:
            cmd.append("--daily")
        proc = subprocess.run(cmd, cwd=str(ROOT))
        if proc.returncode != 0:
            log(f"Enrichment exited {proc.returncode}; sleeping 60s before retry")
            time.sleep(60)

        pending = count_pending(supabase, daily=args.daily)
        log(f"After batch: pending_enrichment={pending}")

    if pending > 0:
        log(f"WARN: Stopped after {max_batches} batches with pending_enrichment={pending}")
        log("Skipping build_films_from_catalog until pending is zero.")
        return 2

    log("Enrichment queue empty (no never-attempted rows).")

    if args.tmdb_only:
        log("--tmdb-only: skipping build_films_from_catalog.")
        return 0

    matched = count_matched_ready(supabase)
    films_count = (
        supabase.table("films").select("id", count="exact").execute().count or 0
    )
    log(
        f"Readiness for films build: matched_rows_with_tmdb_id={matched} "
        f"existing_films_rows={films_count}"
    )

    log("Running build_films_from_catalog.py …")
    proc = subprocess.run([py, str(films_script)], cwd=str(ROOT))
    if proc.returncode != 0:
        log(f"ERROR: build_films_from_catalog exited {proc.returncode}")
        return proc.returncode

    linked = count_linked(supabase)
    films_after = (
        supabase.table("films").select("id", count="exact").execute().count or 0
    )
    log(f"Done: catalog_rows_with_film_id={linked} films_total={films_after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
