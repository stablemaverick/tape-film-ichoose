#!/usr/bin/env python3
"""
Append a pipeline run snapshot to logs/pipeline_run_history.json (trendability).

After catalog_sync or stock_sync, records:
  - timestamp, inserts, updates (from "Operational sync complete" / upsert step in the log)
  - tmdb_matched_pct, film_linked_pct (live DB snapshot)
  - failures (from log), completed, health_exit_code, etc.

Usage:
  venv/bin/python scripts/observability/append_pipeline_run_history.py --log-file logs/catalog_sync_20260322.log
  PIPELINE_HISTORY_FILE=logs/custom.json venv/bin/python scripts/observability/append_pipeline_run_history.py

Typically invoked by run_catalog_sync.sh / run_stock_sync.sh (non-fatal on error).
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client

from app.observability.catalog_metrics import gather_metrics
from app.observability.pipeline_log_parser import parse_log_file
from app.observability.pipeline_run_history import (
    DEFAULT_HISTORY_PATH,
    append_pipeline_run_record,
    build_history_record,
)
from app.observability.supabase_observability import persist_pipeline_observability_safe


def find_latest_catalog_or_stock_log(log_dir: str) -> Optional[str]:
    from pathlib import Path

    log_path = Path(log_dir)
    if not log_path.exists():
        return None
    candidates = [
        p
        for p in log_path.glob("*.log")
        if "catalog_sync" in p.name or "stock_sync" in p.name
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(candidates[0])


def main() -> int:
    parser = argparse.ArgumentParser(description="Append one entry to pipeline_run_history.json")
    parser.add_argument("--log-file", default=None, help="Pipeline log to parse (default: latest catalog/stock log)")
    parser.add_argument("--log-dir", default="logs/", help="Used with default log selection")
    parser.add_argument(
        "--history-file",
        default=os.getenv("PIPELINE_HISTORY_FILE", DEFAULT_HISTORY_PATH),
        help="JSON file to update (default: logs/pipeline_run_history.json or PIPELINE_HISTORY_FILE)",
    )
    parser.add_argument(
        "--pipeline-type",
        choices=["catalog_sync", "stock_sync", "auto"],
        default="auto",
        help="Override pipeline_type in record (default: infer from log)",
    )
    parser.add_argument("--env", default=".env", help="Path to env file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print JSON record only; do not write history file",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Cap number of runs kept (default: PIPELINE_HISTORY_MAX_RUNS or 500)",
    )
    args = parser.parse_args()

    log_file = args.log_file or find_latest_catalog_or_stock_log(args.log_dir)
    if not log_file or not os.path.isfile(log_file):
        print("append_pipeline_run_history: no log file; skip.", file=sys.stderr)
        return 0

    load_dotenv(args.env)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_key:
        print("append_pipeline_run_history: missing Supabase env; skip.", file=sys.stderr)
        return 0

    run = parse_log_file(log_file)
    supabase = create_client(supabase_url, supabase_key)
    metrics = gather_metrics(supabase)

    ptype = None if args.pipeline_type == "auto" else args.pipeline_type
    record = build_history_record(run=run, metrics=metrics, pipeline_type=ptype)

    if args.dry_run:
        import json

        print(json.dumps(record, indent=2, default=str))
        return 0

    run_id = persist_pipeline_observability_safe(supabase, record, metrics)
    if run_id:
        print(f"append_pipeline_run_history: persisted observability run id={run_id}", file=sys.stderr)
    else:
        print(
            "append_pipeline_run_history: Supabase observability persist skipped or failed (non-fatal)",
            file=sys.stderr,
        )

    try:
        append_pipeline_run_record(record, args.history_file, max_runs=args.max_runs)
    except ValueError as exc:
        print(f"append_pipeline_run_history: {exc}", file=sys.stderr)
        return 1

    print(f"append_pipeline_run_history: appended run to {args.history_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
