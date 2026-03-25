#!/usr/bin/env python3
"""
Read latest pipeline_runs + catalog_health_snapshots and verify snapshot → run FK.

Use after a catalog/stock sync + append_pipeline_run_history to confirm Supabase
observability writes. Requires SUPABASE_URL and SUPABASE_SERVICE_KEY in .env.

  ./venv/bin/python scripts/observability/verify_pipeline_observability.py
  ./venv/bin/python scripts/observability/verify_pipeline_observability.py --env .env.prod
  ./venv/bin/python scripts/observability/verify_pipeline_observability.py --require-snapshot-link

Exit codes:
  0  OK (both tables have latest rows; stderr note if snapshot.pipeline_run_id is null)
  1  Missing Supabase env, or only one of the two tables has rows
  2  Both tables empty
  3  --require-snapshot-link and latest snapshot.pipeline_run_id is null
  4  snapshot.pipeline_run_id set but no matching pipeline_runs row (broken FK)
"""

from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client


def main() -> int:
    p = argparse.ArgumentParser(description="Show latest pipeline_runs + catalog_health_snapshots rows")
    p.add_argument("--env", default=".env", help="Dotenv path")
    p.add_argument(
        "--require-snapshot-link",
        action="store_true",
        help="Exit non-zero if latest snapshot is missing pipeline_run_id or FK is broken",
    )
    args = p.parse_args()
    load_dotenv(os.path.join(ROOT, args.env) if not os.path.isabs(args.env) else args.env)

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY", file=sys.stderr)
        return 1

    sb = create_client(url, key)
    cols_pr = (
        "id,created_at,pipeline_type,completed,log_file,health_exit_code,"
        "inserts,updates,duration_seconds"
    )
    cols_ch = "id,created_at,generated_at,exit_code,pipeline_run_id"

    pr = (
        sb.table("pipeline_runs")
        .select(cols_pr)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    ch = (
        sb.table("catalog_health_snapshots")
        .select(cols_ch)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )

    pr_rows = pr.data or []
    ch_rows = ch.data or []
    pr0 = pr_rows[0] if pr_rows else None
    ch0 = ch_rows[0] if ch_rows else None

    linked_run = None
    link_ok: bool | None = None
    pairs_latest_run = None

    if ch0 and ch0.get("pipeline_run_id"):
        pid = str(ch0["pipeline_run_id"])
        lk = (
            sb.table("pipeline_runs")
            .select(cols_pr)
            .eq("id", pid)
            .limit(1)
            .execute()
        )
        rows = lk.data or []
        linked_run = rows[0] if rows else None
        link_ok = linked_run is not None
        if pr0 and ch0.get("pipeline_run_id"):
            pairs_latest_run = str(pr0.get("id")) == str(ch0["pipeline_run_id"])
    elif ch0:
        link_ok = False

    out = {
        "pipeline_runs_latest": pr0,
        "catalog_health_snapshots_latest": ch0,
        "snapshot_linked_run": linked_run,
        "snapshot_pipeline_run_id_matches_row": link_ok,
        "latest_snapshot_pairs_latest_run_by_id": pairs_latest_run,
    }
    print(json.dumps(out, indent=2, default=str))

    if not pr_rows or not ch_rows:
        print(
            "\nNote: one or both tables returned no rows (new project or writes not run yet).",
            file=sys.stderr,
        )
        return 2 if not pr_rows and not ch_rows else 1

    if ch0 and ch0.get("pipeline_run_id") and not linked_run:
        print(
            "\nBroken FK: snapshot.pipeline_run_id does not resolve to pipeline_runs.",
            file=sys.stderr,
        )
        return 4

    if args.require_snapshot_link:
        if not ch0.get("pipeline_run_id"):
            print("Latest catalog_health_snapshots.pipeline_run_id is null", file=sys.stderr)
            return 3
        if not linked_run:
            print(
                f"No pipeline_runs row for snapshot.pipeline_run_id={ch0.get('pipeline_run_id')!r}",
                file=sys.stderr,
            )
            return 4

    if ch0 and not ch0.get("pipeline_run_id"):
        print(
            "\nNote: latest snapshot has pipeline_run_id=null (legacy row or run migration + new sync).",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
