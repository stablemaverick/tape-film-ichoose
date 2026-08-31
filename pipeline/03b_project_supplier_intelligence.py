#!/usr/bin/env python3
"""
Project supplier inventory intelligence after operational catalog upsert.

Non-fatal to the caller when used from pipeline shells (always exit 0 after
printing status). Use --strict to exit non-zero on failed/degraded.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
os.chdir(ROOT)

from dotenv import load_dotenv
from supabase import create_client

from app.services.supplier_intelligence_projection_service import (
    format_projection_status_line,
    project_supplier_intelligence_from_batches,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--moovies-batch", default=None)
    parser.add_argument("--lasgo-batch", default=None)
    parser.add_argument("--pipeline-run-id", default=None)
    parser.add_argument(
        "--pipeline-failed-or-stale",
        action="store_true",
        help="Pass stale/failed guard into dual-write",
    )
    parser.add_argument("--env-file", default=".env")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 on failed/degraded projection status",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Also print full result JSON on stderr",
    )
    args = parser.parse_args(argv)

    load_dotenv(args.env_file, override=True)
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("ERROR: Missing SUPABASE_URL / SUPABASE_SERVICE_KEY", file=sys.stderr)
        print("INVENTORY_INTELLIGENCE_PROJECTION_STATUS=failed enabled=0 offers=0 error=missing_env")
        return 1 if args.strict else 0

    supabase = create_client(url, key)
    result = project_supplier_intelligence_from_batches(
        supabase,
        moovies_batch=args.moovies_batch,
        lasgo_batch=args.lasgo_batch,
        pipeline_run_id=args.pipeline_run_id,
        pipeline_failed_or_stale=bool(args.pipeline_failed_or_stale),
    )
    print(format_projection_status_line(result), flush=True)
    if result.get("stats") is not None:
        print(f"[inventory dual-write] post-catalog projection stats={result['stats']}", flush=True)
    if result.get("error"):
        print(f"WARN: inventory intelligence projection error: {result['error']}", file=sys.stderr)
    if args.json:
        print(json.dumps(result, default=str), file=sys.stderr)

    if args.strict and result.get("status") in {"failed", "degraded"}:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
