#!/usr/bin/env python3
"""
Summarise the last N entries in logs/pipeline_run_history.json with simple trends.

Metrics: duration_seconds, inserts, updates, tmdb_matched_pct, film_linked_pct,
health_exit_code.

Usage:
  venv/bin/python scripts/observability/pipeline_trend_report.py
  venv/bin/python scripts/observability/pipeline_trend_report.py --last 20 --format json
  venv/bin/python scripts/observability/pipeline_trend_report.py --validate-only

If the history file does not exist yet (before first sync append), prints a short message
and exits 0 (including --validate-only).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.observability.pipeline_history_schema import (  # noqa: E402
    EXPECTED_SCHEMA_VERSION,
    load_and_validate_history_file,
)
from app.observability.pipeline_run_history import DEFAULT_HISTORY_PATH  # noqa: E402


def _num_seq(runs: Sequence[Dict[str, Any]], key: str) -> List[Optional[float]]:
    out: List[Optional[float]] = []
    for r in runs:
        v = r.get(key)
        if v is None:
            out.append(None)
        else:
            out.append(float(v))
    return out


def _trend(vals: Sequence[Optional[float]]) -> Dict[str, Any]:
    present = [(i, v) for i, v in enumerate(vals) if v is not None]
    if len(present) < 1:
        return {"first": None, "last": None, "delta": None, "delta_pct": None, "n": 0}
    first_v = present[0][1]
    last_v = present[-1][1]
    delta = last_v - first_v
    if first_v == 0:
        delta_pct: Optional[float] = None
    else:
        delta_pct = round((delta / first_v) * 100, 1)
    return {
        "first": first_v,
        "last": last_v,
        "delta": round(delta, 2) if isinstance(delta, float) else delta,
        "delta_pct": delta_pct,
        "n": len(present),
    }


def _format_trend_line(label: str, t: Dict[str, Any]) -> str:
    if t["n"] == 0:
        return f"  {label}: (no data in window)"
    first, last = t["first"], t["last"]
    d, dp = t["delta"], t["delta_pct"]
    extra = ""
    if d is not None and dp is not None:
        extra = f"  (Δ {d:+.2f}, {dp:+.1f}% vs first in window)"
    elif d is not None:
        extra = f"  (Δ {d:+.2f})"
    return f"  {label}: {first} → {last}{extra}"


def build_summary(runs: List[Dict[str, Any]]) -> Dict[str, Any]:
    codes = [r.get("health_exit_code") for r in runs if r.get("health_exit_code") is not None]
    worst_code = max(codes) if codes else None
    latest_code = runs[-1].get("health_exit_code") if runs else None

    return {
        "runs_in_window": len(runs),
        "duration_seconds": _trend(_num_seq(runs, "duration_seconds")),
        "inserts": _trend(_num_seq(runs, "inserts")),
        "updates": _trend(_num_seq(runs, "updates")),
        "tmdb_matched_pct": _trend(_num_seq(runs, "tmdb_matched_pct")),
        "film_linked_pct": _trend(_num_seq(runs, "film_linked_pct")),
        "health_exit_code": {
            "latest": latest_code,
            "worst_in_window": worst_code,
            "non_zero_count": sum(1 for c in codes if c != 0),
        },
    }


def format_text_report(
    history_path: str,
    runs: List[Dict[str, Any]],
    summary: Dict[str, Any],
) -> str:
    lines = [
        "=" * 70,
        "PIPELINE TREND REPORT",
        f"File: {history_path}",
        f"Runs in window: {summary['runs_in_window']}",
        "=" * 70,
        "",
        "Trends (oldest → newest in window):",
        _format_trend_line("duration_seconds", summary["duration_seconds"]),
        _format_trend_line("inserts", summary["inserts"]),
        _format_trend_line("updates", summary["updates"]),
        _format_trend_line("tmdb_matched_pct", summary["tmdb_matched_pct"]),
        _format_trend_line("film_linked_pct", summary["film_linked_pct"]),
        "",
        "health_exit_code:",
        f"  latest: {summary['health_exit_code']['latest']}",
        f"  worst in window: {summary['health_exit_code']['worst_in_window']}",
        f"  runs with non-zero code: {summary['health_exit_code']['non_zero_count']}",
        "=" * 70,
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pipeline run history trend summary.")
    parser.add_argument(
        "--history-file",
        default=os.getenv("PIPELINE_HISTORY_FILE", DEFAULT_HISTORY_PATH),
        help="Path to pipeline_run_history.json",
    )
    parser.add_argument("--last", type=int, default=10, help="Number of recent runs to analyse")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate file schema and exit (0=ok, 2=invalid)",
    )
    args = parser.parse_args()

    hist_path = Path(args.history_file)
    if not hist_path.is_file():
        msg = f"No pipeline history file yet: {hist_path}"
        hint = (
            "History is appended after each catalog_sync / stock_sync (step at end of run), "
            "or run: venv/bin/python scripts/observability/append_pipeline_run_history.py"
        )
        if args.format == "json":
            print(
                json.dumps(
                    {
                        "status": "no_history",
                        "history_file": str(hist_path),
                        "message": msg,
                        "hint": hint,
                        "runs": [],
                        "summary": None,
                    },
                    indent=2,
                )
            )
        else:
            print(msg)
            print(hint)
            if args.validate_only:
                print("validate-only: no file to validate — treating as OK.")
        return 0

    data, errors = load_and_validate_history_file(args.history_file)
    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        return 2

    if args.validate_only:
        print(
            f"OK: {args.history_file} schema_version={data.get('schema_version')} "
            f"runs={len(data.get('runs', []))}"
        )
        return 0

    assert data is not None
    all_runs: List[Dict[str, Any]] = data.get("runs") or []
    if args.last < 1:
        print("--last must be >= 1", file=sys.stderr)
        return 2
    window = all_runs[-args.last :]
    summary = build_summary(window)

    if args.format == "json":
        out = {
            "history_file": args.history_file,
            "schema_version": EXPECTED_SCHEMA_VERSION,
            "window_size_requested": args.last,
            "summary": summary,
            "runs": window,
        }
        print(json.dumps(out, indent=2, default=str))
    else:
        print(format_text_report(args.history_file, window, summary))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
