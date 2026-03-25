#!/usr/bin/env python3
"""
Pipeline Run Report — Health observability for pipeline execution.

Reads pipeline log files and outputs a structured summary of the **latest
invocation** in each file (split on ``Starting CATALOG SYNC`` / ``Starting STOCK SYNC``;
one log file may contain many runs appended through the day),
including duration, rows processed, retries, and failures.

Usage:
  venv/bin/python scripts/observability/pipeline_run_report.py
  venv/bin/python scripts/observability/pipeline_run_report.py --log-dir logs/
  venv/bin/python scripts/observability/pipeline_run_report.py --last 5

Output includes:
  - Run start/end time and total duration
  - Duration per step
  - Rows processed per step (inserts vs updates)
  - Retries triggered per step
  - Failures per step
  - Whether the run completed fully
  - Whether a lock was encountered
  - Supplier file names used
"""

import argparse
import csv
import io
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.observability.pipeline_log_parser import PipelineRun, parse_log_file


def format_report(run: PipelineRun) -> str:
    """Format a PipelineRun into a human-readable report."""
    lines = [
        "=" * 70,
        f"PIPELINE RUN REPORT",
        "=" * 70,
        f"Log file:       {run.log_file}",
        f"Pipeline type:  {run.pipeline_type or 'unknown'}",
        f"Started:        {run.started_at or 'N/A'}",
        f"Ended:          {run.ended_at or 'N/A'}",
        f"Duration:       {run.duration_seconds:.0f}s ({run.duration_seconds / 60:.1f}m)",
        f"Completed:      {'YES' if run.completed else 'NO'}",
        f"Lock hit:       {'YES' if run.lock_encountered else 'no'}",
    ]

    if run.supplier_files:
        lines.append(f"Supplier files: {', '.join(run.supplier_files)}")

    lines.append("")
    lines.append(f"{'Step':<50} {'Rows':>8} {'Insert':>8} {'Update':>8} {'Retry':>6} {'Fail':>6}")
    lines.append("-" * 90)

    total_rows = 0
    total_inserts = 0
    total_updates = 0
    total_retries = 0
    total_failures = 0

    for step in run.steps:
        rows = step.rows_inserted + step.rows_updated or step.rows_processed
        lines.append(
            f"{step.name:<50} {rows:>8} {step.rows_inserted:>8} "
            f"{step.rows_updated:>8} {step.retries:>6} {step.failures:>6}"
        )
        total_rows += rows
        total_inserts += step.rows_inserted
        total_updates += step.rows_updated
        total_retries += step.retries
        total_failures += step.failures

    lines.append("-" * 90)
    lines.append(
        f"{'TOTAL':<50} {total_rows:>8} {total_inserts:>8} "
        f"{total_updates:>8} {total_retries:>6} {total_failures:>6}"
    )
    lines.append("=" * 70)

    status = "HEALTHY" if run.completed and total_failures == 0 else "NEEDS ATTENTION"
    if total_retries > 0:
        status += f" ({total_retries} retries)"
    lines.append(f"Status: {status}")

    return "\n".join(lines)


def run_to_dict(run: PipelineRun) -> Dict[str, Any]:
    """Convert a PipelineRun to a JSON-serialisable dict."""
    total_retries = sum(s.retries for s in run.steps)
    total_failures = sum(s.failures for s in run.steps)
    return {
        "log_file": run.log_file,
        "pipeline_type": run.pipeline_type,
        "started_at": run.started_at,
        "ended_at": run.ended_at,
        "duration_seconds": run.duration_seconds,
        "completed": run.completed,
        "lock_encountered": run.lock_encountered,
        "supplier_files": run.supplier_files,
        "total_retries": total_retries,
        "total_failures": total_failures,
        "steps": [asdict(s) for s in run.steps],
        "exit_code": 0 if run.completed and total_failures == 0 else 1,
    }


def format_json_report(runs: List[PipelineRun]) -> str:
    data = [run_to_dict(r) for r in runs]
    return json.dumps(data if len(data) > 1 else data[0], indent=2, default=str)


def format_csv_report(runs: List[PipelineRun]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "log_file", "pipeline_type", "started_at", "ended_at",
        "duration_seconds", "completed", "total_retries", "total_failures",
        "step_name", "rows_inserted", "rows_updated", "retries", "failures",
    ])
    for run in runs:
        for step in run.steps:
            writer.writerow([
                run.log_file, run.pipeline_type, run.started_at, run.ended_at,
                run.duration_seconds, run.completed,
                sum(s.retries for s in run.steps),
                sum(s.failures for s in run.steps),
                step.name, step.rows_inserted, step.rows_updated,
                step.retries, step.failures,
            ])
    return buf.getvalue()


def find_latest_logs(log_dir: str, count: int = 1) -> List[str]:
    """Find the most recent log files in a directory."""
    log_path = Path(log_dir)
    if not log_path.exists():
        return []
    logs = sorted(log_path.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    return [str(p) for p in logs[:count]]


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline run health report.")
    parser.add_argument("--log-dir", default="logs/", help="Directory containing pipeline logs")
    parser.add_argument("--log-file", default=None, help="Specific log file to analyse")
    parser.add_argument("--last", type=int, default=1, help="Number of recent runs to report")
    parser.add_argument(
        "--format", choices=["text", "json", "csv"], default="text",
        help="Output format (default: text)",
    )
    parser.add_argument("--output", default=None, help="Write output to file instead of stdout")
    args = parser.parse_args()

    if args.log_file:
        files = [args.log_file]
    else:
        files = find_latest_logs(args.log_dir, args.last)

    if not files:
        print(f"No log files found in {args.log_dir}")
        sys.exit(1)

    runs = []
    for filepath in files:
        if not os.path.exists(filepath):
            print(f"Log file not found: {filepath}", file=sys.stderr)
            continue
        runs.append(parse_log_file(filepath))

    if not runs:
        print("No valid log files found.", file=sys.stderr)
        sys.exit(1)

    if args.format == "json":
        output = format_json_report(runs)
    elif args.format == "csv":
        output = format_csv_report(runs)
    else:
        output = "\n\n".join(format_report(r) for r in runs)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"Report written to {args.output}")
    else:
        print(output)

    worst = max((run_to_dict(r)["exit_code"] for r in runs), default=0)
    sys.exit(worst)


if __name__ == "__main__":
    main()
