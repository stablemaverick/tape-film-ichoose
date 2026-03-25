"""
Append-only JSON history of pipeline runs + post-run catalog snapshot (trendability).
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore

from app.observability.pipeline_history_schema import validate_history_record
from app.observability.pipeline_log_parser import (
    PipelineRun,
    extract_catalog_upsert_counts,
    total_failures,
)

SCHEMA_VERSION = 1
DEFAULT_HISTORY_PATH = "logs/pipeline_run_history.json"


def build_history_record(
    *,
    run: PipelineRun,
    metrics: Dict[str, Any],
    pipeline_type: Optional[str] = None,
) -> Dict[str, Any]:
    ptype = pipeline_type or run.pipeline_type or "unknown"
    inserts, updates = extract_catalog_upsert_counts(run)
    lnk = metrics.get("linkage", {})
    cov = metrics.get("coverage", {})
    com = metrics.get("commercial", {})
    exc = metrics.get("exceptions", {})

    return {
        "timestamp": metrics.get("generated_at"),
        "pipeline_type": ptype,
        "log_file": run.log_file,
        "started_at": run.started_at,
        "ended_at": run.ended_at,
        "duration_seconds": round(run.duration_seconds, 1),
        "completed": run.completed,
        "inserts": inserts,
        "updates": updates,
        "tmdb_matched_pct": lnk.get("tmdb_match_rate_pct"),
        "film_linked_pct": lnk.get("film_link_pct"),
        "catalog_rows_active": cov.get("total_catalog_items_active"),
        "missing_sale_price_pct": com.get("missing_sale_price_pct"),
        "null_barcode_rows": exc.get("null_barcode_rows"),
        "duplicate_films": exc.get("duplicate_films_by_tmdb_id"),
        "failures": total_failures(run),
        "health_exit_code": metrics.get("exit_code"),
        "lock_encountered": run.lock_encountered,
    }


def append_pipeline_run_record(
    record: Dict[str, Any],
    history_path: str,
    *,
    max_runs: Optional[int] = None,
    skip_validation: bool = False,
) -> None:
    """
    Atomically append one run to {"schema_version": 1, "runs": [...]}.
    Uses a lock file next to the history file (fcntl on Unix).
    """
    if not skip_validation:
        errs = validate_history_record(record)
        if errs:
            raise ValueError("invalid pipeline history record: " + "; ".join(errs))

    path = Path(history_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    cap = max_runs if max_runs is not None else int(os.getenv("PIPELINE_HISTORY_MAX_RUNS", "500"))

    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "w", encoding="utf-8") as lockf:
        if fcntl:
            fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)
        try:
            payload: Dict[str, Any] = {"schema_version": SCHEMA_VERSION, "runs": []}
            if path.exists() and path.stat().st_size > 0:
                with open(path, "r", encoding="utf-8") as rf:
                    try:
                        payload = json.load(rf)
                    except json.JSONDecodeError:
                        payload = {"schema_version": SCHEMA_VERSION, "runs": []}
            runs = payload.get("runs")
            if not isinstance(runs, list):
                runs = []
            runs.append(record)
            if len(runs) > cap:
                runs = runs[-cap:]
            payload["runs"] = runs
            payload["schema_version"] = SCHEMA_VERSION

            fd, tmp_name = tempfile.mkstemp(suffix=".json", dir=str(path.parent), text=True)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as wf:
                    json.dump(payload, wf, indent=2)
                os.replace(tmp_name, path)
            except BaseException:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass
                raise
        finally:
            if fcntl:
                fcntl.flock(lockf.fileno(), fcntl.LOCK_UN)
