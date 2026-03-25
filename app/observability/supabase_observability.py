"""
Persist pipeline run + catalog health snapshot to Supabase (Render-friendly).

Tables: pipeline_runs, catalog_health_snapshots (snapshot.pipeline_run_id → pipeline_runs.id)
See: supabase/migrations/20260323120000_pipeline_observability.sql
  and 20260326120000_catalog_health_snapshot_pipeline_run_fk.sql

postgrest-py: ``insert()`` returns SyncQueryRequestBuilder (execute-only).
Do not chain ``.select()`` after ``insert()`` — use default Prefer: return=representation.
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Optional

from postgrest.types import ReturnMethod


def _strip_none(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def _row_id_from_insert_response(data: Any) -> Optional[str]:
    if isinstance(data, list) and data:
        row = data[0]
        if isinstance(row, dict) and row.get("id") is not None:
            return str(row["id"])
    if isinstance(data, dict) and data.get("id") is not None:
        return str(data["id"])
    return None


_MAX_REPRESENTATION_LOG = 6000


def _format_insert_result(table: str, resp: Any) -> str:
    """Human-readable summary for logging (no secrets)."""
    count = getattr(resp, "count", None)
    data = getattr(resp, "data", None)
    n = len(data) if isinstance(data, list) else (1 if isinstance(data, dict) else 0)
    rid = _row_id_from_insert_response(data)
    payload = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else None
    try:
        payload_json = json.dumps(payload, default=str) if payload is not None else "null"
    except (TypeError, ValueError):
        payload_json = repr(payload)
    if len(payload_json) > _MAX_REPRESENTATION_LOG:
        payload_json = payload_json[:_MAX_REPRESENTATION_LOG] + "…(truncated)"
    return (
        f"table={table} rows={n} count_header={count!r} id={rid!r} "
        f"representation={payload_json}"
    )


def persist_pipeline_observability_safe(
    supabase: Any,
    record: Dict[str, Any],
    metrics: Dict[str, Any],
) -> Optional[str]:
    """
    Insert one pipeline_runs row and one catalog_health_snapshots row.
    Returns pipeline_runs.id (uuid str) on full success, else None.
    """
    try:
        pr_row = _strip_none(
            {
                "pipeline_type": record.get("pipeline_type"),
                "log_file": record.get("log_file"),
                "started_at": record.get("started_at"),
                "ended_at": record.get("ended_at"),
                "duration_seconds": record.get("duration_seconds"),
                "completed": record.get("completed"),
                "inserts": record.get("inserts"),
                "updates": record.get("updates"),
                "failures": record.get("failures"),
                "lock_encountered": record.get("lock_encountered"),
                "health_exit_code": record.get("health_exit_code"),
                "tmdb_matched_pct": record.get("tmdb_matched_pct"),
                "film_linked_pct": record.get("film_linked_pct"),
                "catalog_rows_active": record.get("catalog_rows_active"),
                "missing_sale_price_pct": record.get("missing_sale_price_pct"),
                "null_barcode_rows": record.get("null_barcode_rows"),
                "duplicate_films": record.get("duplicate_films"),
                "recorded_at": record.get("timestamp"),
            }
        )

        pr_resp = (
            supabase.table("pipeline_runs")
            .insert(pr_row, returning=ReturnMethod.representation)
            .execute()
        )
        print(
            f"persist_pipeline_observability_safe: insert OK {_format_insert_result('pipeline_runs', pr_resp)}",
            file=sys.stderr,
        )
        rid = _row_id_from_insert_response(pr_resp.data)
        if not rid:
            print(
                "persist_pipeline_observability_safe: pipeline_runs insert returned no id; "
                "skipping catalog_health_snapshots",
                file=sys.stderr,
            )
            return None

        metrics_blob: Dict[str, Any] = dict(metrics)
        alerts: List[Any] = list(metrics_blob.pop("alerts", []) or [])

        snap = _strip_none(
            {
                "generated_at": metrics.get("generated_at"),
                "exit_code": metrics.get("exit_code"),
                "metrics": metrics_blob,
                "alerts": alerts,
            }
        )
        snap["pipeline_run_id"] = rid
        snap_resp = (
            supabase.table("catalog_health_snapshots")
            .insert(snap, returning=ReturnMethod.representation)
            .execute()
        )
        print(
            f"persist_pipeline_observability_safe: insert OK {_format_insert_result('catalog_health_snapshots', snap_resp)}",
            file=sys.stderr,
        )

        return rid
    except Exception as exc:
        print(f"persist_pipeline_observability_safe: {exc}", file=sys.stderr)
        return None
