"""
Post-catalog supplier inventory-intelligence projection.

Runs after operational catalog upsert succeeds. Flag-gated, idempotent,
non-fatal to the operational pipeline.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from app.config.inventory_dual_write import load_inventory_dual_write_flags
from app.services.supplier_offer_dual_write_service import dual_write_supplier_offers

STAGING_SELECT = (
    "supplier,barcode,shopify_variant_id,supplier_sku,cost_price,calculated_sale_price,"
    "availability_status,supplier_stock_status,active,media_type,source_priority,"
    "media_release_date,format,studio,director,title,harmonized_title,harmonized_format,"
    "harmonized_director,harmonized_studio,import_batch_id"
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_staging_offers_for_batches(
    supabase: Any,
    *,
    batch_ids: Sequence[str],
    page_size: int = 500,
) -> List[Dict[str, Any]]:
    """Load staging_supplier_offers rows for the given import_batch_id values."""
    ids = [b for b in batch_ids if b]
    if not ids:
        return []

    all_rows: List[Dict[str, Any]] = []
    for batch_id in ids:
        offset = 0
        while True:
            q = (
                supabase.table("staging_supplier_offers")
                .select(STAGING_SELECT)
                .eq("import_batch_id", batch_id)
                .range(offset, offset + page_size - 1)
            )
            page = q.execute().data or []
            if not page:
                break
            all_rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
    return all_rows


def project_supplier_intelligence_from_batches(
    supabase: Any,
    *,
    moovies_batch: Optional[str] = None,
    lasgo_batch: Optional[str] = None,
    pipeline_run_id: Optional[str] = None,
    pipeline_failed_or_stale: bool = False,
    source_feed_at: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Project supplier offers into inventory-intelligence tables for completed batches.

    Returns a status dict with keys:
      status: skipped|success|failed|degraded
      enabled, offers_loaded, moovies_batch, lasgo_batch, stats, error, timing_ms
    """
    flags = load_inventory_dual_write_flags()
    result: Dict[str, Any] = {
        "status": "skipped",
        "enabled": flags.supplier_enabled,
        "offers_loaded": 0,
        "moovies_batch": moovies_batch,
        "lasgo_batch": lasgo_batch,
        "pipeline_run_id": pipeline_run_id,
        "stats": None,
        "error": None,
        "timing_ms": {},
    }
    if not flags.supplier_enabled:
        return result

    batch_ids = [b for b in (moovies_batch, lasgo_batch) if b]
    if not batch_ids:
        result["status"] = "skipped"
        result["error"] = "no_batch_ids"
        return result

    t0 = time.perf_counter()
    try:
        rows = fetch_staging_offers_for_batches(supabase, batch_ids=batch_ids)
        result["offers_loaded"] = len(rows)
        result["timing_ms"]["load_staging"] = int((time.perf_counter() - t0) * 1000)

        t1 = time.perf_counter()
        stats = dual_write_supplier_offers(
            supabase,
            rows,
            flags=flags,
            pipeline_failed_or_stale=pipeline_failed_or_stale,
            pipeline_run_id=pipeline_run_id,
            source_feed_at=source_feed_at or _now_iso(),
            pipeline_completed_at=_now_iso(),
        )
        result["timing_ms"]["dual_write"] = int((time.perf_counter() - t1) * 1000)
        result["timing_ms"]["total"] = int((time.perf_counter() - t0) * 1000)
        result["stats"] = stats

        errors = int(stats.get("errors") or 0)
        failed_batches = int(stats.get("failed_batches") or 0)
        if errors or failed_batches:
            result["status"] = "degraded"
        else:
            result["status"] = "success"
        return result
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = str(exc)
        result["timing_ms"]["total"] = int((time.perf_counter() - t0) * 1000)
        return result


def format_projection_status_line(result: Dict[str, Any]) -> str:
    """Machine-parseable status line for pipeline logs."""
    status = result.get("status") or "unknown"
    return (
        f"INVENTORY_INTELLIGENCE_PROJECTION_STATUS={status} "
        f"enabled={1 if result.get('enabled') else 0} "
        f"offers={result.get('offers_loaded') or 0} "
        f"moovies_batch={result.get('moovies_batch') or ''} "
        f"lasgo_batch={result.get('lasgo_batch') or ''} "
        f"error={result.get('error') or ''}"
    )
