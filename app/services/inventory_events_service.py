"""
Idempotent inventory_events helpers (Phase 3b).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)


def build_event_dedupe_key(
    event_type: str,
    *,
    release_variant_id: Optional[str] = None,
    supplier_offer_id: Optional[str] = None,
    fingerprint: str,
) -> str:
    scope = release_variant_id or supplier_offer_id or "none"
    return f"evt:{event_type}:{scope}:{fingerprint}"


def emit_inventory_event(
    supabase: Any,
    *,
    event_type: str,
    dedupe_key: str,
    release_variant_id: Optional[str] = None,
    supplier_offer_id: Optional[str] = None,
    observation_id: Optional[str] = None,
    purchase_order_id: Optional[str] = None,
    purchase_order_line_id: Optional[str] = None,
    tape_inventory_level_id: Optional[str] = None,
    before_state: Optional[Mapping[str, Any]] = None,
    after_state: Optional[Mapping[str, Any]] = None,
    pipeline_run_id: Optional[str] = None,
    observed_at: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Insert an inventory event if dedupe_key is new.

    Returns {"inserted": bool, "id": optional id, "skipped_duplicate": bool}.
    """
    payload = {
        "event_type": event_type,
        "release_variant_id": release_variant_id,
        "supplier_offer_id": supplier_offer_id,
        "observation_id": observation_id,
        "purchase_order_id": purchase_order_id,
        "purchase_order_line_id": purchase_order_line_id,
        "tape_inventory_level_id": tape_inventory_level_id,
        "before_state": dict(before_state or {}),
        "after_state": dict(after_state or {}),
        "pipeline_run_id": pipeline_run_id,
        "dedupe_key": dedupe_key,
        "observed_at": observed_at or datetime.now(timezone.utc).isoformat(),
    }
    try:
        existing = (
            supabase.table("inventory_events")
            .select("id")
            .eq("dedupe_key", dedupe_key)
            .limit(1)
            .execute()
        )
        rows = existing.data or []
        if rows:
            return {"inserted": False, "skipped_duplicate": True, "id": rows[0].get("id")}
        result = supabase.table("inventory_events").insert(payload).execute()
        new_id = (result.data or [{}])[0].get("id") if result.data else None
        return {"inserted": True, "skipped_duplicate": False, "id": new_id}
    except Exception as exc:
        # Unique violation on concurrent insert → treat as duplicate.
        msg = str(exc).lower()
        if "duplicate" in msg or "unique" in msg or "23505" in msg:
            return {"inserted": False, "skipped_duplicate": True, "id": None}
        logger.exception("inventory_events insert failed dedupe_key=%s", dedupe_key)
        raise
