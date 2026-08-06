"""
Supplier SKU → release_variant resolution (Phase 3b).

Matching behaviour:
  1. Attempt to resolve to an existing release_variant (barcode / prior resolution).
  2. High confidence → auto_accepted attach.
  3. Uncertain / multi-match → needs_review, leave release_variant_id unset on offer.
  4. No suitable release → optionally create release_variant with publication_status=supplier_only.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.config.inventory_dual_write import (
    InventoryDualWriteFlags,
    normalize_supplier_id,
    supplier_sku_identity,
)
from app.helpers.text_helpers import clean_text

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResolutionResult:
    release_variant_id: Optional[str]
    match_method: str
    match_confidence: float
    review_status: str
    created_release: bool
    notes: str = ""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _find_releases_by_barcode(supabase: Any, barcode: str) -> List[Dict[str, Any]]:
    if not barcode:
        return []
    # Prefer primary_barcode, then variant_identifiers.
    by_primary = (
        supabase.table("release_variants")
        .select("id,primary_barcode,publication_status,active")
        .eq("primary_barcode", barcode)
        .eq("active", True)
        .limit(20)
        .execute()
    )
    rows = list(by_primary.data or [])
    ids = {r["id"] for r in rows}
    ident = (
        supabase.table("variant_identifiers")
        .select("release_variant_id,id_value,conflict_flag,is_valid")
        .eq("id_type", "barcode")
        .eq("id_value", barcode)
        .eq("is_valid", True)
        .limit(50)
        .execute()
    )
    for row in ident.data or []:
        rid = row.get("release_variant_id")
        if rid and rid not in ids:
            ids.add(rid)
            rows.append({"id": rid, "primary_barcode": barcode, "from_identifier": True})
    return rows


def resolve_supplier_offer_to_release(
    supabase: Any,
    *,
    supplier_id: str,
    supplier_sku: str,
    raw_barcode: Optional[str],
    title: Optional[str] = None,
    format_name: Optional[str] = None,
    catalog_item_id: Optional[str] = None,
    film_id: Optional[str] = None,
    flags: InventoryDualWriteFlags,
) -> ResolutionResult:
    """Resolve one supplier offer identity to a release_variant."""
    sid = normalize_supplier_id(supplier_id)
    sku = supplier_sku_identity(supplier_sku=supplier_sku, raw_barcode=raw_barcode)
    if not sku:
        return ResolutionResult(
            release_variant_id=None,
            match_method="unmatched",
            match_confidence=0.0,
            review_status="needs_review",
            created_release=False,
            notes="missing supplier_sku and barcode",
        )

    barcode = clean_text(raw_barcode) or ""

    # Prior active resolution wins if still valid.
    prior = (
        supabase.table("supplier_sku_resolutions")
        .select(
            "id,resolved_release_variant_id,match_method,match_confidence,review_status"
        )
        .eq("supplier_id", sid)
        .eq("supplier_sku", sku)
        .eq("active", True)
        .limit(1)
        .execute()
    )
    prior_rows = prior.data or []
    if prior_rows:
        p = prior_rows[0]
        if p.get("review_status") in {"auto_accepted", "manual"} and p.get(
            "resolved_release_variant_id"
        ):
            return ResolutionResult(
                release_variant_id=p["resolved_release_variant_id"],
                match_method=p.get("match_method") or "prior_resolution",
                match_confidence=float(p.get("match_confidence") or 1.0),
                review_status=p["review_status"],
                created_release=False,
                notes="reused prior resolution",
            )
        if p.get("review_status") == "needs_review":
            return ResolutionResult(
                release_variant_id=None,
                match_method=p.get("match_method") or "prior_needs_review",
                match_confidence=float(p.get("match_confidence") or 0.0),
                review_status="needs_review",
                created_release=False,
                notes="prior resolution still needs review",
            )

    candidates = _find_releases_by_barcode(supabase, barcode) if barcode else []
    if len(candidates) == 1:
        conf = 0.98
        status = (
            "auto_accepted"
            if conf >= flags.auto_accept_min_confidence
            else "needs_review"
        )
        rid = candidates[0]["id"] if status == "auto_accepted" else None
        result = ResolutionResult(
            release_variant_id=rid,
            match_method="barcode_exact",
            match_confidence=conf,
            review_status=status,
            created_release=False,
            notes="single barcode match",
        )
        _upsert_resolution(
            supabase,
            supplier_id=sid,
            supplier_sku=sku,
            raw_barcode=barcode or None,
            result=result,
            resolved_id=candidates[0]["id"] if status == "auto_accepted" else candidates[0]["id"],
            store_resolved=(status == "auto_accepted"),
        )
        return result

    if len(candidates) > 1:
        result = ResolutionResult(
            release_variant_id=None,
            match_method="barcode_ambiguous",
            match_confidence=0.4,
            review_status="needs_review",
            created_release=False,
            notes=f"barcode maps to {len(candidates)} releases",
        )
        _upsert_resolution(
            supabase,
            supplier_id=sid,
            supplier_sku=sku,
            raw_barcode=barcode or None,
            result=result,
            resolved_id=None,
            store_resolved=False,
        )
        # Flag identifier conflicts
        for c in candidates:
            try:
                supabase.table("variant_identifiers").update({"conflict_flag": True}).eq(
                    "release_variant_id", c["id"]
                ).eq("id_type", "barcode").eq("id_value", barcode).execute()
            except Exception:
                logger.exception("failed to flag barcode conflict release=%s", c.get("id"))
        return result

    # No existing release
    if flags.create_supplier_only_releases:
        payload = {
            "film_id": film_id,
            "catalog_item_id": catalog_item_id,
            "primary_barcode": barcode or None,
            "title": clean_text(title),
            "format": clean_text(format_name),
            "publication_status": "supplier_only",
            "active": True,
            "updated_at": _now_iso(),
        }
        inserted = supabase.table("release_variants").insert(payload).execute()
        new_id = (inserted.data or [{}])[0].get("id")
        if new_id and barcode:
            try:
                supabase.table("variant_identifiers").upsert(
                    {
                        "release_variant_id": new_id,
                        "id_type": "barcode",
                        "id_value": barcode,
                        "source": sid,
                        "is_primary": True,
                        "is_valid": True,
                        "conflict_flag": False,
                    },
                    on_conflict="release_variant_id,id_type,id_value",
                ).execute()
            except Exception:
                # Unique index name may differ for on_conflict; insert ignore duplicate.
                try:
                    supabase.table("variant_identifiers").insert(
                        {
                            "release_variant_id": new_id,
                            "id_type": "barcode",
                            "id_value": barcode,
                            "source": sid,
                            "is_primary": True,
                            "is_valid": True,
                            "conflict_flag": False,
                        }
                    ).execute()
                except Exception:
                    pass
        result = ResolutionResult(
            release_variant_id=new_id,
            match_method="created_supplier_only",
            match_confidence=1.0,
            review_status="auto_accepted",
            created_release=True,
            notes="created supplier_only release_variant",
        )
        _upsert_resolution(
            supabase,
            supplier_id=sid,
            supplier_sku=sku,
            raw_barcode=barcode or None,
            result=result,
            resolved_id=new_id,
            store_resolved=True,
        )
        return result

    result = ResolutionResult(
        release_variant_id=None,
        match_method="unmatched",
        match_confidence=0.0,
        review_status="needs_review",
        created_release=False,
        notes="no release and create_supplier_only_releases disabled",
    )
    _upsert_resolution(
        supabase,
        supplier_id=sid,
        supplier_sku=sku,
        raw_barcode=barcode or None,
        result=result,
        resolved_id=None,
        store_resolved=False,
    )
    return result


def _upsert_resolution(
    supabase: Any,
    *,
    supplier_id: str,
    supplier_sku: str,
    raw_barcode: Optional[str],
    result: ResolutionResult,
    resolved_id: Optional[str],
    store_resolved: bool,
) -> None:
    payload = {
        "supplier_id": supplier_id,
        "supplier_sku": supplier_sku,
        "raw_barcode": raw_barcode,
        "resolved_release_variant_id": resolved_id if store_resolved else None,
        "match_method": result.match_method,
        "match_confidence": result.match_confidence,
        "review_status": result.review_status,
        "notes": result.notes,
        "active": True,
        "updated_at": _now_iso(),
    }
    if result.review_status in {"auto_accepted", "manual"}:
        payload["reviewed_at"] = _now_iso()
    try:
        supabase.table("supplier_sku_resolutions").upsert(
            payload, on_conflict="supplier_id,supplier_sku"
        ).execute()
    except Exception:
        # Partial unique index may not work with on_conflict via PostgREST.
        existing = (
            supabase.table("supplier_sku_resolutions")
            .select("id")
            .eq("supplier_id", supplier_id)
            .eq("supplier_sku", supplier_sku)
            .eq("active", True)
            .limit(1)
            .execute()
        )
        rows = existing.data or []
        if rows:
            supabase.table("supplier_sku_resolutions").update(payload).eq(
                "id", rows[0]["id"]
            ).execute()
        else:
            supabase.table("supplier_sku_resolutions").insert(payload).execute()
