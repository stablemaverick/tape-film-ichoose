"""
Dual-write supplier staging/catalog commercial rows into supplier_offers (+ observations).

Never writes tape_inventory_levels or Shopify inventory quantities.

Phase 3b performance path: bounded batches with preloaded lookups and bulk writes
(no one-PostgREST-call-per-row loop).
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from app.config.inventory_dual_write import (
    InventoryDualWriteFlags,
    load_inventory_dual_write_flags,
    normalize_supplier_id,
    supplier_sku_identity,
)
from app.helpers.text_helpers import clean_text, parse_date
from app.rules.availability_rules import (
    build_observation_dedupe_key,
    derive_feed_freshness,
    normalise_supplier_availability,
    observation_material_fingerprint,
)
from app.rules.inventory_invariant_rules import (
    should_emit_inventory_event,
    should_emit_observation,
    validate_stale_feed_no_mass_unavailable,
)
from app.services.inventory_events_service import build_event_dedupe_key
from app.services.supplier_resolution_service import (
    ResolutionResult,
    format_family,
    release_format_compatible,
)

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_future_date(value: Any) -> bool:
    d = parse_date(value) if not hasattr(value, "year") else value
    if d is None:
        return False
    try:
        from datetime import date

        if isinstance(d, date):
            return d > date.today()
    except Exception:
        return False
    return False


def _chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    if size <= 0:
        size = 1
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _event_type_for_change(
    before: Optional[Mapping[str, Any]], after: Mapping[str, Any]
) -> Optional[str]:
    """
    Interpreted change events require a prior supplier-offer state.

    First insert / first observation establishes baseline and must not emit
    supplier_price_changed (or stock delta) events.
    """
    if not before:
        return None

    b_status = (before.get("availability_status") or "").casefold()
    a_status = (after.get("availability_status") or "").casefold()
    b_qty = before.get("reported_quantity")
    a_qty = after.get("reported_quantity")
    b_cost = before.get("unit_cost")
    a_cost = after.get("unit_cost")

    orderable = {"in_stock", "low_stock", "preorder", "backorder"}
    unavail = {"unavailable", "discontinued"}

    if b_status in unavail and a_status in orderable:
        return "supplier_became_available"
    if b_status in orderable and a_status in unavail:
        return "supplier_became_unavailable"
    try:
        if b_qty is not None and a_qty is not None and int(a_qty) > int(b_qty):
            return "supplier_stock_increased"
        if b_qty is not None and a_qty is not None and int(a_qty) < int(b_qty):
            return "supplier_stock_decreased"
    except (TypeError, ValueError):
        pass
    # Price change only when a previous price exists and the new price differs.
    if b_cost is not None and a_cost is not None and b_cost != a_cost:
        return "supplier_price_changed"
    return None


def _select_in(
    supabase: Any,
    table: str,
    columns: str,
    filter_col: str,
    values: Sequence[str],
    *,
    chunk_size: int,
    extra_eq: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    uniq = [v for v in dict.fromkeys(values) if v is not None and str(v) != ""]
    for chunk in _chunked(uniq, chunk_size):
        q = supabase.table(table).select(columns).in_(filter_col, list(chunk))
        if extra_eq:
            for col, val in extra_eq.items():
                q = q.eq(col, val)
        resp = q.execute()
        out.extend(resp.data or [])
    return out


def _bulk_insert(supabase: Any, table: str, rows: List[Dict[str, Any]], *, chunk_size: int = 500) -> List[Dict[str, Any]]:
    inserted: List[Dict[str, Any]] = []
    if not rows:
        return inserted
    for chunk in _chunked(rows, chunk_size):
        resp = supabase.table(table).insert(list(chunk)).execute()
        inserted.extend(resp.data or [])
    return inserted


def _bulk_upsert(
    supabase: Any,
    table: str,
    rows: List[Dict[str, Any]],
    *,
    on_conflict: str,
    chunk_size: int = 500,
) -> List[Dict[str, Any]]:
    upserted: List[Dict[str, Any]] = []
    if not rows:
        return upserted
    for chunk in _chunked(rows, chunk_size):
        resp = (
            supabase.table(table)
            .upsert(list(chunk), on_conflict=on_conflict)
            .execute()
        )
        upserted.extend(resp.data or [])
    return upserted


def dual_write_supplier_offers(
    supabase: Any,
    offer_rows: Iterable[Mapping[str, Any]],
    *,
    flags: Optional[InventoryDualWriteFlags] = None,
    pipeline_failed_or_stale: bool = False,
    pipeline_run_id: Optional[str] = None,
    pipeline_completed_at: Optional[str] = None,
    source_feed_at: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Upsert supplier_offers from normalised staging/catalog-shaped rows.

    Expected row keys (subset): supplier, supplier_sku, barcode, title, format,
    availability_status, supplier_stock_status, cost_price, supplier_currency,
    media_release_date, catalog_item_id, film_id, id (catalog/staging id optional).
    """
    flags = flags or load_inventory_dual_write_flags()
    stats: Dict[str, Any] = {
        "enabled": flags.supplier_enabled,
        "considered": 0,
        "upserted": 0,
        "offers_inserted": 0,
        "offers_updated": 0,
        "observations_inserted": 0,
        "observations_skipped": 0,
        "events_inserted": 0,
        "resolutions_created_releases": 0,
        "releases_reused": 0,
        "needs_review": 0,
        "skipped_identity": 0,
        "blocked_mass_unavailable": False,
        "errors": 0,
        "batches": 0,
        "db_requests": 0,
        "timing_ms": {},
        "batch_results": [],
    }
    if not flags.supplier_enabled:
        return stats

    rows = list(offer_rows)
    stats["considered"] = len(rows)

    proposed_statuses: List[str] = []
    prepared: List[Dict[str, Any]] = []
    for raw in rows:
        supplier_label = clean_text(raw.get("supplier")) or ""
        sid = normalize_supplier_id(supplier_label)
        if sid == "tape_film":
            continue
        barcode = clean_text(raw.get("barcode"))
        sku = supplier_sku_identity(
            supplier_sku=clean_text(raw.get("supplier_sku")),
            raw_barcode=barcode,
        )
        if not sku:
            stats["skipped_identity"] += 1
            continue

        normalised = normalise_supplier_availability(
            raw_status=raw.get("availability_status") or raw.get("raw_status_text"),
            reported_quantity=raw.get("supplier_stock_status")
            if raw.get("reported_quantity") is None
            else raw.get("reported_quantity"),
            release_date_is_future=_is_future_date(
                raw.get("media_release_date") or raw.get("release_date")
            ),
            feed_freshness=None,
        )
        freshness = derive_feed_freshness(
            last_seen_at=_now_iso(),
            source_feed_at=source_feed_at or raw.get("source_feed_at"),
            pipeline_completed_at=pipeline_completed_at or _now_iso(),
            fresh_max_hours=flags.fresh_max_hours,
            aging_max_hours=flags.aging_max_hours,
            pipeline_failed=pipeline_failed_or_stale,
        )
        normalised = normalise_supplier_availability(
            raw_status=normalised.raw_status_text or raw.get("availability_status"),
            reported_quantity=normalised.reported_quantity
            if normalised.quantity_is_exact
            else raw.get("supplier_stock_status"),
            release_date_is_future=_is_future_date(raw.get("media_release_date")),
            feed_freshness=freshness.status,
        )
        proposed_statuses.append(normalised.availability_status)
        prepared.append(
            {
                "raw": raw,
                "supplier_id": sid,
                "supplier_sku": sku,
                "barcode": barcode or "",
                "normalised": normalised,
                "freshness": freshness,
            }
        )

    mass = validate_stale_feed_no_mass_unavailable(
        pipeline_failed_or_stale=pipeline_failed_or_stale,
        proposed_status_updates=proposed_statuses,
    )
    if mass:
        stats["blocked_mass_unavailable"] = True
        logger.error("supplier dual-write blocked: %s", mass[0].message)
        return stats

    completed_at = pipeline_completed_at or _now_iso()
    batch_size = flags.supplier_batch_size
    in_chunk = flags.supplier_in_chunk_size
    t_all = time.perf_counter()

    for batch_idx, batch in enumerate(_chunked(prepared, batch_size)):
        batch_stats = {
            "batch_index": batch_idx,
            "size": len(batch),
            "ok": False,
            "errors": 0,
        }
        try:
            _dual_write_batch(
                supabase,
                list(batch),
                flags=flags,
                pipeline_run_id=pipeline_run_id,
                pipeline_failed_or_stale=pipeline_failed_or_stale,
                source_feed_at=source_feed_at,
                completed_at=completed_at,
                in_chunk=in_chunk,
                stats=stats,
            )
            batch_stats["ok"] = True
            stats["batches"] += 1
        except Exception:
            stats["errors"] += 1
            batch_stats["errors"] = 1
            logger.exception(
                "supplier dual-write batch failed index=%s size=%s",
                batch_idx,
                len(batch),
            )
        stats["batch_results"].append(batch_stats)

    stats["timing_ms"]["total"] = int((time.perf_counter() - t_all) * 1000)
    failed_batches = sum(1 for b in stats["batch_results"] if not b["ok"])
    stats["failed_batches"] = failed_batches
    logger.info("supplier offer dual-write complete: %s", {k: v for k, v in stats.items() if k != "batch_results"})
    return stats


def _dual_write_batch(
    supabase: Any,
    batch: List[Dict[str, Any]],
    *,
    flags: InventoryDualWriteFlags,
    pipeline_run_id: Optional[str],
    pipeline_failed_or_stale: bool,
    source_feed_at: Optional[str],
    completed_at: str,
    in_chunk: int,
    stats: Dict[str, Any],
) -> None:
    def _count_request() -> None:
        stats["db_requests"] = int(stats.get("db_requests") or 0) + 1

    # --- preload existing offers / resolutions / barcode candidates ---
    t0 = time.perf_counter()
    supplier_ids = sorted({item["supplier_id"] for item in batch})
    skus = [item["supplier_sku"] for item in batch]
    barcodes = [item["barcode"] for item in batch if item.get("barcode")]

    existing_offers: Dict[Tuple[str, str], Dict[str, Any]] = {}
    offer_rows = _select_in(
        supabase,
        "supplier_offers",
        "id,supplier_id,supplier_sku,availability_status,reported_quantity,"
        "quantity_is_exact,supplier_can_supply,unit_cost,currency,release_variant_id,created_at",
        "supplier_sku",
        skus,
        chunk_size=in_chunk,
    )
    _count_request()
    for extra in range(max(0, (len(skus) - 1) // in_chunk)):
        _count_request()
    wanted = {(i["supplier_id"], i["supplier_sku"]) for i in batch}
    for row in offer_rows:
        key = (row["supplier_id"], row["supplier_sku"])
        if key in wanted:
            existing_offers[key] = row

    prior_resolutions: Dict[Tuple[str, str], Dict[str, Any]] = {}
    res_rows = _select_in(
        supabase,
        "supplier_sku_resolutions",
        "id,supplier_id,supplier_sku,resolved_release_variant_id,match_method,"
        "match_confidence,review_status,active",
        "supplier_sku",
        skus,
        chunk_size=in_chunk,
        extra_eq={"active": True},
    )
    _count_request()
    for extra in range(max(0, (len(skus) - 1) // in_chunk)):
        _count_request()
    for row in res_rows:
        key = (row["supplier_id"], row["supplier_sku"])
        if key in wanted:
            prior_resolutions[key] = row

    barcode_to_releases: Dict[str, List[Dict[str, Any]]] = {}
    if barcodes:
        primary_rows = _select_in(
            supabase,
            "release_variants",
            "id,primary_barcode,publication_status,active",
            "primary_barcode",
            barcodes,
            chunk_size=in_chunk,
            extra_eq={"active": True},
        )
        _count_request()
        for extra in range(max(0, (len(barcodes) - 1) // in_chunk)):
            _count_request()
        for row in primary_rows:
            bc = row.get("primary_barcode") or ""
            if bc:
                barcode_to_releases.setdefault(bc, []).append(row)

        ident_rows = _select_in(
            supabase,
            "variant_identifiers",
            "release_variant_id,id_value,conflict_flag,is_valid",
            "id_value",
            barcodes,
            chunk_size=in_chunk,
            extra_eq={"id_type": "barcode", "is_valid": True},
        )
        _count_request()
        for extra in range(max(0, (len(barcodes) - 1) // in_chunk)):
            _count_request()
        known_ids = {
            rid
            for lst in barcode_to_releases.values()
            for rid in (r.get("id") for r in lst)
            if rid
        }
        missing_rids = [
            r["release_variant_id"]
            for r in ident_rows
            if r.get("release_variant_id") and r["release_variant_id"] not in known_ids
        ]
        release_by_id: Dict[str, Dict[str, Any]] = {
            r["id"]: r for lst in barcode_to_releases.values() for r in lst if r.get("id")
        }
        if missing_rids:
            fetched = _select_in(
                supabase,
                "release_variants",
                "id,primary_barcode,publication_status,active",
                "id",
                missing_rids,
                chunk_size=in_chunk,
            )
            _count_request()
            for r in fetched:
                release_by_id[r["id"]] = r
        for row in ident_rows:
            bc = row.get("id_value") or ""
            rid = row.get("release_variant_id")
            if not bc or not rid:
                continue
            existing_list = barcode_to_releases.setdefault(bc, [])
            if any(r.get("id") == rid for r in existing_list):
                continue
            existing_list.append(
                release_by_id.get(rid)
                or {"id": rid, "primary_barcode": bc, "from_identifier": True}
            )

    stats["timing_ms"]["preload"] = stats["timing_ms"].get("preload", 0) + int(
        (time.perf_counter() - t0) * 1000
    )

    # --- resolve in memory; stage new releases / resolutions ---
    t1 = time.perf_counter()
    new_releases: List[Dict[str, Any]] = []
    new_identifiers: List[Dict[str, Any]] = []
    resolution_payloads: List[Dict[str, Any]] = []
    # (barcode, format_family) -> provisional release id for creates within this batch
    pending_barcode_release: Dict[Tuple[str, str], Dict[str, Any]] = {}
    resolutions: Dict[Tuple[str, str], ResolutionResult] = {}

    for item in batch:
        key = (item["supplier_id"], item["supplier_sku"])
        prior = prior_resolutions.get(key)
        barcode = item["barcode"]
        raw = item["raw"]
        offer_format = clean_text(raw.get("format") or raw.get("harmonized_format"))
        fmt_key = format_family(offer_format)

        if prior and prior.get("review_status") in {"auto_accepted", "manual"} and prior.get(
            "resolved_release_variant_id"
        ):
            result = ResolutionResult(
                release_variant_id=prior["resolved_release_variant_id"],
                match_method=prior.get("match_method") or "prior_resolution",
                match_confidence=float(prior.get("match_confidence") or 1.0),
                review_status=prior["review_status"],
                created_release=False,
                notes="reused prior resolution",
            )
            resolutions[key] = result
            stats["releases_reused"] += 1
            continue

        if prior and prior.get("review_status") == "needs_review":
            result = ResolutionResult(
                release_variant_id=None,
                match_method=prior.get("match_method") or "prior_needs_review",
                match_confidence=float(prior.get("match_confidence") or 0.0),
                review_status="needs_review",
                created_release=False,
                notes="prior resolution still needs review",
            )
            resolutions[key] = result
            stats["needs_review"] += 1
            continue

        candidates = list(barcode_to_releases.get(barcode, [])) if barcode else []
        # Include pending creates in this batch for same barcode+format family.
        if barcode and (barcode, fmt_key) in pending_barcode_release:
            candidates = candidates + [pending_barcode_release[(barcode, fmt_key)]]

        compatible = [
            c for c in candidates if release_format_compatible(offer_format, c.get("format"))
        ]

        if len(compatible) == 1:
            conf = 0.98
            status = (
                "auto_accepted"
                if conf >= flags.auto_accept_min_confidence
                else "needs_review"
            )
            rid = compatible[0]["id"] if status == "auto_accepted" else None
            result = ResolutionResult(
                release_variant_id=rid,
                match_method="barcode_exact",
                match_confidence=conf,
                review_status=status,
                created_release=False,
                notes="single barcode match (format-compatible)",
            )
            resolutions[key] = result
            if status == "auto_accepted":
                stats["releases_reused"] += 1
            else:
                stats["needs_review"] += 1
            resolution_payloads.append(
                _resolution_payload(
                    supplier_id=item["supplier_id"],
                    supplier_sku=item["supplier_sku"],
                    raw_barcode=barcode or None,
                    result=result,
                    resolved_id=compatible[0]["id"] if status == "auto_accepted" else None,
                    completed_at=completed_at,
                )
            )
            continue

        if len(compatible) > 1:
            result = ResolutionResult(
                release_variant_id=None,
                match_method="barcode_ambiguous",
                match_confidence=0.4,
                review_status="needs_review",
                created_release=False,
                notes=f"barcode maps to {len(compatible)} format-compatible releases",
            )
            resolutions[key] = result
            stats["needs_review"] += 1
            resolution_payloads.append(
                _resolution_payload(
                    supplier_id=item["supplier_id"],
                    supplier_sku=item["supplier_sku"],
                    raw_barcode=barcode or None,
                    result=result,
                    resolved_id=None,
                    completed_at=completed_at,
                )
            )
            continue

        if candidates and not compatible and not flags.create_supplier_only_releases:
            result = ResolutionResult(
                release_variant_id=None,
                match_method="barcode_format_conflict",
                match_confidence=0.2,
                review_status="needs_review",
                created_release=False,
                notes="barcode matches existing release(s) but format conflicts",
            )
            resolutions[key] = result
            stats["needs_review"] += 1
            resolution_payloads.append(
                _resolution_payload(
                    supplier_id=item["supplier_id"],
                    supplier_sku=item["supplier_sku"],
                    raw_barcode=barcode or None,
                    result=result,
                    resolved_id=None,
                    completed_at=completed_at,
                )
            )
            continue

        if flags.create_supplier_only_releases:
            pending_key = (barcode, fmt_key) if barcode else None
            if pending_key and pending_key in pending_barcode_release:
                new_id = pending_barcode_release[pending_key]["id"]
                created = False
            else:
                new_id = str(uuid.uuid4())
                catalog_item_id = (
                    clean_text(raw.get("catalog_item_id") or raw.get("id"))
                    if raw.get("catalog_item_id")
                    else clean_text(raw.get("published_catalog_item_id"))
                )
                new_row = {
                    "id": new_id,
                    "film_id": clean_text(raw.get("film_id")),
                    "catalog_item_id": catalog_item_id,
                    "primary_barcode": barcode or None,
                    "title": clean_text(raw.get("title") or raw.get("harmonized_title")),
                    "format": offer_format,
                    "publication_status": "supplier_only",
                    "active": True,
                    "updated_at": completed_at,
                    "created_at": completed_at,
                }
                new_releases.append(new_row)
                if pending_key:
                    pending_barcode_release[pending_key] = new_row
                if barcode:
                    new_identifiers.append(
                        {
                            "release_variant_id": new_id,
                            "id_type": "barcode",
                            "id_value": barcode,
                            "source": item["supplier_id"],
                            "is_primary": True,
                            "is_valid": True,
                            "conflict_flag": False,
                        }
                    )
                    barcode_to_releases.setdefault(barcode, []).append(new_row)
                created = True
                stats["resolutions_created_releases"] += 1

            notes = "created supplier_only release_variant"
            if candidates and not compatible:
                notes = "created supplier_only release_variant (barcode format conflict)"
            result = ResolutionResult(
                release_variant_id=new_id,
                match_method="created_supplier_only",
                match_confidence=1.0,
                review_status="auto_accepted",
                created_release=created,
                notes=notes,
            )
            resolutions[key] = result
            resolution_payloads.append(
                _resolution_payload(
                    supplier_id=item["supplier_id"],
                    supplier_sku=item["supplier_sku"],
                    raw_barcode=barcode or None,
                    result=result,
                    resolved_id=new_id,
                    completed_at=completed_at,
                )
            )
            continue

        result = ResolutionResult(
            release_variant_id=None,
            match_method="unmatched",
            match_confidence=0.0,
            review_status="needs_review",
            created_release=False,
            notes="no release and create_supplier_only_releases disabled",
        )
        resolutions[key] = result
        stats["needs_review"] += 1
        resolution_payloads.append(
            _resolution_payload(
                supplier_id=item["supplier_id"],
                supplier_sku=item["supplier_sku"],
                raw_barcode=barcode or None,
                result=result,
                resolved_id=None,
                completed_at=completed_at,
            )
        )

    stats["timing_ms"]["resolve"] = stats["timing_ms"].get("resolve", 0) + int(
        (time.perf_counter() - t1) * 1000
    )

    # --- bulk persist releases / identifiers / resolutions ---
    t2 = time.perf_counter()
    if new_releases:
        _bulk_insert(supabase, "release_variants", new_releases, chunk_size=500)
        _count_request()
        for extra in range(max(0, (len(new_releases) - 1) // 500)):
            _count_request()
    if new_identifiers:
        # Prefer bulk insert; duplicates from shared barcodes are ignored.
        try:
            _bulk_insert(supabase, "variant_identifiers", new_identifiers, chunk_size=500)
            _count_request()
            for extra in range(max(0, (len(new_identifiers) - 1) // 500)):
                _count_request()
        except Exception:
            for ident_chunk in _chunked(new_identifiers, 100):
                for ident in ident_chunk:
                    try:
                        supabase.table("variant_identifiers").insert(ident).execute()
                        _count_request()
                    except Exception:
                        pass
    if resolution_payloads:
        res_inserts: List[Dict[str, Any]] = []
        res_updates: List[Tuple[str, Dict[str, Any]]] = []
        for payload in resolution_payloads:
            key = (payload["supplier_id"], payload["supplier_sku"])
            prior = prior_resolutions.get(key)
            if prior and prior.get("id"):
                res_updates.append((prior["id"], payload))
            else:
                res_inserts.append(payload)
        if res_inserts:
            _bulk_insert(supabase, "supplier_sku_resolutions", res_inserts, chunk_size=500)
            _count_request()
            for extra in range(max(0, (len(res_inserts) - 1) // 500)):
                _count_request()
        # Updates are uncommon on first feed; keep bounded and explicit.
        for res_id, payload in res_updates:
            supabase.table("supplier_sku_resolutions").update(payload).eq("id", res_id).execute()
            _count_request()
    stats["timing_ms"]["releases_resolutions"] = stats["timing_ms"].get(
        "releases_resolutions", 0
    ) + int((time.perf_counter() - t2) * 1000)

    # --- build offer payloads ---
    t3 = time.perf_counter()
    offer_inserts: List[Dict[str, Any]] = []
    offer_updates: List[Tuple[str, Dict[str, Any], Dict[str, Any]]] = []
    # track for observation/event staging: key -> (offer_id or None, before, payload, is_new)
    staged: List[Dict[str, Any]] = []

    for item in batch:
        key = (item["supplier_id"], item["supplier_sku"])
        resolution = resolutions.get(key)
        if resolution is None:
            continue
        raw = item["raw"]
        n = item["normalised"]
        payload = {
            "supplier_id": item["supplier_id"],
            "supplier_sku": item["supplier_sku"],
            "raw_barcode": item["barcode"] or None,
            "release_variant_id": resolution.release_variant_id,
            "catalog_item_id": clean_text(raw.get("catalog_item_id")),
            "availability_status": n.availability_status,
            "reported_quantity": n.reported_quantity,
            "quantity_is_exact": n.quantity_is_exact,
            "supplier_can_supply": n.supplier_can_supply,
            "unit_cost": raw.get("cost_price"),
            "currency": clean_text(raw.get("supplier_currency")) or "GBP",
            "release_date": parse_date(raw.get("media_release_date")),
            "last_seen_at": completed_at,
            "source_feed_at": source_feed_at,
            "pipeline_completed_at": completed_at,
            "latest_successful_pipeline_run_id": pipeline_run_id
            if not pipeline_failed_or_stale
            else None,
            "availability_confidence": n.availability_confidence,
            "availability_confidence_version": n.availability_confidence_version,
            "raw_status_text": n.raw_status_text,
            "raw_payload": {
                "title": clean_text(raw.get("title")),
                "format": clean_text(raw.get("format")),
                "source_filename": clean_text(raw.get("source_filename")),
                "legacy_availability_status": clean_text(raw.get("availability_status")),
            },
            "active": True,
            "updated_at": completed_at,
        }
        before = existing_offers.get(key)
        # Decide observation intent before write so last_changed_at can be bulked.
        n = item["normalised"]
        fp = observation_material_fingerprint(
            availability_status=n.availability_status,
            reported_quantity=n.reported_quantity,
            quantity_is_exact=n.quantity_is_exact,
            supplier_can_supply=n.supplier_can_supply,
            unit_cost=payload.get("unit_cost"),
            currency=payload.get("currency"),
        )
        prev_fp = None
        if before:
            prev_fp = observation_material_fingerprint(
                availability_status=str(before.get("availability_status") or "unknown"),
                reported_quantity=before.get("reported_quantity"),
                quantity_is_exact=bool(before.get("quantity_is_exact")),
                supplier_can_supply=before.get("supplier_can_supply"),
                unit_cost=before.get("unit_cost"),
                currency=before.get("currency"),
            )
        will_observe = should_emit_observation(
            previous_fingerprint=prev_fp, new_fingerprint=fp
        )
        if will_observe:
            payload["last_changed_at"] = completed_at

        if before:
            # Copy before-state: later upserts mutate the stored row dict in-place.
            before_snapshot = dict(before)
            offer_updates.append((before["id"], payload, before_snapshot))
            staged.append(
                {
                    "key": key,
                    "offer_id": before["id"],
                    "before": before_snapshot,
                    "payload": payload,
                    "is_new": False,
                    "resolution": resolution,
                    "normalised": n,
                    "fp": fp,
                    "prev_fp": prev_fp,
                    "will_observe": will_observe,
                }
            )
        else:
            new_id = str(uuid.uuid4())
            row = dict(payload)
            row["id"] = new_id
            row["created_at"] = completed_at
            if will_observe:
                row["last_changed_at"] = completed_at
            offer_inserts.append(row)
            staged.append(
                {
                    "key": key,
                    "offer_id": new_id,
                    "before": None,
                    "payload": payload,
                    "is_new": True,
                    "resolution": resolution,
                    "normalised": n,
                    "fp": fp,
                    "prev_fp": prev_fp,
                    "will_observe": will_observe,
                }
            )

    if offer_inserts:
        _bulk_insert(supabase, "supplier_offers", offer_inserts, chunk_size=500)
        _count_request()
        for extra in range(max(0, (len(offer_inserts) - 1) // 500)):
            _count_request()
        stats["offers_inserted"] += len(offer_inserts)
        stats["upserted"] += len(offer_inserts)

    # Updates: PostgREST lacks multi-row arbitrary update; upsert on unique key is fine.
    if offer_updates:
        upsert_rows = []
        for offer_id, payload, before in offer_updates:
            row = dict(payload)
            row["id"] = offer_id
            row["created_at"] = before.get("created_at") or completed_at
            upsert_rows.append(row)
        try:
            _bulk_upsert(
                supabase,
                "supplier_offers",
                upsert_rows,
                on_conflict="id",
                chunk_size=500,
            )
            _count_request()
            for extra in range(max(0, (len(upsert_rows) - 1) // 500)):
                _count_request()
        except Exception:
            for offer_id, payload, _before in offer_updates:
                supabase.table("supplier_offers").update(payload).eq("id", offer_id).execute()
                _count_request()
        stats["offers_updated"] += len(offer_updates)
        stats["upserted"] += len(offer_updates)

    stats["timing_ms"]["offers"] = stats["timing_ms"].get("offers", 0) + int(
        (time.perf_counter() - t3) * 1000
    )

    # --- observations + events (only material changes; no baseline price events) ---
    t4 = time.perf_counter()
    obs_candidates: List[Dict[str, Any]] = []
    event_candidates: List[Dict[str, Any]] = []

    for item in staged:
        n = item["normalised"]
        payload = item["payload"]
        before = item["before"]
        offer_id = item["offer_id"]
        if not offer_id:
            continue
        if not item.get("will_observe"):
            stats["observations_skipped"] += 1
            continue

        fp = item["fp"]
        prev_fp = item["prev_fp"]
        dedupe = build_observation_dedupe_key(str(offer_id), fp)
        obs_payload = {
            "supplier_offer_id": offer_id,
            "pipeline_run_id": pipeline_run_id,
            "observed_at": completed_at,
            "source_feed_at": source_feed_at,
            "availability_status": n.availability_status,
            "reported_quantity": n.reported_quantity,
            "quantity_is_exact": n.quantity_is_exact,
            "supplier_can_supply": n.supplier_can_supply,
            "unit_cost": payload.get("unit_cost"),
            "currency": payload.get("currency"),
            "raw_status_text": n.raw_status_text,
            "raw_payload": payload["raw_payload"],
            "dedupe_key": dedupe,
        }
        obs_candidates.append(
            {
                "obs": obs_payload,
                "before": before,
                "payload": payload,
                "fp": fp,
                "prev_fp": prev_fp,
                "offer_id": offer_id,
                "resolution": item["resolution"],
                "normalised": n,
                "is_new": item["is_new"],
            }
        )

    existing_dedupe: set[str] = set()
    if obs_candidates:
        keys = [c["obs"]["dedupe_key"] for c in obs_candidates]
        existing_rows = _select_in(
            supabase,
            "supplier_offer_observations",
            "id,dedupe_key",
            "dedupe_key",
            keys,
            chunk_size=in_chunk,
        )
        _count_request()
        for extra in range(max(0, (len(keys) - 1) // in_chunk)):
            _count_request()
        existing_dedupe = {r["dedupe_key"] for r in existing_rows if r.get("dedupe_key")}

    obs_to_insert: List[Dict[str, Any]] = []
    for cand in obs_candidates:
        dedupe = cand["obs"]["dedupe_key"]
        if dedupe in existing_dedupe:
            stats["observations_skipped"] += 1
            continue
        obs_id = str(uuid.uuid4())
        row = dict(cand["obs"])
        row["id"] = obs_id
        obs_to_insert.append(row)
        cand["obs_id"] = obs_id

        evt_type = _event_type_for_change(cand["before"], cand["payload"])
        if (
            evt_type
            and not cand["is_new"]
            and should_emit_inventory_event(
                previous_fingerprint=cand["prev_fp"], new_fingerprint=cand["fp"]
            )
        ):
            event_candidates.append(
                {
                    "id": str(uuid.uuid4()),
                    "event_type": evt_type,
                    "release_variant_id": cand["resolution"].release_variant_id,
                    "supplier_offer_id": str(cand["offer_id"]),
                    "observation_id": obs_id,
                    "before_state": cand["before"] or {},
                    "after_state": {
                        "availability_status": cand["normalised"].availability_status,
                        "reported_quantity": cand["normalised"].reported_quantity,
                        "unit_cost": cand["payload"].get("unit_cost"),
                    },
                    "pipeline_run_id": pipeline_run_id,
                    "dedupe_key": build_event_dedupe_key(
                        evt_type,
                        release_variant_id=cand["resolution"].release_variant_id,
                        supplier_offer_id=str(cand["offer_id"]),
                        fingerprint=cand["fp"],
                    ),
                    "observed_at": completed_at,
                }
            )

    if obs_to_insert:
        _bulk_insert(supabase, "supplier_offer_observations", obs_to_insert, chunk_size=500)
        _count_request()
        for extra in range(max(0, (len(obs_to_insert) - 1) // 500)):
            _count_request()
        stats["observations_inserted"] += len(obs_to_insert)

    if event_candidates:
        existing_evt = _select_in(
            supabase,
            "inventory_events",
            "id,dedupe_key",
            "dedupe_key",
            [e["dedupe_key"] for e in event_candidates],
            chunk_size=in_chunk,
        )
        _count_request()
        existing_evt_keys = {r["dedupe_key"] for r in existing_evt if r.get("dedupe_key")}
        evt_insert = [e for e in event_candidates if e["dedupe_key"] not in existing_evt_keys]
        if evt_insert:
            _bulk_insert(supabase, "inventory_events", evt_insert, chunk_size=500)
            _count_request()
            stats["events_inserted"] += len(evt_insert)

    stats["timing_ms"]["observations_events"] = stats["timing_ms"].get(
        "observations_events", 0
    ) + int((time.perf_counter() - t4) * 1000)


def _resolution_payload(
    *,
    supplier_id: str,
    supplier_sku: str,
    raw_barcode: Optional[str],
    result: ResolutionResult,
    resolved_id: Optional[str],
    completed_at: str,
) -> Dict[str, Any]:
    payload = {
        "supplier_id": supplier_id,
        "supplier_sku": supplier_sku,
        "raw_barcode": raw_barcode,
        "resolved_release_variant_id": resolved_id,
        "match_method": result.match_method,
        "match_confidence": result.match_confidence,
        "review_status": result.review_status,
        "notes": result.notes,
        "active": True,
        "updated_at": completed_at,
    }
    if result.review_status in {"auto_accepted", "manual"}:
        payload["reviewed_at"] = completed_at
    return payload
