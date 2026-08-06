"""
Dual-write supplier staging/catalog commercial rows into supplier_offers (+ observations).

Never writes tape_inventory_levels or Shopify inventory quantities.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

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
from app.services.inventory_events_service import build_event_dedupe_key, emit_inventory_event
from app.services.supplier_resolution_service import resolve_supplier_offer_to_release

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


def _event_type_for_change(before: Mapping[str, Any], after: Mapping[str, Any]) -> Optional[str]:
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
    if b_cost != a_cost and a_cost is not None:
        return "supplier_price_changed"
    return None


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
    stats = {
        "enabled": flags.supplier_enabled,
        "considered": 0,
        "upserted": 0,
        "observations_inserted": 0,
        "observations_skipped": 0,
        "events_inserted": 0,
        "resolutions_created_releases": 0,
        "needs_review": 0,
        "skipped_identity": 0,
        "blocked_mass_unavailable": False,
        "errors": 0,
    }
    if not flags.supplier_enabled:
        return stats

    rows = list(offer_rows)
    stats["considered"] = len(rows)

    # Stale/failed feed guard: do not mass-flip to unavailable.
    proposed_statuses: List[str] = []
    prepared: List[Dict[str, Any]] = []
    for raw in rows:
        supplier_label = clean_text(raw.get("supplier")) or ""
        sid = normalize_supplier_id(supplier_label)
        if sid == "tape_film":
            # Tape Film commercial state belongs on tape_inventory / Shopify channel, not supplier_offers.
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
            release_date_is_future=_is_future_date(raw.get("media_release_date") or raw.get("release_date")),
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
        # Recompute confidence with derived freshness.
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
                "barcode": barcode,
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
        logger.error(
            "supplier dual-write blocked: %s",
            mass[0].message,
        )
        return stats

    completed_at = pipeline_completed_at or _now_iso()

    for item in prepared:
        try:
            raw = item["raw"]
            resolution = resolve_supplier_offer_to_release(
                supabase,
                supplier_id=item["supplier_id"],
                supplier_sku=item["supplier_sku"],
                raw_barcode=item["barcode"],
                title=clean_text(raw.get("title") or raw.get("harmonized_title")),
                format_name=clean_text(raw.get("format") or raw.get("harmonized_format")),
                catalog_item_id=clean_text(raw.get("catalog_item_id") or raw.get("id"))
                if raw.get("catalog_item_id")
                else clean_text(raw.get("published_catalog_item_id")),
                film_id=clean_text(raw.get("film_id")),
                flags=flags,
            )
            if resolution.created_release:
                stats["resolutions_created_releases"] += 1
            if resolution.review_status == "needs_review":
                stats["needs_review"] += 1

            n = item["normalised"]
            payload = {
                "supplier_id": item["supplier_id"],
                "supplier_sku": item["supplier_sku"],
                "raw_barcode": item["barcode"],
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

            existing = (
                supabase.table("supplier_offers")
                .select(
                    "id,availability_status,reported_quantity,quantity_is_exact,"
                    "supplier_can_supply,unit_cost,currency"
                )
                .eq("supplier_id", item["supplier_id"])
                .eq("supplier_sku", item["supplier_sku"])
                .limit(1)
                .execute()
            )
            before = (existing.data or [None])[0]
            if before:
                supabase.table("supplier_offers").update(payload).eq("id", before["id"]).execute()
                offer_id = before["id"]
            else:
                payload["created_at"] = completed_at
                inserted = supabase.table("supplier_offers").insert(payload).execute()
                offer_id = (inserted.data or [{}])[0].get("id")
                before = {}
            stats["upserted"] += 1

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

            if offer_id and should_emit_observation(previous_fingerprint=prev_fp, new_fingerprint=fp):
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
                try:
                    obs = supabase.table("supplier_offer_observations").insert(obs_payload).execute()
                    obs_id = (obs.data or [{}])[0].get("id")
                    stats["observations_inserted"] += 1
                    supabase.table("supplier_offers").update(
                        {"last_changed_at": completed_at}
                    ).eq("id", offer_id).execute()
                except Exception as exc:
                    msg = str(exc).lower()
                    if "duplicate" in msg or "unique" in msg or "23505" in msg:
                        stats["observations_skipped"] += 1
                        obs_id = None
                    else:
                        raise
                else:
                    evt_type = _event_type_for_change(before or {}, payload)
                    if evt_type and should_emit_inventory_event(
                        previous_fingerprint=prev_fp, new_fingerprint=fp
                    ):
                        emit_inventory_event(
                            supabase,
                            event_type=evt_type,
                            dedupe_key=build_event_dedupe_key(
                                evt_type,
                                release_variant_id=resolution.release_variant_id,
                                supplier_offer_id=str(offer_id),
                                fingerprint=fp,
                            ),
                            release_variant_id=resolution.release_variant_id,
                            supplier_offer_id=str(offer_id),
                            observation_id=obs_id,
                            before_state=before or {},
                            after_state={
                                "availability_status": n.availability_status,
                                "reported_quantity": n.reported_quantity,
                                "unit_cost": payload.get("unit_cost"),
                            },
                            pipeline_run_id=pipeline_run_id,
                            observed_at=completed_at,
                        )
                        stats["events_inserted"] += 1
            else:
                stats["observations_skipped"] += 1
        except Exception:
            stats["errors"] += 1
            logger.exception(
                "supplier offer dual-write failed supplier=%s sku=%s",
                item.get("supplier_id"),
                item.get("supplier_sku"),
            )

    logger.info("supplier offer dual-write complete: %s", stats)
    return stats
