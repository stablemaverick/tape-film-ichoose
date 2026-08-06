"""
Dual-write inbound supplier PO CSV snapshots into purchase_orders / lines.

Also updates po_incoming_confirmed on tape_inventory_levels for matched releases.
Does not mutate Shopify inventory quantities.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.config.inventory_dual_write import (
    InventoryDualWriteFlags,
    load_inventory_dual_write_flags,
    normalize_supplier_id,
    supplier_sku_identity,
)
from app.helpers.text_helpers import clean_text
from app.rules.inventory_invariant_rules import validate_purchase_order_line
from app.services.inventory_events_service import build_event_dedupe_key, emit_inventory_event
from app.services.supplier_po_inbound import OPEN_PO_STATUSES, SupplierPoLine, normalize_match_key

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _map_po_status(raw_status: str) -> str:
    key = normalize_match_key(raw_status)
    if key in {"pre-order", "preorder", "awaiting stock"}:
        return "confirmed"
    if key == "picking":
        return "confirmed"
    if key in {"cancelled", "canceled"}:
        return "cancelled"
    if key in {"received", "complete", "completed"}:
        return "received"
    return "ordered"


def dual_write_purchase_orders(
    supabase: Any,
    lines: List[SupplierPoLine],
    *,
    supplier_label: str = "moovies",
    source_filename: Optional[str] = None,
    location_id: Optional[str] = None,
    flags: Optional[InventoryDualWriteFlags] = None,
) -> Dict[str, Any]:
    flags = flags or load_inventory_dual_write_flags()
    stats = {
        "enabled": flags.po_enabled,
        "lines": len(lines),
        "orders_upserted": 0,
        "lines_upserted": 0,
        "po_incoming_updated": 0,
        "events_inserted": 0,
        "unmatched_releases": 0,
        "errors": 0,
    }
    if not flags.po_enabled:
        return stats

    sid = normalize_supplier_id(supplier_label)
    loc = clean_text(location_id) or clean_text(
        __import__("os").getenv("SHOPIFY_INVENTORY_LOCATION_ID")
    )
    now = _now_iso()

    # Group by order_id
    by_order: Dict[str, List[SupplierPoLine]] = {}
    for line in lines:
        oid = clean_text(line.order_id) or "UNKNOWN"
        by_order.setdefault(oid, []).append(line)

    # Aggregate confirmed qty by release for po_incoming_confirmed refresh.
    incoming_by_release: Dict[str, int] = {}

    for order_number, order_lines in by_order.items():
        try:
            status = _map_po_status(order_lines[0].status)
            po_payload = {
                "supplier_id": sid,
                "purchase_order_number": order_number,
                "status": status,
                "source_filename": source_filename,
                "updated_at": now,
            }
            existing = (
                supabase.table("purchase_orders")
                .select("id,status")
                .eq("supplier_id", sid)
                .eq("purchase_order_number", order_number)
                .limit(1)
                .execute()
            )
            before_status = None
            if existing.data:
                po_id = existing.data[0]["id"]
                before_status = existing.data[0].get("status")
                supabase.table("purchase_orders").update(po_payload).eq("id", po_id).execute()
            else:
                po_payload["created_at"] = now
                po_payload["ordered_at"] = now[:10]
                ins = supabase.table("purchase_orders").insert(po_payload).execute()
                po_id = (ins.data or [{}])[0].get("id")
                emit_inventory_event(
                    supabase,
                    event_type="purchase_order_created",
                    dedupe_key=build_event_dedupe_key(
                        "purchase_order_created",
                        fingerprint=f"{sid}:{order_number}",
                    ),
                    purchase_order_id=po_id,
                    after_state={"status": status, "purchase_order_number": order_number},
                    observed_at=now,
                )
                stats["events_inserted"] += 1
            stats["orders_upserted"] += 1

            if before_status and before_status != status and status == "confirmed":
                emit_inventory_event(
                    supabase,
                    event_type="purchase_order_confirmed",
                    dedupe_key=build_event_dedupe_key(
                        "purchase_order_confirmed",
                        fingerprint=f"{sid}:{order_number}:{status}",
                    ),
                    purchase_order_id=po_id,
                    before_state={"status": before_status},
                    after_state={"status": status},
                    observed_at=now,
                )
                stats["events_inserted"] += 1

            for line in order_lines:
                sku = supplier_sku_identity(supplier_sku=line.sku, raw_barcode=None)
                release_id = _resolve_release_for_po_line(supabase, sid, line)
                if not release_id:
                    stats["unmatched_releases"] += 1

                qty = max(0, int(line.qty or 0))
                open_status = normalize_match_key(line.status) in OPEN_PO_STATUSES
                confirmed = qty if open_status else 0
                line_payload = {
                    "purchase_order_id": po_id,
                    "release_variant_id": release_id,
                    "supplier_sku": sku,
                    "title": clean_text(line.title),
                    "quantity_ordered": qty,
                    "quantity_confirmed": confirmed,
                    "quantity_received": 0,
                    "quantity_cancelled": 0,
                    "unit_cost": None,
                    "updated_at": now,
                }
                # Try parse unit cost
                try:
                    if line.unit_cost not in (None, ""):
                        line_payload["unit_cost"] = float(str(line.unit_cost).replace(",", ""))
                except (TypeError, ValueError):
                    pass

                for v in validate_purchase_order_line(line_payload):
                    if v.severity == "error":
                        logger.error("PO line invariant: %s %s", v.code, v.message)

                # Idempotent line upsert by po_id + supplier_sku/title
                q = (
                    supabase.table("purchase_order_lines")
                    .select("id")
                    .eq("purchase_order_id", po_id)
                )
                if sku:
                    q = q.eq("supplier_sku", sku)
                else:
                    q = q.eq("title", clean_text(line.title) or "")
                existing_line = q.limit(1).execute()
                if existing_line.data:
                    supabase.table("purchase_order_lines").update(line_payload).eq(
                        "id", existing_line.data[0]["id"]
                    ).execute()
                else:
                    line_payload["created_at"] = now
                    supabase.table("purchase_order_lines").insert(line_payload).execute()
                stats["lines_upserted"] += 1

                if release_id and confirmed > 0:
                    incoming_by_release[release_id] = (
                        incoming_by_release.get(release_id, 0) + confirmed
                    )
        except Exception:
            stats["errors"] += 1
            logger.exception("PO dual-write failed order=%s", order_number)

    if loc:
        for release_id, confirmed_qty in incoming_by_release.items():
            try:
                existing = (
                    supabase.table("tape_inventory_levels")
                    .select("id,po_incoming_confirmed")
                    .eq("release_variant_id", release_id)
                    .eq("shopify_location_id", loc)
                    .limit(1)
                    .execute()
                )
                if not existing.data:
                    # Supplier-only / not yet stocked — skip tape level (no Shopify stock required).
                    continue
                supabase.table("tape_inventory_levels").update(
                    {"po_incoming_confirmed": confirmed_qty, "updated_at": now}
                ).eq("id", existing.data[0]["id"]).execute()
                stats["po_incoming_updated"] += 1
            except Exception:
                stats["errors"] += 1
                logger.exception("po_incoming update failed release=%s", release_id)

    logger.info("PO dual-write complete: %s", stats)
    return stats


def _resolve_release_for_po_line(
    supabase: Any, supplier_id: str, line: SupplierPoLine
) -> Optional[str]:
    sku = supplier_sku_identity(supplier_sku=line.sku, raw_barcode=None)
    if sku:
        res = (
            supabase.table("supplier_sku_resolutions")
            .select("resolved_release_variant_id,review_status")
            .eq("supplier_id", supplier_id)
            .eq("supplier_sku", sku)
            .eq("active", True)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        if rows and rows[0].get("review_status") in {"auto_accepted", "manual"}:
            return rows[0].get("resolved_release_variant_id")
        offer = (
            supabase.table("supplier_offers")
            .select("release_variant_id")
            .eq("supplier_id", supplier_id)
            .eq("supplier_sku", sku)
            .limit(1)
            .execute()
        )
        if offer.data and offer.data[0].get("release_variant_id"):
            return offer.data[0]["release_variant_id"]
    return None
