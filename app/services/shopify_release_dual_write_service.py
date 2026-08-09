"""
Dual-write Shopify store sync rows into release_variants + channels + tape inventory.

Safeguards:
  * Never writes supplier quantities into tape/Shopify inventory.
  * Preorder must not create positive on_hand from non-Shopify sources (this path is Shopify).
  * Only physically stocked / published channels receive tape_inventory_levels.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

from app.config.inventory_dual_write import (
    InventoryDualWriteFlags,
    load_inventory_dual_write_flags,
)
from app.helpers.text_helpers import clean_text
from app.rules.inventory_invariant_rules import (
    validate_preorder_no_positive_tape_on_hand,
    validate_tape_inventory_levels,
)
from app.services.inventory_events_service import build_event_dedupe_key, emit_inventory_event
from app.services.shopify_ii_product_domain import is_vinyl_soundtrack_listing
from app.services.shopify_inventory_settings_audit import is_gift_card_product_type
from app.services.shopify_release_mapping import classify_shopify_listing_mapping

logger = logging.getLogger(__name__)

# Exact product_title matches (casefold) for known non-release Shopify products.
# Needed because store-sync snapshots often have empty product_type for gift cards.
SHOPIFY_II_EXACT_EXCLUDED_PRODUCT_TITLES = frozenset(
    {
        "tape! film gift card",
        "test film - hell or high water (not for sale)",
    }
)


def shopify_ii_dual_write_exclusion_reason(
    row: Mapping[str, Any],
    *,
    soundtrack_product_ids: Optional[set[str]] = None,
) -> Optional[str]:
    """
    Deterministic exclusions for Shopify → *film* Inventory Intelligence dual-write.

    Reasons:
      - gift_card_product_type / gift_card_exact_title / test_product_exact_title
      - vinyl_soundtrack (valid commerce product; different inventory domain)

    Prefer structured Shopify fields (product_type, collection handles, media_format,
    soundtracks collection product IDs). Never broad title substring filters.
    """
    if is_gift_card_product_type(row.get("product_type")):
        return "gift_card_product_type"
    title = (clean_text(row.get("product_title") or row.get("title")) or "").casefold()
    if title in SHOPIFY_II_EXACT_EXCLUDED_PRODUCT_TITLES:
        if "gift card" in title:
            return "gift_card_exact_title"
        return "test_product_exact_title"
    if is_vinyl_soundtrack_listing(row, soundtrack_product_ids=soundtrack_product_ids):
        return "vinyl_soundtrack"
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _qty_map(level: Any) -> Dict[str, int]:
    if not level:
        return {}
    out: Dict[str, int] = {}
    for q in level.get("quantities") or []:
        name = q.get("name")
        if name:
            try:
                out[str(name)] = int(q.get("quantity") or 0)
            except (TypeError, ValueError):
                out[str(name)] = 0
    return out


def _is_preorder_listing(row: Mapping[str, Any]) -> bool:
    # media_release_date in the future OR inventory policy CONTINUE with non-positive available
    # is handled by callers; here use explicit flag if provided.
    if row.get("is_preorder") is True:
        return True
    media = clean_text(row.get("media_release_date"))
    if not media:
        return False
    try:
        from datetime import date

        d = date.fromisoformat(media[:10])
        return d > date.today()
    except Exception:
        return False


def dual_write_shopify_listings_to_releases(
    supabase: Any,
    listing_rows: Sequence[Mapping[str, Any]],
    *,
    shop: str,
    location_id: Optional[str] = None,
    inventory_levels_by_variant: Optional[Mapping[str, Mapping[str, int]]] = None,
    flags: Optional[InventoryDualWriteFlags] = None,
    pipeline_run_id: Optional[str] = None,
    soundtrack_product_ids: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """
    For each Shopify listing row:
      - upsert/find release_variant (by existing channel or barcode)
      - upsert release_shopify_listings channel
      - upsert tape_inventory_levels when stocked (has inventory item / tracks inventory)
    """
    flags = flags or load_inventory_dual_write_flags()
    stats = {
        "enabled": flags.shopify_enabled,
        "considered": len(listing_rows),
        "releases_upserted": 0,
        "channels_upserted": 0,
        "tape_levels_upserted": 0,
        "events_inserted": 0,
        "skipped_ambiguous": 0,
        "skipped_non_release": 0,
        "skipped_non_release_by_reason": {},
        "created_new_release": 0,
        "warnings": 0,
        "errors": 0,
    }
    if not flags.shopify_enabled:
        return stats

    loc = clean_text(location_id) or clean_text(
        __import__("os").getenv("SHOPIFY_INVENTORY_LOCATION_ID")
    )
    levels = inventory_levels_by_variant or {}
    now = _now_iso()
    skip_reasons: Dict[str, int] = {}

    for row in listing_rows:
        try:
            exclude_reason = shopify_ii_dual_write_exclusion_reason(
                row, soundtrack_product_ids=soundtrack_product_ids
            )
            if exclude_reason:
                stats["skipped_non_release"] += 1
                skip_reasons[exclude_reason] = skip_reasons.get(exclude_reason, 0) + 1
                continue

            vid = clean_text(row.get("shopify_variant_id"))
            if not vid:
                continue
            barcode = clean_text(row.get("barcode"))
            title = clean_text(row.get("product_title") or row.get("title"))
            catalog_item_id = clean_text(row.get("catalog_item_id"))

            release_id, created_new = _resolve_or_create_release_for_shopify(
                supabase,
                shop=shop,
                shopify_variant_id=vid,
                shopify_product_id=clean_text(row.get("shopify_product_id")),
                barcode=barcode,
                title=title,
                catalog_item_id=catalog_item_id,
                now=now,
            )
            if release_id is None:
                stats["skipped_ambiguous"] += 1
                stats["warnings"] += 1
                logger.warning(
                    "shopify dual-write skipped ambiguous mapping variant=%s barcode=%s",
                    vid,
                    barcode,
                )
                continue
            stats["releases_upserted"] += 1
            if created_new:
                stats["created_new_release"] += 1

            channel = {
                "release_variant_id": release_id,
                "shop": shop,
                "shopify_product_id": clean_text(row.get("shopify_product_id")),
                "shopify_variant_id": vid,
                "shopify_inventory_item_id": clean_text(row.get("shopify_inventory_item_id")),
                "is_primary": True,
                "updated_at": now,
            }
            existing_ch = (
                supabase.table("release_shopify_listings")
                .select("id")
                .eq("shop", shop)
                .eq("shopify_variant_id", vid)
                .limit(1)
                .execute()
            )
            if existing_ch.data:
                supabase.table("release_shopify_listings").update(channel).eq(
                    "id", existing_ch.data[0]["id"]
                ).execute()
            else:
                channel["created_at"] = now
                supabase.table("release_shopify_listings").insert(channel).execute()
            stats["channels_upserted"] += 1

            if barcode:
                _ensure_barcode_identifier(supabase, release_id, barcode, source="shopify")

            # Tape inventory only for physically stocked / tracked items.
            tracks = row.get("tracks_inventory")
            inv_item = clean_text(row.get("shopify_inventory_item_id"))
            if not loc or (tracks is False and not inv_item):
                continue

            q = levels.get(vid) or {}
            available = int(q.get("available", row.get("inventory_quantity") or 0) or 0)
            on_hand = int(q.get("on_hand", max(available, 0)) or 0)
            committed = int(q.get("committed", 0) or 0)
            incoming = int(q.get("incoming", 0) or 0)
            # Prefer Shopify identity when detailed levels present.
            if "available" in q and "on_hand" in q and "committed" in q:
                available = int(q["available"])
                on_hand = int(q["on_hand"])
                committed = int(q["committed"])

            is_preorder = _is_preorder_listing(row)
            for v in validate_preorder_no_positive_tape_on_hand(
                is_preorder=is_preorder,
                on_hand=on_hand,
                source="shopify_store_sync",
            ):
                stats["warnings"] += 1
                logger.warning("tape inventory warning: %s %s", v.code, v.context)

            level_payload = {
                "release_variant_id": release_id,
                "shopify_location_id": loc,
                "on_hand": on_hand,
                "committed": committed,
                "available": available,
                "shopify_incoming_reported": incoming,
                # po_incoming_confirmed owned by PO dual-write — do not overwrite here.
                "last_synced_at": now,
                "pipeline_run_id": pipeline_run_id,
                "updated_at": now,
            }
            for v in validate_tape_inventory_levels(level_payload):
                if v.severity == "warning":
                    stats["warnings"] += 1
                    logger.warning("tape inventory warning: %s %s", v.code, v.message)

            existing_lvl = (
                supabase.table("tape_inventory_levels")
                .select(
                    "id,on_hand,committed,available,shopify_incoming_reported,po_incoming_confirmed"
                )
                .eq("release_variant_id", release_id)
                .eq("shopify_location_id", loc)
                .limit(1)
                .execute()
            )
            before = (existing_lvl.data or [None])[0]
            if before:
                # Preserve PO-owned incoming.
                level_payload["po_incoming_confirmed"] = int(
                    before.get("po_incoming_confirmed") or 0
                )
                supabase.table("tape_inventory_levels").update(level_payload).eq(
                    "id", before["id"]
                ).execute()
                level_id = before["id"]
            else:
                level_payload["po_incoming_confirmed"] = 0
                level_payload["created_at"] = now
                ins = supabase.table("tape_inventory_levels").insert(level_payload).execute()
                level_id = (ins.data or [{}])[0].get("id")
            stats["tape_levels_upserted"] += 1

            fp = f"{on_hand}|{committed}|{available}|{incoming}"
            prev_fp = None
            if before:
                prev_fp = (
                    f"{before.get('on_hand')}|{before.get('committed')}|"
                    f"{before.get('available')}|{before.get('shopify_incoming_reported')}"
                )
            if prev_fp != fp:
                emit_inventory_event(
                    supabase,
                    event_type="tape_stock_synced",
                    dedupe_key=build_event_dedupe_key(
                        "tape_stock_synced",
                        release_variant_id=release_id,
                        fingerprint=fp,
                    ),
                    release_variant_id=release_id,
                    tape_inventory_level_id=level_id,
                    before_state=before or {},
                    after_state={
                        "on_hand": on_hand,
                        "committed": committed,
                        "available": available,
                        "shopify_incoming_reported": incoming,
                    },
                    pipeline_run_id=pipeline_run_id,
                    observed_at=now,
                )
                stats["events_inserted"] += 1
        except Exception:
            stats["errors"] += 1
            logger.exception(
                "shopify dual-write failed variant=%s",
                row.get("shopify_variant_id"),
            )

    stats["skipped_non_release_by_reason"] = skip_reasons
    logger.info("shopify release dual-write complete: %s", stats)
    return stats


def _resolve_or_create_release_for_shopify(
    supabase: Any,
    *,
    shop: str,
    shopify_variant_id: str,
    shopify_product_id: Optional[str],
    barcode: Optional[str],
    title: Optional[str],
    catalog_item_id: Optional[str],
    now: str,
) -> tuple[Optional[str], bool]:
    """
    Resolve Shopify variant → release_variant.

    Returns (release_variant_id|None, created_new).
    Ambiguous barcode/catalog mapping → (None, False); never guess.
    Unmapped curated Shopify products may create a new release_variant (II inbound only).
    """
    _ = shopify_product_id  # reserved for future channel metadata
    classified = classify_shopify_listing_mapping(
        supabase,
        shop=shop,
        shopify_variant_id=shopify_variant_id,
        barcode=barcode,
        catalog_item_id=catalog_item_id,
    )
    if classified.status == "ambiguous_barcode":
        return None, False

    if classified.release_variant_id:
        rid = classified.release_variant_id
        update_payload: Dict[str, Any] = {
            "publication_status": "published",
            "updated_at": now,
        }
        if catalog_item_id:
            update_payload["catalog_item_id"] = catalog_item_id
        if barcode:
            update_payload["primary_barcode"] = barcode
        if title:
            update_payload["title"] = title
        supabase.table("release_variants").update(update_payload).eq("id", rid).execute()
        return rid, False

    payload = {
        "catalog_item_id": catalog_item_id,
        "primary_barcode": barcode,
        "title": title,
        "publication_status": "published",
        "active": True,
        "created_at": now,
        "updated_at": now,
    }
    ins = supabase.table("release_variants").insert(payload).execute()
    return (ins.data or [{}])[0].get("id"), True


def _ensure_barcode_identifier(
    supabase: Any, release_variant_id: str, barcode: str, *, source: str
) -> None:
    try:
        existing = (
            supabase.table("variant_identifiers")
            .select("id")
            .eq("release_variant_id", release_variant_id)
            .eq("id_type", "barcode")
            .eq("id_value", barcode)
            .limit(1)
            .execute()
        )
        if existing.data:
            return
        supabase.table("variant_identifiers").insert(
            {
                "release_variant_id": release_variant_id,
                "id_type": "barcode",
                "id_value": barcode,
                "source": source,
                "is_primary": True,
                "is_valid": True,
                "conflict_flag": False,
            }
        ).execute()
    except Exception:
        logger.exception(
            "variant_identifiers insert failed release=%s barcode=%s",
            release_variant_id,
            barcode,
        )


def fetch_inventory_levels_for_variants(
    shopify_client: Any,
    *,
    location_id: str,
    variant_rows: Sequence[Mapping[str, Any]],
) -> Dict[str, Dict[str, int]]:
    """
    Fetch on_hand/committed/available/incoming for listing rows that have inventory item ids.
    Returns map shopify_variant_id -> qty dict.
    """
    # Prefer querying by inventory item via productVariant when needed — batch by variant id.
    query = """
    query ($id: ID!, $locId: ID!) {
      productVariant(id: $id) {
        id
        inventoryItem {
          inventoryLevel(locationId: $locId) {
            quantities(names: ["available", "committed", "on_hand", "incoming"]) {
              name
              quantity
            }
          }
        }
      }
    }
    """
    out: Dict[str, Dict[str, int]] = {}
    for row in variant_rows:
        vid = clean_text(row.get("shopify_variant_id"))
        if not vid:
            continue
        try:
            data = shopify_client.graphql(query, {"id": vid, "locId": location_id})
            node = (data or {}).get("productVariant") or {}
            inv = (node.get("inventoryItem") or {}).get("inventoryLevel")
            out[vid] = _qty_map(inv)
        except Exception:
            logger.exception("inventory level fetch failed variant=%s", vid)
    return out
