"""
Supplier-backed Shopify inventoryPolicy sync for configured studio labels.

Originally Arrow-only (A + B). Generalised to an explicit studio/label set
(Arrow, Second Sight, Criterion Collection) without changing the Arrow
classifier or production apply wrapper.

Rules (Shopify qty exactly 0 only — TAPE store quantity from Shopify):
  A) DENY + supplier available (fresh/aging Moovies/Lasgo) → CONTINUE
  B) CONTINUE + supplier not available with clear evidence → DENY
     - clear evidence: all offers fresh/aging unavailable, or no offer found
     - skip stale/ambiguous supplier state
     - protect valid future preorders from DENY flips
     - optional extra CONTINUE protections: backorder metafield / preorder flag

Shopify mutations (when apply=True) change ONLY inventoryPolicy.
"""

from __future__ import annotations

import csv
import json
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from supabase import create_client

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text
from app.rules.availability_rules import derive_feed_freshness
from app.services.shopify_inventory_settings_audit import parse_shopify_bool_metafield
from app.services.stock_availability_service import (
    freshness_hours_for_supplier,
    map_supplier_api_availability,
)

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
WHOLESALE_SUPPLIER_IDS = frozenset({"moovies", "lasgo"})

# Live custom.studio values (2026-08-18 scan): Arrow Video, Arrow Films,
# Second Sight, Criterion Collection. Normalize only those obvious variants.
DEFAULT_ELIGIBLE_STUDIO_LABELS = ("Arrow", "Second Sight", "Criterion Collection")
ARROW_ONLY_LABELS = ("Arrow",)

ACTION_SET_CONTINUE = "SET_CONTINUE"
ACTION_SET_DENY = "SET_DENY"
ACTION_NO_CHANGE = "NO_CHANGE"
ACTION_SKIP = "SKIP"

SAFETY_TAPE_IN_STOCK = "tape_stock_available_no_change"
SAFETY_DENY_TO_CONTINUE = "deny_to_continue"
SAFETY_SAFE_UNAVAILABLE = "safe_supplier_unavailable_correction"
SAFETY_PREORDER = "preorder_protected"
SAFETY_BACKORDER = "backorder_protected"
SAFETY_MANUAL = "deliberate_manual_continue_suspected"
SAFETY_AMBIGUOUS = "ambiguous_requires_review"
SAFETY_ALREADY_CORRECT = "already_correct"
SAFETY_SKIP_OTHER = "skipped_other"
SAFETY_REGION_EXCLUDED = "region_not_b_excluded"


def sydney_today() -> date:
    return datetime.now(SYDNEY_TZ).date()


def parse_iso_date(raw: Any) -> Optional[date]:
    text = clean_text(raw) or ""
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def is_valid_future_preorder(pre_order_value: bool, release_date: Optional[date], today: date) -> bool:
    return bool(pre_order_value and release_date and release_date > today)


def _collapse_studio(raw: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (clean_text(raw) or "")).strip()


def normalize_studio_label(raw: Optional[str]) -> str:
    """Map live custom.studio values to canonical labels. Studio metafield only."""
    text = _collapse_studio(raw)
    if not text:
        return ""
    low = text.casefold()
    if "arrow" in low:
        return "Arrow"
    if "second sight" in low:
        return "Second Sight"
    if "criterion collection" in low:
        return "Criterion Collection"
    return text


def is_eligible_studio(raw: Optional[str], labels: Sequence[str] | None = None) -> bool:
    allowed = {str(x).strip() for x in (labels or DEFAULT_ELIGIBLE_STUDIO_LABELS) if str(x).strip()}
    return normalize_studio_label(raw) in allowed


def is_arrow_studio(studio: Optional[str]) -> bool:
    """Arrow Video / Arrow Films (and any custom.studio containing 'arrow')."""
    return "arrow" in (clean_text(studio) or "").casefold()


def normalize_region(raw: Optional[str]) -> str:
    """Canonicalise custom.region. Region A = US (out of catalogue); Region B = UK/PAL."""
    text = _collapse_studio(raw)
    if not text:
        return ""
    low = text.casefold()
    compact = low.replace("-", " ").replace("_", " ")
    compact = re.sub(r"\s+", " ", compact).strip()
    if compact in {"a", "region a", "us", "usa", "ntsc"} or compact.startswith("region a"):
        return "A"
    if compact in {"b", "region b", "uk", "pal"} or compact.startswith("region b"):
        return "B"
    if "region a" in compact:
        return "A"
    if "region b" in compact:
        return "B"
    return text


def is_region_b(raw: Optional[str]) -> bool:
    return normalize_region(raw) == "B"


def resolve_variant_region(*, product_region: Optional[str], variant_region: Optional[str]) -> str:
    """Variant metafield wins when set; otherwise product custom.region."""
    variant = normalize_region(variant_region)
    if variant:
        return variant
    return normalize_region(product_region)


PRODUCTS_QUERY = """
query StudioInventoryPolicyProducts($cursor: String, $q: String) {
  products(first: 50, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      handle
      status
      studio: metafield(namespace: "custom", key: "studio") { value }
      region: metafield(namespace: "custom", key: "region") { value }
      preOrder: metafield(namespace: "custom", key: "pre_order") { value }
      backorder: metafield(namespace: "custom", key: "backorder") { value }
      mediaReleaseDate: metafield(namespace: "custom", key: "media_release_date") { value }
      variants(first: 25) {
        nodes {
          id
          title
          sku
          barcode
          price
          inventoryPolicy
          inventoryQuantity
          region: metafield(namespace: "custom", key: "region") { value }
        }
      }
    }
  }
}
"""

VARIANTS_BULK_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id inventoryPolicy }
    userErrors { field message }
  }
}
"""


@dataclass
class PolicyDecision:
    product_id: str
    title: str
    handle: str
    studio: str
    variant_id: str
    variant_title: str
    sku: str
    barcode: str
    shopify_qty: Optional[int]
    inventory_policy: str
    pre_order: bool
    media_release_date: str
    release_variant_id: str = ""
    resolution_method: str = ""
    supplier_id: str = ""
    supplier: str = ""
    supplier_availability: str = ""
    supplier_qty: Optional[int] = None
    supplier_freshness: str = ""
    supplier_observed_at: str = ""
    all_supplier_states: str = ""
    action: str = ACTION_NO_CHANGE
    reason: str = ""
    apply_status: str = ""
    apply_error: str = ""
    normalized_studio: str = ""
    backorder: bool = False
    tape_on_hand: Optional[int] = None
    tape_committed: Optional[int] = None
    tape_available: Optional[int] = None
    proposed_inventory_policy: str = ""
    safety_classification: str = ""
    data_quality: str = ""
    mapped_suppliers: str = ""
    available_suppliers: str = ""
    legacy_action: str = ""
    legacy_reason: str = ""
    product_status: str = ""
    region_raw: str = ""
    normalized_region: str = ""
    shopify_price: Optional[float] = None
    supplier_cost_gbp: Optional[float] = None


@dataclass
class SyncSummary:
    products_scanned: int = 0
    arrow_products: int = 0
    eligible_products: int = 0
    variants_examined: int = 0
    zero_stock_variants: int = 0
    set_continue: int = 0
    set_deny: int = 0
    no_change: int = 0
    skipped: int = 0
    applied_ok: int = 0
    applied_failed: int = 0
    dry_run: bool = True
    csv_path: str = ""
    json_path: str = ""
    labels: List[str] = field(default_factory=list)
    extra_continue_protections: bool = False
    arrow_decision_mismatches: int = 0


def supplier_is_usable(evaluated: Sequence[dict[str, Any]]) -> bool:
    """True when a wholesale offer is available and not stale."""
    for e in evaluated:
        sid = str(e.get("supplier_id") or "").strip().lower()
        if sid and sid not in WHOLESALE_SUPPLIER_IDS:
            continue
        if e.get("api_status") != "available":
            continue
        fresh = str(e.get("freshness") or "")
        if fresh in {"fresh", "aging"}:
            return True
        # Catalog-fallback path may only have unknown freshness; require qty > 0.
        if fresh == "unknown" and (e.get("qty") or 0) > 0:
            return True
    return False


def supplier_confirmed_unavailable(evaluated: Sequence[dict[str, Any]]) -> bool:
    wholesale = [
        e
        for e in evaluated
        if str(e.get("supplier_id") or "").strip().lower() in WHOLESALE_SUPPLIER_IDS
        or not str(e.get("supplier_id") or "").strip()
    ]
    pool = wholesale or list(evaluated)
    if not pool:
        return False
    return all(
        e.get("api_status") == "unavailable" and e.get("freshness") in {"fresh", "aging"}
        for e in pool
    )


def classify_zero_stock_policy(
    *,
    inventory_policy: str,
    shopify_qty: Optional[int],
    supplier_usable: bool,
    confirmed_unavailable: bool,
    has_any_offer: bool,
    protect_future_preorder: bool,
) -> tuple[str, str]:
    """
    Pure A/B classifier (Arrow-compatible).

    Returns (action, reason). Does not apply extra backorder/preorder-flag overlays.
    """
    policy = (inventory_policy or "").upper()
    if shopify_qty is None:
        return ACTION_SKIP, "shopify_qty_unknown"
    if shopify_qty != 0:
        return ACTION_NO_CHANGE, "shopify_qty_not_zero"

    if policy == "DENY":
        if supplier_usable:
            return ACTION_SET_CONTINUE, "deny_zero_stock_supplier_available"
        return ACTION_NO_CHANGE, "deny_zero_stock_no_usable_supplier"

    if policy == "CONTINUE":
        if protect_future_preorder:
            return ACTION_SKIP, "valid_future_preorder_protected"
        if supplier_usable:
            return ACTION_NO_CHANGE, "continue_zero_stock_supplier_available"
        if confirmed_unavailable:
            return ACTION_SET_DENY, "continue_zero_stock_supplier_unavailable"
        if not has_any_offer:
            return ACTION_SET_DENY, "continue_zero_stock_no_supplier_offer"
        return ACTION_SKIP, "continue_zero_stock_supplier_ambiguous"

    return ACTION_SKIP, f"unsupported_policy_{policy or 'blank'}"


def classify_arrow_zero_stock_policy(
    *,
    inventory_policy: str,
    shopify_qty: Optional[int],
    supplier_usable: bool,
    confirmed_unavailable: bool,
    has_any_offer: bool,
    protect_future_preorder: bool,
) -> tuple[str, str]:
    """Arrow-compatible alias of the shared classifier. Signature unchanged."""
    return classify_zero_stock_policy(
        inventory_policy=inventory_policy,
        shopify_qty=shopify_qty,
        supplier_usable=supplier_usable,
        confirmed_unavailable=confirmed_unavailable,
        has_any_offer=has_any_offer,
        protect_future_preorder=protect_future_preorder,
    )


def apply_extra_continue_protections(
    *,
    action: str,
    reason: str,
    pre_order: bool,
    backorder: bool,
    protect_future_preorder: bool,
) -> tuple[str, str, str]:
    """
    Overlay that never flips CONTINUE→DENY for preorder/backorder workflows.

    Returns (action, reason, safety_classification_hint).
    """
    if action != ACTION_SET_DENY:
        return action, reason, ""
    if backorder:
        return ACTION_SKIP, "backorder_protected", SAFETY_BACKORDER
    if pre_order:
        tag = "valid_future_preorder_protected" if protect_future_preorder else "preorder_flag_protected"
        return ACTION_SKIP, tag, SAFETY_PREORDER
    return action, reason, ""


def safety_for_decision(
    *,
    action: str,
    reason: str,
    inventory_policy: str,
    shopify_qty: Optional[int],
    pre_order: bool,
    backorder: bool,
    confirmed_unavailable: bool,
    has_any_offer: bool,
) -> str:
    policy = (inventory_policy or "").upper()
    if shopify_qty is not None and shopify_qty != 0:
        return SAFETY_TAPE_IN_STOCK
    if action == ACTION_SET_CONTINUE:
        return SAFETY_DENY_TO_CONTINUE
    if action == ACTION_SET_DENY:
        if confirmed_unavailable:
            return SAFETY_SAFE_UNAVAILABLE
        if not has_any_offer:
            return SAFETY_MANUAL
        return SAFETY_SAFE_UNAVAILABLE
    if action == ACTION_SKIP:
        if "backorder" in reason:
            return SAFETY_BACKORDER
        if "preorder" in reason:
            return SAFETY_PREORDER
        if "ambiguous" in reason:
            return SAFETY_AMBIGUOUS
        if "region" in reason:
            return SAFETY_REGION_EXCLUDED
        return SAFETY_SKIP_OTHER
    if action == ACTION_NO_CHANGE:
        if policy == "CONTINUE" and shopify_qty == 0:
            return SAFETY_ALREADY_CORRECT
        if policy == "DENY" and shopify_qty == 0:
            return SAFETY_ALREADY_CORRECT
        return SAFETY_ALREADY_CORRECT
    return SAFETY_SKIP_OTHER


def proposed_policy_for(action: str, current_policy: str) -> str:
    if action == ACTION_SET_CONTINUE:
        return "CONTINUE"
    if action == ACTION_SET_DENY:
        return "DENY"
    return (current_policy or "").upper()


def _chunked(items: Sequence[str], size: int = 200) -> Iterable[Sequence[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _graphql_with_retry(
    client: ShopifyClient,
    query: str,
    variables: dict[str, Any],
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    tries = 0
    while True:
        tries += 1
        try:
            return client.graphql(query, variables)
        except Exception as exc:
            if "THROTTLED" in str(exc) and tries < 8:
                sleep(min(2 * tries, 10))
                continue
            raise


def iter_active_eligible_products(
    client: ShopifyClient,
    *,
    product_query: str = "status:active",
    labels: Sequence[str] = DEFAULT_ELIGIBLE_STUDIO_LABELS,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[int, List[dict[str, Any]]]:
    """Return (products_scanned, eligible_product_nodes) filtered by custom.studio."""
    out: List[dict[str, Any]] = []
    scanned = 0
    cursor: Optional[str] = None
    while True:
        data = _graphql_with_retry(
            client,
            PRODUCTS_QUERY,
            {"cursor": cursor, "q": product_query},
            sleep=sleep,
        )
        block = data["products"]
        for node in block.get("nodes") or []:
            scanned += 1
            studio = clean_text((node.get("studio") or {}).get("value")) or ""
            if is_eligible_studio(studio, labels):
                out.append(node)
        page = block.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        sleep(0.12)
    return scanned, out


def iter_active_arrow_products(
    client: ShopifyClient,
    *,
    product_query: str = "status:active",
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[int, List[dict[str, Any]]]:
    """Return (products_scanned, arrow_product_nodes)."""
    return iter_active_eligible_products(
        client,
        product_query=product_query,
        labels=ARROW_ONLY_LABELS,
        sleep=sleep,
    )


def _eval_offers(
    offers: Sequence[dict[str, Any]],
    *,
    suppliers: Dict[str, str],
    now: datetime,
) -> List[dict[str, Any]]:
    out: List[dict[str, Any]] = []
    for o in offers:
        sid = str(o.get("supplier_id") or "").strip().lower()
        fh, ah = freshness_hours_for_supplier(sid)
        fres = derive_feed_freshness(
            last_seen_at=o.get("last_seen_at"),
            source_feed_at=o.get("source_feed_at"),
            pipeline_completed_at=o.get("pipeline_completed_at"),
            now=now,
            fresh_max_hours=fh,
            aging_max_hours=ah,
        )
        api_status, _last_known = map_supplier_api_availability(
            offer_status=str(o.get("availability_status") or "unknown"),
            feed_freshness=fres.status,
        )
        try:
            qty = int(o["reported_quantity"]) if o.get("reported_quantity") is not None else None
        except (TypeError, ValueError):
            qty = None
        try:
            cost = float(o["unit_cost"]) if o.get("unit_cost") is not None else None
        except (TypeError, ValueError):
            cost = None
        out.append(
            {
                "supplier_id": sid,
                "supplier": suppliers.get(sid, sid),
                "supplier_sku": o.get("supplier_sku"),
                "api_status": api_status,
                "freshness": fres.status,
                "qty": qty,
                "unit_cost": cost,
                "observed_at": o.get("last_seen_at") or o.get("source_feed_at") or "",
            }
        )
    return out


def _load_supplier_context(
    supabase: Any,
    *,
    variant_ids: Sequence[str],
    barcodes: Sequence[str],
) -> dict[str, Any]:
    rsl: Dict[str, str] = {}
    for chunk in _chunked(list(variant_ids)):
        rows = (
            supabase.table("release_shopify_listings")
            .select("shopify_variant_id,release_variant_id")
            .in_("shopify_variant_id", list(chunk))
            .execute()
            .data
            or []
        )
        for row in rows:
            rid = row.get("release_variant_id")
            vid = row.get("shopify_variant_id")
            if vid and rid:
                rsl[str(vid)] = str(rid)

    offers_by_release: Dict[str, List[dict[str, Any]]] = {}
    release_ids = sorted(set(rsl.values()))
    for chunk in _chunked(release_ids):
        rows = (
            supabase.table("supplier_offers")
            .select(
                "id,release_variant_id,catalog_item_id,supplier_id,supplier_sku,raw_barcode,"
                "availability_status,reported_quantity,quantity_is_exact,unit_cost,currency,"
                "last_seen_at,source_feed_at,pipeline_completed_at,active"
            )
            .in_("release_variant_id", list(chunk))
            .eq("active", True)
            .execute()
            .data
            or []
        )
        for row in rows:
            rid = str(row.get("release_variant_id") or "")
            if rid:
                offers_by_release.setdefault(rid, []).append(row)

    offers_by_barcode: Dict[str, List[dict[str, Any]]] = {}
    for chunk in _chunked(list(barcodes)):
        rows = (
            supabase.table("supplier_offers")
            .select(
                "id,release_variant_id,catalog_item_id,supplier_id,supplier_sku,raw_barcode,"
                "availability_status,reported_quantity,quantity_is_exact,unit_cost,currency,"
                "last_seen_at,source_feed_at,pipeline_completed_at,active"
            )
            .in_("raw_barcode", list(chunk))
            .eq("active", True)
            .execute()
            .data
            or []
        )
        for row in rows:
            bc = clean_text(row.get("raw_barcode")) or ""
            if bc:
                offers_by_barcode.setdefault(bc, []).append(row)

    tape_by_release: Dict[str, dict[str, int]] = {}
    for chunk in _chunked(release_ids):
        try:
            rows = (
                supabase.table("tape_inventory_levels")
                .select("release_variant_id,on_hand,committed,available")
                .in_("release_variant_id", list(chunk))
                .execute()
                .data
                or []
            )
        except Exception:
            rows = []
        for row in rows:
            rid = str(row.get("release_variant_id") or "")
            if not rid:
                continue
            agg = tape_by_release.setdefault(rid, {"on_hand": 0, "committed": 0, "available": 0})
            agg["on_hand"] += int(row.get("on_hand") or 0)
            agg["committed"] += int(row.get("committed") or 0)
            agg["available"] += int(row.get("available") or 0)

    suppliers: Dict[str, str] = {}
    try:
        for s in supabase.table("suppliers").select("id,display_name").execute().data or []:
            suppliers[str(s["id"])] = s.get("display_name") or s["id"]
    except Exception:
        pass

    return {
        "rsl": rsl,
        "offers_by_release": offers_by_release,
        "offers_by_barcode": offers_by_barcode,
        "tape_by_release": tape_by_release,
        "suppliers": suppliers,
    }


def _data_quality_flags(
    *,
    row: dict[str, Any],
    rid: Optional[str],
    evaluated: Sequence[dict[str, Any]],
    usable: bool,
    tape: dict[str, Any],
) -> str:
    flags: List[str] = []
    if not row.get("studio"):
        flags.append("missing_custom_studio")
    if not row.get("barcode"):
        flags.append("missing_barcode")
    if not rid:
        flags.append("missing_release_variant_mapping")
        if row.get("shopify_qty") == 0:
            flags.append("missing_shopify_mapping")
    elif row.get("shopify_qty") == 0 and not evaluated:
        flags.append("supplier_mapping_but_no_current_offer")
    if evaluated and not usable and all(
        (e.get("qty") is None) or (e.get("qty") is not None and int(e.get("qty") or 0) < 0)
        for e in evaluated
        if e.get("api_status") == "available"
    ) and any(e.get("api_status") == "available" for e in evaluated):
        flags.append("supplier_offer_invalid_quantity")
    if any(e.get("supplier_id") for e in evaluated) and not any(
        e.get("api_status") == "available" for e in evaluated
    ):
        flags.append("supplier_mapping_no_current_available_offer")
    if any(e.get("freshness") == "stale" for e in evaluated):
        flags.append("stale_supplier_data")
    available_count = sum(
        1
        for e in evaluated
        if e.get("api_status") == "available"
        and (
            e.get("freshness") in {"fresh", "aging"}
            or (e.get("freshness") == "unknown" and (e.get("qty") or 0) > 0)
        )
    )
    if available_count > 1:
        flags.append("multiple_available_suppliers")
    shopify_qty = row.get("shopify_qty")
    tape_avail = tape.get("available") if tape else None
    if shopify_qty is not None and tape_avail is not None and int(shopify_qty) != int(tape_avail):
        flags.append("shopify_qty_inconsistent_with_ii_tape")
    region = str(row.get("normalized_region") or "")
    if region == "A":
        flags.append("region_a_us_release")
    elif region != "B":
        flags.append("region_missing_or_unknown")
    return ";".join(flags)


def build_decisions_for_products(
    products: Sequence[dict[str, Any]],
    *,
    supabase: Any,
    now: Optional[datetime] = None,
    extra_continue_protections: bool = False,
    require_region_b: bool = True,
) -> List[PolicyDecision]:
    now = now or datetime.now(timezone.utc)
    today = sydney_today()
    flat: List[dict[str, Any]] = []
    for p in products:
        studio = clean_text((p.get("studio") or {}).get("value")) or ""
        product_region = clean_text((p.get("region") or {}).get("value")) or ""
        product_status = (clean_text(p.get("status")) or "").upper()
        pre = parse_shopify_bool_metafield((p.get("preOrder") or {}).get("value"))
        back = parse_shopify_bool_metafield((p.get("backorder") or {}).get("value"))
        media_raw = clean_text((p.get("mediaReleaseDate") or {}).get("value")) or ""
        for v in ((p.get("variants") or {}).get("nodes") or []):
            try:
                qty = int(v["inventoryQuantity"]) if v.get("inventoryQuantity") is not None else None
            except (TypeError, ValueError):
                qty = None
            try:
                price = float(v["price"]) if v.get("price") is not None else None
            except (TypeError, ValueError):
                price = None
            variant_region = clean_text((v.get("region") or {}).get("value")) or ""
            region_code = resolve_variant_region(
                product_region=product_region, variant_region=variant_region
            )
            region_raw = variant_region or product_region
            flat.append(
                {
                    "product_id": p.get("id") or "",
                    "title": p.get("title") or "",
                    "handle": p.get("handle") or "",
                    "studio": studio,
                    "product_status": product_status,
                    "pre_order": bool(pre),
                    "backorder": bool(back),
                    "media_release_date": media_raw,
                    "variant_id": v.get("id") or "",
                    "variant_title": v.get("title") or "",
                    "sku": clean_text(v.get("sku")) or "",
                    "barcode": clean_text(v.get("barcode")) or "",
                    "inventory_policy": (clean_text(v.get("inventoryPolicy")) or "").upper(),
                    "shopify_qty": qty,
                    "shopify_price": price,
                    "region_raw": region_raw,
                    "normalized_region": region_code,
                }
            )

    ctx = _load_supplier_context(
        supabase,
        variant_ids=[r["variant_id"] for r in flat if r["variant_id"]],
        barcodes=sorted({r["barcode"] for r in flat if r["barcode"]}),
    )

    decisions: List[PolicyDecision] = []
    for row in flat:
        vid = row["variant_id"]
        rid = ctx["rsl"].get(vid)
        offers: List[dict[str, Any]] = []
        resolution = ""
        if rid:
            offers = list(ctx["offers_by_release"].get(str(rid), []))
            if offers:
                resolution = "release_shopify_listings"
        if not offers and row["barcode"]:
            offers = list(ctx["offers_by_barcode"].get(row["barcode"], []))
            if offers:
                resolution = "supplier_offers_by_barcode"

        evaluated = _eval_offers(offers, suppliers=ctx["suppliers"], now=now)
        usable = supplier_is_usable(evaluated)
        confirmed_unavail = supplier_confirmed_unavailable(evaluated)
        best = next(
            (
                e
                for e in evaluated
                if e.get("api_status") == "available"
                and (
                    e.get("freshness") in {"fresh", "aging"}
                    or (e.get("freshness") == "unknown" and (e.get("qty") or 0) > 0)
                )
            ),
            evaluated[0] if evaluated else {},
        )
        protect = is_valid_future_preorder(
            bool(row["pre_order"]),
            parse_iso_date(row["media_release_date"]),
            today,
        )
        legacy_action, legacy_reason = classify_arrow_zero_stock_policy(
            inventory_policy=row["inventory_policy"],
            shopify_qty=row["shopify_qty"],
            supplier_usable=usable,
            confirmed_unavailable=confirmed_unavail,
            has_any_offer=bool(evaluated),
            protect_future_preorder=protect,
        )
        action, reason = legacy_action, legacy_reason
        if require_region_b and row.get("normalized_region") != "B":
            if row.get("normalized_region") == "A":
                action, reason = ACTION_SKIP, "region_a_us_excluded"
            else:
                action, reason = ACTION_SKIP, "region_missing_or_not_b"
        elif extra_continue_protections:
            action, reason, _hint = apply_extra_continue_protections(
                action=action,
                reason=reason,
                pre_order=bool(row["pre_order"]),
                backorder=bool(row["backorder"]),
                protect_future_preorder=protect,
            )
        tape = ctx["tape_by_release"].get(str(rid or "")) or {}
        safety = safety_for_decision(
            action=action,
            reason=reason,
            inventory_policy=row["inventory_policy"],
            shopify_qty=row["shopify_qty"],
            pre_order=bool(row["pre_order"]),
            backorder=bool(row["backorder"]),
            confirmed_unavailable=confirmed_unavail,
            has_any_offer=bool(evaluated),
        )
        mapped = sorted({str(e.get("supplier_id") or "") for e in evaluated if e.get("supplier_id")})
        available = sorted(
            {
                str(e.get("supplier_id") or "")
                for e in evaluated
                if e.get("api_status") == "available"
                and (
                    e.get("freshness") in {"fresh", "aging"}
                    or (e.get("freshness") == "unknown" and (e.get("qty") or 0) > 0)
                )
            }
        )
        decisions.append(
            PolicyDecision(
                product_id=row["product_id"],
                title=row["title"],
                handle=row["handle"],
                studio=row["studio"],
                variant_id=vid,
                variant_title=row["variant_title"],
                sku=row["sku"],
                barcode=row["barcode"],
                shopify_qty=row["shopify_qty"],
                inventory_policy=row["inventory_policy"],
                pre_order=bool(row["pre_order"]),
                media_release_date=row["media_release_date"],
                release_variant_id=str(rid or ""),
                resolution_method=resolution or ("unresolved" if row["shopify_qty"] == 0 else ""),
                supplier_id=str(best.get("supplier_id") or ""),
                supplier=str(best.get("supplier") or ""),
                supplier_availability=str(best.get("api_status") or ""),
                supplier_qty=best.get("qty"),
                supplier_freshness=str(best.get("freshness") or ""),
                supplier_observed_at=str(best.get("observed_at") or ""),
                all_supplier_states="; ".join(
                    f"{e.get('supplier_id')}:{e.get('api_status')}:{e.get('freshness')}:qty={e.get('qty')}"
                    for e in evaluated
                ),
                action=action,
                reason=reason,
                normalized_studio=normalize_studio_label(row["studio"]),
                backorder=bool(row["backorder"]),
                tape_on_hand=tape.get("on_hand") if tape else None,
                tape_committed=tape.get("committed") if tape else None,
                tape_available=tape.get("available") if tape else None,
                proposed_inventory_policy=proposed_policy_for(action, row["inventory_policy"]),
                safety_classification=safety,
                data_quality=_data_quality_flags(
                    row=row,
                    rid=rid,
                    evaluated=evaluated,
                    usable=usable,
                    tape=tape,
                ),
                mapped_suppliers=",".join(mapped),
                available_suppliers=",".join(available),
                legacy_action=legacy_action,
                legacy_reason=legacy_reason,
                product_status=str(row.get("product_status") or ""),
                region_raw=str(row.get("region_raw") or ""),
                normalized_region=str(row.get("normalized_region") or ""),
                shopify_price=row.get("shopify_price"),
                supplier_cost_gbp=best.get("unit_cost"),
            )
        )
    return decisions


def apply_policy_decisions(
    client: ShopifyClient,
    decisions: Sequence[PolicyDecision],
    *,
    dry_run: bool,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[int, int]:
    ok = 0
    failed = 0
    targets = [d for d in decisions if d.action in {ACTION_SET_CONTINUE, ACTION_SET_DENY}]
    for d in targets:
        new_policy = "CONTINUE" if d.action == ACTION_SET_CONTINUE else "DENY"
        if dry_run:
            d.apply_status = "dry_run"
            ok += 1
            continue
        try:
            data = _graphql_with_retry(
                client,
                VARIANTS_BULK_UPDATE,
                {
                    "productId": d.product_id,
                    "variants": [{"id": d.variant_id, "inventoryPolicy": new_policy}],
                },
                sleep=sleep,
            )
            block = data.get("productVariantsBulkUpdate") or {}
            errs = block.get("userErrors") or []
            if errs:
                raise RuntimeError(str(errs))
            d.apply_status = "ok"
            d.inventory_policy = new_policy
            ok += 1
            sleep(0.08)
        except Exception as exc:
            d.apply_status = "failed"
            d.apply_error = str(exc)
            failed += 1
            sleep(0.2)
    return ok, failed


def write_decisions_csv(path: Path, decisions: Sequence[PolicyDecision]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(asdict(decisions[0]).keys()) if decisions else [f.name for f in PolicyDecision.__dataclass_fields__.values()]  # type: ignore[attr-defined]
    # dataclass fields order is stable
    fields = list(PolicyDecision.__dataclass_fields__.keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for d in decisions:
            writer.writerow(asdict(d))


def write_decisions_json(
    path: Path,
    decisions: Sequence[PolicyDecision],
    *,
    summary: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "summary": summary,
        "decisions": [asdict(d) for d in decisions],
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def summarize_decisions(
    decisions: Sequence[PolicyDecision],
    *,
    products_scanned: int,
    arrow_products: int,
    dry_run: bool,
    csv_path: str,
    applied_ok: int = 0,
    applied_failed: int = 0,
    eligible_products: Optional[int] = None,
    json_path: str = "",
    labels: Optional[Sequence[str]] = None,
    extra_continue_protections: bool = False,
) -> SyncSummary:
    summary = SyncSummary(
        products_scanned=products_scanned,
        arrow_products=arrow_products,
        eligible_products=eligible_products if eligible_products is not None else arrow_products,
        variants_examined=len(decisions),
        zero_stock_variants=sum(1 for d in decisions if d.shopify_qty == 0),
        dry_run=dry_run,
        csv_path=csv_path,
        json_path=json_path,
        labels=list(labels or ARROW_ONLY_LABELS),
        extra_continue_protections=extra_continue_protections,
        applied_ok=applied_ok,
        applied_failed=applied_failed,
        arrow_decision_mismatches=sum(
            1
            for d in decisions
            if d.normalized_studio == "Arrow"
            and d.normalized_region == "B"
            and d.legacy_action
            and d.action != d.legacy_action
        ),
    )
    for d in decisions:
        if d.action == ACTION_SET_CONTINUE:
            summary.set_continue += 1
        elif d.action == ACTION_SET_DENY:
            summary.set_deny += 1
        elif d.action == ACTION_SKIP:
            summary.skipped += 1
        else:
            summary.no_change += 1
    return summary


def run_studio_inventory_policy_sync(
    *,
    env_file: str = ".env",
    api_version: str = "2026-04",
    apply: bool = False,
    product_query: str = "status:active",
    csv_path: Optional[str] = None,
    json_path: Optional[str] = None,
    client: Optional[ShopifyClient] = None,
    supabase: Any = None,
    labels: Sequence[str] = DEFAULT_ELIGIBLE_STUDIO_LABELS,
    extra_continue_protections: bool = False,
    require_region_b: bool = True,
) -> tuple[List[PolicyDecision], SyncSummary]:
    path = Path(env_file)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[2] / env_file
    load_dotenv(path, override=True)

    shopify = client or ShopifyClient(api_version=api_version)
    if supabase is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")
        if not url or not key:
            raise SystemExit("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
        supabase = create_client(url, key)

    scanned, nodes = iter_active_eligible_products(
        shopify, product_query=product_query, labels=labels
    )
    arrow_products = sum(
        1 for n in nodes if is_arrow_studio(clean_text((n.get("studio") or {}).get("value")) or "")
    )

    decisions = build_decisions_for_products(
        nodes,
        supabase=supabase,
        extra_continue_protections=extra_continue_protections,
        require_region_b=require_region_b,
    )
    applied_ok, applied_failed = apply_policy_decisions(
        shopify,
        decisions,
        dry_run=not apply,
    )

    out = Path(
        csv_path
        or (
            Path(__file__).resolve().parents[2]
            / "tmp"
            / f"studio_inventory_policy_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
    )
    write_decisions_csv(out, decisions)
    summary = summarize_decisions(
        decisions,
        products_scanned=scanned,
        arrow_products=arrow_products,
        eligible_products=len(nodes),
        dry_run=not apply,
        csv_path=str(out),
        json_path=str(json_path or ""),
        labels=labels,
        extra_continue_protections=extra_continue_protections,
        applied_ok=applied_ok,
        applied_failed=applied_failed,
    )
    if json_path:
        jp = Path(json_path)
        write_decisions_json(jp, decisions, summary=asdict(summary))
        summary.json_path = str(jp)
    return decisions, summary


def run_arrow_inventory_policy_sync(
    *,
    env_file: str = ".env",
    api_version: str = "2026-04",
    apply: bool = False,
    product_query: str = "status:active",
    csv_path: Optional[str] = None,
    client: Optional[ShopifyClient] = None,
    supabase: Any = None,
) -> tuple[List[PolicyDecision], SyncSummary]:
    """Production Arrow wrapper: Arrow labels only, no extra CONTINUE overlays."""
    return run_studio_inventory_policy_sync(
        env_file=env_file,
        api_version=api_version,
        apply=apply,
        product_query=product_query,
        csv_path=csv_path,
        client=client,
        supabase=supabase,
        labels=ARROW_ONLY_LABELS,
        extra_continue_protections=False,
        require_region_b=True,
    )
