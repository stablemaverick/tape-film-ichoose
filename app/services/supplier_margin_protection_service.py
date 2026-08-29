"""
Supplier replacement-cost monitoring and 28% ex-GST margin exposure reporting.

Daily stock-sync step 04d: evaluates replacement economics from supplier intelligence
and reports margin exposure. Does NOT mutate the existing Shopify catalogue unless
an explicit scoped allowlist is provided with apply enabled.

New product creation continues to use catalog_shopify_publish_service pricing.
"""

from __future__ import annotations

import csv
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client
from app.helpers.text_helpers import clean_text
from app.rules.pricing_rules import (
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    classify_supplier_gbp_cost_movement,
    effective_pricing_assumptions,
    exact_ex_gst_margin_ok,
    exact_ex_gst_margin_ratio,
    log_pricing_assumptions,
    replacement_landed_cost_aud,
)
from app.services.arrow_inventory_policy_sync_service import (
    WHOLESALE_SUPPLIER_IDS,
    _chunked,
    _eval_offers,
    _load_supplier_context,
    supplier_is_usable,
)
from app.services.shopify_release_dual_write_service import shopify_ii_dual_write_exclusion_reason
from app.services.stock_availability_service import pick_preferred_supplier

logger = logging.getLogger(__name__)

# Monitoring actions (existing catalogue — advisory only)
MONITOR_MARGIN_SAFE = "MONITOR_MARGIN_SAFE"
MONITOR_COST_DOWN = "MONITOR_COST_DOWN"
MONITOR_PRICE_INCREASE_INDICATED = "MONITOR_PRICE_INCREASE_INDICATED"
REVIEW_COST_ANOMALY = "REVIEW_COST_ANOMALY"
PREFERRED_SUPPLIER_CHANGED = "PREFERRED_SUPPLIER_CHANGED"
NO_CURRENT_SUPPLIER = "NO_CURRENT_SUPPLIER"
STALE_SUPPLIER = "STALE_SUPPLIER"
AMBIGUOUS_MAPPING = "AMBIGUOUS_MAPPING"
OUT_OF_SCOPE = "OUT_OF_SCOPE"
INVALID_COST = "INVALID_COST"
ERROR = "ERROR"

VARIANT_PRICE_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id price }
    userErrors { field message }
  }
}
"""

LIVE_VARIANT_PRICE = """
query LiveVariantPrice($id: ID!) {
  node(id: $id) {
    ... on ProductVariant {
      id
      price
      product { id }
    }
  }
}
"""


@dataclass(frozen=True)
class ApplyAllowlist:
    """Explicit population permitted for future scoped price apply (mutate narrowly)."""

    variant_ids: FrozenSet[str] = frozenset()
    barcodes: FrozenSet[str] = frozenset()

    def matches(self, row: Mapping[str, Any]) -> bool:
        """Match allowlisted rows.

        When any variant IDs are present, require variant-ID match so barcode-only
        hits cannot select duplicate-barcode Shopify siblings.
        """
        vid = clean_text(row.get("variant_id")) or ""
        bc = clean_text(row.get("barcode")) or ""
        if self.variant_ids:
            return bool(vid) and vid in self.variant_ids
        return bool(bc) and bc in self.barcodes

    @property
    def empty(self) -> bool:
        return not self.variant_ids and not self.barcodes


@dataclass
class ProtectionSummary:
    evaluated: int = 0
    unchanged: int = 0
    cost_increased: int = 0
    cost_decreased: int = 0
    significant_increases: int = 0
    significant_decreases: int = 0
    preferred_supplier_changed: int = 0
    margin_safe: int = 0
    below_floor_indicated: int = 0
    price_increases_applied: int = 0
    price_increases_failed: int = 0
    review_anomaly: int = 0
    no_current_supplier: int = 0
    stale_supplier: int = 0
    ambiguous_mapping: int = 0
    invalid_cost: int = 0
    out_of_scope: int = 0
    errors: int = 0
    monitoring_only: bool = True
    movement_csv: str = ""
    monitor_csv: str = ""
    ii_reconcile: str = ""
    alerts: List[str] = field(default_factory=list)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _margin_pct(price: Optional[float], landed: Optional[float]) -> Optional[float]:
    ratio = exact_ex_gst_margin_ratio(price, landed)
    if ratio is None:
        return None
    return float(ratio * Decimal("100"))


def parse_apply_allowlist(
    *,
    variant_ids: Optional[Sequence[str]] = None,
    barcodes: Optional[Sequence[str]] = None,
    barcodes_csv: Optional[str] = None,
) -> ApplyAllowlist:
    """Build allowlist from CLI args, env, or CSV (barcode column or first column)."""
    vids: set[str] = set()
    bcs: set[str] = set()
    env_barcodes = (os.getenv("SUPPLIER_MARGIN_PROTECTION_ALLOWLIST_BARCODES") or "").strip()
    if env_barcodes:
        bcs.update(b.strip() for b in env_barcodes.split(",") if b.strip())
    env_variants = (os.getenv("SUPPLIER_MARGIN_PROTECTION_ALLOWLIST_VARIANT_IDS") or "").strip()
    if env_variants:
        vids.update(v.strip() for v in env_variants.split(",") if v.strip())
    if variant_ids:
        vids.update(clean_text(v) or "" for v in variant_ids if clean_text(v))
    if barcodes:
        bcs.update(clean_text(b) or "" for b in barcodes if clean_text(b))
    if barcodes_csv:
        path = Path(barcodes_csv)
        if path.is_file():
            with path.open(newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                fields = set(reader.fieldnames or [])
                if "barcode" in fields or "variant_id" in fields:
                    for row in reader:
                        bc = clean_text(row.get("barcode"))
                        if bc:
                            bcs.add(bc)
                        # Prefer variant-ID targeting when present (duplicate-barcode safe).
                        vid = clean_text(row.get("variant_id"))
                        if vid:
                            vids.add(vid)
                else:
                    fh.seek(0)
                    for row in csv.reader(fh):
                        if row and row[0].strip() and row[0] != "barcode":
                            bcs.add(row[0].strip())
    return ApplyAllowlist(variant_ids=frozenset(vids), barcodes=frozenset(bcs))


def to_preferred_pool(evaluated: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    pool = []
    for e in evaluated:
        sid = str(e.get("supplier_id") or "").strip().lower()
        if sid not in WHOLESALE_SUPPLIER_IDS:
            continue
        pool.append(
            {
                "supplier_id": sid,
                "supplier": e.get("supplier"),
                "supplier_sku": e.get("supplier_sku"),
                "availability_status": e.get("api_status"),
                "is_stale": str(e.get("freshness") or "") == "stale",
                "unit_cost": e.get("unit_cost"),
                "qty": e.get("qty"),
                "freshness": e.get("freshness"),
                "observed_at": e.get("observed_at"),
                "offer_id": e.get("offer_id"),
            }
        )
    return pool


def build_previous_pool(
    offers: Sequence[dict[str, Any]],
    prev_obs_by_offer: Dict[str, dict[str, Any]],
    *,
    suppliers: Dict[str, str],
    now: datetime,
) -> list[dict[str, Any]]:
    synthetic: list[dict[str, Any]] = []
    for o in offers:
        oid = str(o.get("id") or "")
        prev = prev_obs_by_offer.get(oid)
        if not prev:
            continue
        sid = str(o.get("supplier_id") or "").strip().lower()
        if sid not in WHOLESALE_SUPPLIER_IDS:
            continue
        row = dict(o)
        row["availability_status"] = prev.get("availability_status") or o.get("availability_status")
        row["reported_quantity"] = prev.get("reported_quantity")
        row["unit_cost"] = prev.get("unit_cost")
        row["last_seen_at"] = prev.get("observed_at")
        row["source_feed_at"] = prev.get("source_feed_at")
        row["pipeline_completed_at"] = prev.get("observed_at")
        synthetic.append(row)
    evaluated = _eval_offers(synthetic, suppliers=suppliers, now=now)
    return to_preferred_pool(evaluated)


def fetch_previous_observations(
    supabase: Any, offer_ids: Sequence[str]
) -> Dict[str, dict[str, Any]]:
    out: Dict[str, dict[str, Any]] = {}
    for chunk in _chunked(list(offer_ids)):
        rows = (
            supabase.table("supplier_offer_observations")
            .select(
                "supplier_offer_id,unit_cost,availability_status,reported_quantity,"
                "observed_at,source_feed_at"
            )
            .in_("supplier_offer_id", list(chunk))
            .order("observed_at", desc=True)
            .execute()
            .data
            or []
        )
        by_offer: Dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            oid = str(r.get("supplier_offer_id") or "")
            if oid:
                by_offer.setdefault(oid, []).append(r)
        for oid, obs_list in by_offer.items():
            if len(obs_list) >= 2:
                out[oid] = obs_list[1]
    return out


def load_eligible_listings(supabase: Any, shop: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    page = 500
    while True:
        batch = (
            supabase.table("shopify_listings")
            .select(
                "shop,shopify_variant_id,shopify_product_id,product_title,variant_title,"
                "barcode,price_amount,inventory_policy,inventory_quantity,product_status,"
                "product_type,media_format,collection_handles,match_status,catalog_item_id"
            )
            .eq("shop", shop)
            .eq("match_status", "matched")
            .range(offset, offset + page - 1)
            .execute()
            .data
            or []
        )
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows


def _resolve_monitoring_action(
    *,
    margin_ok: bool,
    movement: dict[str, Any],
    preferred_changed: bool,
    shopify_price: Optional[float],
    floor_price: Optional[float],
) -> tuple[str, str]:
    """Map evaluation to monitoring-only action labels (no mutation implied)."""
    direction = movement.get("direction")
    anomalous = movement.get("anomalous")

    if shopify_price is None:
        return ERROR, "missing_shopify_price"

    if preferred_changed and margin_ok:
        if direction == "DOWN":
            return PREFERRED_SUPPLIER_CHANGED, "preferred_supplier_changed;replacement_cost_down;margin_safe"
        return PREFERRED_SUPPLIER_CHANGED, "preferred_supplier_changed;margin_safe"

    if margin_ok:
        if direction == "DOWN":
            return MONITOR_COST_DOWN, "replacement_cost_down;margin_safe"
        return MONITOR_MARGIN_SAFE, "margin_at_or_above_floor"

    if floor_price is not None and shopify_price is not None and floor_price > shopify_price:
        if anomalous:
            return REVIEW_COST_ANOMALY, "extreme_supplier_cost_movement;below_floor_indicated"
        if preferred_changed:
            return MONITOR_PRICE_INCREASE_INDICATED, "preferred_supplier_changed;below_28pct_floor"
        return MONITOR_PRICE_INCREASE_INDICATED, "below_28pct_floor"

    return MONITOR_MARGIN_SAFE, "proposed_floor_not_above_current"


def evaluate_variant_row(
    *,
    listing: dict[str, Any],
    ctx: dict[str, Any],
    prev_obs_by_offer: Dict[str, dict[str, Any]],
    cfg: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    vid = clean_text(listing.get("shopify_variant_id")) or ""
    title = clean_text(listing.get("product_title")) or ""
    barcode = clean_text(listing.get("barcode")) or ""
    shopify_price = _f(listing.get("price_amount"))
    policy = (clean_text(listing.get("inventory_policy")) or "").upper()
    tape_qty = listing.get("inventory_quantity")
    try:
        tape_qty_i = int(tape_qty) if tape_qty is not None else 0
    except (TypeError, ValueError):
        tape_qty_i = 0

    exclusion = shopify_ii_dual_write_exclusion_reason(listing, soundtrack_product_ids=None)
    if exclusion:
        return _result_row(
            listing,
            monitoring_action=OUT_OF_SCOPE,
            reason=exclusion,
            shopify_price=shopify_price,
            tape_qty=tape_qty_i,
            policy=policy,
        )

    rid = ctx["rsl"].get(vid)
    offers: list[dict[str, Any]] = []
    if rid:
        offers = list(ctx["offers_by_release"].get(str(rid), []))
    if not offers and barcode:
        offers = list(ctx["offers_by_barcode"].get(barcode, []))
    if not rid and not offers:
        return _result_row(
            listing,
            monitoring_action=AMBIGUOUS_MAPPING,
            reason="no_release_or_supplier_mapping",
            shopify_price=shopify_price,
            tape_qty=tape_qty_i,
            policy=policy,
        )
    if not offers:
        return _result_row(
            listing,
            monitoring_action=NO_CURRENT_SUPPLIER,
            reason="no_supplier_offers",
            shopify_price=shopify_price,
            tape_qty=tape_qty_i,
            policy=policy,
            release_variant_id=str(rid) if rid else "",
        )

    for o in offers:
        o["offer_id"] = o.get("id")

    evaluated = _eval_offers(offers, suppliers=ctx["suppliers"], now=now)
    if not supplier_is_usable(evaluated):
        stale = any(str(e.get("freshness") or "") == "stale" for e in evaluated)
        return _result_row(
            listing,
            monitoring_action=STALE_SUPPLIER if stale else NO_CURRENT_SUPPLIER,
            reason="supplier_not_usable",
            shopify_price=shopify_price,
            tape_qty=tape_qty_i,
            policy=policy,
            release_variant_id=str(rid) if rid else "",
        )

    current_pool = to_preferred_pool(evaluated)
    preferred = pick_preferred_supplier(current_pool)
    if not preferred or preferred.get("availability_status") != "available":
        return _result_row(
            listing,
            monitoring_action=NO_CURRENT_SUPPLIER,
            reason="no_fresh_preferred_supplier",
            shopify_price=shopify_price,
            tape_qty=tape_qty_i,
            policy=policy,
            release_variant_id=str(rid) if rid else "",
        )
    if preferred.get("is_stale"):
        return _result_row(
            listing,
            monitoring_action=STALE_SUPPLIER,
            reason="preferred_supplier_stale",
            shopify_price=shopify_price,
            tape_qty=tape_qty_i,
            policy=policy,
            release_variant_id=str(rid) if rid else "",
        )

    curr_gbp = _f(preferred.get("unit_cost"))
    if curr_gbp is None or curr_gbp <= 0:
        return _result_row(
            listing,
            monitoring_action=INVALID_COST,
            reason="missing_supplier_gbp_cost",
            shopify_price=shopify_price,
            tape_qty=tape_qty_i,
            policy=policy,
            current_supplier=preferred.get("supplier_id"),
            release_variant_id=str(rid) if rid else "",
        )

    previous_pool = build_previous_pool(
        offers, prev_obs_by_offer, suppliers=ctx["suppliers"], now=now
    )
    prev_preferred = pick_preferred_supplier(previous_pool) if previous_pool else None
    preferred_changed = bool(
        prev_preferred
        and prev_preferred.get("supplier_id") != preferred.get("supplier_id")
    )

    prev_gbp: Optional[float] = None
    movement_kind = "UNCHANGED"
    if preferred_changed and prev_preferred:
        prev_gbp = _f(prev_preferred.get("unit_cost"))
        movement_kind = "PREFERRED_SUPPLIER_CHANGED"
    else:
        offer_id = str(preferred.get("offer_id") or "")
        prev_obs = prev_obs_by_offer.get(offer_id)
        if prev_obs:
            prev_gbp = _f(prev_obs.get("unit_cost"))
            movement_kind = "SAME_SUPPLIER_COST_CHANGE"
        elif prev_preferred and not preferred_changed:
            prev_gbp = _f(prev_preferred.get("unit_cost"))

    gbp_aud = cfg["gbp_aud_rate"]
    markup = cfg["landed_cost_markup"]
    floor_ratio = cfg["margin_floor_ratio"]
    replacement = replacement_landed_cost_aud(curr_gbp, gbp_aud, markup)
    prev_replacement = replacement_landed_cost_aud(prev_gbp, gbp_aud, markup) if prev_gbp else None
    floor_price = calculate_sale_price_with_margin_floor_from_gbp_cost(
        curr_gbp,
        gbp_aud_rate=gbp_aud,
        landed_cost_markup=markup,
        margin_floor_ratio=floor_ratio,
    )
    margin_ok = exact_ex_gst_margin_ok(shopify_price, replacement, margin_floor_ratio=floor_ratio)
    current_margin = _margin_pct(shopify_price, replacement)

    movement = classify_supplier_gbp_cost_movement(
        prev_gbp,
        curr_gbp,
        alert_gbp=cfg["supplier_cost_alert_gbp"],
        alert_pct=cfg["supplier_cost_alert_pct"],
        anomaly_pct=cfg["supplier_cost_anomaly_pct"],
    )

    significant = movement.get("significant")
    anomaly_status = "ANOMALOUS" if movement.get("anomalous") else "OK"

    theoretical_price_delta = None
    if shopify_price is not None and floor_price is not None:
        theoretical_price_delta = round(floor_price - shopify_price, 2)

    monitoring_action, reason = _resolve_monitoring_action(
        margin_ok=margin_ok,
        movement=movement,
        preferred_changed=preferred_changed,
        shopify_price=shopify_price,
        floor_price=floor_price,
    )

    return {
        "title": title,
        "variant_id": vid,
        "product_id": clean_text(listing.get("shopify_product_id")) or "",
        "barcode": barcode,
        "region": "",
        "status": clean_text(listing.get("product_status")) or "",
        "inventoryPolicy": policy,
        "TAPE_qty": tape_qty_i,
        "previous_supplier": (prev_preferred or {}).get("supplier_id") or "",
        "current_supplier": preferred.get("supplier_id") or "",
        "previous_GBP_cost": prev_gbp,
        "current_GBP_cost": curr_gbp,
        "cost_direction": movement.get("direction"),
        "GBP_delta": movement.get("gbp_delta"),
        "cost_delta_pct": movement.get("pct_delta"),
        "significant_movement": significant,
        "movement_kind": movement_kind,
        "preferred_supplier_changed": preferred_changed,
        "supplier_offer_age": preferred.get("observed_at") or "",
        "current_replacement_landed_AUD": replacement,
        "previous_replacement_landed_AUD": prev_replacement,
        "current_Shopify_price": shopify_price,
        "exact_current_margin_pct": current_margin,
        "theoretical_28pct_floor": floor_price,
        "theoretical_price_delta": theoretical_price_delta,
        "anomaly_status": anomaly_status,
        "monitoring_action": monitoring_action,
        "reason": reason,
        "release_variant_id": str(rid) if rid else "",
    }


def _result_row(
    listing: dict[str, Any],
    *,
    monitoring_action: str,
    reason: str,
    shopify_price: Optional[float],
    tape_qty: int,
    policy: str,
    current_supplier: str = "",
    release_variant_id: str = "",
) -> dict[str, Any]:
    return {
        "title": clean_text(listing.get("product_title")) or "",
        "variant_id": clean_text(listing.get("shopify_variant_id")) or "",
        "product_id": clean_text(listing.get("shopify_product_id")) or "",
        "barcode": clean_text(listing.get("barcode")) or "",
        "region": "",
        "status": clean_text(listing.get("product_status")) or "",
        "inventoryPolicy": policy,
        "TAPE_qty": tape_qty,
        "previous_supplier": "",
        "current_supplier": current_supplier,
        "previous_GBP_cost": None,
        "current_GBP_cost": None,
        "cost_direction": None,
        "GBP_delta": None,
        "cost_delta_pct": None,
        "significant_movement": False,
        "movement_kind": "",
        "preferred_supplier_changed": False,
        "supplier_offer_age": "",
        "current_replacement_landed_AUD": None,
        "previous_replacement_landed_AUD": None,
        "current_Shopify_price": shopify_price,
        "exact_current_margin_pct": None,
        "theoretical_28pct_floor": None,
        "theoretical_price_delta": None,
        "anomaly_status": "",
        "monitoring_action": monitoring_action,
        "reason": reason,
        "release_variant_id": release_variant_id,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for r in rows:
        for k in r:
            if k not in fields:
                fields.append(k)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields or ["empty"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _apply_price_updates(
    client: ShopifyClient,
    rows: list[dict[str, Any]],
    *,
    sleep_s: float = 0.12,
) -> Tuple[int, int]:
    ok = 0
    failed = 0
    for row in rows:
        try:
            live = client.graphql(LIVE_VARIANT_PRICE, {"id": row["variant_id"]})
            node = live.get("node") or {}
            live_price = _f(node.get("price"))
            expected_current = _f(row.get("current_Shopify_price"))
            if live_price is not None and expected_current is not None:
                if abs(live_price - expected_current) > 0.011:
                    logger.warning(
                        "Skip scoped apply stale read variant=%s live=%s expected=%s",
                        row["variant_id"],
                        live_price,
                        expected_current,
                    )
                    failed += 1
                    continue
            product_id = row.get("product_id") or (node.get("product") or {}).get("id")
            proposed = _f(row.get("theoretical_28pct_floor"))
            if not product_id or proposed is None:
                failed += 1
                continue
            data = client.graphql(
                VARIANT_PRICE_UPDATE,
                {
                    "productId": product_id,
                    "variants": [{"id": row["variant_id"], "price": f"{proposed:.2f}"}],
                },
            )
            block = data.get("productVariantsBulkUpdate") or {}
            if block.get("userErrors"):
                failed += 1
                continue
            verify = client.graphql(LIVE_VARIANT_PRICE, {"id": row["variant_id"]})
            vprice = _f((verify.get("node") or {}).get("price"))
            if vprice is None or abs(vprice - proposed) > 0.011:
                failed += 1
                continue
            ok += 1
            time.sleep(sleep_s)
        except Exception as exc:
            logger.error("Scoped price apply failed variant=%s: %s", row.get("variant_id"), exc)
            failed += 1
    return ok, failed


def _collect_alerts(rows: list[dict[str, Any]]) -> List[str]:
    alerts: List[str] = []
    for r in rows:
        if r.get("monitoring_action") == REVIEW_COST_ANOMALY:
            alerts.append(
                f"COST_ANOMALY|{r.get('title')}|"
                f"£{r.get('previous_GBP_cost')}→£{r.get('current_GBP_cost')}|"
                f"pct={r.get('cost_delta_pct')}"
            )
        delta = _f(r.get("theoretical_price_delta"))
        if (
            r.get("monitoring_action") == MONITOR_PRICE_INCREASE_INDICATED
            and delta is not None
            and delta >= 20.0
        ):
            alerts.append(
                f"LARGE_FLOOR_GAP|{r.get('title')}|"
                f"shopify=A${r.get('current_Shopify_price')}|"
                f"floor=A${r.get('theoretical_28pct_floor')}|"
                f"delta=A${delta:.2f}"
            )
    return alerts[:50]


def _summarize(rows: list[dict[str, Any]], summary: ProtectionSummary) -> None:
    summary.alerts = _collect_alerts(rows)
    for r in rows:
        action = r.get("monitoring_action") or ""
        if action == OUT_OF_SCOPE:
            summary.out_of_scope += 1
            continue
        if action == AMBIGUOUS_MAPPING:
            summary.ambiguous_mapping += 1
            continue
        if action == NO_CURRENT_SUPPLIER:
            summary.no_current_supplier += 1
            continue
        if action == STALE_SUPPLIER:
            summary.stale_supplier += 1
            continue
        if action == INVALID_COST:
            summary.invalid_cost += 1
            continue
        if action == ERROR:
            summary.errors += 1
            continue

        summary.evaluated += 1
        direction = r.get("cost_direction")
        if direction == "UP":
            summary.cost_increased += 1
        elif direction == "DOWN":
            summary.cost_decreased += 1
        else:
            summary.unchanged += 1
        if r.get("significant_movement"):
            if direction == "UP":
                summary.significant_increases += 1
            elif direction == "DOWN":
                summary.significant_decreases += 1
        if r.get("preferred_supplier_changed"):
            summary.preferred_supplier_changed += 1
        margin = r.get("exact_current_margin_pct")
        if action == MONITOR_PRICE_INCREASE_INDICATED:
            summary.below_floor_indicated += 1
        elif action == REVIEW_COST_ANOMALY and margin is not None and margin < 28.0:
            summary.below_floor_indicated += 1
        elif margin is not None and margin >= 28.0:
            summary.margin_safe += 1
        if action == REVIEW_COST_ANOMALY:
            summary.review_anomaly += 1


def format_status_line(summary: ProtectionSummary) -> str:
    mode = "monitoring_only" if summary.monitoring_only else "scoped_apply"
    return (
        "SUPPLIER_MARGIN_MONITOR_STATUS="
        f"{mode} "
        f"evaluated={summary.evaluated} "
        f"unchanged={summary.unchanged} "
        f"cost_up={summary.cost_increased} "
        f"cost_down={summary.cost_decreased} "
        f"sig_up={summary.significant_increases} "
        f"sig_down={summary.significant_decreases} "
        f"pref_changed={summary.preferred_supplier_changed} "
        f"below_floor_indicated={summary.below_floor_indicated} "
        f"anomaly_review={summary.review_anomaly} "
        f"scoped_apply_ok={summary.price_increases_applied} "
        f"scoped_apply_failed={summary.price_increases_failed} "
        f"stale={summary.stale_supplier} "
        f"no_supplier={summary.no_current_supplier} "
        f"errors={summary.errors}"
    )


def run_supplier_margin_protection(
    *,
    env_file: str = ".env",
    apply: bool = False,
    allowlist: Optional[ApplyAllowlist] = None,
    movement_csv: Optional[str] = None,
    monitor_csv: Optional[str] = None,
    reconcile_after_apply: bool = True,
    now: Optional[datetime] = None,
) -> Tuple[list[dict[str, Any]], ProtectionSummary]:
    root = _repo_root()
    path = Path(env_file)
    if not path.is_absolute():
        path = root / path
    load_dotenv(path, override=True)
    cfg = log_pricing_assumptions(logger)

    shop = os.getenv("SHOPIFY_SHOP", "").strip()
    if not shop:
        raise SystemExit("Missing SHOPIFY_SHOP")

    now = now or datetime.now(timezone.utc)
    allowlist = allowlist or parse_apply_allowlist()
    # Default production: monitoring only. Apply requires explicit allowlist.
    scoped_apply = apply and not allowlist.empty
    summary = ProtectionSummary(monitoring_only=not scoped_apply)
    supabase = create_fresh_client()

    listings = load_eligible_listings(supabase, shop)
    variant_ids = [clean_text(r.get("shopify_variant_id")) or "" for r in listings]
    barcodes = sorted({clean_text(r.get("barcode")) or "" for r in listings if r.get("barcode")})

    ctx = _load_supplier_context(supabase, variant_ids=variant_ids, barcodes=barcodes)

    offer_ids: list[str] = []
    for offers in ctx["offers_by_release"].values():
        for o in offers:
            if o.get("id"):
                offer_ids.append(str(o["id"]))
    for offers in ctx["offers_by_barcode"].values():
        for o in offers:
            if o.get("id"):
                offer_ids.append(str(o["id"]))
    prev_obs = fetch_previous_observations(supabase, sorted(set(offer_ids)))

    rows: list[dict[str, Any]] = []
    for listing in listings:
        try:
            rows.append(
                evaluate_variant_row(
                    listing=listing,
                    ctx=ctx,
                    prev_obs_by_offer=prev_obs,
                    cfg=cfg,
                    now=now,
                )
            )
        except Exception as exc:
            logger.error("Evaluate failed variant=%s: %s", listing.get("shopify_variant_id"), exc)
            rows.append(
                _result_row(
                    listing,
                    monitoring_action=ERROR,
                    reason=str(exc),
                    shopify_price=_f(listing.get("price_amount")),
                    tape_qty=0,
                    policy=(clean_text(listing.get("inventory_policy")) or "").upper(),
                )
            )

    _summarize(rows, summary)

    stamp = now.strftime("%Y%m%d_%H%M%S")
    movement_path = Path(movement_csv or root / "tmp" / f"supplier_cost_movement_{stamp}.csv")
    monitor_path = Path(monitor_csv or root / "tmp" / f"supplier_margin_monitor_{stamp}.csv")
    significant = [
        r
        for r in rows
        if r.get("significant_movement")
        or r.get("monitoring_action") == REVIEW_COST_ANOMALY
        or r.get("monitoring_action") == MONITOR_PRICE_INCREASE_INDICATED
    ]
    _write_csv(movement_path, significant)
    _write_csv(monitor_path, rows)
    summary.movement_csv = str(movement_path)
    summary.monitor_csv = str(monitor_path)

    for alert in summary.alerts:
        logger.warning("SUPPLIER_MARGIN_ALERT %s", alert)

    if apply and allowlist.empty:
        logger.warning(
            "SUPPLIER_MARGIN_PROTECTION_APPLY requested but allowlist empty — "
            "no Shopify mutations (monitor broadly, mutate narrowly)"
        )
        print(
            "SUPPLIER_MARGIN_MONITOR apply_skipped=allowlist_empty",
            flush=True,
        )

    to_apply = [
        r
        for r in rows
        if r.get("monitoring_action") == MONITOR_PRICE_INCREASE_INDICATED
        and r.get("anomaly_status") != "ANOMALOUS"
        and allowlist.matches(r)
    ]
    if scoped_apply and to_apply:
        client = ShopifyClient()
        ok, failed = _apply_price_updates(client, to_apply)
        summary.price_increases_applied = ok
        summary.price_increases_failed = failed
        summary.monitoring_only = False
        if reconcile_after_apply and ok > 0:
            try:
                from app.services.shopify_store_sync_service import run_shopify_store_sync

                sync_result = run_shopify_store_sync(env_file=str(path), dry_run=False)
                summary.ii_reconcile = str(sync_result.get("status", "ok"))
            except Exception as exc:
                logger.error("II reconciliation after scoped apply failed: %s", exc)
                summary.ii_reconcile = f"failed:{exc}"

    return rows, summary
