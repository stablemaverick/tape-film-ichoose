#!/usr/bin/env python3
"""
Existing-catalogue repricing simulation (READ-ONLY).

Region B: GBP supplier cost → canonical 28% floor calculator
  (calculate_sale_price_with_margin_floor_from_gbp_cost).

Region A: NO encoded USD pricing formula in codebase — reported as a gap.
  Shopify inventoryItem.unitCost (AUD) is captured for observed-margin context only
  and is NOT treated as a USD→AUD calculator.

Never mutates Shopify / suppliers / inventory / inventoryPolicy.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv
from supabase import create_client

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text
from app.rules.pricing_rules import (
    DEFAULT_GBP_AUD_RATE,
    DEFAULT_LANDED_COST_MARKUP,
    DEFAULT_MARGIN_FLOOR_RATIO,
    GST_RATE,
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    calculate_sale_price_with_margin_floor_from_landed_cost,
    calculate_shopify_cost_aud,
    effective_pricing_assumptions,
    exact_ex_gst_margin_ok,
    exact_ex_gst_margin_ratio,
    replacement_landed_cost_aud,
)
from app.services.arrow_inventory_policy_sync_service import (
    WHOLESALE_SUPPLIER_IDS,
    _chunked,
    _eval_offers,
    _load_supplier_context,
    normalize_region,
    normalize_studio_label,
    resolve_variant_region,
    supplier_is_usable,
)
from app.services.film_product_class import (
    ACTION_OUT_OF_SCOPE_NON_FILM,
    ACTION_SKIP_AMBIGUOUS_PRODUCT,
    PRODUCT_CLASS_AMBIGUOUS,
    PRODUCT_CLASS_BOOK,
    PRODUCT_CLASS_CD,
    PRODUCT_CLASS_FILM,
    PRODUCT_CLASS_GAME,
    PRODUCT_CLASS_OTHER_NON_FILM,
    PRODUCT_CLASS_VINYL,
    classify_product_class,
    product_class_to_action,
)
from app.services.shopify_ii_product_domain import fetch_soundtracks_collection_product_ids
from app.services.stock_availability_service import pick_preferred_supplier

ACTION_PRICE_INCREASE = "PRICE_INCREASE"
ACTION_KEEP_CURRENT_PRICE = "KEEP_CURRENT_PRICE"
ACTION_NO_CHANGE = "NO_CHANGE"
ACTION_REVIEW_LARGE_INCREASE = "REVIEW_LARGE_INCREASE"
ACTION_REVIEW_COST_ANOMALY = "REVIEW_COST_ANOMALY"
ACTION_NO_CURRENT_COST = "NO_CURRENT_COST"
ACTION_STALE_SUPPLIER = "STALE_SUPPLIER"
ACTION_AMBIGUOUS_MAPPING = "AMBIGUOUS_MAPPING"
ACTION_REGION_OR_CURRENCY_AMBIGUOUS = "REGION_OR_CURRENCY_AMBIGUOUS"
ACTION_OUT_OF_SCOPE = "OUT_OF_SCOPE"  # legacy alias
ACTION_REGION_A_FORMULA_GAP = "REGION_A_USD_FORMULA_MISSING"
ACTION_ERROR = "ERROR"

LARGE_INCREASE_THRESHOLD = 10.0

PRODUCTS_QUERY = """
query CatalogueRepricingSim($cursor: String, $q: String) {
  products(first: 50, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      handle
      status
      productType
      vendor
      tags
      studio: metafield(namespace: "custom", key: "studio") { value }
      region: metafield(namespace: "custom", key: "region") { value }
      formatMeta: metafield(namespace: "custom", key: "format") { value }
      mediaFormat: metafield(namespace: "custom", key: "media_format") { value }
      collections(first: 30) {
        nodes { handle title }
      }
      variants(first: 25) {
        nodes {
          id
          title
          sku
          barcode
          price
          inventoryQuantity
          region: metafield(namespace: "custom", key: "region") { value }
          inventoryItem { unitCost { amount currencyCode } }
        }
      }
    }
  }
}
"""

ORDERS_QUERY = """
query CatalogueRepricingOrders($cursor: String) {
  orders(first: 50, after: $cursor, sortKey: CREATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      createdAt
      lineItems(first: 50) {
        nodes {
          quantity
          discountedTotalSet { shopMoney { amount } }
          variant { id }
        }
      }
    }
  }
}
"""


@dataclass
class SimRow:
    title: str
    variant_title: str
    barcode: str
    sku: str
    product_id: str
    variant_id: str
    studio_raw: str
    studio_norm: str
    region_raw: str
    normalized_region: str
    product_type: str
    media_format: str
    product_class: str
    category_flag: str
    collection_handles: str
    pricing_path: str
    preferred_supplier: str
    source_currency: str
    source_cost: Optional[float]
    gbp_aud_rate: Optional[float]
    base_aud_cost: Optional[float]
    landed_aud_cost: Optional[float]
    shopify_unit_cost_aud: Optional[float]
    shopify_cost_currency: str
    current_retail: Optional[float]
    current_gp_pct: Optional[float]
    min_retail_required: Optional[float]
    proposed_retail: Optional[float]
    proposed_gp_pct: Optional[float]
    dollar_change: Optional[float]
    pct_change: Optional[float]
    action: str
    reason: str
    supplier_freshness: str = ""
    all_suppliers: str = ""
    data_notes: str = ""


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


def _norm_studio(raw: Optional[str]) -> str:
    canon = normalize_studio_label(raw)
    if canon in {"Arrow", "Second Sight", "Criterion Collection"}:
        return canon
    text = re.sub(r"\s+", " ", (clean_text(raw) or "")).strip()
    if not text:
        return "(blank)"
    low = text.casefold()
    if "88" in low and "film" in low:
        return "88 Films"
    if "eureka" in low:
        return "Eureka"
    if "indicator" in low:
        return "Indicator"
    if text.upper() == "BFI" or low.startswith("bfi"):
        return "BFI"
    if "radiance" in low:
        return "Radiance Films"
    if "shout" in low:
        return "Shout! Factory"
    if "warner" in low:
        return "Warner Bros"
    if "disney" in low:
        return "Walt Disney"
    if "universal" in low:
        return "Universal Pictures"
    if "paramount" in low:
        return "Paramount Pictures"
    if "sony" in low:
        return "Sony Pictures"
    if "studio canal" in low or "studiocanal" in low:
        return "StudioCanal"
    if "fox" in low:
        return "20th Century Fox"
    if "a24" in low:
        return "A24"
    if "mubi" in low:
        return "MUBI"
    return text


def _gql(client: ShopifyClient, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    tries = 0
    while True:
        tries += 1
        try:
            return client.graphql(query, variables)
        except Exception as exc:
            if "THROTTLED" in str(exc) and tries < 8:
                time.sleep(min(2 * tries, 10))
                continue
            raise


def fetch_active_products(client: ShopifyClient) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cursor = None
    while True:
        data = _gql(client, PRODUCTS_QUERY, {"cursor": cursor, "q": "status:active"})
        block = data["products"]
        out.extend(block.get("nodes") or [])
        page = block.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        time.sleep(0.08)
    return out


def to_preferred_pool(evaluated: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            }
        )
    return pool


def decide_action(
    *,
    current: Optional[float],
    proposed: Optional[float],
    large_threshold: float = LARGE_INCREASE_THRESHOLD,
) -> tuple[str, str, Optional[float], Optional[float]]:
    if current is None or proposed is None:
        return ACTION_ERROR, "missing_price_or_proposed", None, None
    delta = round(proposed - current, 2)
    pct = round((delta / current) * 100, 2) if current else None
    if abs(delta) < 0.005:
        return ACTION_NO_CHANGE, "price_already_at_floor", delta, pct
    if delta < 0:
        return ACTION_KEEP_CURRENT_PRICE, "proposed_below_current_keep_margin", delta, pct
    if delta > large_threshold:
        return ACTION_REVIEW_LARGE_INCREASE, "increase_above_auto_threshold", delta, pct
    return ACTION_PRICE_INCREASE, "current_below_28pct_floor", delta, pct


def price_bucket(price: Optional[float]) -> str:
    if price is None:
        return "missing"
    if price < 30:
        return "<$30"
    if price < 40:
        return "$30–39.99"
    if price < 50:
        return "$40–49.99"
    if price < 60:
        return "$50–59.99"
    if price < 70:
        return "$60–69.99"
    if price < 80:
        return "$70–79.99"
    if price < 100:
        return "$80–99.99"
    return "$100+"


def criterion_band(price: Optional[float]) -> str:
    if price is None:
        return "missing"
    if price < 60:
        return "below $60"
    if price < 70:
        return "$60–69.99"
    if abs(price - 74.99) < 0.005:
        return "$74.99"
    if price < 80:
        return "$75–79.99"
    if price < 90:
        return "$80–89.99"
    if price < 100:
        return "$90–99.99"
    return "$100+"


def movement_bucket(delta: Optional[float], action: str) -> str:
    if action == ACTION_KEEP_CURRENT_PRICE or (delta is not None and delta < -0.005):
        return "decrease indicated"
    if delta is None or abs(delta) < 0.005 or action == ACTION_NO_CHANGE:
        return "no change"
    if delta <= 2:
        return "+$1–$2"
    if delta <= 4:
        return "+$3–$4"
    if delta <= 6:
        return "+$5–$6"
    if delta <= 10:
        return "+$7–$10"
    return ">$10"


def increase_band(delta: Optional[float]) -> str:
    if delta is None or delta <= 0:
        return "none"
    if delta <= 3:
        return "≤$3"
    if delta <= 6:
        return "$3.01–$6"
    if delta <= 10:
        return "$6.01–$10"
    return ">$10"


def weighted_gp(rows: list[SimRow], price_attr: str, landed_attr: str = "landed_aud_cost") -> Optional[float]:
    """Units-unweighted product average of ex-GST GP% where both price and landed exist."""
    vals = []
    for r in rows:
        price = getattr(r, price_attr)
        landed = getattr(r, landed_attr)
        m = _margin_pct(price, landed)
        if m is not None:
            vals.append(m)
    if not vals:
        return None
    return round(statistics.mean(vals), 2)


def revenue_weighted_gp(rows: list[SimRow], price_attr: str) -> Optional[float]:
    """Weight by current retail as proxy when unit sales unavailable."""
    num = Decimal("0")
    den = Decimal("0")
    for r in rows:
        price = getattr(r, price_attr)
        landed = r.landed_aud_cost
        if price is None or landed is None or price <= 0:
            continue
        ex = Decimal(str(price)) / Decimal(str(GST_RATE))
        gp = ex - Decimal(str(landed))
        num += gp
        den += ex
    if den <= 0:
        return None
    return float((num / den * Decimal("100")).quantize(Decimal("0.01")))


def cohort_metrics(rows: list[SimRow], *, eligible_only: bool = True) -> dict[str, Any]:
    assessed = rows
    if eligible_only:
        elig = [
            r
            for r in rows
            if r.action
            in {
                ACTION_PRICE_INCREASE,
                ACTION_KEEP_CURRENT_PRICE,
                ACTION_NO_CHANGE,
                ACTION_REVIEW_LARGE_INCREASE,
            }
        ]
    else:
        elig = rows
    increases = [r for r in elig if r.action in {ACTION_PRICE_INCREASE, ACTION_REVIEW_LARGE_INCREASE}]
    keep = [r for r in elig if r.action == ACTION_KEEP_CURRENT_PRICE]
    no_change = [r for r in elig if r.action == ACTION_NO_CHANGE]
    deltas = [r.dollar_change for r in increases if r.dollar_change is not None]
    cur_prices = [r.current_retail for r in elig if r.current_retail is not None]
    prop_prices = []
    for r in elig:
        if r.action == ACTION_KEEP_CURRENT_PRICE:
            prop_prices.append(r.current_retail)
        elif r.proposed_retail is not None:
            prop_prices.append(r.proposed_retail)
    review = [r for r in rows if r.action in {ACTION_REVIEW_LARGE_INCREASE, ACTION_REVIEW_COST_ANOMALY}]
    return {
        "variants_assessed": len(assessed),
        "variants_eligible": len(elig),
        "variants_requiring_increase": len(increases),
        "already_adequately_priced": len(no_change) + len(keep),
        "keep_current_price_improved_margin": len(keep),
        "no_change": len(no_change),
        "review_large_increase": sum(1 for r in rows if r.action == ACTION_REVIEW_LARGE_INCREASE),
        "current_avg_gp_pct": revenue_weighted_gp(elig, "current_retail"),
        "proposed_avg_gp_pct": revenue_weighted_gp(
            [
                SimRow(**{**asdict(r), "current_retail": r.current_retail if r.action == ACTION_KEEP_CURRENT_PRICE else r.proposed_retail})
                for r in elig
                if (r.current_retail if r.action == ACTION_KEEP_CURRENT_PRICE else r.proposed_retail) is not None
            ],
            "current_retail",
        )
        if elig
        else None,
        "current_average_retail": round(statistics.mean(cur_prices), 2) if cur_prices else None,
        "proposed_average_retail": round(statistics.mean([p for p in prop_prices if p is not None]), 2)
        if prop_prices
        else None,
        "median_increase": round(statistics.median(deltas), 2) if deltas else None,
        "average_increase": round(statistics.mean(deltas), 2) if deltas else None,
        "maximum_increase": round(max(deltas), 2) if deltas else None,
        "manual_review_count": len(review),
        "increase_bands": dict(Counter(increase_band(r.dollar_change) for r in increases)),
        "movement_bands": dict(Counter(movement_bucket(r.dollar_change, r.action) for r in elig)),
        "current_price_dist": dict(Counter(price_bucket(r.current_retail) for r in elig)),
        "proposed_price_dist": dict(
            Counter(
                price_bucket(r.current_retail if r.action == ACTION_KEEP_CURRENT_PRICE else r.proposed_retail)
                for r in elig
            )
        ),
    }


def label_rollup(rows: list[SimRow]) -> list[dict[str, Any]]:
    by_label: dict[str, list[SimRow]] = defaultdict(list)
    for r in rows:
        by_label[r.studio_norm].append(r)
    out = []
    for label, group in sorted(by_label.items(), key=lambda x: (-len(x[1]), x[0])):
        elig = [
            r
            for r in group
            if r.action
            in {
                ACTION_PRICE_INCREASE,
                ACTION_KEEP_CURRENT_PRICE,
                ACTION_NO_CHANGE,
                ACTION_REVIEW_LARGE_INCREASE,
            }
        ]
        increases = [r for r in elig if r.action in {ACTION_PRICE_INCREASE, ACTION_REVIEW_LARGE_INCREASE}]
        deltas = [r.dollar_change for r in increases if r.dollar_change is not None]
        cur = [r.current_retail for r in elig if r.current_retail is not None]
        prop = [
            (r.current_retail if r.action == ACTION_KEEP_CURRENT_PRICE else r.proposed_retail)
            for r in elig
        ]
        prop = [p for p in prop if p is not None]
        out.append(
            {
                "label": label,
                "eligible": len(elig),
                "assessed": len(group),
                "avg_current_price": round(statistics.mean(cur), 2) if cur else None,
                "avg_proposed_price": round(statistics.mean(prop), 2) if prop else None,
                "current_weighted_gp": revenue_weighted_gp(elig, "current_retail"),
                "proposed_weighted_gp": revenue_weighted_gp(
                    [
                        SimRow(
                            **{
                                **asdict(r),
                                "current_retail": r.current_retail
                                if r.action == ACTION_KEEP_CURRENT_PRICE
                                else r.proposed_retail,
                            }
                        )
                        for r in elig
                        if (r.current_retail if r.action == ACTION_KEEP_CURRENT_PRICE else r.proposed_retail)
                        is not None
                    ],
                    "current_retail",
                )
                if elig
                else None,
                "price_increases": len(increases),
                "avg_increase": round(statistics.mean(deltas), 2) if deltas else None,
                "max_increase": round(max(deltas), 2) if deltas else None,
                "gt10_reviews": sum(1 for r in group if r.action == ACTION_REVIEW_LARGE_INCREASE),
            }
        )
    return out


_IDENTITY_KEYS = (
    "title",
    "variant_title",
    "barcode",
    "sku",
    "product_id",
    "variant_id",
    "studio_raw",
    "studio_norm",
    "region_raw",
    "normalized_region",
    "product_type",
    "media_format",
    "product_class",
    "category_flag",
    "collection_handles",
    "current_retail",
    "shopify_unit_cost_aud",
    "shopify_cost_currency",
)


def _identity(row: dict[str, Any]) -> dict[str, Any]:
    return {k: row[k] for k in _IDENTITY_KEYS}


def build_rows(
    products: list[dict[str, Any]],
    *,
    supabase: Any,
    gbp_aud: float,
    landed_markup: float,
    margin_floor: float,
    soundtrack_product_ids: Optional[set[str]] = None,
) -> list[SimRow]:
    soundtrack_product_ids = soundtrack_product_ids or set()
    flat: list[dict[str, Any]] = []
    for p in products:
        studio_raw = clean_text((p.get("studio") or {}).get("value")) or ""
        product_region = clean_text((p.get("region") or {}).get("value")) or ""
        format_value = clean_text((p.get("formatMeta") or {}).get("value")) or ""
        media_format = clean_text((p.get("mediaFormat") or {}).get("value")) or ""
        # Prefer custom.format (authoritative for Vinyl/CD); fall back to media_format.
        effective_format = format_value or media_format
        product_type = clean_text(p.get("productType")) or ""
        tags = list(p.get("tags") or [])
        handles = {
            (clean_text(n.get("handle")) or "").casefold()
            for n in ((p.get("collections") or {}).get("nodes") or [])
            if clean_text((n or {}).get("handle"))
        }
        product_class, class_reason = classify_product_class(
            title=p.get("title") or "",
            product_id=p.get("id") or "",
            product_type=product_type,
            format_value=format_value,
            media_format=media_format,
            tags=tags,
            collection_handles=handles,
            soundtrack_product_ids=soundtrack_product_ids,
        )
        for v in ((p.get("variants") or {}).get("nodes") or []):
            variant_region = clean_text((v.get("region") or {}).get("value")) or ""
            region_code = resolve_variant_region(
                product_region=product_region, variant_region=variant_region
            )
            cost_obj = (v.get("inventoryItem") or {}).get("unitCost") or {}
            flat.append(
                {
                    "title": p.get("title") or "",
                    "variant_title": v.get("title") or "",
                    "barcode": clean_text(v.get("barcode")) or "",
                    "sku": clean_text(v.get("sku")) or "",
                    "product_id": p.get("id") or "",
                    "variant_id": v.get("id") or "",
                    "studio_raw": studio_raw,
                    "studio_norm": _norm_studio(studio_raw),
                    "region_raw": variant_region or product_region,
                    "normalized_region": region_code,
                    "product_type": product_type,
                    "media_format": effective_format,
                    "product_class": product_class,
                    "category_flag": class_reason,
                    "collection_handles": ",".join(sorted(handles)),
                    "current_retail": _f(v.get("price")),
                    "shopify_unit_cost_aud": _f(cost_obj.get("amount"))
                    if (cost_obj.get("currencyCode") or "AUD") == "AUD"
                    else None,
                    "shopify_cost_currency": cost_obj.get("currencyCode") or "",
                }
            )

    ctx = _load_supplier_context(
        supabase,
        variant_ids=[r["variant_id"] for r in flat if r["variant_id"]],
        barcodes=sorted({r["barcode"] for r in flat if r["barcode"]}),
    )
    now = datetime.now(timezone.utc)
    rows: list[SimRow] = []

    for row in flat:
        # Product-class exclusion ALWAYS takes precedence over region/GBP eligibility.
        gate_action, gate_reason = product_class_to_action(row["product_class"], row["category_flag"])
        if gate_action:
            rows.append(
                SimRow(
                    **_identity(row),
                    pricing_path="excluded_non_film"
                    if gate_action == ACTION_OUT_OF_SCOPE_NON_FILM
                    else "skipped_ambiguous_product",
                    preferred_supplier="",
                    source_currency="",
                    source_cost=None,
                    gbp_aud_rate=None,
                    base_aud_cost=None,
                    landed_aud_cost=None,
                    min_retail_required=None,
                    proposed_retail=None,
                    current_gp_pct=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=gate_action,
                    reason=gate_reason,
                )
            )
            continue

        region = row["normalized_region"]
        if region not in {"A", "B"}:
            rows.append(
                SimRow(
                    **_identity(row),
                    pricing_path="ambiguous_region",
                    preferred_supplier="",
                    source_currency="",
                    source_cost=None,
                    gbp_aud_rate=None,
                    base_aud_cost=None,
                    landed_aud_cost=row["shopify_unit_cost_aud"],
                    min_retail_required=None,
                    proposed_retail=None,
                    current_gp_pct=_margin_pct(row["current_retail"], row["shopify_unit_cost_aud"]),
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_REGION_OR_CURRENCY_AMBIGUOUS,
                    reason="region_missing_or_not_ab",
                )
            )
            continue

        # --- Region A: formula gap ---
        if region == "A":
            shop_cost = row["shopify_unit_cost_aud"]
            info_floor = (
                calculate_sale_price_with_margin_floor_from_landed_cost(
                    shop_cost, margin_floor_ratio=margin_floor
                )
                if shop_cost
                else None
            )
            rows.append(
                SimRow(
                    **_identity(row),
                    pricing_path="region_a_usd_gap",
                    preferred_supplier="",
                    source_currency="USD_EXPECTED_BUT_UNENCODED",
                    source_cost=None,
                    gbp_aud_rate=None,
                    base_aud_cost=None,
                    landed_aud_cost=shop_cost,
                    min_retail_required=info_floor,
                    proposed_retail=None,
                    current_gp_pct=_margin_pct(row["current_retail"], shop_cost),
                    proposed_gp_pct=_margin_pct(info_floor, shop_cost) if info_floor else None,
                    dollar_change=(
                        round(info_floor - row["current_retail"], 2)
                        if info_floor is not None and row["current_retail"] is not None
                        else None
                    ),
                    pct_change=None,
                    action=ACTION_REGION_A_FORMULA_GAP,
                    reason="no_encoded_usd_pricing_formula;shopify_aud_unit_cost_observed_only",
                    data_notes=(
                        f"info_floor_from_shopify_aud_unit_cost={info_floor}"
                        if info_floor is not None
                        else "missing_shopify_unit_cost"
                    ),
                )
            )
            continue

        # --- Region B: GBP wholesale path ---
        vid = row["variant_id"]
        rid = ctx["rsl"].get(vid)
        offers: list[dict[str, Any]] = []
        if rid:
            offers = list(ctx["offers_by_release"].get(str(rid), []))
        if not offers and row["barcode"]:
            offers = list(ctx["offers_by_barcode"].get(row["barcode"], []))

        if not rid and not offers:
            rows.append(
                SimRow(
                    **_identity(row),
                    pricing_path="region_b_gbp",
                    preferred_supplier="",
                    source_currency="GBP",
                    source_cost=None,
                    gbp_aud_rate=gbp_aud,
                    base_aud_cost=None,
                    landed_aud_cost=None,
                    min_retail_required=None,
                    proposed_retail=None,
                    current_gp_pct=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_AMBIGUOUS_MAPPING,
                    reason="no_release_or_supplier_mapping",
                )
            )
            continue

        evaluated = _eval_offers(offers, suppliers=ctx["suppliers"], now=now)
        if not supplier_is_usable(evaluated):
            stale = any(str(e.get("freshness") or "") == "stale" for e in evaluated)
            rows.append(
                SimRow(
                    **_identity(row),
                    pricing_path="region_b_gbp",
                    preferred_supplier="",
                    source_currency="GBP",
                    source_cost=None,
                    gbp_aud_rate=gbp_aud,
                    base_aud_cost=None,
                    landed_aud_cost=None,
                    min_retail_required=None,
                    proposed_retail=None,
                    current_gp_pct=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_STALE_SUPPLIER if stale else ACTION_NO_CURRENT_COST,
                    reason="supplier_not_usable",
                    all_suppliers=";".join(
                        f"{e.get('supplier_id')}:{e.get('api_status')}:{e.get('freshness')}"
                        for e in evaluated
                    ),
                )
            )
            continue

        pool = to_preferred_pool(evaluated)
        preferred = pick_preferred_supplier(pool) if pool else None
        if not preferred or preferred.get("unit_cost") is None:
            rows.append(
                SimRow(
                    **_identity(row),
                    pricing_path="region_b_gbp",
                    preferred_supplier="",
                    source_currency="GBP",
                    source_cost=None,
                    gbp_aud_rate=gbp_aud,
                    base_aud_cost=None,
                    landed_aud_cost=None,
                    min_retail_required=None,
                    proposed_retail=None,
                    current_gp_pct=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_NO_CURRENT_COST,
                    reason="preferred_supplier_missing_unit_cost",
                )
            )
            continue

        cost_gbp = _f(preferred.get("unit_cost"))
        if cost_gbp is None or cost_gbp <= 0:
            rows.append(
                SimRow(
                    **_identity(row),
                    pricing_path="region_b_gbp",
                    preferred_supplier=str(preferred.get("supplier_id") or ""),
                    source_currency="GBP",
                    source_cost=cost_gbp,
                    gbp_aud_rate=gbp_aud,
                    base_aud_cost=None,
                    landed_aud_cost=None,
                    min_retail_required=None,
                    proposed_retail=None,
                    current_gp_pct=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_NO_CURRENT_COST,
                    reason="invalid_gbp_cost",
                )
            )
            continue

        landed = replacement_landed_cost_aud(
            cost_gbp, gbp_aud_rate=gbp_aud, landed_cost_markup=landed_markup
        )
        base_aud = round(cost_gbp * gbp_aud, 2)
        floor = calculate_sale_price_with_margin_floor_from_gbp_cost(
            cost_gbp,
            gbp_aud_rate=gbp_aud,
            landed_cost_markup=landed_markup,
            margin_floor_ratio=margin_floor,
        )
        current = row["current_retail"]
        shop_cost = row["shopify_unit_cost_aud"]
        notes = []
        if shop_cost and landed and landed > 0:
            drift = abs(shop_cost - landed) / landed
            if drift >= 0.25:
                notes.append(f"shopify_cost_vs_replacement_drift={drift:.0%}")

        if notes and floor is not None and current is not None and floor > current:
            action = ACTION_REVIEW_COST_ANOMALY
            reason = "cost_anomaly;" + ";".join(notes)
            delta = round(floor - current, 2)
            pct = round((delta / current) * 100, 2) if current else None
            proposed = floor
        else:
            if floor is None:
                action, reason, delta, pct = ACTION_ERROR, "floor_calc_failed", None, None
                proposed = None
            else:
                action, reason, delta, pct = decide_action(current=current, proposed=floor)
                proposed = current if action == ACTION_KEEP_CURRENT_PRICE else floor

        rows.append(
            SimRow(
                **_identity(row),
                pricing_path="region_b_gbp",
                preferred_supplier=str(preferred.get("supplier_id") or ""),
                source_currency="GBP",
                source_cost=cost_gbp,
                gbp_aud_rate=gbp_aud,
                base_aud_cost=base_aud,
                landed_aud_cost=landed,
                min_retail_required=floor,
                proposed_retail=proposed,
                current_gp_pct=_margin_pct(current, landed),
                proposed_gp_pct=_margin_pct(
                    current if action == ACTION_KEEP_CURRENT_PRICE else proposed, landed
                ),
                dollar_change=delta,
                pct_change=pct,
                action=action,
                reason=reason,
                supplier_freshness=str(preferred.get("freshness") or ""),
                all_suppliers=";".join(
                    f"{e.get('supplier_id')}:{e.get('api_status')}:{e.get('freshness')}:£{e.get('unit_cost')}"
                    for e in evaluated
                    if str(e.get("supplier_id") or "") in WHOLESALE_SUPPLIER_IDS
                ),
                data_notes=";".join(notes),
            )
        )
    return rows


def fetch_sales_units(client: ShopifyClient, *, max_orders: int = 500) -> dict[str, int]:
    """Recent order units by variant_id. Best-effort; stops after max_orders."""
    units: dict[str, int] = defaultdict(int)
    cursor = None
    seen = 0
    while seen < max_orders:
        data = _gql(client, ORDERS_QUERY, {"cursor": cursor})
        block = data["orders"]
        nodes = block.get("nodes") or []
        if not nodes:
            break
        for order in nodes:
            seen += 1
            for li in (order.get("lineItems") or {}).get("nodes") or []:
                vid = ((li.get("variant") or {}).get("id")) or ""
                qty = int(li.get("quantity") or 0)
                if vid and qty:
                    units[vid] += qty
        page = block.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        time.sleep(0.08)
    return dict(units)


def sales_sensitivity(rows: list[SimRow], units: dict[str, int]) -> dict[str, Any]:
    """Like-for-like on variants with both sales units and eligible Region B pricing."""
    rev_cur = Decimal("0")
    rev_prop = Decimal("0")
    gp_cur = Decimal("0")
    gp_prop = Decimal("0")
    matched = 0
    units_total = 0
    for r in rows:
        u = units.get(r.variant_id, 0)
        if u <= 0:
            continue
        if r.action not in {
            ACTION_PRICE_INCREASE,
            ACTION_KEEP_CURRENT_PRICE,
            ACTION_NO_CHANGE,
            ACTION_REVIEW_LARGE_INCREASE,
        }:
            continue
        if r.current_retail is None or r.landed_aud_cost is None:
            continue
        prop = r.current_retail if r.action == ACTION_KEEP_CURRENT_PRICE else (r.proposed_retail or r.current_retail)
        if prop is None:
            continue
        matched += 1
        units_total += u
        cur_p = Decimal(str(r.current_retail))
        prop_p = Decimal(str(prop))
        landed = Decimal(str(r.landed_aud_cost))
        gst = Decimal(str(GST_RATE))
        rev_cur += cur_p * u
        rev_prop += prop_p * u
        gp_cur += (cur_p / gst - landed) * u
        gp_prop += (prop_p / gst - landed) * u
    if matched == 0:
        return {"available": False, "reason": "no_overlapping_sales_and_eligible_variants"}
    return {
        "available": True,
        "orders_window": "most_recent_up_to_500_shopify_orders",
        "variants_with_sales": matched,
        "units": units_total,
        "revenue_current": float(rev_cur.quantize(Decimal("0.01"))),
        "revenue_proposed": float(rev_prop.quantize(Decimal("0.01"))),
        "revenue_delta": float((rev_prop - rev_cur).quantize(Decimal("0.01"))),
        "gp_dollars_current": float(gp_cur.quantize(Decimal("0.01"))),
        "gp_dollars_proposed": float(gp_prop.quantize(Decimal("0.01"))),
        "gp_dollars_delta": float((gp_prop - gp_cur).quantize(Decimal("0.01"))),
        "gp_pct_current": float((gp_cur / (rev_cur / Decimal(str(GST_RATE))) * 100).quantize(Decimal("0.01")))
        if rev_cur > 0
        else None,
        "gp_pct_proposed": float((gp_prop / (rev_prop / Decimal(str(GST_RATE))) * 100).quantize(Decimal("0.01")))
        if rev_prop > 0
        else None,
    }


def pick_comparison_examples(rows: list[SimRow]) -> list[dict[str, Any]]:
    def pack(r: SimRow, note: str) -> dict[str, Any]:
        return {
            "note": note,
            "title": r.title,
            "region": r.normalized_region,
            "studio": r.studio_norm,
            "source_cost": r.source_cost,
            "source_currency": r.source_currency,
            "landed_aud": r.landed_aud_cost,
            "current_retail": r.current_retail,
            "proposed_retail": r.proposed_retail
            if r.action != ACTION_REGION_A_FORMULA_GAP
            else r.min_retail_required,
            "gp_pct_current": r.current_gp_pct,
            "gp_pct_proposed": r.proposed_gp_pct,
            "action": r.action,
            "pricing_path": r.pricing_path,
        }

    examples = []
    b = [r for r in rows if r.normalized_region == "B" and r.action in {
        ACTION_PRICE_INCREASE, ACTION_KEEP_CURRENT_PRICE, ACTION_NO_CHANGE, ACTION_REVIEW_LARGE_INCREASE
    }]
    a = [r for r in rows if r.normalized_region == "A" and r.studio_norm == "Criterion Collection"]

    def find(pool, *needles):
        for r in pool:
            t = r.title.casefold()
            if all(n in t for n in needles):
                return r
        return None

    for needles, note in [
        (("blu-ray",), "Region B standard Blu-ray"),
        (("4k",), "Region B standard 4K"),
        (("limited", "blu"), "Region B Limited Edition Blu-ray"),
        (("limited", "4k"), "Region B Limited Edition 4K"),
    ]:
        hit = find(b, *needles)
        if hit:
            examples.append(pack(hit, note))

    for needles, note in [
        (("blu-ray", "criterion"), "Region A Criterion Blu-ray"),
        (("4k", "criterion"), "Region A Criterion 4K"),
    ]:
        hit = find(a, *needles)
        if hit:
            examples.append(pack(hit, note))

    # Higher-cost Criterion A by shopify unit cost
    a_costed = [r for r in a if r.landed_aud_cost]
    if a_costed:
        hi = max(a_costed, key=lambda r: r.landed_aud_cost or 0)
        examples.append(pack(hi, "Region A Criterion higher Shopify AUD unitCost"))
    return examples


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only existing catalogue repricing simulation")
    parser.add_argument("--env", default=".env.prod")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument("--out-dir", default="tmp")
    parser.add_argument("--skip-sales", action="store_true")
    args = parser.parse_args(argv)

    env_path = Path(args.env)
    if not env_path.is_absolute():
        env_path = _REPO / env_path
    load_dotenv(env_path, override=True)
    cfg = effective_pricing_assumptions()
    gbp_aud = float(cfg["gbp_aud_rate"])
    markup = float(cfg["landed_cost_markup"])
    floor = float(cfg["margin_floor_ratio"])

    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    client = ShopifyClient(api_version=args.api_version)

    print("Scanning active Shopify products…", flush=True)
    products = fetch_active_products(client)
    print(f"products={len(products)}", flush=True)
    print("Fetching soundtracks collection product IDs…", flush=True)
    soundtrack_ids = fetch_soundtracks_collection_product_ids(client)
    print(f"soundtracks_collection_products={len(soundtrack_ids)}", flush=True)
    rows = build_rows(
        products,
        supabase=sb,
        gbp_aud=gbp_aud,
        landed_markup=markup,
        margin_floor=floor,
        soundtrack_product_ids=soundtrack_ids,
    )

    # Film-only population for all pricing / margin metrics.
    films = [r for r in rows if r.product_class == PRODUCT_CLASS_FILM]
    region_b = [r for r in films if r.normalized_region == "B"]
    region_a = [r for r in films if r.normalized_region == "A"]
    crit_a = [r for r in region_a if r.studio_norm == "Criterion Collection"]
    crit_b = [r for r in region_b if r.studio_norm == "Criterion Collection"]
    region_b_elig = [
        r
        for r in region_b
        if r.action
        in {
            ACTION_PRICE_INCREASE,
            ACTION_KEEP_CURRENT_PRICE,
            ACTION_NO_CHANGE,
            ACTION_REVIEW_LARGE_INCREASE,
        }
    ]

    product_class_counts = {
        "films_assessed": len(films),
        "vinyl_excluded": sum(1 for r in rows if r.product_class == PRODUCT_CLASS_VINYL),
        "cds_excluded": sum(1 for r in rows if r.product_class == PRODUCT_CLASS_CD),
        "books_excluded": sum(1 for r in rows if r.product_class == PRODUCT_CLASS_BOOK),
        "games_excluded": sum(1 for r in rows if r.product_class == PRODUCT_CLASS_GAME),
        "other_non_film_excluded": sum(
            1 for r in rows if r.product_class == PRODUCT_CLASS_OTHER_NON_FILM
        ),
        "ambiguous_product_type": sum(
            1 for r in rows if r.product_class == PRODUCT_CLASS_AMBIGUOUS
        ),
        "product_class_breakdown": dict(Counter(r.product_class for r in rows)),
    }

    sales = {"available": False, "reason": "skipped"}
    if not args.skip_sales:
        print("Fetching recent Shopify orders for sensitivity…", flush=True)
        try:
            units = fetch_sales_units(client)
            sales = sales_sensitivity(region_b_elig, units)
            sales["raw_variants_in_orders"] = len(units)
        except Exception as exc:
            sales = {"available": False, "reason": f"orders_query_failed:{exc}"}

    # Criterion A informational bands from Shopify AUD unitCost floor (gap path)
    crit_a_info = []
    for r in crit_a:
        if r.min_retail_required is None:
            continue
        crit_a_info.append(r)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = _REPO / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"catalogue_repricing_simulation_{stamp}.csv"
    json_path = out_dir / f"catalogue_repricing_simulation_{stamp}.json"

    fieldnames = list(SimRow.__dataclass_fields__.keys())
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))

    action_counts = dict(Counter(r.action for r in rows))
    cohort_counts = {
        "total_variants": len(rows),
        "films_assessed": len(films),
        "region_b_films": len(region_b),
        "region_a_films": len(region_a),
        "criterion_region_a": len(crit_a),
        "criterion_region_b": len(crit_b),
        "missing_region_among_films": sum(
            1 for r in films if r.normalized_region not in {"A", "B"}
        ),
        "out_of_scope_non_film": sum(
            1 for r in rows if r.action == ACTION_OUT_OF_SCOPE_NON_FILM
        ),
        "ambiguous_product_type_skipped": sum(
            1 for r in rows if r.action == ACTION_SKIP_AMBIGUOUS_PRODUCT
        ),
        "region_b_valid_gbp": len(region_b_elig),
        "region_b_no_cost": sum(1 for r in region_b if r.action == ACTION_NO_CURRENT_COST),
        "region_b_stale": sum(1 for r in region_b if r.action == ACTION_STALE_SUPPLIER),
        "region_b_ambiguous": sum(1 for r in region_b if r.action == ACTION_AMBIGUOUS_MAPPING),
        "region_a_formula_gap": sum(1 for r in region_a if r.action == ACTION_REGION_A_FORMULA_GAP),
    }

    increases = [
        r for r in region_b_elig if r.action in {ACTION_PRICE_INCREASE, ACTION_REVIEW_LARGE_INCREASE}
    ]
    top_increases = sorted(
        increases, key=lambda r: r.dollar_change or 0, reverse=True
    )[:25]
    low_margin = sorted(
        [r for r in region_b_elig if r.current_gp_pct is not None],
        key=lambda r: r.current_gp_pct or 0,
    )[:25]
    anomalies = [r for r in rows if r.action == ACTION_REVIEW_COST_ANOMALY]

    def short(r: SimRow) -> dict[str, Any]:
        return {
            "title": r.title,
            "label": r.studio_norm,
            "region": r.normalized_region,
            "barcode": r.barcode,
            "source_cost": r.source_cost,
            "source_currency": r.source_currency,
            "preferred_supplier": r.preferred_supplier,
            "landed_aud": r.landed_aud_cost,
            "current_retail": r.current_retail,
            "proposed_retail": r.proposed_retail,
            "dollar_change": r.dollar_change,
            "current_gp_pct": r.current_gp_pct,
            "proposed_gp_pct": r.proposed_gp_pct,
            "action": r.action,
            "reason": r.reason,
        }

    # Region A Criterion: informational distribution from Shopify AUD unitCost floor
    crit_a_proposed_dist = dict(
        Counter(criterion_band(r.min_retail_required) for r in crit_a_info)
    )
    crit_a_current_dist = dict(Counter(criterion_band(r.current_retail) for r in crit_a))

    # Fix proposed GP metrics for region B without the awkward SimRow rebuild bug
    def proposed_price(r: SimRow) -> Optional[float]:
        if r.action == ACTION_KEEP_CURRENT_PRICE:
            return r.current_retail
        return r.proposed_retail

    def metrics_for(group: list[SimRow]) -> dict[str, Any]:
        elig = [
            r
            for r in group
            if r.action
            in {
                ACTION_PRICE_INCREASE,
                ACTION_KEEP_CURRENT_PRICE,
                ACTION_NO_CHANGE,
                ACTION_REVIEW_LARGE_INCREASE,
            }
        ]
        increases_g = [r for r in elig if r.action in {ACTION_PRICE_INCREASE, ACTION_REVIEW_LARGE_INCREASE}]
        deltas = [r.dollar_change for r in increases_g if r.dollar_change is not None]
        cur = [r.current_retail for r in elig if r.current_retail is not None]
        prop = [proposed_price(r) for r in elig]
        prop = [p for p in prop if p is not None]

        # revenue-weighted GP at proposed prices
        num = Decimal("0")
        den = Decimal("0")
        for r in elig:
            p = proposed_price(r)
            if p is None or r.landed_aud_cost is None or p <= 0:
                continue
            ex = Decimal(str(p)) / Decimal(str(GST_RATE))
            num += ex - Decimal(str(r.landed_aud_cost))
            den += ex
        prop_gp = float((num / den * 100).quantize(Decimal("0.01"))) if den > 0 else None

        return {
            "variants_assessed": len(group),
            "variants_eligible": len(elig),
            "variants_requiring_increase": len(increases_g),
            "already_adequately_priced": sum(
                1 for r in elig if r.action in {ACTION_NO_CHANGE, ACTION_KEEP_CURRENT_PRICE}
            ),
            "current_weighted_gp_pct": revenue_weighted_gp(elig, "current_retail"),
            "proposed_weighted_gp_pct": prop_gp,
            "current_average_retail": round(statistics.mean(cur), 2) if cur else None,
            "proposed_average_retail": round(statistics.mean(prop), 2) if prop else None,
            "median_increase": round(statistics.median(deltas), 2) if deltas else None,
            "average_increase": round(statistics.mean(deltas), 2) if deltas else None,
            "maximum_increase": round(max(deltas), 2) if deltas else None,
            "manual_review_count": sum(
                1
                for r in group
                if r.action in {ACTION_REVIEW_LARGE_INCREASE, ACTION_REVIEW_COST_ANOMALY}
            ),
            "increase_bands": dict(Counter(increase_band(r.dollar_change) for r in increases_g)),
            "movement_bands": dict(Counter(movement_bucket(r.dollar_change, r.action) for r in elig)),
            "current_price_dist": dict(Counter(price_bucket(r.current_retail) for r in elig)),
            "proposed_price_dist": dict(Counter(price_bucket(proposed_price(r)) for r in elig)),
        }

    region_a_info_metrics = {
        "variants_assessed": len(region_a),
        "note": "No USD formula encoded. Metrics below use Shopify AUD unitCost as observed landed cost only.",
        "with_shopify_aud_unit_cost": sum(1 for r in region_a if r.landed_aud_cost),
        "current_weighted_gp_pct_vs_shopify_cost": revenue_weighted_gp(
            [r for r in region_a if r.landed_aud_cost], "current_retail"
        ),
        "info_floor_vs_current": {
            "would_increase": sum(
                1
                for r in region_a
                if r.min_retail_required
                and r.current_retail
                and r.min_retail_required > r.current_retail + 0.005
            ),
            "would_keep_or_decrease": sum(
                1
                for r in region_a
                if r.min_retail_required
                and r.current_retail
                and r.min_retail_required <= r.current_retail + 0.005
            ),
            "avg_info_floor": round(
                statistics.mean([r.min_retail_required for r in region_a if r.min_retail_required]), 2
            )
            if any(r.min_retail_required for r in region_a)
            else None,
            "avg_current": round(
                statistics.mean([r.current_retail for r in region_a if r.current_retail]), 2
            )
            if any(r.current_retail for r in region_a)
            else None,
        },
        "current_price_dist": dict(Counter(price_bucket(r.current_retail) for r in region_a)),
        "info_floor_price_dist": dict(
            Counter(price_bucket(r.min_retail_required) for r in region_a if r.min_retail_required)
        ),
    }

    crit_a_metrics = {
        "variants_assessed": len(crit_a),
        "note": "Criterion Region A — USD formula missing; informational bands from Shopify AUD unitCost floor only.",
        "current_price_bands": crit_a_current_dist,
        "info_floor_price_bands": crit_a_proposed_dist,
        "avg_current_retail": round(
            statistics.mean([r.current_retail for r in crit_a if r.current_retail]), 2
        )
        if crit_a
        else None,
        "avg_shopify_aud_unit_cost": round(
            statistics.mean([r.landed_aud_cost for r in crit_a if r.landed_aud_cost]), 2
        )
        if any(r.landed_aud_cost for r in crit_a)
        else None,
        "avg_info_floor": round(
            statistics.mean([r.min_retail_required for r in crit_a if r.min_retail_required]), 2
        )
        if any(r.min_retail_required for r in crit_a)
        else None,
        "current_weighted_gp_vs_shopify_cost": revenue_weighted_gp(
            [r for r in crit_a if r.landed_aud_cost], "current_retail"
        ),
    }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": True,
        "mutations": False,
        "pricing_assumptions": cfg,
        "canonical_region_b": {
            "function": "calculate_sale_price_with_margin_floor_from_gbp_cost",
            "module": "app.rules.pricing_rules",
            "pipeline": "cost_gbp × GBP_AUD_RATE × LANDED_COST_MARKUP → landed; floor 28% ex-GST; GST; round_up_to_99 with exact decimal bump",
            "env": {
                "GBP_AUD_RATE": gbp_aud,
                "LANDED_COST_MARKUP": markup,
                "DEFAULT_MARGIN_FLOOR_RATIO": floor,
                "GST_RATE": GST_RATE,
            },
        },
        "canonical_region_a": {
            "status": "NOT_ENCODED",
            "gap": True,
            "detail": (
                "No USD→AUD sale-price calculator exists in app/rules/pricing_rules.py or "
                "catalog_shopify_publish_service. New listings always assume GBP cost_price. "
                "Shopify Criterion Region A unitCost is stored in AUD (observed landed), not USD."
            ),
            "observed_commercial_heuristic_only": {
                "approx_us_wholesale": "US$33.75 (user-stated, not in code)",
                "approx_retail": "A$74.99 (user-stated / common on store)",
                "note": "Do not treat as encoded formula",
            },
        },
        "existing_mutation_restriction": {
            "service": "app.services.supplier_margin_protection_service",
            "default": "monitoring_only=True",
            "apply_requires": "explicit ApplyAllowlist (barcodes or variant_ids) AND apply=True",
            "empty_allowlist": "blocks all mutations even with --apply",
            "docs": "docs/data-pipeline-operations.md § Supplier margin monitoring",
        },
        "film_only_scope": {
            "rule": "Product-class exclusion precedes region/currency/supplier eligibility",
            "action_non_film": ACTION_OUT_OF_SCOPE_NON_FILM,
            "action_ambiguous": ACTION_SKIP_AMBIGUOUS_PRODUCT,
            "signals": [
                "custom.format",
                "collections (incl. soundtracks)",
                "soundtracks collection product IDs",
                "product type",
                "tags (conservative)",
                "existing is_vinyl_soundtrack_listing helper",
            ],
            "excluded_categories": ["vinyl", "cd", "book", "game", "other_non_film"],
        },
        "product_class_counts": product_class_counts,
        "cohort_counts": cohort_counts,
        "action_counts": action_counts,
        "region_b_metrics": metrics_for(region_b),
        "region_a_metrics": region_a_info_metrics,
        "criterion_region_a_metrics": crit_a_metrics,
        "criterion_region_b_metrics": metrics_for(crit_b),
        "whole_eligible_region_b_film": metrics_for(region_b),
        "label_analysis_region_b_eligible": [
            x for x in label_rollup(region_b) if x["eligible"] > 0
        ],
        "label_analysis_films_assessed": label_rollup(films),
        "comparison_examples": pick_comparison_examples(films),
        "sales_sensitivity": sales,
        "top_25_increases": [short(r) for r in top_increases],
        "top_25_lowest_current_margin": [short(r) for r in low_margin],
        "cost_anomalies": [short(r) for r in anomalies[:50] if r.product_class == PRODUCT_CLASS_FILM],
        "csv_path": str(csv_path),
        "json_path": str(json_path),
    }

    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    print(json.dumps({
        "csv": str(csv_path),
        "json": str(json_path),
        "product_class_counts": product_class_counts,
        "cohort_counts": cohort_counts,
        "region_b_metrics": payload["region_b_metrics"],
        "criterion_region_a_metrics": crit_a_metrics,
        "sales_sensitivity": sales,
        "top_5_increases": payload["top_25_increases"][:5],
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
