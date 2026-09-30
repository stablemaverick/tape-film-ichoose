#!/usr/bin/env python3
"""
Sale-candidate analysis (READ-ONLY).

Matches a supplied candidate TSV against live Shopify, confirms release status,
resolves on_hand / committed / available inventory, proposes 10/15/20% discounts,
and writes proposal CSVs for review.

Safety: NO Shopify mutations. Inventory + pricing GraphQL reads only.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text
from app.rules.pricing_rules import DEFAULT_MARGIN_FLOOR_RATIO, GST_RATE, round_up_to_99
from app.services.catalog_shopify_publish_service import shopify_inventory_location_id
from app.services.shopify_inventory_settings_audit import parse_shopify_bool_metafield
from app.services.supplier_orders_report_service import parse_release_date

PRODUCTS_QUERY = """
query SaleCandidatesAnalyse($cursor: String, $locId: ID!, $q: String) {
  products(first: 50, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      handle
      status
      tags
      preOrder: metafield(namespace: "custom", key: "pre_order") { value }
      preorderAlt: metafield(namespace: "custom", key: "preorder") { value }
      mediaRelease: metafield(namespace: "custom", key: "media_release_date") { value }
      variants(first: 50) {
        nodes {
          id
          title
          sku
          barcode
          price
          compareAtPrice
          inventoryQuantity
          inventoryItem {
            id
            unitCost { amount currencyCode }
            inventoryLevel(locationId: $locId) {
              quantities(names: ["available", "committed", "on_hand", "incoming"]) {
                name
                quantity
              }
            }
          }
        }
      }
    }
  }
}
"""

TODAY = date(2026, 9, 22)
MARGIN_FLOOR_PCT = DEFAULT_MARGIN_FLOOR_RATIO * 100.0


@dataclass
class Candidate:
    row_num: int
    product_title: str
    variant_title: str
    sku: str
    cost_report: Optional[float]
    abc_grade: str
    ending_units: Optional[int]
    ending_cost_value: Optional[float]
    ending_retail_value: Optional[float]


@dataclass
class ShopifyVariant:
    product_id: str
    product_title: str
    handle: str
    status: str
    tags: List[str]
    pre_order: bool
    media_release_date: str
    variant_id: str
    variant_title: str
    sku: str
    barcode: str
    price: Optional[float]
    compare_at_price: Optional[float]
    inventory_quantity: Optional[int]
    unit_cost: Optional[float]
    cost_currency: str
    available: Optional[int]
    committed: Optional[int]
    on_hand: Optional[int]
    incoming: Optional[int]
    levels_present: bool


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> Optional[int]:
    if v in (None, "", "None"):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _norm_key(s: str) -> str:
    text = unicodedata.normalize("NFKD", s or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.casefold()
    text = text.replace("’", "'").replace("`", "'")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _qty_map(level: Optional[Dict[str, Any]]) -> Dict[str, int]:
    if not level:
        return {}
    out: Dict[str, int] = {}
    for q in level.get("quantities") or []:
        name = clean_text(q.get("name"))
        if not name:
            continue
        try:
            out[name] = int(q.get("quantity") or 0)
        except (TypeError, ValueError):
            out[name] = 0
    return out


def _metafield_bool(product: Dict[str, Any], *keys: str) -> bool:
    for key in keys:
        mf = product.get(key)
        if isinstance(mf, dict) and mf.get("value") is not None:
            return bool(parse_shopify_bool_metafield(mf.get("value")))
    return False


def load_candidates(path: Path) -> List[Candidate]:
    rows: List[Candidate] = []
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for i, raw in enumerate(reader, start=2):
            title = clean_text(raw.get("Product title") or "")
            if not title:
                continue
            # Strip mojibake leftovers from export.
            title = title.replace("¬¨‚Ä†", "").strip()
            rows.append(
                Candidate(
                    row_num=i,
                    product_title=title,
                    variant_title=clean_text(raw.get("Product variant title") or "") or "Default Title",
                    sku=clean_text(raw.get("Product variant SKU") or "") or "",
                    cost_report=_f(raw.get("Inventory item cost")),
                    abc_grade=clean_text(raw.get("Product variant ABC grade") or ""),
                    ending_units=_i(raw.get("Ending inventory units")),
                    ending_cost_value=_f(raw.get("Ending inventory value")),
                    ending_retail_value=_f(raw.get("Ending inventory retail value")),
                )
            )
    return rows


def fetch_shopify_variants(client: ShopifyClient, location_id: str) -> List[ShopifyVariant]:
    out: List[ShopifyVariant] = []
    cursor = None
    pages = 0
    while True:
        pages += 1
        tries = 0
        while True:
            tries += 1
            try:
                data = client.graphql(
                    PRODUCTS_QUERY,
                    {
                        "cursor": cursor,
                        "locId": location_id,
                        "q": "status:active OR status:draft",
                    },
                )
                break
            except Exception as exc:  # noqa: BLE001
                if ("THROTTLED" in str(exc) or "429" in str(exc)) and tries < 8:
                    time.sleep(min(2 * tries, 12))
                    continue
                raise
        block = data["products"]
        for product in block.get("nodes") or []:
            tags_raw = product.get("tags") or []
            if isinstance(tags_raw, str):
                tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
            else:
                tags = [clean_text(t) for t in tags_raw if clean_text(t)]
            media_release = clean_text((product.get("mediaRelease") or {}).get("value"))
            pre_order = _metafield_bool(product, "preOrder", "preorderAlt")
            for variant in (product.get("variants") or {}).get("nodes") or []:
                inv = variant.get("inventoryItem") or {}
                cost_obj = inv.get("unitCost") or {}
                qmap = _qty_map(inv.get("inventoryLevel"))
                levels_present = bool(inv.get("inventoryLevel"))
                out.append(
                    ShopifyVariant(
                        product_id=clean_text(product.get("id")),
                        product_title=clean_text(product.get("title")),
                        handle=clean_text(product.get("handle")),
                        status=clean_text(product.get("status")),
                        tags=tags,
                        pre_order=pre_order,
                        media_release_date=media_release,
                        variant_id=clean_text(variant.get("id")),
                        variant_title=clean_text(variant.get("title")) or "Default Title",
                        sku=clean_text(variant.get("sku")),
                        barcode=clean_text(variant.get("barcode")),
                        price=_f(variant.get("price")),
                        compare_at_price=_f(variant.get("compareAtPrice")),
                        inventory_quantity=_i(variant.get("inventoryQuantity")),
                        unit_cost=_f(cost_obj.get("amount")),
                        cost_currency=clean_text(cost_obj.get("currencyCode")),
                        available=qmap.get("available") if levels_present else None,
                        committed=qmap.get("committed") if levels_present else None,
                        on_hand=qmap.get("on_hand") if levels_present else None,
                        incoming=qmap.get("incoming") if levels_present else None,
                        levels_present=levels_present,
                    )
                )
        page = block.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        time.sleep(0.25)
    print(f"Fetched {len(out)} Shopify variants across {pages} pages", flush=True)
    return out


def build_indexes(
    variants: List[ShopifyVariant],
) -> Tuple[Dict[str, List[ShopifyVariant]], Dict[str, List[ShopifyVariant]], Dict[str, List[ShopifyVariant]]]:
    by_barcode: Dict[str, List[ShopifyVariant]] = {}
    by_sku: Dict[str, List[ShopifyVariant]] = {}
    by_title: Dict[str, List[ShopifyVariant]] = {}
    for v in variants:
        if v.barcode:
            by_barcode.setdefault(v.barcode.casefold(), []).append(v)
        if v.sku:
            by_sku.setdefault(v.sku.casefold(), []).append(v)
        by_title.setdefault(_norm_key(v.product_title), []).append(v)
    return by_barcode, by_sku, by_title


def match_candidate(
    cand: Candidate,
    by_barcode: Dict[str, List[ShopifyVariant]],
    by_sku: Dict[str, List[ShopifyVariant]],
    by_title: Dict[str, List[ShopifyVariant]],
) -> Tuple[str, Optional[ShopifyVariant], str]:
    """Return (status, match, detail). status: matched|ambiguous|unmatched."""
    sku = (cand.sku or "").strip()
    # Prefer SKU when it looks like a barcode/EAN (>=8 digits) OR alphanumeric catalog SKU.
    if sku:
        hits = by_sku.get(sku.casefold()) or []
        # Also try SKU against barcode index (Shopify often stores EAN as barcode).
        if not hits and sku.isdigit() and len(sku) >= 8:
            hits = by_barcode.get(sku.casefold()) or []
        if len(hits) == 1:
            return "matched", hits[0], "sku"
        if len(hits) > 1:
            # Prefer exact product title among SKU hits.
            title_hits = [h for h in hits if _norm_key(h.product_title) == _norm_key(cand.product_title)]
            if len(title_hits) == 1:
                return "matched", title_hits[0], "sku+title"
            return "ambiguous", None, f"sku_multiple:{len(hits)}"

    # Exact normalised product title.
    title_hits = by_title.get(_norm_key(cand.product_title)) or []
    if len(title_hits) == 1:
        return "matched", title_hits[0], "exact_title"
    if len(title_hits) > 1:
        # If report cost + ending retail imply a unit retail, prefer price match.
        unit_retail = None
        if cand.ending_retail_value and cand.ending_units and cand.ending_units > 0:
            unit_retail = round(cand.ending_retail_value / cand.ending_units, 2)
        narrowed = title_hits
        if unit_retail is not None:
            price_hits = [h for h in title_hits if h.price is not None and abs(h.price - unit_retail) < 0.02]
            if len(price_hits) == 1:
                return "matched", price_hits[0], "exact_title+price"
            if price_hits:
                narrowed = price_hits
        if cand.cost_report is not None:
            cost_hits = [
                h
                for h in narrowed
                if h.unit_cost is not None and abs(h.unit_cost - cand.cost_report) < 0.05
            ]
            if len(cost_hits) == 1:
                return "matched", cost_hits[0], "exact_title+cost"
        return "ambiguous", None, f"title_multiple:{len(title_hits)}"

    return "unmatched", None, "no_confident_match"


def margin_pct(price_inc_gst: float, unit_cost: float) -> float:
    ex = price_inc_gst / GST_RATE
    return (ex - unit_cost) / ex * 100.0


def round_sale_price(raw: float) -> float:
    """Consumer .99 rounding for sale prices (Tape convention)."""
    candidate = round_up_to_99(raw)
    # If rounding *up* increases price vs raw by > $0.50 and raw already near .x9,
    # still keep .99 convention (never round down below intended discount floor
    # when that would undershoot the intended sale by more than a cent of intent).
    return candidate


def propose_discount(
    *,
    current_price: float,
    unit_cost: Optional[float],
    available: int,
    release: Optional[date],
    abc_grade: str,
) -> Tuple[int, float, float, Optional[float], List[str]]:
    """
    Returns (discount_pct, proposed_price, sale_margin_pct_or_nan, gp_per_unit, flags).
    Prefers smallest useful discount among 10/15/20.
    """
    flags: List[str] = []
    age_days = (TODAY - release).days if release else None

    options: List[Tuple[int, float, Optional[float], Optional[float]]] = []
    for pct in (10, 15, 20):
        raw = current_price * (1.0 - pct / 100.0)
        proposed = round_sale_price(raw)
        # Ensure proposed is not higher than current (rounding edge cases).
        if proposed >= current_price - 0.001:
            proposed = round_sale_price(max(0.99, current_price - 1.0))
        m = margin_pct(proposed, unit_cost) if unit_cost and unit_cost > 0 else None
        gp = (proposed / GST_RATE - unit_cost) if unit_cost and unit_cost > 0 else None
        options.append((pct, proposed, m, gp))

    # Score: prefer lower discount; penalise below margin floor; boost for age/qty.
    best = None
    best_score = None
    for pct, proposed, m, gp in options:
        score = 100 - pct  # prefer smaller discount
        if m is not None:
            if m < MARGIN_FLOOR_PCT:
                score -= (MARGIN_FLOOR_PCT - m) * 3.0
            else:
                score += min(10.0, m - MARGIN_FLOOR_PCT) * 0.2
        else:
            score -= 5
        if age_days is not None:
            if age_days >= 365 and pct >= 15:
                score += 4
            elif age_days >= 180 and pct >= 15:
                score += 2
            elif age_days < 60 and pct > 10:
                score -= 6  # recent release: prefer 10%
        if available >= 5 and pct >= 15:
            score += 3
        elif available >= 3 and pct == 15:
            score += 1
        if (abc_grade or "").upper() == "C" and pct == 15:
            score += 1
        # Avoid 20% unless stock is heavy or margin still healthy / age high.
        if pct == 20:
            if available < 4 and (age_days is None or age_days < 270):
                score -= 8
            if m is not None and m < MARGIN_FLOOR_PCT + 2:
                score -= 10
        if best_score is None or score > best_score:
            best_score = score
            best = (pct, proposed, m, gp)

    assert best is not None
    pct, proposed, m, gp = best
    if m is not None and m < MARGIN_FLOOR_PCT:
        flags.append(f"low_margin_below_{MARGIN_FLOOR_PCT:.0f}pct")
    if m is not None and m < 15:
        flags.append("unusually_low_margin")
    if age_days is not None and age_days < 45:
        flags.append("recent_release")
    return pct, proposed, (m if m is not None else float("nan")), gp, flags


def classify_row(
    cand: Candidate,
    match_status: str,
    match_method: str,
    sv: Optional[ShopifyVariant],
) -> Dict[str, Any]:
    base = {
        "candidate_row": cand.row_num,
        "candidate_title": cand.product_title,
        "candidate_sku": cand.sku,
        "candidate_abc": cand.abc_grade,
        "candidate_ending_units": cand.ending_units,
        "candidate_cost": cand.cost_report,
        "match_status": match_status,
        "match_method": match_method,
        "bucket": "",
        "exclude_reason": "",
        "review_reason": "",
        "shopify_product_id": "",
        "shopify_variant_id": "",
        "product_title": "",
        "variant_title": "",
        "barcode": "",
        "sku": "",
        "release_date": "",
        "pre_order": "",
        "product_status": "",
        "tags": "",
        "unit_cost": "",
        "cost_currency": "",
        "inventory_quantity": "",
        "on_hand": "",
        "committed": "",
        "available": "",
        "incoming": "",
        "current_price": "",
        "compare_at_price": "",
        "discount_pct": "",
        "proposed_sale_price": "",
        "sale_gp_per_unit": "",
        "sale_margin_pct": "",
        "current_margin_pct": "",
        "flags": "",
        "age_days": "",
    }

    title_l = cand.product_title.casefold()
    if title_l.startswith("test ") or "not for sale" in title_l or title_l.startswith("test product"):
        base["bucket"] = "excluded"
        base["exclude_reason"] = "test_product"
        return base

    if match_status == "unmatched":
        base["bucket"] = "review"
        base["review_reason"] = "unmatched"
        return base
    if match_status == "ambiguous":
        base["bucket"] = "review"
        base["review_reason"] = match_method
        return base
    if sv is None:
        base["bucket"] = "review"
        base["review_reason"] = "missing_match_payload"
        return base

    base.update(
        {
            "shopify_product_id": sv.product_id,
            "shopify_variant_id": sv.variant_id,
            "product_title": sv.product_title,
            "variant_title": sv.variant_title,
            "barcode": sv.barcode,
            "sku": sv.sku,
            "release_date": sv.media_release_date,
            "pre_order": str(sv.pre_order),
            "product_status": sv.status,
            "tags": ",".join(sv.tags),
            "unit_cost": sv.unit_cost if sv.unit_cost is not None else "",
            "cost_currency": sv.cost_currency,
            "inventory_quantity": sv.inventory_quantity if sv.inventory_quantity is not None else "",
            "on_hand": sv.on_hand if sv.on_hand is not None else "",
            "committed": sv.committed if sv.committed is not None else "",
            "available": sv.available if sv.available is not None else "",
            "incoming": sv.incoming if sv.incoming is not None else "",
            "current_price": sv.price if sv.price is not None else "",
            "compare_at_price": sv.compare_at_price if sv.compare_at_price is not None else "",
        }
    )

    if not sv.levels_present or sv.committed is None or sv.available is None or sv.on_hand is None:
        base["bucket"] = "review"
        base["review_reason"] = "committed_qty_unavailable"
        return base

    release = parse_release_date(sv.media_release_date)
    if release is None:
        base["bucket"] = "review"
        base["review_reason"] = "release_date_unknown"
        return base
    if release > TODAY or sv.pre_order:
        base["bucket"] = "excluded"
        base["exclude_reason"] = "future_release_or_preorder"
        base["age_days"] = (TODAY - release).days
        return base

    if sv.available <= 0:
        base["bucket"] = "excluded"
        base["exclude_reason"] = "no_uncommitted_stock"
        return base

    if sv.price is None or sv.price <= 0:
        base["bucket"] = "review"
        base["review_reason"] = "missing_price"
        return base

    if sv.unit_cost is None or sv.unit_cost <= 0:
        base["bucket"] = "review"
        base["review_reason"] = "missing_unit_cost"
        return base

    discount, proposed, sale_m, gp, flags = propose_discount(
        current_price=sv.price,
        unit_cost=sv.unit_cost,
        available=sv.available,
        release=release,
        abc_grade=cand.abc_grade,
    )
    if sv.compare_at_price is not None and sv.compare_at_price > 0:
        flags.append("existing_compare_at")
    if "Sale" in sv.tags:
        flags.append("already_has_sale_tag")

    cur_m = margin_pct(sv.price, sv.unit_cost)
    base.update(
        {
            "discount_pct": discount,
            "proposed_sale_price": round(proposed, 2),
            "sale_gp_per_unit": round(gp, 2) if gp is not None else "",
            "sale_margin_pct": round(sale_m, 2) if sale_m == sale_m else "",
            "current_margin_pct": round(cur_m, 2),
            "flags": "|".join(flags),
            "age_days": (TODAY - release).days,
        }
    )

    # Do not auto-propose sales that deepen an already-negative gross profit.
    if gp is not None and gp < 0:
        base["bucket"] = "review"
        base["review_reason"] = "sale_creates_negative_gross_profit"
        return base
    if cur_m < 0:
        base["bucket"] = "review"
        base["review_reason"] = "currently_negative_gross_profit"
        return base

    base["bucket"] = "eligible"
    return base


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only sale candidate analysis")
    parser.add_argument("--env", default=str(_REPO / ".env.prod"))
    parser.add_argument(
        "--candidates",
        default=str(_REPO / "tmp/sale_candidates_20260922/candidates.tsv"),
    )
    parser.add_argument(
        "--out-dir",
        default=str(_REPO / "tmp/sale_candidates_20260922"),
    )
    args = parser.parse_args()

    load_dotenv(args.env)
    # Same canonical TAPE fulfilment location fallback as supplier_orders_report_service.
    location_id = shopify_inventory_location_id() or "gid://shopify/Location/78213775584"
    print(f"Using inventory location {location_id}", flush=True)

    candidates = load_candidates(Path(args.candidates))
    print(f"Loaded {len(candidates)} candidates from {args.candidates}", flush=True)

    client = ShopifyClient()
    variants = fetch_shopify_variants(client, location_id)
    by_barcode, by_sku, by_title = build_indexes(variants)

    rows: List[Dict[str, Any]] = []
    for cand in candidates:
        status, match, method = match_candidate(cand, by_barcode, by_sku, by_title)
        rows.append(classify_row(cand, status, method, match))

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    fields = list(rows[0].keys()) if rows else []
    write_csv(out_dir / "all_results.csv", rows, fields)

    eligible = [r for r in rows if r["bucket"] == "eligible"]
    excluded = [r for r in rows if r["bucket"] == "excluded"]
    review = [r for r in rows if r["bucket"] == "review"]

    eligible_sorted = sorted(
        eligible,
        key=lambda r: (
            -int(r["discount_pct"] or 0),
            -float(r["available"] or 0) * float(r["current_price"] or 0),
            str(r["product_title"]),
        ),
    )
    write_csv(out_dir / "eligible.csv", eligible_sorted, fields)
    write_csv(out_dir / "excluded.csv", excluded, fields)
    write_csv(out_dir / "review.csv", review, fields)

    # Snapshot for a future dry-run apply script.
    snapshot = []
    for r in eligible_sorted:
        snapshot.append(
            {
                "product_title": r["product_title"],
                "variant_title": r["variant_title"],
                "barcode": r["barcode"],
                "sku": r["sku"],
                "shopify_product_id": r["shopify_product_id"],
                "shopify_variant_id": r["shopify_variant_id"],
                "release_date": r["release_date"],
                "unit_cost": r["unit_cost"],
                "inventory_quantity": r["inventory_quantity"],
                "on_hand": r["on_hand"],
                "committed": r["committed"],
                "available": r["available"],
                "current_price": r["current_price"],
                "compare_at_price": r["compare_at_price"] if r["compare_at_price"] != "" else None,
                "discount_pct": r["discount_pct"],
                "proposed_sale_price": r["proposed_sale_price"],
                "sale_margin_pct": r["sale_margin_pct"],
                "tags": r["tags"],
                "flags": r["flags"],
                "snapshot_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    (out_dir / "eligible_snapshot.json").write_text(
        json.dumps(snapshot, indent=2), encoding="utf-8"
    )

    total_avail = sum(int(r["available"] or 0) for r in eligible)
    current_retail = sum(int(r["available"] or 0) * float(r["current_price"] or 0) for r in eligible)
    sale_retail = sum(int(r["available"] or 0) * float(r["proposed_sale_price"] or 0) for r in eligible)
    cost_value = sum(int(r["available"] or 0) * float(r["unit_cost"] or 0) for r in eligible)
    gp_sale = sum(int(r["available"] or 0) * float(r["sale_gp_per_unit"] or 0) for r in eligible)

    summary = {
        "as_of": TODAY.isoformat(),
        "candidates": len(candidates),
        "eligible_variants": len(eligible),
        "excluded": len(excluded),
        "review": len(review),
        "total_available_units": total_avail,
        "current_retail_value": round(current_retail, 2),
        "proposed_sale_retail_value": round(sale_retail, 2),
        "retail_value_reduction": round(current_retail - sale_retail, 2),
        "total_cost_value": round(cost_value, 2),
        "estimated_gross_profit_at_sale": round(gp_sale, 2),
        "discount_counts": {
            "10": sum(1 for r in eligible if int(r["discount_pct"] or 0) == 10),
            "15": sum(1 for r in eligible if int(r["discount_pct"] or 0) == 15),
            "20": sum(1 for r in eligible if int(r["discount_pct"] or 0) == 20),
        },
        "exclude_reasons": {},
        "review_reasons": {},
        "field_paths": {
            "price": "ProductVariant.price",
            "compare_at_price": "ProductVariant.compareAtPrice",
            "unit_cost": "ProductVariant.inventoryItem.unitCost.amount",
            "inventory_quantity": "ProductVariant.inventoryQuantity",
            "available": "inventoryItem.inventoryLevel(locationId).quantities[name=available]",
            "committed": "inventoryItem.inventoryLevel(locationId).quantities[name=committed]",
            "on_hand": "inventoryItem.inventoryLevel(locationId).quantities[name=on_hand]",
            "release_date": "Product.metafield(namespace=custom, key=media_release_date)",
        },
        "margin_method": (
            "gross_profit_per_unit = proposed_sale_price/GST_RATE - unit_cost; "
            "gross_margin_percentage = gross_profit_per_unit / (proposed_sale_price/GST_RATE) * 100; "
            f"GST_RATE={GST_RATE}; floor={MARGIN_FLOOR_PCT}%"
        ),
    }
    for r in excluded:
        k = r["exclude_reason"] or "unknown"
        summary["exclude_reasons"][k] = summary["exclude_reasons"].get(k, 0) + 1
    for r in review:
        k = r["review_reason"] or "unknown"
        summary["review_reasons"][k] = summary["review_reasons"].get(k, 0) + 1

    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Wrote outputs under {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
