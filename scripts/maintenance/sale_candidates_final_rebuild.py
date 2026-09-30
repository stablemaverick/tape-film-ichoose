#!/usr/bin/env python3
"""
Rebuild sale proposal from manually approved YES titles.

ANALYSIS ONLY — no Shopify mutations.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text
from app.rules.pricing_rules import GST_RATE, round_up_to_99
from app.services.catalog_shopify_publish_service import shopify_inventory_location_id
from app.services.shopify_inventory_settings_audit import parse_shopify_bool_metafield
from app.services.supplier_orders_report_service import parse_release_date

TODAY = date(2026, 9, 22)
MARGIN_FLOOR = 28.0

PRODUCTS_QUERY = """
query SaleFinalRebuild($cursor: String, $locId: ID!, $q: String) {
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

NO_TITLES = [
    "Satantango 4K Ultra HD",
    "Alice Doesnt Live Here Anymore 4K Ultra HD + Blu-Ray",
    "Desperate Living 4K Ultra HD + Blu-Ray",
    "Vampires Kiss Limited Edition 4K Ultra HD",
    "Body Heat 4K Ultra HD + Blu-Ray",
    "Jurassic Park Limited Edition Steelbook 4K Ultra HD + Blu-Ray",
    "12 Angry Men 4K Ultra HD + Blu-Ray",
    "10 Cloverfield Lane Limited Edition Steelbook 4K Ultra HD + Blu-Ray",
    "Beverly Hills Cop Limited Edition Steelbook 4K Ultra HD",
    "The Evil Dead (1981) Limited Edition Steelbook 4K Ultra HD + Blu-Ray",
    "Poltergeist - The Film Vault Limited Edition Steelbook 4K Ultra HD + Blu-Ray",
    "The Mask Limited Edition 4K Ultra HD",
    "Thief Limited Edition 4K Ultra HD",
    "To Live And Die in LA Limited Edition 4K Ultra HD",
    "City On Fire Limited Edition 4K Ultra HD",
    "Damnation 4K Ultra HD + Blu-Ray",
    "Inglourious Basterds 4K Ultra HD",
    "Alpha (2025) 4K Ultra HD + Blu-Ray",
    "Misery Limited Edition 4K Ultra HD",
    "The Ugly Stepsister 4K Ultra HD",
    "Jeanne Dielman, 23, Quai Du Commerce, 1080 Bruxelles Blu-Ray",
]


def norm_key(s: str) -> str:
    text = unicodedata.normalize("NFKD", s or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.casefold().replace("’", "'").replace("`", "'")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


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


def margin_parts(price_inc_gst: float, unit_cost: float) -> Tuple[float, float]:
    net = price_inc_gst / GST_RATE
    gp = net - unit_cost
    margin = (gp / net * 100.0) if net else float("nan")
    return gp, margin


def sale_price_for_discount(current: float, pct: int) -> float:
    raw = current * (1.0 - pct / 100.0)
    proposed = round_up_to_99(raw)
    if proposed >= current - 0.001:
        proposed = round_up_to_99(max(0.99, current - 1.0))
    return round(proposed, 2)


@dataclass
class Candidate:
    title: str
    sku: str
    cost_report: Optional[float]
    ending_units: Optional[int]
    ending_retail: Optional[float]
    abc: str


@dataclass
class ShopifyVariant:
    product_id: str
    product_title: str
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


def load_yes_candidates(path: Path) -> Tuple[List[Candidate], List[str], List[str]]:
    no_set = {norm_key(t) for t in NO_TITLES}
    yes: List[Candidate] = []
    excluded_no: List[str] = []
    skipped_tests: List[str] = []
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for raw in reader:
            title = clean_text(raw.get("Product title") or "") or ""
            title = title.replace("¬¨‚Ä†", "").strip()
            if not title:
                continue
            tl = title.casefold()
            if tl.startswith("test ") or "not for sale" in tl or tl.startswith("test product"):
                skipped_tests.append(title)
                continue
            if norm_key(title) in no_set:
                excluded_no.append(title)
                no_set.discard(norm_key(title))
                continue
            yes.append(
                Candidate(
                    title=title,
                    sku=(clean_text(raw.get("Product variant SKU") or "") or ""),
                    cost_report=_f(raw.get("Inventory item cost")),
                    ending_units=_i(raw.get("Ending inventory units")),
                    ending_retail=_f(raw.get("Ending inventory retail value")),
                    abc=(clean_text(raw.get("Product variant ABC grade") or "") or ""),
                )
            )
    if no_set:
        print(f"WARN: NO titles not found in candidates: {sorted(no_set)}", flush=True)
    return yes, excluded_no, skipped_tests


def fetch_shopify(client: ShopifyClient, location_id: str) -> List[ShopifyVariant]:
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
            media_release = clean_text((product.get("mediaRelease") or {}).get("value")) or ""
            pre_order = _metafield_bool(product, "preOrder", "preorderAlt")
            for variant in (product.get("variants") or {}).get("nodes") or []:
                inv = variant.get("inventoryItem") or {}
                cost_obj = inv.get("unitCost") or {}
                qmap = _qty_map(inv.get("inventoryLevel"))
                levels_present = bool(inv.get("inventoryLevel"))
                out.append(
                    ShopifyVariant(
                        product_id=clean_text(product.get("id")) or "",
                        product_title=clean_text(product.get("title")) or "",
                        status=clean_text(product.get("status")) or "",
                        tags=tags,
                        pre_order=pre_order,
                        media_release_date=media_release,
                        variant_id=clean_text(variant.get("id")) or "",
                        variant_title=clean_text(variant.get("title")) or "Default Title",
                        sku=clean_text(variant.get("sku")) or "",
                        barcode=clean_text(variant.get("barcode")) or "",
                        price=_f(variant.get("price")),
                        compare_at_price=_f(variant.get("compareAtPrice")),
                        inventory_quantity=_i(variant.get("inventoryQuantity")),
                        unit_cost=_f(cost_obj.get("amount")),
                        cost_currency=clean_text(cost_obj.get("currencyCode")) or "",
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
    print(f"Fetched {len(out)} Shopify variants ({pages} pages)", flush=True)
    return out


def build_indexes(variants: List[ShopifyVariant]):
    by_barcode: Dict[str, List[ShopifyVariant]] = {}
    by_sku: Dict[str, List[ShopifyVariant]] = {}
    by_title: Dict[str, List[ShopifyVariant]] = {}
    by_vid: Dict[str, ShopifyVariant] = {}
    for v in variants:
        if v.barcode:
            by_barcode.setdefault(v.barcode.casefold(), []).append(v)
        if v.sku:
            by_sku.setdefault(v.sku.casefold(), []).append(v)
        by_title.setdefault(norm_key(v.product_title), []).append(v)
        by_vid[v.variant_id] = v
    return by_barcode, by_sku, by_title, by_vid


def match_candidate(
    cand: Candidate,
    by_barcode,
    by_sku,
    by_title,
    prior_by_title: Dict[str, Dict[str, Any]],
) -> Tuple[str, Optional[ShopifyVariant], str]:
    sku = (cand.sku or "").strip()
    if sku:
        hits = by_sku.get(sku.casefold()) or []
        if not hits and sku.isdigit() and len(sku) >= 8:
            hits = by_barcode.get(sku.casefold()) or []
        if len(hits) == 1:
            return "matched", hits[0], "sku"
        if len(hits) > 1:
            title_hits = [h for h in hits if norm_key(h.product_title) == norm_key(cand.title)]
            if len(title_hits) == 1:
                return "matched", title_hits[0], "sku+title"
            return "ambiguous", None, f"sku_multiple:{len(hits)}"

    # Prior analysis variant id if title matches uniquely in prior snapshot.
    prior = prior_by_title.get(norm_key(cand.title))
    if prior and prior.get("shopify_variant_id"):
        # Prefer re-resolve via indexes using prior barcode/sku when present.
        pb = (prior.get("barcode") or "").strip()
        ps = (prior.get("sku") or "").strip()
        if pb and pb.casefold() in by_barcode and len(by_barcode[pb.casefold()]) == 1:
            return "matched", by_barcode[pb.casefold()][0], "prior_barcode"
        if ps and ps.casefold() in by_sku and len(by_sku[ps.casefold()]) == 1:
            return "matched", by_sku[ps.casefold()][0], "prior_sku"

    title_hits = by_title.get(norm_key(cand.title)) or []
    if len(title_hits) == 1:
        return "matched", title_hits[0], "exact_title"
    if len(title_hits) > 1:
        unit_retail = None
        if cand.ending_retail and cand.ending_units and cand.ending_units > 0:
            unit_retail = round(cand.ending_retail / cand.ending_units, 2)
        narrowed = title_hits
        if unit_retail is not None:
            price_hits = [
                h for h in title_hits if h.price is not None and abs(h.price - unit_retail) < 0.02
            ]
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


def recommend_discount(
    *,
    age_days: Optional[int],
    available: int,
    current_margin: Optional[float],
    tiers: Dict[int, Tuple[float, Optional[float], Optional[float]]],
    future_or_preorder: bool,
    inventory_uncertain: bool,
) -> Tuple[str, Optional[float], Optional[float], str]:
    """
    Returns (label, target_price, target_margin, rationale)
    label in {20,15,10,NO_DISCOUNT}
    """
    if future_or_preorder:
        return "NO_DISCOUNT", None, None, "future_release_or_preorder"
    if inventory_uncertain:
        return "NO_DISCOUNT", None, None, "inventory_uncertain"
    if available <= 0:
        return "NO_DISCOUNT", None, None, "no_uncommitted_stock"

    # Economics at each tier.
    def ok(pct: int, min_margin: float = -999.0) -> bool:
        _price, gp, m = tiers[pct]
        if gp is None or m is None:
            return False
        return gp >= 0 and m >= min_margin

    # Hard stop: if even 10% creates negative GP, no discount.
    p10, gp10, m10 = tiers[10]
    if gp10 is not None and gp10 < 0:
        return "NO_DISCOUNT", None, None, "ten_pct_negative_gp"
    if current_margin is not None and current_margin < 0:
        return "NO_DISCOUNT", None, None, "currently_negative_gp"

    age = age_days if age_days is not None else -1
    # Prefer deeper markdowns only when age/qty justify and GP stays non-negative.
    # 20%: older + excess stock.
    if available >= 4 and age >= 270 and ok(20, 0):
        p, _gp, m = tiers[20]
        return "20", p, m, "old_stock_high_qty"
    if available >= 5 and age >= 180 and ok(20, 5):
        p, _gp, m = tiers[20]
        return "20", p, m, "high_qty_semi_old"
    if available >= 3 and age >= 365 and ok(20, 0):
        p, _gp, m = tiers[20]
        return "20", p, m, "year_plus_multi_unit"

    # 15%: moderate age or multi-unit.
    if available >= 3 and age >= 120 and ok(15, 0):
        p, _gp, m = tiers[15]
        return "15", p, m, "multi_unit_aged"
    if available >= 4 and age >= 60 and ok(15, 5):
        p, _gp, m = tiers[15]
        return "15", p, m, "excess_stock"
    if available >= 2 and age >= 270 and ok(15, 0):
        p, _gp, m = tiers[15]
        return "15", p, m, "old_dual_unit"
    if available >= 6 and ok(15, 8):
        p, _gp, m = tiers[15]
        return "15", p, m, "very_high_qty"

    # 10%: default useful markdown when stock is saleable and GP stays >= 0.
    if ok(10, 0):
        # Recent / single unit stays at 10 rather than deeper.
        if age >= 0 and age < 90 and available <= 2:
            return "10", p10, m10, "recent_or_low_qty"
        if available == 1:
            return "10", p10, m10, "single_unit"
        # If 15 was close but failed margin gate, fall back to 10.
        return "10", p10, m10, "standard_markdown"

    return "NO_DISCOUNT", None, None, "economics_not_supportive"


def build_row(cand: Candidate, status: str, method: str, sv: Optional[ShopifyVariant]) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "include": "",
        "candidate_title": cand.title,
        "match_status": status,
        "match_method": method,
        "product": "",
        "variant": "",
        "barcode": "",
        "sku": "",
        "shopify_product_id": "",
        "shopify_variant_id": "",
        "release_date": "",
        "age_days": "",
        "unit_cost": "",
        "current_price": "",
        "current_margin_pct": "",
        "existing_compare_at": "",
        "on_hand": "",
        "available_qty": "",
        "committed_qty": "",
        "inventory_quantity": "",
        "price_10": "",
        "margin_10": "",
        "price_15": "",
        "margin_15": "",
        "price_20": "",
        "margin_20": "",
        "recommended_discount": "",
        "recommended_discount_pct": "",
        "target_sale_price": "",
        "target_margin_pct": "",
        "recommend_rationale": "",
        "flags": "",
        "compare_at_action": "",
    }
    if status != "matched" or sv is None:
        row["flags"] = "UNMATCHED" if status == "unmatched" else "AMBIGUOUS"
        return row

    flags: List[str] = []
    release = parse_release_date(sv.media_release_date)
    age_days = (TODAY - release).days if release else None
    future_or_preorder = bool(sv.pre_order or (release and release > TODAY))
    inventory_uncertain = not (
        sv.levels_present
        and sv.available is not None
        and sv.committed is not None
        and sv.on_hand is not None
    )
    if inventory_uncertain:
        flags.append("INVENTORY_UNCERTAIN")
    elif sv.on_hand is not None and sv.committed is not None and sv.available is not None:
        if sv.available != sv.on_hand - sv.committed:
            flags.append("INVENTORY_IDENTITY_MISMATCH")
            inventory_uncertain = True
    if release is None:
        flags.append("RELEASE_DATE_UNKNOWN")
    if future_or_preorder:
        flags.append("FUTURE_RELEASE_OR_PREORDER")
    if sv.committed and sv.committed > 0:
        flags.append("HAS_COMMITTED_DEMAND")

    current_margin = None
    current_gp = None
    tiers: Dict[int, Tuple[float, Optional[float], Optional[float]]] = {}
    if sv.price and sv.unit_cost and sv.unit_cost > 0 and sv.price > 0:
        current_gp, current_margin = margin_parts(sv.price, sv.unit_cost)
        for pct in (10, 15, 20):
            sp = sale_price_for_discount(sv.price, pct)
            gp, m = margin_parts(sp, sv.unit_cost)
            tiers[pct] = (sp, gp, m)
    else:
        flags.append("MISSING_PRICE_OR_COST")
        for pct in (10, 15, 20):
            tiers[pct] = (0.0, None, None)

    avail = int(sv.available or 0) if not inventory_uncertain else 0
    rec_label, target_price, target_margin, rationale = recommend_discount(
        age_days=age_days,
        available=avail if not inventory_uncertain else 0,
        current_margin=current_margin,
        tiers=tiers,
        future_or_preorder=future_or_preorder,
        inventory_uncertain=inventory_uncertain or "MISSING_PRICE_OR_COST" in flags,
    )

    # Flag economics on the recommended target (or on each proposed if discounting).
    if rec_label != "NO_DISCOUNT" and target_margin is not None:
        if target_margin < MARGIN_FLOOR:
            flags.append("BELOW_28_MARGIN")
        gp_t = None
        if target_price is not None and sv.unit_cost:
            gp_t, _ = margin_parts(target_price, sv.unit_cost)
        if gp_t is not None and gp_t < 0:
            flags.append("NEGATIVE_GROSS_PROFIT")
    elif rec_label == "NO_DISCOUNT" and current_gp is not None and current_gp < 0:
        flags.append("NEGATIVE_GROSS_PROFIT")

    compare_action = ""
    if sv.compare_at_price is not None and sv.compare_at_price > 0:
        flags.append("EXISTING_COMPARE_AT_REVIEW")
        compare_action = (
            f"DO_NOT_OVERWRITE existing_compare_at={sv.compare_at_price:.2f}; "
            f"current_price={sv.price}; "
            f"if sale approved, keep compare-at unchanged and only reduce price "
            f"(or skip until manually resolved)"
        )

    row.update(
        {
            "include": "YES",
            "product": sv.product_title,
            "variant": sv.variant_title,
            "barcode": sv.barcode,
            "sku": sv.sku,
            "shopify_product_id": sv.product_id,
            "shopify_variant_id": sv.variant_id,
            "release_date": release.isoformat() if release else (sv.media_release_date or ""),
            "age_days": age_days if age_days is not None else "",
            "unit_cost": round(sv.unit_cost, 2) if sv.unit_cost is not None else "",
            "current_price": round(sv.price, 2) if sv.price is not None else "",
            "current_margin_pct": round(current_margin, 2) if current_margin is not None else "",
            "existing_compare_at": round(sv.compare_at_price, 2)
            if sv.compare_at_price is not None
            else "",
            "on_hand": sv.on_hand if sv.on_hand is not None else "",
            "available_qty": sv.available if sv.available is not None else "",
            "committed_qty": sv.committed if sv.committed is not None else "",
            "inventory_quantity": sv.inventory_quantity if sv.inventory_quantity is not None else "",
            "price_10": round(tiers[10][0], 2) if tiers[10][2] is not None else "",
            "margin_10": round(tiers[10][2], 2) if tiers[10][2] is not None else "",
            "price_15": round(tiers[15][0], 2) if tiers[15][2] is not None else "",
            "margin_15": round(tiers[15][2], 2) if tiers[15][2] is not None else "",
            "price_20": round(tiers[20][0], 2) if tiers[20][2] is not None else "",
            "margin_20": round(tiers[20][2], 2) if tiers[20][2] is not None else "",
            "recommended_discount": rec_label
            if rec_label == "NO_DISCOUNT"
            else f"{rec_label}%",
            "recommended_discount_pct": ""
            if rec_label == "NO_DISCOUNT"
            else rec_label,
            "target_sale_price": round(target_price, 2) if target_price is not None else "",
            "target_margin_pct": round(target_margin, 2) if target_margin is not None else "",
            "recommend_rationale": rationale,
            "flags": "|".join(flags),
            "compare_at_action": compare_action,
        }
    )
    return row


def sort_key(r: Dict[str, Any]):
    order = {"20%": 0, "15%": 1, "10%": 2, "NO_DISCOUNT": 3, "": 4}
    disc = r.get("recommended_discount") or ""
    avail = _i(r.get("available_qty")) or 0
    price = _f(r.get("current_price")) or 0.0
    return (order.get(disc, 9), -(avail * price), r.get("product") or r.get("candidate_title") or "")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=str(_REPO / ".env.prod"))
    parser.add_argument(
        "--candidates",
        default=str(_REPO / "tmp/sale_candidates_20260922/candidates.tsv"),
    )
    parser.add_argument(
        "--prior",
        default=str(_REPO / "tmp/sale_candidates_20260922/all_results.csv"),
    )
    parser.add_argument(
        "--out-dir",
        default=str(_REPO / "tmp/sale_candidates_20260922"),
    )
    args = parser.parse_args()

    load_dotenv(args.env)
    location_id = shopify_inventory_location_id() or "gid://shopify/Location/78213775584"
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    yes, excluded_no, tests = load_yes_candidates(Path(args.candidates))
    print(
        f"YES={len(yes)} explicit_NO_excluded={len(excluded_no)} tests_skipped={len(tests)}",
        flush=True,
    )

    prior_by_title: Dict[str, Dict[str, Any]] = {}
    prior_path = Path(args.prior)
    if prior_path.exists():
        with prior_path.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                t = r.get("product_title") or r.get("candidate_title") or ""
                if t and r.get("shopify_variant_id"):
                    prior_by_title[norm_key(t)] = r

    client = ShopifyClient()
    variants = fetch_shopify(client, location_id)
    by_barcode, by_sku, by_title, _by_vid = build_indexes(variants)

    rows: List[Dict[str, Any]] = []
    for cand in yes:
        status, match, method = match_candidate(
            cand, by_barcode, by_sku, by_title, prior_by_title
        )
        rows.append(build_row(cand, status, method, match))

    matched = [r for r in rows if r["match_status"] == "matched"]
    ambiguous = [r for r in rows if r["match_status"] == "ambiguous"]
    unmatched = [r for r in rows if r["match_status"] == "unmatched"]
    matched_sorted = sorted(matched, key=sort_key)

    # Approval CSV — only successfully matched YES rows.
    approval_fields = [
        "include",
        "product",
        "barcode",
        "sku",
        "variant_id",
        "available_qty",
        "unit_cost",
        "current_price",
        "current_margin_pct",
        "recommended_discount_pct",
        "target_sale_price",
        "target_margin_pct",
        "existing_compare_at",
        "flags",
    ]
    approval_path = out_dir / "final_sale_approval.csv"
    with approval_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=approval_fields)
        w.writeheader()
        for r in matched_sorted:
            w.writerow(
                {
                    "include": "YES",
                    "product": r["product"],
                    "barcode": r["barcode"],
                    "sku": r["sku"],
                    "variant_id": r["shopify_variant_id"],
                    "available_qty": r["available_qty"],
                    "unit_cost": r["unit_cost"],
                    "current_price": r["current_price"],
                    "current_margin_pct": r["current_margin_pct"],
                    "recommended_discount_pct": r["recommended_discount_pct"],
                    "target_sale_price": r["target_sale_price"],
                    "target_margin_pct": r["target_margin_pct"],
                    "existing_compare_at": r["existing_compare_at"],
                    "flags": r["flags"],
                }
            )

    # Full proposal table CSV
    full_path = out_dir / "final_sale_proposal_full.csv"
    full_fields = list(matched_sorted[0].keys()) if matched_sorted else list(rows[0].keys())
    with full_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=full_fields, extrasaction="ignore")
        w.writeheader()
        for r in matched_sorted:
            w.writerow(r)
        for r in ambiguous + unmatched:
            w.writerow(r)

    def disc_group(label: str) -> List[Dict[str, Any]]:
        return [r for r in matched_sorted if r["recommended_discount"] == label]

    g20, g15, g10, gnone = disc_group("20%"), disc_group("15%"), disc_group("10%"), disc_group(
        "NO_DISCOUNT"
    )

    # Proposal economics only for recommended discounts (not NO_DISCOUNT).
    proposal = [r for r in matched_sorted if r["recommended_discount"] in {"10%", "15%", "20%"}]
    total_avail = sum(_i(r["available_qty"]) or 0 for r in proposal)
    current_retail = sum(
        (_i(r["available_qty"]) or 0) * (_f(r["current_price"]) or 0) for r in proposal
    )
    sale_retail = sum(
        (_i(r["available_qty"]) or 0) * (_f(r["target_sale_price"]) or 0) for r in proposal
    )
    cost_value = sum(
        (_i(r["available_qty"]) or 0) * (_f(r["unit_cost"]) or 0) for r in proposal
    )
    # Est GP ex GST at target sale prices
    est_gp = 0.0
    for r in proposal:
        qty = _i(r["available_qty"]) or 0
        sp = _f(r["target_sale_price"])
        cost = _f(r["unit_cost"])
        if qty and sp is not None and cost is not None:
            gp, _ = margin_parts(sp, cost)
            est_gp += qty * gp
    weighted_margin = (est_gp / (sale_retail / GST_RATE) * 100.0) if sale_retail else 0.0

    def flag_count(token: str) -> int:
        return sum(1 for r in matched_sorted if token in (r.get("flags") or ""))

    summary = {
        "as_of": TODAY.isoformat(),
        "manually_approved_yes_titles": len(yes),
        "explicit_no_excluded": len(excluded_no),
        "tests_skipped": len(tests),
        "successfully_matched": len(matched),
        "ambiguous": len(ambiguous),
        "unmatched": len(unmatched),
        "total_available_units_on_recommended_discounts": total_avail,
        "recommendations": {
            "20": {
                "titles": len(g20),
                "units": sum(_i(r["available_qty"]) or 0 for r in g20),
            },
            "15": {
                "titles": len(g15),
                "units": sum(_i(r["available_qty"]) or 0 for r in g15),
            },
            "10": {
                "titles": len(g10),
                "units": sum(_i(r["available_qty"]) or 0 for r in g10),
            },
            "NO_DISCOUNT": {
                "titles": len(gnone),
                "units": sum(_i(r["available_qty"]) or 0 for r in gnone),
            },
        },
        "BELOW_28_MARGIN_count": flag_count("BELOW_28_MARGIN"),
        "NEGATIVE_GROSS_PROFIT_count": flag_count("NEGATIVE_GROSS_PROFIT"),
        "EXISTING_COMPARE_AT_REVIEW_count": flag_count("EXISTING_COMPARE_AT_REVIEW"),
        "proposal_economics": {
            "current_retail_value": round(current_retail, 2),
            "proposed_sale_retail_value": round(sale_retail, 2),
            "markdown_dollars": round(current_retail - sale_retail, 2),
            "markdown_pct": round((1 - sale_retail / current_retail) * 100, 2)
            if current_retail
            else 0,
            "inventory_cost": round(cost_value, 2),
            "estimated_gross_profit_ex_gst": round(est_gp, 2),
            "weighted_gross_margin_pct": round(weighted_margin, 2),
        },
        "no_discount_rationales": {},
        "ambiguous_titles": [r["candidate_title"] for r in ambiguous],
        "unmatched_titles": [r["candidate_title"] for r in unmatched],
        "explicit_no_titles": excluded_no,
        "sales_velocity_note": (
            "No last-sale/velocity data used; none was loaded from repository for this rebuild."
        ),
        "mutations_enabled": False,
    }
    for r in gnone:
        k = r.get("recommend_rationale") or "unknown"
        summary["no_discount_rationales"][k] = summary["no_discount_rationales"].get(k, 0) + 1

    (out_dir / "final_sale_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )

    # Markdown report
    md: List[str] = []
    md.append("# Final Sale Proposal (manual YES review) — 2026-09-22")
    md.append("")
    md.append("ANALYSIS ONLY. No Shopify mutations. MUTATIONS_ENABLED remains False.")
    md.append("")
    md.append("## Summary")
    md.append("")
    md.append(f"- Manually approved YES titles: **{len(yes)}**")
    md.append(f"- Successfully matched: **{len(matched)}**")
    md.append(f"- Ambiguous: **{len(ambiguous)}**")
    md.append(f"- Unmatched: **{len(unmatched)}**")
    md.append(f"- Explicit NO excluded: **{len(excluded_no)}**")
    md.append(
        f"- Available units on recommended discounts: **{total_avail}**"
    )
    md.append(
        f"- 20%: **{len(g20)}** titles / **{summary['recommendations']['20']['units']}** units"
    )
    md.append(
        f"- 15%: **{len(g15)}** titles / **{summary['recommendations']['15']['units']}** units"
    )
    md.append(
        f"- 10%: **{len(g10)}** titles / **{summary['recommendations']['10']['units']}** units"
    )
    md.append(
        f"- NO_DISCOUNT: **{len(gnone)}** titles / **{summary['recommendations']['NO_DISCOUNT']['units']}** units"
    )
    md.append(f"- BELOW_28_MARGIN: **{summary['BELOW_28_MARGIN_count']}**")
    md.append(f"- NEGATIVE_GROSS_PROFIT: **{summary['NEGATIVE_GROSS_PROFIT_count']}**")
    md.append(
        f"- EXISTING_COMPARE_AT_REVIEW: **{summary['EXISTING_COMPARE_AT_REVIEW_count']}**"
    )
    pe = summary["proposal_economics"]
    md.append("")
    md.append("### Recommended-sale economics")
    md.append("")
    md.append(f"- Current retail value: **${pe['current_retail_value']:,.2f}**")
    md.append(f"- Proposed sale retail value: **${pe['proposed_sale_retail_value']:,.2f}**")
    md.append(f"- Markdown $: **${pe['markdown_dollars']:,.2f}**")
    md.append(f"- Markdown %: **{pe['markdown_pct']:.2f}%**")
    md.append(f"- Inventory cost: **${pe['inventory_cost']:,.2f}**")
    md.append(
        f"- Estimated gross profit (ex GST): **${pe['estimated_gross_profit_ex_gst']:,.2f}**"
    )
    md.append(
        f"- Weighted gross margin %: **{pe['weighted_gross_margin_pct']:.2f}%**"
    )
    md.append("")
    md.append(
        "| Product | Barcode | SKU | Release | Avail | Cost | Current $ | Cur M% | "
        "10% $/M% | 15% $/M% | 20% $/M% | Rec | Target $ | Target M% | Compare-at | Flags |"
    )
    md.append("|---|---|---|---|---:|---:|---:|---:|---|---|---|---|---:|---:|---|---|")
    for r in matched_sorted:
        md.append(
            f"| {r['product']} | {r['barcode']} | {r['sku']} | {r['release_date']} | "
            f"{r['available_qty']} | {r['unit_cost']} | {r['current_price']} | {r['current_margin_pct']} | "
            f"{r['price_10']}/{r['margin_10']} | {r['price_15']}/{r['margin_15']} | "
            f"{r['price_20']}/{r['margin_20']} | {r['recommended_discount']} | "
            f"{r['target_sale_price']} | {r['target_margin_pct']} | "
            f"{r['existing_compare_at']} | {r['flags']} |"
        )
    if ambiguous or unmatched:
        md.append("")
        md.append("## Ambiguous / unmatched")
        for r in ambiguous + unmatched:
            md.append(
                f"- {r['match_status']}: {r['candidate_title']} ({r['match_method']})"
            )
    (out_dir / "FINAL_SALE_REPORT.md").write_text("\n".join(md), encoding="utf-8")

    print(json.dumps(summary, indent=2))
    print(f"Wrote {approval_path}")
    print(f"Wrote {full_path}")
    print(f"Wrote {out_dir / 'FINAL_SALE_REPORT.md'}")
    print("NO SHOPIFY MUTATIONS EXECUTED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
