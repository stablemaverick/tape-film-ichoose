#!/usr/bin/env python3
"""
Clearance / cash-generation sale proposal rebuild.

Uses ONLY the manually approved YES list.
Applies Steelbook / premium-label / other / soundtrack discount rules.
28% margin floor does NOT apply.

ANALYSIS / DRY-RUN PREP ONLY — MUTATIONS_ENABLED = False.
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
from app.services.arrow_inventory_policy_sync_service import normalize_studio_label
from app.services.catalog_shopify_publish_service import shopify_inventory_location_id
from app.services.shopify_inventory_settings_audit import parse_shopify_bool_metafield
from app.services.supplier_orders_report_service import parse_release_date

TODAY = date(2026, 9, 22)
MUTATIONS_ENABLED = False

PRODUCTS_QUERY = """
query ClearanceSaleRebuild($cursor: String, $locId: ID!, $q: String) {
  products(first: 50, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      handle
      status
      vendor
      tags
      studio: metafield(namespace: "custom", key: "studio") { value }
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

PREMIUM = {"Arrow", "Criterion Collection", "Second Sight"}


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


def effective_discount_pct(original: float, sale: float) -> float:
    if original <= 0:
        return 0.0
    return round((1.0 - sale / original) * 100.0, 2)


def is_steelbook(title: str, sku: str, tags: List[str]) -> bool:
    blob = " ".join([title or "", sku or "", " ".join(tags or [])]).casefold()
    return "steelbook" in blob or "steel book" in blob


def is_soundtrack_or_merch(title: str, tags: List[str]) -> bool:
    t = (title or "").casefold()
    tag_blob = " ".join(tags or []).casefold()
    keywords = [
        "soundtrack",
        "original score",
        "original motion picture",
        "vinyl",
        " lp)",
        "(lp)",
        " splatter vinyl",
        "colored vinyl",
        "image album",
        "music from",
        "retrospective",  # books like Ridley Scott / Spielberg / Martin Scorsese books often titled simply
    ]
    if any(k in t for k in keywords):
        return True
    # Common book-like short titles in this YES list without format suffixes.
    if t in {"spielberg", "martin scorsese", "ridley scott: a retrospective"}:
        return True
    if "vinyl" in tag_blob or "soundtrack" in tag_blob or "book" in tag_blob:
        return True
    # Titles ending with soundtrack-ish patterns.
    if "score)" in t or "soundtrack)" in t:
        return True
    return False


def classify_premium_label(studio_raw: str, title: str, vendor: str, tags: List[str]) -> Optional[str]:
    """Return Arrow / Criterion Collection / Second Sight or None."""
    canon = normalize_studio_label(studio_raw or "")
    if canon in PREMIUM:
        return canon
    blob = " ".join(
        [studio_raw or "", title or "", vendor or "", " ".join(tags or [])]
    ).casefold()
    if "second sight" in blob:
        return "Second Sight"
    if "criterion" in blob:
        return "Criterion Collection"
    if re.search(r"\barrow(\s|/|-|$)", blob) or "arrow video" in blob or "arrow films" in blob:
        return "Arrow"
    return None


def edition_type(*, steelbook: bool, premium: Optional[str], soundtrack: bool) -> str:
    parts: List[str] = []
    if steelbook:
        parts.append("Steelbook")
    if premium:
        parts.append(premium)
    if soundtrack:
        parts.append("Soundtrack/Merch")
    if not parts:
        parts.append("Standard physical")
    return " + ".join(parts)


def discount_rule(
    *,
    steelbook: bool,
    premium: Optional[str],
    soundtrack: bool,
    available: int,
) -> Tuple[str, int]:
    """
    Returns (rule_name, nominal_discount_pct).
    Priority: premium label (incl. premium steelbooks) > steelbook > soundtrack/merch > other.
    """
    if premium:
        if available >= 2:
            return f"premium_label_{premium.replace(' ', '_').lower()}_2plus", 15
        return f"premium_label_{premium.replace(' ', '_').lower()}_1unit", 10
    if steelbook:
        if available >= 3:
            return "steelbook_3plus", 20
        return "steelbook_1_2", 15
    if soundtrack:
        return "soundtrack_vinyl_book_merch", 20
    return "other_physical_media", 20


@dataclass
class Candidate:
    title: str
    sku: str
    cost_report: Optional[float]
    ending_units: Optional[int]
    ending_retail: Optional[float]


@dataclass
class ShopifyVariant:
    product_id: str
    product_title: str
    status: str
    vendor: str
    studio_raw: str
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


def load_yes_candidates(path: Path) -> List[Candidate]:
    no_set = {norm_key(t) for t in NO_TITLES}
    yes: List[Candidate] = []
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for raw in reader:
            title = (clean_text(raw.get("Product title") or "") or "").replace("¬¨‚Ä†", "").strip()
            if not title:
                continue
            tl = title.casefold()
            if tl.startswith("test ") or "not for sale" in tl or tl.startswith("test product"):
                continue
            if norm_key(title) in no_set:
                continue
            yes.append(
                Candidate(
                    title=title,
                    sku=(clean_text(raw.get("Product variant SKU") or "") or ""),
                    cost_report=_f(raw.get("Inventory item cost")),
                    ending_units=_i(raw.get("Ending inventory units")),
                    ending_retail=_f(raw.get("Ending inventory retail value")),
                )
            )
    return yes


def fetch_shopify(client: ShopifyClient, location_id: str) -> List[ShopifyVariant]:
    out: List[ShopifyVariant] = []
    cursor = None
    while True:
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
            studio_raw = clean_text((product.get("studio") or {}).get("value")) or ""
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
                        vendor=clean_text(product.get("vendor")) or "",
                        studio_raw=studio_raw,
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
    print(f"Fetched {len(out)} Shopify variants", flush=True)
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
    prior_by_vid: Dict[str, str],
    by_vid: Dict[str, ShopifyVariant],
) -> Tuple[str, Optional[ShopifyVariant], str]:
    # Prefer prior validated variant_id from approval CSV when present.
    prior_vid = prior_by_vid.get(norm_key(cand.title))
    if prior_vid and prior_vid in by_vid:
        return "matched", by_vid[prior_vid], "prior_variant_id"

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


def classify_row(cand: Candidate, status: str, method: str, sv: Optional[ShopifyVariant]) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "product": cand.title,
        "match_status": status,
        "match_method": method,
        "label_studio": "",
        "edition_type": "",
        "barcode": "",
        "sku": "",
        "shopify_product_id": "",
        "shopify_variant_id": "",
        "release_date": "",
        "on_hand": "",
        "committed": "",
        "available_sale_qty": "",
        "cost": "",
        "original_price": "",
        "original_margin_pct": "",
        "discount_rule": "",
        "nominal_discount_pct": "",
        "sale_price": "",
        "effective_discount_pct": "",
        "sale_margin_pct": "",
        "gross_profit_per_unit": "",
        "original_compare_at": "",
        "decision": "",
        "flags": "",
        "product_status": "",
        "tags": "",
        "snapshot_at": datetime.now(timezone.utc).isoformat(),
    }
    if status != "matched" or sv is None:
        base["decision"] = "INVENTORY_REVIEW"
        base["flags"] = status.upper()
        return base

    release = parse_release_date(sv.media_release_date)
    future = bool(sv.pre_order or (release and release > TODAY))

    steel = is_steelbook(sv.product_title, sv.sku, sv.tags)
    premium = classify_premium_label(sv.studio_raw, sv.product_title, sv.vendor, sv.tags)
    soundtrack = is_soundtrack_or_merch(sv.product_title, sv.tags)
    label = premium or normalize_studio_label(sv.studio_raw) or (sv.studio_raw or sv.vendor or "")

    inventory_ok = (
        sv.levels_present
        and sv.available is not None
        and sv.committed is not None
        and sv.on_hand is not None
    )
    free_qty = None
    flags: List[str] = []
    if inventory_ok:
        # Prefer Shopify available; cross-check identity.
        expected = int(sv.on_hand) - int(sv.committed)
        if int(sv.available) != expected:
            flags.append("INVENTORY_IDENTITY_MISMATCH")
            inventory_ok = False
        else:
            free_qty = max(0, int(sv.available))
    else:
        flags.append("INVENTORY_LEVELS_MISSING")

    if release is None:
        flags.append("RELEASE_DATE_UNKNOWN")

    base.update(
        {
            "product": sv.product_title,
            "label_studio": label,
            "edition_type": edition_type(steelbook=steel, premium=premium, soundtrack=soundtrack),
            "barcode": sv.barcode,
            "sku": sv.sku,
            "shopify_product_id": sv.product_id,
            "shopify_variant_id": sv.variant_id,
            "release_date": release.isoformat() if release else "",
            "on_hand": sv.on_hand if sv.on_hand is not None else "",
            "committed": sv.committed if sv.committed is not None else "",
            "available_sale_qty": free_qty if free_qty is not None else "",
            "cost": round(sv.unit_cost, 2) if sv.unit_cost is not None else "",
            "original_price": round(sv.price, 2) if sv.price is not None else "",
            "original_compare_at": round(sv.compare_at_price, 2)
            if sv.compare_at_price is not None
            else "",
            "product_status": sv.status,
            "tags": ",".join(sv.tags),
        }
    )

    if future:
        base["decision"] = "FUTURE_RELEASE_OR_PREORDER"
        base["flags"] = "|".join(flags + ["FUTURE_RELEASE_OR_PREORDER"])
        return base

    if not inventory_ok or free_qty is None:
        base["decision"] = "INVENTORY_REVIEW"
        base["flags"] = "|".join(flags + ["INVENTORY_REVIEW"])
        return base

    if free_qty <= 0:
        base["decision"] = "INVENTORY_REVIEW"
        base["flags"] = "|".join(flags + ["NO_FREE_STOCK"])
        return base

    if sv.price is None or sv.price <= 0 or sv.unit_cost is None:
        base["decision"] = "INVENTORY_REVIEW"
        base["flags"] = "|".join(flags + ["MISSING_PRICE_OR_COST"])
        return base

    orig_gp, orig_m = margin_parts(sv.price, sv.unit_cost)
    rule, nom_pct = discount_rule(
        steelbook=steel, premium=premium, soundtrack=soundtrack, available=free_qty
    )
    sale = sale_price_for_discount(sv.price, nom_pct)
    sale_gp, sale_m = margin_parts(sale, sv.unit_cost)
    eff = effective_discount_pct(sv.price, sale)

    base.update(
        {
            "original_margin_pct": round(orig_m, 2),
            "discount_rule": rule,
            "nominal_discount_pct": nom_pct,
            "sale_price": sale,
            "effective_discount_pct": eff,
            "sale_margin_pct": round(sale_m, 2),
            "gross_profit_per_unit": round(sale_gp, 2),
        }
    )

    # Decision priority after rules: existing compare-at review, then negative GP review, else sale.
    if sv.compare_at_price is not None and sv.compare_at_price > 0:
        base["decision"] = "EXISTING_COMPARE_AT_REVIEW"
        base["flags"] = "|".join(flags + ["EXISTING_COMPARE_AT_REVIEW"])
        return base

    if sale_gp < 0:
        base["decision"] = "NEGATIVE_GP_REVIEW"
        base["flags"] = "|".join(flags + ["NEGATIVE_GP_REVIEW"])
        return base

    if nom_pct == 20:
        base["decision"] = "SALE_20"
    elif nom_pct == 15:
        base["decision"] = "SALE_15"
    else:
        base["decision"] = "SALE_10"
    base["flags"] = "|".join(flags)
    return base


def economics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    titles = len(rows)
    units = sum(_i(r["available_sale_qty"]) or 0 for r in rows)
    cost = sum((_i(r["available_sale_qty"]) or 0) * (_f(r["cost"]) or 0) for r in rows)
    original = sum(
        (_i(r["available_sale_qty"]) or 0) * (_f(r["original_price"]) or 0) for r in rows
    )
    sale = sum((_i(r["available_sale_qty"]) or 0) * (_f(r["sale_price"]) or 0) for r in rows)
    gp = sum(
        (_i(r["available_sale_qty"]) or 0) * (_f(r["gross_profit_per_unit"]) or 0) for r in rows
    )
    rev_ex = sale / GST_RATE if sale else 0.0
    return {
        "titles": titles,
        "units": units,
        "inventory_cost": round(cost, 2),
        "original_retail_value": round(original, 2),
        "proposed_sale_retail_value": round(sale, 2),
        "markdown_dollars": round(original - sale, 2),
        "effective_weighted_markdown_pct": round((1 - sale / original) * 100, 2) if original else 0,
        "expected_revenue_ex_gst": round(rev_ex, 2),
        "expected_gross_profit": round(gp, 2),
        "weighted_gross_margin_pct": round((gp / rev_ex) * 100, 2) if rev_ex else 0,
    }


def write_csv(path: Path, rows: List[Dict[str, Any]], fields: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=str(_REPO / ".env.prod"))
    parser.add_argument(
        "--candidates",
        default=str(_REPO / "tmp/sale_candidates_20260922/candidates.tsv"),
    )
    parser.add_argument(
        "--prior-approval",
        default=str(_REPO / "tmp/sale_candidates_20260922/final_sale_approval.csv"),
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

    yes = load_yes_candidates(Path(args.candidates))
    print(f"Manually approved YES titles: {len(yes)}", flush=True)

    prior_by_vid: Dict[str, str] = {}
    prior_path = Path(args.prior_approval)
    if prior_path.exists():
        with prior_path.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if (r.get("include") or "").strip().upper() == "YES" and r.get("variant_id"):
                    prior_by_vid[norm_key(r.get("product") or "")] = r["variant_id"]

    client = ShopifyClient()
    variants = fetch_shopify(client, location_id)
    by_barcode, by_sku, by_title, by_vid = build_indexes(variants)

    rows: List[Dict[str, Any]] = []
    for cand in yes:
        status, match, method = match_candidate(
            cand, by_barcode, by_sku, by_title, prior_by_vid, by_vid
        )
        rows.append(classify_row(cand, status, method, match))

    groups = {
        "SALE_20": [r for r in rows if r["decision"] == "SALE_20"],
        "SALE_15": [r for r in rows if r["decision"] == "SALE_15"],
        "SALE_10": [r for r in rows if r["decision"] == "SALE_10"],
        "NEGATIVE_GP_REVIEW": [r for r in rows if r["decision"] == "NEGATIVE_GP_REVIEW"],
        "EXISTING_COMPARE_AT_REVIEW": [
            r for r in rows if r["decision"] == "EXISTING_COMPARE_AT_REVIEW"
        ],
        "INVENTORY_REVIEW": [r for r in rows if r["decision"] == "INVENTORY_REVIEW"],
        "FUTURE_RELEASE_OR_PREORDER": [
            r for r in rows if r["decision"] == "FUTURE_RELEASE_OR_PREORDER"
        ],
    }
    for k in groups:
        groups[k] = sorted(
            groups[k],
            key=lambda r: -(
                (_i(r["available_sale_qty"]) or 0) * (_f(r["original_price"]) or 0)
            ),
        )

    sale_rows = groups["SALE_20"] + groups["SALE_15"] + groups["SALE_10"]
    fields = [
        "product",
        "label_studio",
        "edition_type",
        "available_sale_qty",
        "cost",
        "original_price",
        "original_margin_pct",
        "discount_rule",
        "nominal_discount_pct",
        "sale_price",
        "effective_discount_pct",
        "sale_margin_pct",
        "gross_profit_per_unit",
        "original_compare_at",
        "decision",
        "barcode",
        "sku",
        "shopify_product_id",
        "shopify_variant_id",
        "release_date",
        "on_hand",
        "committed",
        "flags",
        "product_status",
        "tags",
        "match_status",
        "match_method",
        "snapshot_at",
    ]

    write_csv(out_dir / "clearance_sale_all.csv", rows, fields)
    write_csv(out_dir / "clearance_sale_SALE_20.csv", groups["SALE_20"], fields)
    write_csv(out_dir / "clearance_sale_SALE_15.csv", groups["SALE_15"], fields)
    write_csv(out_dir / "clearance_sale_SALE_10.csv", groups["SALE_10"], fields)
    write_csv(out_dir / "clearance_sale_NEGATIVE_GP_REVIEW.csv", groups["NEGATIVE_GP_REVIEW"], fields)
    write_csv(
        out_dir / "clearance_sale_EXISTING_COMPARE_AT_REVIEW.csv",
        groups["EXISTING_COMPARE_AT_REVIEW"],
        fields,
    )
    write_csv(out_dir / "clearance_sale_INVENTORY_REVIEW.csv", groups["INVENTORY_REVIEW"], fields)
    write_csv(
        out_dir / "clearance_sale_FUTURE_RELEASE_OR_PREORDER.csv",
        groups["FUTURE_RELEASE_OR_PREORDER"],
        fields,
    )

    # Executable sale snapshot for future dry-run (SALE_10/15/20 only).
    snapshot = []
    for r in sale_rows:
        snapshot.append(
            {
                "decision": r["decision"],
                "product_title": r["product"],
                "barcode": r["barcode"],
                "sku": r["sku"],
                "shopify_product_id": r["shopify_product_id"],
                "shopify_variant_id": r["shopify_variant_id"],
                "available_sale_qty": r["available_sale_qty"],
                "on_hand": r["on_hand"],
                "committed": r["committed"],
                "unit_cost": r["cost"],
                "current_price": r["original_price"],
                "compare_at_price": None,  # must be blank to proceed
                "proposed_sale_price": r["sale_price"],
                "nominal_discount_pct": r["nominal_discount_pct"],
                "effective_discount_pct": r["effective_discount_pct"],
                "discount_rule": r["discount_rule"],
                "tags": r["tags"],
                "product_status": r["product_status"],
                "snapshot_at": r["snapshot_at"],
            }
        )
    (out_dir / "clearance_sale_executable_snapshot.json").write_text(
        json.dumps(snapshot, indent=2), encoding="utf-8"
    )

    # Approval CSV for sale-ready only
    approval_fields = [
        "include",
        "decision",
        "product",
        "barcode",
        "sku",
        "variant_id",
        "available_qty",
        "unit_cost",
        "current_price",
        "sale_price",
        "nominal_discount_pct",
        "effective_discount_pct",
        "sale_margin_pct",
        "gross_profit_per_unit",
        "discount_rule",
        "label_studio",
        "edition_type",
        "flags",
    ]
    with (out_dir / "clearance_sale_approval.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=approval_fields)
        w.writeheader()
        for r in sale_rows:
            w.writerow(
                {
                    "include": "YES",
                    "decision": r["decision"],
                    "product": r["product"],
                    "barcode": r["barcode"],
                    "sku": r["sku"],
                    "variant_id": r["shopify_variant_id"],
                    "available_qty": r["available_sale_qty"],
                    "unit_cost": r["cost"],
                    "current_price": r["original_price"],
                    "sale_price": r["sale_price"],
                    "nominal_discount_pct": r["nominal_discount_pct"],
                    "effective_discount_pct": r["effective_discount_pct"],
                    "sale_margin_pct": r["sale_margin_pct"],
                    "gross_profit_per_unit": r["gross_profit_per_unit"],
                    "discount_rule": r["discount_rule"],
                    "label_studio": r["label_studio"],
                    "edition_type": r["edition_type"],
                    "flags": r["flags"],
                }
            )

    summary = {
        "as_of": TODAY.isoformat(),
        "mutations_enabled": MUTATIONS_ENABLED,
        "strategy": "cash_generation_inventory_clearance",
        "margin_floor_28_applied": False,
        "yes_titles": len(yes),
        "matched": sum(1 for r in rows if r["match_status"] == "matched"),
        "counts": {k: len(v) for k, v in groups.items()},
        "sale_combined": economics(sale_rows),
        "sale_20": economics(groups["SALE_20"]),
        "sale_15": economics(groups["SALE_15"]),
        "sale_10": economics(groups["SALE_10"]),
        "negative_gp_review": {
            "titles": len(groups["NEGATIVE_GP_REVIEW"]),
            "units": sum(_i(r["available_sale_qty"]) or 0 for r in groups["NEGATIVE_GP_REVIEW"]),
            "original_retail_value": round(
                sum(
                    (_i(r["available_sale_qty"]) or 0) * (_f(r["original_price"]) or 0)
                    for r in groups["NEGATIVE_GP_REVIEW"]
                ),
                2,
            ),
            "proposed_sale_retail_value": round(
                sum(
                    (_i(r["available_sale_qty"]) or 0) * (_f(r["sale_price"]) or 0)
                    for r in groups["NEGATIVE_GP_REVIEW"]
                ),
                2,
            ),
        },
        "existing_compare_at_review_titles": len(groups["EXISTING_COMPARE_AT_REVIEW"]),
        "inventory_review_titles": len(groups["INVENTORY_REVIEW"]),
        "future_preorder_titles": len(groups["FUTURE_RELEASE_OR_PREORDER"]),
        "executable_snapshot_rows": len(snapshot),
    }
    (out_dir / "clearance_sale_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )

    # Markdown report
    md: List[str] = []
    md.append("# Clearance Sale Proposal — 2026-09-22")
    md.append("")
    md.append("CASH-GENERATION / INVENTORY-CLEARANCE. 28% floor NOT applied.")
    md.append("ANALYSIS / DRY-RUN ONLY. MUTATIONS_ENABLED=False. No Shopify changes.")
    md.append("")
    md.append("## Summary economics (SALE_10 + SALE_15 + SALE_20)")
    md.append("")
    for label, eco in [
        ("COMBINED", summary["sale_combined"]),
        ("20%", summary["sale_20"]),
        ("15%", summary["sale_15"]),
        ("10%", summary["sale_10"]),
    ]:
        md.append(f"### {label}")
        md.append(
            f"- titles={eco['titles']} units={eco['units']} "
            f"cost=${eco['inventory_cost']:,.2f} "
            f"orig=${eco['original_retail_value']:,.2f} "
            f"sale=${eco['proposed_sale_retail_value']:,.2f} "
            f"markdown=${eco['markdown_dollars']:,.2f} ({eco['effective_weighted_markdown_pct']}%) "
            f"rev_ex_gst=${eco['expected_revenue_ex_gst']:,.2f} "
            f"gp=${eco['expected_gross_profit']:,.2f} "
            f"wm={eco['weighted_gross_margin_pct']}%"
        )
        md.append("")

    md.append(
        f"- NEGATIVE_GP_REVIEW: {summary['negative_gp_review']['titles']} titles / "
        f"{summary['negative_gp_review']['units']} units / "
        f"orig ${summary['negative_gp_review']['original_retail_value']:,.2f} / "
        f"proposed ${summary['negative_gp_review']['proposed_sale_retail_value']:,.2f}"
    )
    md.append(
        f"- EXISTING_COMPARE_AT_REVIEW: {summary['existing_compare_at_review_titles']}"
    )
    md.append(f"- INVENTORY_REVIEW: {summary['inventory_review_titles']}")
    md.append(f"- FUTURE_RELEASE_OR_PREORDER: {summary['future_preorder_titles']}")
    md.append("")

    def section(title: str, items: List[Dict[str, Any]]) -> None:
        md.append(f"## {title} ({len(items)})")
        md.append("")
        md.append(
            "| Product | Label/Studio | Edition | Avail | Cost | Orig $ | Orig M% | Rule | Nom% | "
            "Sale $ | Eff% | Sale M% | GP/u | Compare-at | Decision |"
        )
        md.append("|---|---|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---|---|")
        for r in items:
            md.append(
                f"| {r['product']} | {r['label_studio']} | {r['edition_type']} | "
                f"{r['available_sale_qty']} | {r['cost']} | {r['original_price']} | "
                f"{r['original_margin_pct']} | {r['discount_rule']} | {r['nominal_discount_pct']} | "
                f"{r['sale_price']} | {r['effective_discount_pct']} | {r['sale_margin_pct']} | "
                f"{r['gross_profit_per_unit']} | {r['original_compare_at']} | {r['decision']} |"
            )
        md.append("")

    section("20% SALE", groups["SALE_20"])
    section("15% SALE", groups["SALE_15"])
    section("10% SALE", groups["SALE_10"])
    section("NEGATIVE GP REVIEW", groups["NEGATIVE_GP_REVIEW"])
    section("EXISTING COMPARE-AT REVIEW", groups["EXISTING_COMPARE_AT_REVIEW"])
    section("INVENTORY REVIEW", groups["INVENTORY_REVIEW"])
    section("FUTURE/PREORDER EXCLUDED", groups["FUTURE_RELEASE_OR_PREORDER"])

    md.append("## Shopify update preparation")
    md.append("")
    md.append(
        f"Executable snapshot rows (SALE_10/15/20 only): {len(snapshot)} "
        f"→ `clearance_sale_executable_snapshot.json`"
    )
    md.append("Future apply: set price=sale_price, compareAtPrice=original_price, add tag Sale.")
    md.append("Pre-mutation checks: price unchanged, compare-at still blank, free stock > 0.")
    md.append("MUTATIONS_ENABLED=False — not executed.")
    md.append("")
    md.append("CLEARANCE SALE DRY RUN READY — NO SHOPIFY CHANGES MADE")

    (out_dir / "CLEARANCE_SALE_REPORT.md").write_text("\n".join(md), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Wrote outputs under {out_dir}")
    print("CLEARANCE SALE DRY RUN READY — NO SHOPIFY CHANGES MADE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
