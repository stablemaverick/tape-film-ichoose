#!/usr/bin/env python3
"""
Final clearance-sale dry-run execution manifest.

- Keeps approved discount assignments from clearance_sale_all.csv
- Deduplicates by Shopify variant ID
- Excludes UNLISTED / non-ACTIVE products from executable set
- Splits negative GP into SMALL_LOSS_CLEARANCE (<=$3) and LARGE_LOSS_REVIEW (>$3)
- EXISTING_SALE_KEEP_PRICE for populated compare-at rows
- Resolves Wicked (2 SKUs) and Infernal Affairs (1 ACTIVE) explicitly

NO Shopify mutations. MUTATIONS_ENABLED = False.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text
from app.rules.pricing_rules import GST_RATE
from app.services.catalog_shopify_publish_service import shopify_inventory_location_id
from app.services.shopify_inventory_settings_audit import parse_shopify_bool_metafield
from app.services.supplier_orders_report_service import parse_release_date

TODAY = date(2026, 9, 22)
MUTATIONS_ENABLED = False
SALE_TAG = "Sale"
SMALL_LOSS_MAX = 3.00  # ex-GST absolute loss magnitude

PRODUCTS_QUERY = """
query FinalManifestCatalog($cursor: String, $locId: ID!, $q: String) {
  products(first: 50, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      vendor
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
          inventoryItem {
            unitCost { amount currencyCode }
            inventoryLevel(locationId: $locId) {
              quantities(names: ["available", "committed", "on_hand"]) {
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

SEARCH_QUERY = """
query FinalManifestSearch($q: String, $locId: ID!) {
  products(first: 25, query: $q) {
    nodes {
      id
      title
      vendor
      status
      tags
      mediaRelease: metafield(namespace: "custom", key: "media_release_date") { value }
      preOrder: metafield(namespace: "custom", key: "pre_order") { value }
      variants(first: 20) {
        nodes {
          id
          title
          sku
          barcode
          price
          compareAtPrice
          inventoryItem {
            unitCost { amount }
            inventoryLevel(locationId: $locId) {
              quantities(names: ["available", "committed", "on_hand"]) {
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

EXISTING_COMPARE_AT_TITLES = {
    "the last of us season 2 limited edition steelbook 4k ultra hd",
    "a fistful of dollars limited edition 4k ultra hd",
    "deep crimson 4k - the criterion collection (us edition)",
}

FUTURE_TITLE = "28 years later - the bone temple limited edition steelbook 4k ultra hd + blu-ray"

NO_TITLES = {
    "satantango 4k ultra hd",
    "alice doesnt live here anymore 4k ultra hd + blu-ray",
    "desperate living 4k ultra hd + blu-ray",
    "vampires kiss limited edition 4k ultra hd",
    "body heat 4k ultra hd + blu-ray",
    "jurassic park limited edition steelbook 4k ultra hd + blu-ray",
    "12 angry men 4k ultra hd + blu-ray",
    "10 cloverfield lane limited edition steelbook 4k ultra hd + blu-ray",
    "beverly hills cop limited edition steelbook 4k ultra hd",
    "the evil dead (1981) limited edition steelbook 4k ultra hd + blu-ray",
    "poltergeist - the film vault limited edition steelbook 4k ultra hd + blu-ray",
    "the mask limited edition 4k ultra hd",
    "thief limited edition 4k ultra hd",
    "to live and die in la limited edition 4k ultra hd",
    "city on fire limited edition 4k ultra hd",
    "damnation 4k ultra hd + blu-ray",
    "inglourious basterds 4k ultra hd",
    "alpha (2025) 4k ultra hd + blu-ray",
    "misery limited edition 4k ultra hd",
    "the ugly stepsister 4k ultra hd",
    "jeanne dielman, 23, quai du commerce, 1080 bruxelles blu-ray",
}


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


def tags_list(raw: Any) -> List[str]:
    if isinstance(raw, str):
        return [t.strip() for t in raw.split(",") if t.strip()]
    if isinstance(raw, list):
        return [clean_text(t) for t in raw if clean_text(t)]
    return []


def with_sale_tag(tags: List[str]) -> List[str]:
    return tags if SALE_TAG in tags else tags + [SALE_TAG]


def fetch_catalog(client: ShopifyClient, location_id: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
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
                        "q": "status:active OR status:draft OR status:unlisted OR status:archived",
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
            tags = tags_list(product.get("tags"))
            media_release = clean_text((product.get("mediaRelease") or {}).get("value")) or ""
            pre_order = _metafield_bool(product, "preOrder", "preorderAlt")
            for variant in (product.get("variants") or {}).get("nodes") or []:
                inv = variant.get("inventoryItem") or {}
                cost_obj = inv.get("unitCost") or {}
                qmap = _qty_map(inv.get("inventoryLevel"))
                levels = bool(inv.get("inventoryLevel"))
                out.append(
                    {
                        "product_id": clean_text(product.get("id")) or "",
                        "product_title": clean_text(product.get("title")) or "",
                        "vendor": clean_text(product.get("vendor")) or "",
                        "status": (clean_text(product.get("status")) or "").upper(),
                        "tags": tags,
                        "pre_order": pre_order,
                        "media_release_date": media_release,
                        "variant_id": clean_text(variant.get("id")) or "",
                        "variant_title": clean_text(variant.get("title")) or "Default Title",
                        "sku": clean_text(variant.get("sku")) or "",
                        "barcode": clean_text(variant.get("barcode")) or "",
                        "price": _f(variant.get("price")),
                        "compare_at_price": _f(variant.get("compareAtPrice")),
                        "unit_cost": _f(cost_obj.get("amount")),
                        "available": qmap.get("available") if levels else None,
                        "committed": qmap.get("committed") if levels else None,
                        "on_hand": qmap.get("on_hand") if levels else None,
                        "levels_present": levels,
                    }
                )
        page = block.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        time.sleep(0.25)
    print(f"Fetched {len(out)} variants (all statuses)", flush=True)
    return out


def search_products(client: ShopifyClient, location_id: str, q: str) -> List[Dict[str, Any]]:
    data = client.graphql(SEARCH_QUERY, {"q": q, "locId": location_id})
    rows: List[Dict[str, Any]] = []
    for product in (data.get("products") or {}).get("nodes") or []:
        for variant in (product.get("variants") or {}).get("nodes") or []:
            inv = variant.get("inventoryItem") or {}
            qmap = _qty_map(inv.get("inventoryLevel"))
            rows.append(
                {
                    "product_title": product.get("title"),
                    "product_id": product.get("id"),
                    "vendor": product.get("vendor"),
                    "status": product.get("status"),
                    "variant_title": variant.get("title"),
                    "variant_id": variant.get("id"),
                    "sku": variant.get("sku"),
                    "barcode": variant.get("barcode"),
                    "price": variant.get("price"),
                    "compare_at_price": variant.get("compareAtPrice"),
                    "unit_cost": ((inv.get("unitCost") or {}).get("amount")),
                    "available": qmap.get("available"),
                    "committed": qmap.get("committed"),
                    "on_hand": qmap.get("on_hand"),
                    "release_date": (product.get("mediaRelease") or {}).get("value"),
                    "pre_order": (product.get("preOrder") or {}).get("value"),
                    "tags": tags_list(product.get("tags")),
                }
            )
    return rows


def load_candidates(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8-sig") as fh:
        for raw in csv.DictReader(fh, delimiter="\t"):
            title = (clean_text(raw.get("Product title") or "") or "").replace("¬¨‚Ä†", "").strip()
            if not title:
                continue
            tl = title.casefold()
            if tl.startswith("test ") or "not for sale" in tl or tl.startswith("test product"):
                continue
            if norm_key(title) in NO_TITLES:
                continue
            rows.append(
                {
                    "title": title,
                    "sku": clean_text(raw.get("Product variant SKU") or "") or "",
                    "cost": _f(raw.get("Inventory item cost")),
                    "units": _i(raw.get("Ending inventory units")),
                    "retail": _f(raw.get("Ending inventory retail value")),
                }
            )
    return rows


def load_prior_proposal(path: Path) -> Dict[str, Any]:
    """Index prior clearance rows by variant_id, title+sku, and title."""
    by_vid: Dict[str, Dict[str, Any]] = {}
    by_title_sku: Dict[Tuple[str, str], Dict[str, Any]] = {}
    by_title: Dict[str, List[Dict[str, Any]]] = {}
    with path.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            vid = r.get("shopify_variant_id") or ""
            if vid:
                by_vid[vid] = r
            title_k = norm_key(r.get("product") or "")
            key = (title_k, (r.get("sku") or "").casefold())
            by_title_sku[key] = r
            by_title.setdefault(title_k, []).append(r)
    return {"by_vid": by_vid, "by_title_sku": by_title_sku, "by_title": by_title}


def resolve_prior(
    *,
    live: Dict[str, Any],
    cand: Dict[str, Any],
    prior_idx: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Resolve prior clearance economics without recalculating strategy.

    Prefer exact variant_id, then title+sku, then a same-title sibling with the
    same original price (covers Wicked UPSESB0206 which collapsed in the prior run).
    """
    prior = prior_idx["by_vid"].get(live["variant_id"])
    if prior:
        return prior
    prior = prior_idx["by_title_sku"].get(
        (norm_key(cand["title"]), (cand.get("sku") or "").casefold())
    )
    if prior:
        return prior
    siblings = prior_idx["by_title"].get(norm_key(live["product_title"])) or []
    live_price = live.get("price")
    usable = []
    for s in siblings:
        if (s.get("decision") or "") not in {
            "SALE_20",
            "SALE_15",
            "SALE_10",
            "NEGATIVE_GP_REVIEW",
        }:
            continue
        sp = _f(s.get("original_price"))
        if live_price is not None and sp is not None and abs(sp - live_price) < 0.02:
            usable.append(s)
    if len(usable) == 1:
        inherited = dict(usable[0])
        inherited["_inherited_from_variant_id"] = usable[0].get("shopify_variant_id")
        inherited["_inherited_note"] = "sibling_same_title_same_price"
        return inherited
    # If multiple siblings agree on decision + sale price, use that.
    if usable:
        keys = {
            (
                u.get("decision"),
                _f(u.get("sale_price")),
                _i(u.get("nominal_discount_pct")),
            )
            for u in usable
        }
        if len(keys) == 1:
            inherited = dict(usable[0])
            inherited["_inherited_from_variant_id"] = usable[0].get("shopify_variant_id")
            inherited["_inherited_note"] = "sibling_consensus_same_title_same_price"
            return inherited
    return None


def match_candidate(
    cand: Dict[str, Any],
    by_sku: Dict[str, List[Dict[str, Any]]],
    by_barcode: Dict[str, List[Dict[str, Any]]],
    by_title: Dict[str, List[Dict[str, Any]]],
) -> Tuple[str, Optional[Dict[str, Any]], str]:
    sku = (cand.get("sku") or "").strip()
    if sku:
        hits = by_sku.get(sku.casefold()) or []
        if not hits and sku.isdigit() and len(sku) >= 8:
            hits = by_barcode.get(sku.casefold()) or []
        # Prefer ACTIVE when multiple
        active = [h for h in hits if h["status"] == "ACTIVE"]
        pool = active or hits
        if len(pool) == 1:
            return "matched", pool[0], "sku"
        if len(pool) > 1:
            title_hits = [h for h in pool if norm_key(h["product_title"]) == norm_key(cand["title"])]
            if len(title_hits) == 1:
                return "matched", title_hits[0], "sku+title"
            return "ambiguous", None, f"sku_multiple:{len(pool)}"

    title_hits = by_title.get(norm_key(cand["title"])) or []
    active = [h for h in title_hits if h["status"] == "ACTIVE"]
    # Prefer ACTIVE for unique title match
    if len(active) == 1:
        return "matched", active[0], "exact_title_active"
    if len(active) > 1:
        # Disambiguate by cost / unit retail from candidate
        unit_retail = None
        if cand.get("retail") and cand.get("units") and cand["units"] > 0:
            unit_retail = round(cand["retail"] / cand["units"], 2)
        narrowed = active
        if unit_retail is not None:
            price_hits = [
                h for h in active if h.get("price") is not None and abs(h["price"] - unit_retail) < 0.02
            ]
            if len(price_hits) == 1:
                return "matched", price_hits[0], "exact_title_active+price"
            if price_hits:
                narrowed = price_hits
        if cand.get("cost") is not None:
            cost_hits = [
                h
                for h in narrowed
                if h.get("unit_cost") is not None and abs(h["unit_cost"] - cand["cost"]) < 0.05
            ]
            if len(cost_hits) == 1:
                return "matched", cost_hits[0], "exact_title_active+cost"
        return "ambiguous", None, f"title_active_multiple:{len(active)}"
    if len(title_hits) == 1:
        return "matched", title_hits[0], "exact_title_nonactive"
    if len(title_hits) > 1:
        return "ambiguous", None, f"title_multiple_nonunique_active:{len(title_hits)}"
    return "unmatched", None, "no_confident_match"


def free_qty(sv: Dict[str, Any]) -> Optional[int]:
    if not sv.get("levels_present"):
        return None
    if sv.get("available") is None or sv.get("committed") is None or sv.get("on_hand") is None:
        return None
    if int(sv["available"]) != int(sv["on_hand"]) - int(sv["committed"]):
        return None
    return max(0, int(sv["available"]))


def build_manifest_row(
    *,
    group: str,
    action: str,
    live: Dict[str, Any],
    prior: Optional[Dict[str, Any]],
    target_price: Optional[float],
    target_compare_at: Optional[float],
    notes: str = "",
) -> Dict[str, Any]:
    tags = list(live.get("tags") or [])
    target_tags = with_sale_tag(tags)
    cost = live.get("unit_cost")
    price = live.get("price")
    qty = free_qty(live)
    gp_after = None
    eff = None
    if target_price is not None and cost is not None and cost > 0:
        gp_after, _m = margin_parts(target_price, cost)
        gp_after = round(gp_after, 2)
    if target_price is not None and price and price > 0 and abs(target_price - price) > 0.005:
        eff = round((1.0 - target_price / price) * 100.0, 2)
    elif target_price is not None and price and abs(target_price - price) <= 0.005:
        eff = 0.0

    return {
        "group": group,
        "action": action,
        "shopify_product_id": live["product_id"],
        "shopify_variant_id": live["variant_id"],
        "title": live["product_title"],
        "variant_title": live["variant_title"],
        "sku": live.get("sku") or "",
        "barcode": live.get("barcode") or "",
        "vendor": live.get("vendor") or "",
        "product_status": live.get("status") or "",
        "available_free_qty": qty if qty is not None else "",
        "on_hand": live.get("on_hand") if live.get("on_hand") is not None else "",
        "committed": live.get("committed") if live.get("committed") is not None else "",
        "cost": round(cost, 2) if cost is not None else "",
        "current_price": round(price, 2) if price is not None else "",
        "target_sale_price": round(target_price, 2) if target_price is not None else "",
        "current_compare_at": round(live["compare_at_price"], 2)
        if live.get("compare_at_price") is not None
        else "",
        "target_compare_at": round(target_compare_at, 2) if target_compare_at is not None else "",
        "current_tags": ",".join(tags),
        "target_tags": ",".join(target_tags),
        "effective_discount_pct": eff if eff is not None else "",
        "gp_per_unit_after_sale": gp_after if gp_after is not None else "",
        "prior_decision": (prior or {}).get("decision", ""),
        "prior_sale_price": (prior or {}).get("sale_price", ""),
        "prior_discount_rule": (prior or {}).get("discount_rule", ""),
        "release_date": live.get("media_release_date") or "",
        "notes": notes,
        "snapshot_at": datetime.now(timezone.utc).isoformat(),
        "executable": group
        in {
            "SALE_20",
            "SALE_15",
            "SALE_10",
            "SMALL_LOSS_CLEARANCE",
            "EXISTING_SALE_KEEP_PRICE",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=str(_REPO / ".env.prod"))
    parser.add_argument(
        "--candidates",
        default=str(_REPO / "tmp/sale_candidates_20260922/candidates.tsv"),
    )
    parser.add_argument(
        "--prior",
        default=str(_REPO / "tmp/sale_candidates_20260922/clearance_sale_all.csv"),
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

    client = ShopifyClient()

    # --- Explicit investigation reports ---
    infernal_matches = search_products(client, location_id, "title:Infernal Affairs")
    wicked_matches = search_products(client, location_id, "title:Wicked - For Good")
    time.sleep(0.2)

    print("\n===== INFERNAL AFFAIRS CHECK (Shopify search) =====")
    for m in infernal_matches:
        print(json.dumps(m, indent=2, default=str))

    # Select ACTIVE Infernal Affairs matching approved candidate cost 100.02 / retail 152.99
    infernal_selected = None
    infernal_reason = ""
    active_infernal = [m for m in infernal_matches if str(m.get("status")).upper() == "ACTIVE"]
    unlisted_infernal = [m for m in infernal_matches if str(m.get("status")).upper() == "UNLISTED"]
    if len(active_infernal) == 1:
        infernal_selected = active_infernal[0]
        infernal_reason = (
            "Single ACTIVE Shopify product; barcode 5050629306033; "
            f"price={infernal_selected.get('price')} cost={infernal_selected.get('unit_cost')} "
            "matches approved candidate row cost=100.02 / ending retail unit=152.99. "
            f"Excluded UNLISTED duplicate variant {unlisted_infernal[0]['variant_id'] if unlisted_infernal else 'n/a'}."
        )
    else:
        infernal_reason = f"Could not uniquely select ACTIVE Infernal Affairs (active={len(active_infernal)})"

    print("\nSELECTED VARIANT:")
    print(json.dumps(infernal_selected, indent=2, default=str))
    print("REASON:", infernal_reason)

    print("\n===== WICKED FOR GOOD CHECK =====")
    wicked_for_good = [
        m
        for m in wicked_matches
        if (m.get("product_title") or "").startswith("Wicked - For Good")
    ]
    for m in wicked_for_good:
        print(json.dumps(m, indent=2, default=str))

    # Load data
    candidates = load_candidates(Path(args.candidates))
    prior_idx = load_prior_proposal(Path(args.prior))
    catalog = fetch_catalog(client, location_id)

    by_sku: Dict[str, List[Dict[str, Any]]] = {}
    by_barcode: Dict[str, List[Dict[str, Any]]] = {}
    by_title: Dict[str, List[Dict[str, Any]]] = {}
    by_vid: Dict[str, Dict[str, Any]] = {}
    for v in catalog:
        by_vid[v["variant_id"]] = v
        if v["sku"]:
            by_sku.setdefault(v["sku"].casefold(), []).append(v)
        if v["barcode"]:
            by_barcode.setdefault(v["barcode"].casefold(), []).append(v)
        by_title.setdefault(norm_key(v["product_title"]), []).append(v)

    # Match each YES candidate (SKU-first so Wicked both survive)
    matched_rows: List[Tuple[Dict[str, Any], Dict[str, Any], str]] = []
    ambiguous: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []
    for cand in candidates:
        # Special-case Infernal Affairs: force selected ACTIVE variant only once
        if norm_key(cand["title"]).startswith("the infernal affairs trilogy"):
            if infernal_selected is None:
                ambiguous.append(
                    {
                        "title": cand["title"],
                        "sku": cand["sku"],
                        "cost": cand["cost"],
                        "reason": "AMBIGUOUS_VARIANT_REVIEW",
                    }
                )
                continue
            # Only keep the candidate that matches selected economics
            sel_price = _f(infernal_selected.get("price"))
            sel_cost = _f(infernal_selected.get("unit_cost"))
            unit_retail = (
                round(cand["retail"] / cand["units"], 2)
                if cand.get("retail") and cand.get("units")
                else None
            )
            cost_ok = cand.get("cost") is not None and sel_cost is not None and abs(cand["cost"] - sel_cost) < 0.05
            price_ok = unit_retail is not None and sel_price is not None and abs(unit_retail - sel_price) < 0.05
            if not (cost_ok or price_ok):
                # Skip the other candidate row (UNLISTED economics)
                continue
            live = by_vid.get(infernal_selected["variant_id"])
            if live is None:
                ambiguous.append({"title": cand["title"], "reason": "selected_variant_missing_in_catalog"})
                continue
            matched_rows.append((cand, live, "infernal_affairs_forced_active"))
            continue

        status, live, method = match_candidate(cand, by_sku, by_barcode, by_title)
        if status == "matched" and live is not None:
            matched_rows.append((cand, live, method))
        elif status == "ambiguous":
            ambiguous.append({"title": cand["title"], "sku": cand["sku"], "reason": method})
        else:
            unmatched.append({"title": cand["title"], "sku": cand["sku"], "reason": method})

    # Deduplicate by variant_id (first wins; report collisions)
    seen_vid: Dict[str, Tuple[Dict[str, Any], Dict[str, Any], str]] = {}
    collisions: List[Dict[str, Any]] = []
    for cand, live, method in matched_rows:
        vid = live["variant_id"]
        if vid in seen_vid:
            collisions.append(
                {
                    "variant_id": vid,
                    "first_title": seen_vid[vid][0]["title"],
                    "second_title": cand["title"],
                    "first_sku": seen_vid[vid][0].get("sku"),
                    "second_sku": cand.get("sku"),
                }
            )
            continue
        seen_vid[vid] = (cand, live, method)

    if collisions:
        print("\nVARIANT ID DEDUP COLLISIONS (kept first):")
        for c in collisions:
            print(json.dumps(c, indent=2))

    # Build groups from unique variants
    groups: Dict[str, List[Dict[str, Any]]] = {
        "SALE_20": [],
        "SALE_15": [],
        "SALE_10": [],
        "SMALL_LOSS_CLEARANCE": [],
        "EXISTING_SALE_KEEP_PRICE": [],
        "LARGE_LOSS_REVIEW": [],
        "FUTURE_RELEASE_OR_PREORDER": [],
        "AMBIGUOUS_VARIANT_REVIEW": [],
        "SNAPSHOT_CHANGED_REVIEW": [],
        "UNLISTED_EXCLUDED": [],
        "NON_ACTIVE_EXCLUDED": [],
    }

    for item in ambiguous:
        groups["AMBIGUOUS_VARIANT_REVIEW"].append(
            {
                "group": "AMBIGUOUS_VARIANT_REVIEW",
                "action": "EXCLUDE_UNTIL_CONFIRMED",
                "title": item["title"],
                "sku": item.get("sku", ""),
                "notes": item.get("reason", ""),
                "executable": False,
                "shopify_variant_id": "",
            }
        )

    unlisted_found: List[Dict[str, Any]] = []

    for cand, live, method in seen_vid.values():
        title_key = norm_key(live["product_title"])
        status = (live.get("status") or "").upper()
        release = parse_release_date(live.get("media_release_date"))
        future = bool(live.get("pre_order") or (release and release > TODAY))
        qty = free_qty(live)
        prior = resolve_prior(live=live, cand=cand, prior_idx=prior_idx)

        # UNLISTED / non-ACTIVE hard exclude from executable
        if status == "UNLISTED":
            unlisted_found.append(live)
            row = build_manifest_row(
                group="UNLISTED_EXCLUDED",
                action="EXCLUDE_UNLISTED",
                live=live,
                prior=prior,
                target_price=None,
                target_compare_at=None,
                notes=f"status=UNLISTED; match={method}",
            )
            row["executable"] = False
            groups["UNLISTED_EXCLUDED"].append(row)
            continue
        if status != "ACTIVE":
            row = build_manifest_row(
                group="NON_ACTIVE_EXCLUDED",
                action=f"EXCLUDE_STATUS_{status}",
                live=live,
                prior=prior,
                target_price=None,
                target_compare_at=None,
                notes=f"status={status}; match={method}",
            )
            row["executable"] = False
            groups["NON_ACTIVE_EXCLUDED"].append(row)
            continue

        if future or title_key == FUTURE_TITLE:
            row = build_manifest_row(
                group="FUTURE_RELEASE_OR_PREORDER",
                action="EXCLUDE_FUTURE",
                live=live,
                prior=prior,
                target_price=None,
                target_compare_at=None,
                notes=f"release={live.get('media_release_date')}",
            )
            row["executable"] = False
            groups["FUTURE_RELEASE_OR_PREORDER"].append(row)
            continue

        if qty is None:
            row = build_manifest_row(
                group="SNAPSHOT_CHANGED_REVIEW",
                action="EXCLUDE_INVENTORY_UNSAFE",
                live=live,
                prior=prior,
                target_price=None,
                target_compare_at=None,
                notes="cannot prove free stock",
            )
            row["executable"] = False
            groups["SNAPSHOT_CHANGED_REVIEW"].append(row)
            continue
        if qty <= 0:
            row = build_manifest_row(
                group="SNAPSHOT_CHANGED_REVIEW",
                action="EXCLUDE_NO_FREE_STOCK",
                live=live,
                prior=prior,
                target_price=None,
                target_compare_at=None,
                notes="available<=0",
            )
            row["executable"] = False
            groups["SNAPSHOT_CHANGED_REVIEW"].append(row)
            continue

        # Existing compare-at keep-price path
        if title_key in EXISTING_COMPARE_AT_TITLES or (
            live.get("compare_at_price") is not None and live["compare_at_price"] > 0
        ):
            # Only the three named / any with compare-at: keep price
            if live.get("compare_at_price") is not None and live["compare_at_price"] > 0:
                has_sale = SALE_TAG in (live.get("tags") or [])
                row = build_manifest_row(
                    group="EXISTING_SALE_KEEP_PRICE",
                    action="ADD_SALE_TAG_ONLY" if not has_sale else "NO_CHANGE_ALREADY_SALE_TAGGED",
                    live=live,
                    prior=prior,
                    target_price=live.get("price"),
                    target_compare_at=live.get("compare_at_price"),
                    notes="preserve existing price and compare-at",
                )
                groups["EXISTING_SALE_KEEP_PRICE"].append(row)
                continue

        # Resolve prior proposed sale price / decision without recalculating strategy
        prior_decision = (prior or {}).get("decision") or ""
        prior_sale = _f((prior or {}).get("sale_price"))
        if prior_sale is None or live.get("price") is None or live.get("unit_cost") is None:
            row = build_manifest_row(
                group="SNAPSHOT_CHANGED_REVIEW",
                action="EXCLUDE_MISSING_PRIOR_OR_LIVE",
                live=live,
                prior=prior,
                target_price=None,
                target_compare_at=None,
                notes=f"prior_decision={prior_decision}",
            )
            row["executable"] = False
            groups["SNAPSHOT_CHANGED_REVIEW"].append(row)
            continue

        # If prior was sale but live compare-at now populated → snapshot changed
        if live.get("compare_at_price") is not None and live["compare_at_price"] > 0:
            row = build_manifest_row(
                group="SNAPSHOT_CHANGED_REVIEW",
                action="EXCLUDE_COMPARE_AT_NOW_POPULATED",
                live=live,
                prior=prior,
                target_price=prior_sale,
                target_compare_at=None,
                notes="compare-at became populated",
            )
            row["executable"] = False
            groups["SNAPSHOT_CHANGED_REVIEW"].append(row)
            continue

        sale_gp, _ = margin_parts(prior_sale, live["unit_cost"])
        cur_gp, _ = margin_parts(live["price"], live["unit_cost"])

        # Negative GP split from prior NEGATIVE_GP_REVIEW or live calculation at prior sale
        if prior_decision == "NEGATIVE_GP_REVIEW" or sale_gp < 0:
            loss = -sale_gp
            if sale_gp < 0 and loss <= SMALL_LOSS_MAX + 1e-9:
                row = build_manifest_row(
                    group="SMALL_LOSS_CLEARANCE",
                    action="SET_SALE_PRICE_AND_COMPARE_AT_ADD_SALE_TAG",
                    live=live,
                    prior=prior,
                    target_price=prior_sale,
                    target_compare_at=live["price"],
                    notes=f"small_loss_gp={round(sale_gp,2)}",
                )
                groups["SMALL_LOSS_CLEARANCE"].append(row)
            else:
                # LARGE_LOSS_REVIEW detail
                qty_i = qty
                cash_current = round(qty_i * live["price"], 2)
                cash_sale = round(qty_i * prior_sale, 2)
                markdown = round(cash_current - cash_sale, 2)
                incremental_loss = round(qty_i * (cur_gp - sale_gp), 2) if cur_gp is not None else ""
                row = build_manifest_row(
                    group="LARGE_LOSS_REVIEW",
                    action="HOLD_NO_AUTO_MARKDOWN",
                    live=live,
                    prior=prior,
                    target_price=prior_sale,
                    target_compare_at=live["price"],
                    notes="large_loss_hold",
                )
                row["executable"] = False
                row["current_gp_per_unit"] = round(cur_gp, 2)
                row["proposed_gp_per_unit"] = round(sale_gp, 2)
                row["cash_at_current_price"] = cash_current
                row["cash_at_proposed_sale"] = cash_sale
                row["incremental_markdown_dollars"] = markdown
                row["incremental_gross_loss_from_markdown"] = incremental_loss
                groups["LARGE_LOSS_REVIEW"].append(row)
            continue

        # Map prior SALE_* decisions
        if prior_decision in {"SALE_20", "SALE_15", "SALE_10"}:
            inherit_note = ""
            if prior and prior.get("_inherited_note"):
                inherit_note = (
                    f"; inherited_from={prior.get('_inherited_from_variant_id')} "
                    f"via {prior.get('_inherited_note')}"
                )
            row = build_manifest_row(
                group=prior_decision,
                action="SET_SALE_PRICE_AND_COMPARE_AT_ADD_SALE_TAG",
                live=live,
                prior=prior,
                target_price=prior_sale,
                target_compare_at=live["price"],
                notes=f"match={method}{inherit_note}",
            )
            groups[prior_decision].append(row)
            continue

        # Fallback: if prior was something else but has sale price and positive GP, route by nominal
        nom = _i((prior or {}).get("nominal_discount_pct"))
        if nom in (10, 15, 20) and sale_gp >= 0:
            g = f"SALE_{nom}"
            row = build_manifest_row(
                group=g,
                action="SET_SALE_PRICE_AND_COMPARE_AT_ADD_SALE_TAG",
                live=live,
                prior=prior,
                target_price=prior_sale,
                target_compare_at=live["price"],
                notes=f"fallback_from_prior={prior_decision}; match={method}",
            )
            groups[g].append(row)
            continue

        row = build_manifest_row(
            group="SNAPSHOT_CHANGED_REVIEW",
            action="EXCLUDE_UNMAPPED_PRIOR",
            live=live,
            prior=prior,
            target_price=prior_sale,
            target_compare_at=None,
            notes=f"prior_decision={prior_decision}",
        )
        row["executable"] = False
        groups["SNAPSHOT_CHANGED_REVIEW"].append(row)

    # Also capture UNLISTED Infernal Affairs explicitly if not already
    for m in unlisted_infernal:
        vid = m["variant_id"]
        if any(r.get("shopify_variant_id") == vid for r in groups["UNLISTED_EXCLUDED"]):
            continue
        live = by_vid.get(vid)
        if live:
            unlisted_found.append(live)
            row = build_manifest_row(
                group="UNLISTED_EXCLUDED",
                action="EXCLUDE_UNLISTED",
                live=live,
                prior=None,
                target_price=None,
                target_compare_at=None,
                notes="Infernal Affairs UNLISTED duplicate; not in executable manifest",
            )
            row["executable"] = False
            groups["UNLISTED_EXCLUDED"].append(row)

    executable = []
    for g in ("SALE_20", "SALE_15", "SALE_10", "SMALL_LOSS_CLEARANCE", "EXISTING_SALE_KEEP_PRICE"):
        executable.extend(groups[g])

    # Final live validation on executable rows
    validated: List[Dict[str, Any]] = []
    for row in executable:
        live = by_vid.get(row["shopify_variant_id"])
        if live is None:
            row2 = dict(row)
            row2["group"] = "SNAPSHOT_CHANGED_REVIEW"
            row2["action"] = "EXCLUDE_VARIANT_MISSING"
            row2["executable"] = False
            groups["SNAPSHOT_CHANGED_REVIEW"].append(row2)
            continue
        status = (live.get("status") or "").upper()
        if status == "UNLISTED":
            row2 = dict(row)
            row2["group"] = "UNLISTED_EXCLUDED"
            row2["action"] = "EXCLUDE_UNLISTED"
            row2["executable"] = False
            row2["notes"] = (row2.get("notes") or "") + "|final_validation_unlisted"
            groups["UNLISTED_EXCLUDED"].append(row2)
            continue
        if status != "ACTIVE":
            row2 = dict(row)
            row2["group"] = "NON_ACTIVE_EXCLUDED"
            row2["action"] = f"EXCLUDE_STATUS_{status}"
            row2["executable"] = False
            groups["NON_ACTIVE_EXCLUDED"].append(row2)
            continue
        qty = free_qty(live)
        if qty is None or qty <= 0:
            row2 = dict(row)
            row2["group"] = "SNAPSHOT_CHANGED_REVIEW"
            row2["action"] = "EXCLUDE_NO_FREE_STOCK_FINAL"
            row2["executable"] = False
            groups["SNAPSHOT_CHANGED_REVIEW"].append(row2)
            continue
        release = parse_release_date(live.get("media_release_date"))
        if live.get("pre_order") or (release and release > TODAY):
            row2 = dict(row)
            row2["group"] = "FUTURE_RELEASE_OR_PREORDER"
            row2["action"] = "EXCLUDE_FUTURE_FINAL"
            row2["executable"] = False
            groups["FUTURE_RELEASE_OR_PREORDER"].append(row2)
            continue
        # Price must still match row current_price
        if live.get("price") is None or abs(float(live["price"]) - float(row["current_price"])) > 0.005:
            row2 = dict(row)
            row2["group"] = "SNAPSHOT_CHANGED_REVIEW"
            row2["action"] = "EXCLUDE_PRICE_DRIFT"
            row2["executable"] = False
            row2["notes"] = f"live_price={live.get('price')} snapshot={row['current_price']}"
            groups["SNAPSHOT_CHANGED_REVIEW"].append(row2)
            continue
        if row["group"] in {"SALE_20", "SALE_15", "SALE_10", "SMALL_LOSS_CLEARANCE"}:
            if live.get("compare_at_price") is not None and live["compare_at_price"] > 0:
                row2 = dict(row)
                row2["group"] = "SNAPSHOT_CHANGED_REVIEW"
                row2["action"] = "EXCLUDE_COMPARE_AT_POPULATED_FINAL"
                row2["executable"] = False
                groups["SNAPSHOT_CHANGED_REVIEW"].append(row2)
                continue
        if row["group"] == "EXISTING_SALE_KEEP_PRICE":
            if live.get("compare_at_price") is None:
                row2 = dict(row)
                row2["group"] = "SNAPSHOT_CHANGED_REVIEW"
                row2["action"] = "EXCLUDE_EXPECTED_COMPARE_AT_MISSING"
                row2["executable"] = False
                groups["SNAPSHOT_CHANGED_REVIEW"].append(row2)
                continue
            # expected compare-at still present
            if abs(float(live["compare_at_price"]) - float(row["current_compare_at"])) > 0.005:
                row2 = dict(row)
                row2["group"] = "SNAPSHOT_CHANGED_REVIEW"
                row2["action"] = "EXCLUDE_COMPARE_AT_DRIFT"
                row2["executable"] = False
                groups["SNAPSHOT_CHANGED_REVIEW"].append(row2)
                continue
        # refresh qty from live
        row["available_free_qty"] = qty
        validated.append(row)

    # Uniqueness assertion
    vids = [r["shopify_variant_id"] for r in validated]
    unique_ok = len(vids) == len(set(vids))
    print("\nEXECUTABLE VARIANT IDS UNIQUE:", "PASS" if unique_ok else "FAIL")
    print(f"executable_count={len(vids)} unique={len(set(vids))}")
    if not unique_ok:
        from collections import Counter

        dupes = [vid for vid, n in Counter(vids).items() if n > 1]
        print("DUPLICATE VARIANT IDS:", dupes)
        print("STOPPING — no manifest written with duplicate variant IDs.")
        return 2

    # Sort executable for output
    order = {"SALE_20": 0, "SALE_15": 1, "SALE_10": 2, "SMALL_LOSS_CLEARANCE": 3, "EXISTING_SALE_KEEP_PRICE": 4}
    validated.sort(
        key=lambda r: (
            order.get(r["group"], 9),
            -((_i(r["available_free_qty"]) or 0) * (_f(r["current_price"]) or 0)),
            r["title"],
        )
    )

    # Write outputs
    manifest_fields = [
        "group",
        "action",
        "shopify_product_id",
        "shopify_variant_id",
        "title",
        "variant_title",
        "sku",
        "barcode",
        "available_free_qty",
        "cost",
        "current_price",
        "target_sale_price",
        "current_compare_at",
        "target_compare_at",
        "current_tags",
        "target_tags",
        "effective_discount_pct",
        "gp_per_unit_after_sale",
        "on_hand",
        "committed",
        "product_status",
        "vendor",
        "release_date",
        "prior_decision",
        "prior_sale_price",
        "prior_discount_rule",
        "notes",
        "executable",
        "snapshot_at",
    ]

    with (out_dir / "clearance_sale_final_manifest.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=manifest_fields, extrasaction="ignore")
        w.writeheader()
        for r in validated:
            w.writerow(r)

    # Large loss review CSV
    ll_fields = manifest_fields + [
        "current_gp_per_unit",
        "proposed_gp_per_unit",
        "cash_at_current_price",
        "cash_at_proposed_sale",
        "incremental_markdown_dollars",
        "incremental_gross_loss_from_markdown",
    ]
    with (out_dir / "clearance_sale_large_loss_review.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=ll_fields, extrasaction="ignore")
        w.writeheader()
        for r in groups["LARGE_LOSS_REVIEW"]:
            w.writerow(r)

    # Full review dump
    all_non_exec = []
    for g, rows in groups.items():
        if g in {"SALE_20", "SALE_15", "SALE_10", "SMALL_LOSS_CLEARANCE", "EXISTING_SALE_KEEP_PRICE"}:
            continue
        all_non_exec.extend(rows)
    with (out_dir / "clearance_sale_final_non_executable.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=ll_fields, extrasaction="ignore")
        w.writeheader()
        for r in all_non_exec:
            w.writerow(r)

    # Full UNLISTED audit across every matched YES variant + search discoveries.
    unlisted_audit = []
    for cand, live, method in seen_vid.values():
        if (live.get("status") or "").upper() == "UNLISTED":
            unlisted_audit.append(
                {
                    "title": live["product_title"],
                    "product_id": live["product_id"],
                    "variant_id": live["variant_id"],
                    "sku": live.get("sku"),
                    "barcode": live.get("barcode"),
                    "price": live.get("price"),
                    "match_method": method,
                    "candidate_sku": cand.get("sku"),
                }
            )
    for m in unlisted_infernal:
        if not any(u["variant_id"] == m["variant_id"] for u in unlisted_audit):
            unlisted_audit.append(
                {
                    "title": m.get("product_title"),
                    "product_id": m.get("product_id"),
                    "variant_id": m.get("variant_id"),
                    "sku": m.get("sku"),
                    "barcode": m.get("barcode"),
                    "price": _f(m.get("price")),
                    "match_method": "search_infernal_affairs",
                    "candidate_sku": "",
                }
            )

    print("\n===== UNLISTED AUDIT (matched YES universe) =====")
    print(f"unlisted_count={len(unlisted_audit)}")
    for u in unlisted_audit:
        print(json.dumps(u, indent=2, default=str))
    if any((r.get("product_status") or "").upper() == "UNLISTED" for r in validated):
        print("FATAL: UNLISTED product present in executable validated set")
        return 3
    print("UNLISTED IN EXECUTABLE: PASS (none)")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mutations_enabled": MUTATIONS_ENABLED,
        "executable_variant_ids_unique": "PASS",
        "unlisted_in_executable": "PASS",
        "executable_count": len(validated),
        "counts": {k: len(v) for k, v in groups.items()},
        "executable_group_counts": {
            g: sum(1 for r in validated if r["group"] == g)
            for g in ("SALE_20", "SALE_15", "SALE_10", "SMALL_LOSS_CLEARANCE", "EXISTING_SALE_KEEP_PRICE")
        },
        "unlisted_audit": unlisted_audit,
        "unlisted_excluded": [
            {
                "title": r.get("title"),
                "product_id": r.get("shopify_product_id"),
                "variant_id": r.get("shopify_variant_id"),
                "sku": r.get("sku"),
                "barcode": r.get("barcode"),
                "price": r.get("current_price"),
                "status": r.get("product_status"),
            }
            for r in groups["UNLISTED_EXCLUDED"]
        ],
        "wicked_for_good_check": wicked_for_good,
        "infernal_affairs_check": {
            "matches": infernal_matches,
            "selected": infernal_selected,
            "reason": infernal_reason,
        },
        "executable": validated,
        "large_loss_review": groups["LARGE_LOSS_REVIEW"],
        "ambiguous_variant_review": groups["AMBIGUOUS_VARIANT_REVIEW"],
        "future_release_or_preorder": groups["FUTURE_RELEASE_OR_PREORDER"],
        "snapshot_changed_review": groups["SNAPSHOT_CHANGED_REVIEW"],
    }
    (out_dir / "clearance_sale_final_manifest.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )

    # Summary print
    print("\n===== SUMMARY =====")
    print("executable_group_counts:", payload["executable_group_counts"])
    print("UNLISTED_EXCLUDED:", len(groups["UNLISTED_EXCLUDED"]))
    for r in groups["UNLISTED_EXCLUDED"]:
        print(
            f"  UNLISTED {r.get('title')} | pid={r.get('shopify_product_id')} | "
            f"vid={r.get('shopify_variant_id')} | sku={r.get('sku')} | price={r.get('current_price')}"
        )
    print("LARGE_LOSS_REVIEW:", len(groups["LARGE_LOSS_REVIEW"]))
    print("SMALL_LOSS_CLEARANCE:", len(groups["SMALL_LOSS_CLEARANCE"]))
    print("EXISTING_SALE_KEEP_PRICE:", len(groups["EXISTING_SALE_KEEP_PRICE"]))
    print("FUTURE:", len(groups["FUTURE_RELEASE_OR_PREORDER"]))
    print("AMBIGUOUS:", len(groups["AMBIGUOUS_VARIANT_REVIEW"]))
    print("SNAPSHOT_CHANGED:", len(groups["SNAPSHOT_CHANGED_REVIEW"]))
    print("NON_ACTIVE_EXCLUDED:", len(groups["NON_ACTIVE_EXCLUDED"]))

    # Prove Wicked both present
    wicked_exec = [r for r in validated if r["title"].startswith("Wicked - For Good")]
    print("\nWICKED FOR GOOD IN EXECUTABLE:", len(wicked_exec))
    for r in wicked_exec:
        print(
            f"  sku={r['sku']} barcode={r['barcode']} vid={r['shopify_variant_id']} "
            f"pid={r['shopify_product_id']} price={r['current_price']} qty={r['available_free_qty']} group={r['group']}"
        )

    infernal_exec = [r for r in validated if "Infernal Affairs" in r["title"]]
    print("\nINFERNAL AFFAIRS IN EXECUTABLE:", len(infernal_exec))
    for r in infernal_exec:
        print(
            f"  vid={r['shopify_variant_id']} price={r['current_price']} cost={r['cost']} "
            f"barcode={r['barcode']} group={r['group']}"
        )

    print("\nFINAL CLEARANCE SALE MANIFEST READY — NO SHOPIFY CHANGES MADE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
