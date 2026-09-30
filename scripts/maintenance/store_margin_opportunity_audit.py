"""
Store-wide pricing and gross-margin opportunity audit (read-only).

Outputs:
- tmp/store_margin_opportunity_audit.csv
- tmp/store_margin_opportunity_audit.json
- tmp/studio_margin_opportunity_summary.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.rules.pricing_rules import round_up_to_99

GST = 1.10
TARGET_MARGIN = 0.28

PRODUCTS_QUERY = """
query StoreMarginProducts($cursor: String, $q: String) {
  products(first: 50, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      status
      productType
      vendor
      tags
      studio: metafield(namespace: "custom", key: "studio") { value }
      variants(first: 50) {
        nodes {
          id
          title
          sku
          barcode
          price
          inventoryItem { unitCost { amount currencyCode } }
        }
      }
    }
  }
}
"""

ORDERS_QUERY = """
query StoreMarginOrders($cursor: String) {
  orders(first: 50, after: $cursor, sortKey: CREATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      name
      createdAt
      lineItems(first: 50) {
        nodes {
          quantity
          discountedTotalSet { shopMoney { amount currencyCode } }
          variant { id sku barcode }
          product {
            title
            studio: metafield(namespace: "custom", key: "studio") { value }
          }
        }
      }
    }
  }
}
"""


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _norm_studio(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s)
    lower = s.casefold()
    if "arrow" in lower and ("video" in lower or "film" in lower):
        return "Arrow"
    if "second sight" in lower:
        return "Second Sight"
    # safe normalization only: trim + collapse spacing + title case
    return s


def _is_arrow(raw: str) -> bool:
    return "arrow" in (raw or "").casefold()


def _is_second(raw: str) -> bool:
    return "second sight" in (raw or "").casefold()


def _category_flag(title: str, product_type: str, tags: list[str]) -> str:
    t = f"{title} {product_type} {' '.join(tags)}".casefold()
    if "gift card" in t:
        return "gift_card"
    if any(k in t for k in ["vinyl", "lp ", "soundtrack", "cd ", "music"]):
        return "vinyl_music"
    if any(k in t for k in ["book", "hardcover", "paperback", "novel"]):
        return "books"
    if any(k in t for k in ["game", "xbox", "playstation", "nintendo", "switch", "ps5", "ps4"]):
        return "games"
    if any(k in t for k in ["t-shirt", "shirt", "poster", "merch", "hoodie", "cap", "hat", "mug", "figure"]):
        return "non_film_merch"
    return ""


def _margin(price_inc_gst: float, cost: float) -> Optional[float]:
    if price_inc_gst <= 0:
        return None
    ex = price_inc_gst / GST
    return ((ex - cost) / ex) * 100


def _floor_28(cost: float) -> float:
    raw = (cost / (1 - TARGET_MARGIN)) * GST
    p = round_up_to_99(raw)
    while True:
        m = _margin(p, cost)
        if m is not None and m >= 28 - 1e-6:
            return p
        p = round_up_to_99(p + 1.0)


def _gql(client: ShopifyClient, query: str, vars: dict, retries: int = 6) -> dict:
    for i in range(retries):
        try:
            return client.graphql(query, vars)
        except Exception as e:  # noqa: BLE001
            if ("THROTTLED" in str(e) or "429" in str(e)) and i < retries - 1:
                time.sleep(min(2 + i, 10))
                continue
            raise
    raise RuntimeError("GraphQL failed after retries")


@dataclass
class VariantAudit:
    product_title: str
    variant_title: str
    barcode: str
    sku: str
    shopify_product_id: str
    shopify_variant_id: str
    studio_raw: str
    studio_norm: str
    product_type: str
    category_flag: str
    current_price: Optional[float]
    unit_cost: Optional[float]
    cost_currency: str
    current_gp: Optional[float]
    current_margin_pct: Optional[float]
    floor_price_28: Optional[float]
    proposed_price: Optional[float]
    proposed_increase: Optional[float]
    proposed_increase_pct: Optional[float]
    proposed_gp: Optional[float]
    proposed_margin_pct: Optional[float]
    gp_uplift_per_unit: Optional[float]
    valid_cost: bool
    excluded_from_main: bool
    exclusion_reason: str
    units_sold: int
    sales_revenue_current: float
    sales_gp_current: float
    sales_gp_proposed: float
    sales_gp_uplift: float


def run_audit(client: ShopifyClient, max_order_pages: int = 20) -> dict:
    variants: list[VariantAudit] = []
    studio_raw_map: dict[str, set[str]] = defaultdict(set)
    cursor = None

    # 1) Pull active product/variant catalog
    while True:
        data = _gql(client, PRODUCTS_QUERY, {"cursor": cursor, "q": "status:active"})
        block = data["products"]
        for p in block.get("nodes") or []:
            if p.get("status") != "ACTIVE":
                continue
            title = p.get("title", "")
            pid = p.get("id", "")
            ptype = p.get("productType", "") or ""
            tags = p.get("tags") or []
            studio_raw = ((p.get("studio") or {}).get("value") or "").strip()
            studio_norm = _norm_studio(studio_raw)
            if studio_norm:
                studio_raw_map[studio_norm].add(studio_raw)

            cat_flag = _category_flag(title, ptype, tags)

            for v in (p.get("variants") or {}).get("nodes") or []:
                vid = v.get("id", "")
                price = _f(v.get("price"))
                bc = v.get("barcode") or ""
                sku = v.get("sku") or ""
                vc = (v.get("inventoryItem") or {}).get("unitCost") or {}
                cost = _f(vc.get("amount"))
                cc = vc.get("currencyCode", "") or ""
                valid_cost = bool(cost is not None and cost > 0 and (cc in ("", "AUD")))

                excluded = False
                reason = ""
                if _is_arrow(studio_raw):
                    excluded, reason = True, "benchmark_arrow"
                elif _is_second(studio_raw):
                    excluded, reason = True, "benchmark_second_sight"
                elif not studio_raw:
                    excluded, reason = True, "missing_studio"
                elif cat_flag:
                    excluded, reason = True, cat_flag

                cur_gp = cur_margin = floor = proposed = inc = inc_pct = prop_gp = prop_margin = gp_uplift = None
                if valid_cost and price is not None and price > 0:
                    ex = price / GST
                    cur_gp = ex - cost
                    cur_margin = _margin(price, cost)
                    floor = _floor_28(cost)
                    proposed = max(price, floor)
                    inc = round(proposed - price, 2)
                    inc_pct = round((inc / price) * 100, 2) if price > 0 else None
                    pex = proposed / GST
                    prop_gp = pex - cost
                    prop_margin = _margin(proposed, cost)
                    gp_uplift = prop_gp - cur_gp

                variants.append(
                    VariantAudit(
                        product_title=title,
                        variant_title=v.get("title", ""),
                        barcode=bc,
                        sku=sku,
                        shopify_product_id=pid,
                        shopify_variant_id=vid,
                        studio_raw=studio_raw,
                        studio_norm=studio_norm,
                        product_type=ptype,
                        category_flag=cat_flag,
                        current_price=price,
                        unit_cost=cost,
                        cost_currency=cc,
                        current_gp=round(cur_gp, 2) if cur_gp is not None else None,
                        current_margin_pct=round(cur_margin, 2) if cur_margin is not None else None,
                        floor_price_28=floor,
                        proposed_price=proposed,
                        proposed_increase=inc,
                        proposed_increase_pct=inc_pct,
                        proposed_gp=round(prop_gp, 2) if prop_gp is not None else None,
                        proposed_margin_pct=round(prop_margin, 2) if prop_margin is not None else None,
                        gp_uplift_per_unit=round(gp_uplift, 2) if gp_uplift is not None else None,
                        valid_cost=valid_cost,
                        excluded_from_main=excluded,
                        exclusion_reason=reason,
                        units_sold=0,
                        sales_revenue_current=0.0,
                        sales_gp_current=0.0,
                        sales_gp_proposed=0.0,
                        sales_gp_uplift=0.0,
                    )
                )
        pi = block.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        time.sleep(0.15)

    by_vid = {v.shopify_variant_id: v for v in variants}

    # 2) Pull recent sales and attach units/revenue/gp realized
    cursor = None
    pages = 0
    while pages < max_order_pages:
        pages += 1
        data = _gql(client, ORDERS_QUERY, {"cursor": cursor})
        block = data["orders"]
        for o in block.get("nodes") or []:
            for li in (o.get("lineItems") or {}).get("nodes") or []:
                var = li.get("variant") or {}
                vid = var.get("id") or ""
                if not vid or vid not in by_vid:
                    continue
                row = by_vid[vid]
                q = int(li.get("quantity") or 0)
                if q <= 0:
                    continue
                line_total = _f((((li.get("discountedTotalSet") or {}).get("shopMoney") or {}).get("amount")) or 0) or 0.0
                row.units_sold += q
                row.sales_revenue_current += line_total
                if row.unit_cost is not None:
                    ex_actual = line_total / GST
                    row.sales_gp_current += ex_actual - (row.unit_cost * q)
                    if row.proposed_price is not None:
                        ex_modeled = (row.proposed_price / GST) * q
                        row.sales_gp_proposed += ex_modeled - (row.unit_cost * q)
                        row.sales_gp_uplift = row.sales_gp_proposed - row.sales_gp_current
        pi = block.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        time.sleep(0.12)

    for v in variants:
        v.sales_revenue_current = round(v.sales_revenue_current, 2)
        v.sales_gp_current = round(v.sales_gp_current, 2)
        v.sales_gp_proposed = round(v.sales_gp_proposed, 2)
        v.sales_gp_uplift = round(v.sales_gp_uplift, 2)

    # 3) Data quality checks
    missing_studio = [v for v in variants if not v.studio_raw]
    missing_cost = [v for v in variants if v.unit_cost is None]
    invalid_cost = [v for v in variants if v.unit_cost is not None and v.unit_cost <= 0]
    suspicious_low_cost = [v for v in variants if v.unit_cost is not None and v.unit_cost < 2]
    missing_barcode = [v for v in variants if not v.barcode]
    dup_map: dict[str, list[VariantAudit]] = defaultdict(list)
    for v in variants:
        if v.barcode:
            dup_map[v.barcode].append(v)
    duplicate_barcodes = {k: vals for k, vals in dup_map.items() if len(vals) > 1}

    # Compare live data vs local audit snapshots where available (Arrow / Second Sight)
    local_diff = {"compared_rows": 0, "price_diff_count": 0, "cost_diff_count": 0}
    for path in [Path("tmp/arrow_repricing_audit.csv"), Path("tmp/second_sight_margin_audit.csv")]:
        if not path.exists():
            continue
        with path.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                vid = r.get("shopify_variant_id") or ""
                live = by_vid.get(vid)
                if not live:
                    continue
                local_diff["compared_rows"] += 1
                ap = _f(r.get("current_price"))
                ac = _f(r.get("unit_cost"))
                if ap is not None and live.current_price is not None and abs(ap - live.current_price) > 0.01:
                    local_diff["price_diff_count"] += 1
                if ac is not None and live.unit_cost is not None and abs(ac - live.unit_cost) > 0.01:
                    local_diff["cost_diff_count"] += 1

    # 4) Studio-level summary (for non-excluded main analysis)
    main = [v for v in variants if v.valid_cost and not v.excluded_from_main]
    bench_arrow = [v for v in variants if v.valid_cost and _is_arrow(v.studio_raw)]
    bench_second = [v for v in variants if v.valid_cost and _is_second(v.studio_raw)]

    def studio_stats(rows: list[VariantAudit]) -> dict:
        n = len(rows)
        if n == 0:
            return {}
        below = [r for r in rows if (r.current_margin_pct or 0) < 28]
        incs = [r.proposed_increase or 0 for r in rows]
        current_weighted = (
            sum((r.current_margin_pct or 0) * (r.current_price or 0) for r in rows) / max(sum((r.current_price or 0) for r in rows), 1e-9)
        )
        units = sum(r.units_sold for r in rows)
        gp_cur = sum(r.sales_gp_current for r in rows)
        gp_prop = sum(r.sales_gp_proposed for r in rows)
        ex_sales = sum((r.sales_revenue_current / GST) for r in rows)
        sales_weighted = ((gp_cur / ex_sales) * 100) if ex_sales > 0 else None
        easy_rows = [r for r in rows if (r.proposed_increase or 0) <= 10]
        easy_uplift = sum(r.sales_gp_uplift for r in easy_rows)
        total_uplift = sum(r.sales_gp_uplift for r in rows)
        return {
            "active_variants": n,
            "units_sold": units,
            "current_weighted_margin_pct": round(current_weighted, 2),
            "sales_weighted_margin_pct": round(sales_weighted, 2) if sales_weighted is not None else None,
            "variants_below_28": len(below),
            "pct_below_28": round((len(below) / n) * 100, 2),
            "avg_increase": round(statistics.mean(incs), 2),
            "median_increase": round(statistics.median(incs), 2),
            "largest_increase": round(max(incs), 2),
            "gp_current_sales_mix": round(gp_cur, 2),
            "gp_modeled_28_sales_mix": round(gp_prop, 2),
            "gp_uplift": round(total_uplift, 2),
            "gp_uplift_pct": round((total_uplift / gp_cur) * 100, 2) if gp_cur > 0 else None,
            "count_inc_<=2": len([x for x in rows if 0 < (x.proposed_increase or 0) <= 2]),
            "count_inc_2_01_5": len([x for x in rows if 2 < (x.proposed_increase or 0) <= 5]),
            "count_inc_5_01_10": len([x for x in rows if 5 < (x.proposed_increase or 0) <= 10]),
            "count_inc_>10": len([x for x in rows if (x.proposed_increase or 0) > 10]),
            "gp_uplift_<=10": round(easy_uplift, 2),
            "pct_uplift_capturable_<=10": round((easy_uplift / total_uplift) * 100, 2) if total_uplift > 0 else 0.0,
        }

    by_studio: dict[str, list[VariantAudit]] = defaultdict(list)
    for r in main:
        by_studio[r.studio_norm].append(r)
    studio_summary = []
    for studio, rows_s in by_studio.items():
        s = studio_stats(rows_s)
        s["studio_norm"] = studio
        s["raw_studio_values"] = sorted(studio_raw_map.get(studio, {studio}))
        studio_summary.append(s)

    # 5) Rankings
    def top_by(key: str, reverse: bool = True, n: int = 10):
        return sorted(studio_summary, key=lambda x: x.get(key) if x.get(key) is not None else -math.inf, reverse=reverse)[:n]

    rank_gp = top_by("gp_uplift", True, 10)
    rank_low_margin = sorted(
        [s for s in studio_summary if s.get("sales_weighted_margin_pct") is not None],
        key=lambda x: x["sales_weighted_margin_pct"],
    )[:10]
    rank_exposure = sorted(studio_summary, key=lambda x: x["units_sold"] * x["pct_below_28"] / 100.0, reverse=True)[:10]
    rank_easy = top_by("gp_uplift_<=10", True, 10)

    # Composite priority (transparent weighted score)
    # score = 40% normalized gp uplift + 25% margin shortfall + 20% sales volume + 15% easy-capture ratio
    max_gp = max([s["gp_uplift"] for s in studio_summary] + [1])
    max_units = max([s["units_sold"] for s in studio_summary] + [1])
    for s in studio_summary:
        gp_norm = s["gp_uplift"] / max_gp if max_gp else 0
        shortfall = max(0.0, 28.0 - (s.get("sales_weighted_margin_pct") or 0)) / 28.0
        vol = s["units_sold"] / max_units if max_units else 0
        easy = (s.get("pct_uplift_capturable_<=10") or 0) / 100.0
        s["composite_priority_score"] = round((0.40 * gp_norm) + (0.25 * shortfall) + (0.20 * vol) + (0.15 * easy), 4)
    rank_composite = sorted(studio_summary, key=lambda x: x["composite_priority_score"], reverse=True)[:10]

    # 6) Store-wide economics
    def store_rollup(rows: list[VariantAudit]) -> dict:
        n = len(rows)
        units = sum(r.units_sold for r in rows)
        below = [r for r in rows if (r.current_margin_pct or 0) < 28]
        units_below = sum(r.units_sold for r in below)
        weighted_margin = (
            sum((r.current_margin_pct or 0) * (r.current_price or 0) for r in rows) / max(sum((r.current_price or 0) for r in rows), 1e-9)
        )
        ex_sales = sum((r.sales_revenue_current / GST) for r in rows)
        gp_cur = sum(r.sales_gp_current for r in rows)
        gp_prop = sum(r.sales_gp_proposed for r in rows)
        uplift = gp_prop - gp_cur
        easy_rows = [r for r in rows if (r.proposed_increase or 0) <= 10]
        easy_uplift = sum(r.sales_gp_uplift for r in easy_rows)
        sales_weighted = ((gp_cur / ex_sales) * 100) if ex_sales > 0 else None
        return {
            "eligible_variants": n,
            "recent_units_sold": units,
            "current_weighted_margin_pct": round(weighted_margin, 2),
            "current_sales_weighted_margin_pct": round(sales_weighted, 2) if sales_weighted is not None else None,
            "pct_variants_below_28": round((len(below) / n) * 100, 2) if n else 0,
            "pct_units_below_28": round((units_below / units) * 100, 2) if units else 0,
            "current_modeled_gp": round(gp_cur, 2),
            "modeled_gp_at_28_floor": round(gp_prop, 2),
            "total_theoretical_gp_opportunity": round(uplift, 2),
            "total_theoretical_gp_opportunity_pct": round((uplift / gp_cur) * 100, 2) if gp_cur > 0 else None,
            "gp_opportunity_<=10": round(easy_uplift, 2),
            "pct_opportunity_<=10": round((easy_uplift / uplift) * 100, 2) if uplift > 0 else 0.0,
        }

    all_valid = [v for v in variants if v.valid_cost]
    rest_valid = [v for v in variants if v.valid_cost and not _is_arrow(v.studio_raw) and not _is_second(v.studio_raw)]
    overall = store_rollup(all_valid)
    remaining = store_rollup(rest_valid)
    bench = {
        "Arrow": store_rollup(bench_arrow),
        "Second Sight": store_rollup(bench_second),
        "Rest of Store": store_rollup([v for v in rest_valid if not v.excluded_from_main]),
    }

    # 7) Product opportunity views
    main_sales = [v for v in main if v.units_sold > 0]
    top_products = sorted(main_sales, key=lambda x: x.sales_gp_uplift, reverse=True)[:20]
    high_volume_under = sorted(
        [v for v in main_sales if (v.current_margin_pct or 0) < 28],
        key=lambda x: x.units_sold,
        reverse=True,
    )[:20]
    quick_wins = sorted(
        [v for v in main_sales if (v.current_margin_pct or 0) < 28 and (v.proposed_increase or 0) <= 5],
        key=lambda x: x.sales_gp_uplift,
        reverse=True,
    )[:50]
    material_corr = sorted(
        [v for v in main if (v.proposed_increase or 0) > 10],
        key=lambda x: x.proposed_increase or 0,
        reverse=True,
    )
    very_low = {
        "below_20": [v for v in main if (v.current_margin_pct or 0) < 20],
        "below_10": [v for v in main if (v.current_margin_pct or 0) < 10],
        "below_0": [v for v in main if (v.current_margin_pct or 0) < 0],
    }

    # 8) Distribution counts with units (remaining eligible catalogue)
    dist_labels = ["$0", "$0.01-$2", "$2.01-$5", "$5.01-$10", "$10.01-$20", "$20+"]
    dist = {k: {"variant_count": 0, "units_sold": 0} for k in dist_labels}
    for v in main:
        inc = v.proposed_increase or 0
        if inc <= 0:
            key = "$0"
        elif inc <= 2:
            key = "$0.01-$2"
        elif inc <= 5:
            key = "$2.01-$5"
        elif inc <= 10:
            key = "$5.01-$10"
        elif inc <= 20:
            key = "$10.01-$20"
        else:
            key = "$20+"
        dist[key]["variant_count"] += 1
        dist[key]["units_sold"] += v.units_sold

    return {
        "variants": variants,
        "studio_raw_map": {k: sorted(v) for k, v in studio_raw_map.items()},
        "studio_summary": studio_summary,
        "rankings": {
            "largest_absolute_gp_opportunity": rank_gp,
            "lowest_current_margin": rank_low_margin,
            "highest_sales_exposure_below_28": rank_exposure,
            "easiest_gp_opportunity_<=10": rank_easy,
            "highest_priority_commercial_opportunity": rank_composite,
            "composite_method": "40% uplift + 25% margin shortfall + 20% volume + 15% <=$10 capture ratio",
        },
        "store_wide": {"all_valid": overall, "remaining_after_arrow_second": remaining},
        "benchmark_groups": bench,
        "product_opportunities": {
            "top_20_gp_uplift": [asdict(v) for v in top_products],
            "top_20_high_volume_under_margin": [asdict(v) for v in high_volume_under],
            "quick_wins": [asdict(v) for v in quick_wins],
            "material_corrections_gt_10": [asdict(v) for v in material_corr],
            "very_low_margin": {
                "below_20": [asdict(v) for v in very_low["below_20"]],
                "below_10": [asdict(v) for v in very_low["below_10"]],
                "below_0": [asdict(v) for v in very_low["below_0"]],
            },
        },
        "price_increase_distribution_remaining": dist,
        "data_quality": {
            "missing_studio_count": len(missing_studio),
            "missing_unit_cost_count": len(missing_cost),
            "invalid_cost_count": len(invalid_cost),
            "missing_barcode_count": len(missing_barcode),
            "suspicious_low_cost_count": len(suspicious_low_cost),
            "duplicate_barcode_count": len(duplicate_barcodes),
            "duplicate_barcodes": {
                k: [
                    {
                        "title": x.product_title,
                        "variant_id": x.shopify_variant_id,
                        "studio": x.studio_raw,
                        "price": x.current_price,
                    }
                    for x in vals
                ]
                for k, vals in duplicate_barcodes.items()
            },
            "live_vs_local_snapshot_diff": local_diff,
        },
        "separate_groups": {
            "missing_studio": [asdict(v) for v in missing_studio],
            "vinyl_music": [asdict(v) for v in variants if v.category_flag == "vinyl_music"],
            "books": [asdict(v) for v in variants if v.category_flag == "books"],
            "games": [asdict(v) for v in variants if v.category_flag == "games"],
            "gift_cards": [asdict(v) for v in variants if v.category_flag == "gift_card"],
            "non_film_merch": [asdict(v) for v in variants if v.category_flag == "non_film_merch"],
        },
    }


def write_outputs(data: dict, out_csv: Path, out_json: Path, studio_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    variants: list[VariantAudit] = data["variants"]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[x.name for x in fields(VariantAudit)])
        w.writeheader()
        for r in variants:
            w.writerow(asdict(r))

    studio_summary = data["studio_summary"]
    if studio_summary:
        keys = sorted({k for row in studio_summary for k in row.keys()})
        with studio_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for row in studio_summary:
                rr = row.copy()
                if isinstance(rr.get("raw_studio_values"), list):
                    rr["raw_studio_values"] = " | ".join(rr["raw_studio_values"])
                w.writerow(rr)

    payload = data.copy()
    payload["variants"] = [asdict(v) for v in variants]
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    p = argparse.ArgumentParser(description="Store-wide margin opportunity audit")
    p.add_argument("--env", default=".env.prod")
    p.add_argument("--api-version", default="2026-04")
    p.add_argument("--out-csv", default="tmp/store_margin_opportunity_audit.csv")
    p.add_argument("--out-json", default="tmp/store_margin_opportunity_audit.json")
    p.add_argument("--out-studio-csv", default="tmp/studio_margin_opportunity_summary.csv")
    p.add_argument("--order-pages", type=int, default=20)
    args = p.parse_args()

    load_dotenv(args.env, override=True)
    client = ShopifyClient(api_version=args.api_version)
    data = run_audit(client, max_order_pages=args.order_pages)
    write_outputs(data, Path(args.out_csv), Path(args.out_json), Path(args.out_studio_csv))

    print("STORE_WIDE|" + json.dumps(data["store_wide"]))
    print("BENCHMARK|" + json.dumps(data["benchmark_groups"]))
    print(f"CSV={args.out_csv}")
    print(f"JSON={args.out_json}")
    print(f"STUDIO_CSV={args.out_studio_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
