"""
Reusable studio margin audit (read-only).

Calculates current and modeled 28% ex-GST margin floor for active Shopify variants
identified by custom.studio metafield match.
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.rules.pricing_rules import round_up_to_99

GST_RATE = 1.10
TARGET_MARGIN_RATIO = 0.28

PRODUCTS_QUERY = """
query StudioMarginAuditProducts($cursor: String, $q: String) {
  products(first: 50, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      status
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
query StudioMarginOrders($cursor: String) {
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
          variant {
            id
            sku
            barcode
          }
          product {
            studio: metafield(namespace: "custom", key: "studio") { value }
          }
        }
      }
    }
  }
}
"""


def _f(value) -> Optional[float]:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _contains_studio(studio: str, needle: str) -> bool:
    return needle.casefold() in (studio or "").casefold()


def _calc_margin_pct(price_inc_gst: float, unit_cost: float) -> Optional[float]:
    if not price_inc_gst or price_inc_gst <= 0:
        return None
    ex_gst = price_inc_gst / GST_RATE
    return (ex_gst - unit_cost) / ex_gst * 100


def _min_28_price(unit_cost: float) -> float:
    raw = (unit_cost / (1 - TARGET_MARGIN_RATIO)) * GST_RATE
    candidate = round_up_to_99(raw)
    # Defensive check to ensure not below 28% due to rounding edge
    while True:
        m = _calc_margin_pct(candidate, unit_cost)
        if m is not None and m >= 28.0 - 1e-6:
            return candidate
        candidate = round_up_to_99(candidate + 1.0)


def _gql(client: ShopifyClient, query: str, variables: dict, retries: int = 6) -> dict:
    for i in range(retries):
        try:
            return client.graphql(query, variables)
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if ("THROTTLED" in msg or "429" in msg) and i < retries - 1:
                time.sleep(min(2 + i, 10))
                continue
            raise
    raise RuntimeError("GraphQL failed after retries")


@dataclass
class AuditRow:
    product_title: str
    variant_title: str
    barcode: str
    sku: str
    shopify_product_id: str
    shopify_variant_id: str
    studio: str
    unit_cost: Optional[float]
    cost_currency: str
    cost_source: str
    current_price: Optional[float]
    current_gp: Optional[float]
    current_margin_pct: Optional[float]
    floor_price_28: Optional[float]
    proposed_price: Optional[float]
    proposed_margin_pct: Optional[float]
    price_increase: Optional[float]
    price_increase_pct: Optional[float]
    would_change: bool
    skip: bool
    skip_reason: str


def run_audit(client: ShopifyClient, studio_match: str, product_query: str) -> tuple[list[AuditRow], list[str]]:
    rows: list[AuditRow] = []
    matching_studios: set[str] = set()
    cursor = None
    while True:
        data = _gql(client, PRODUCTS_QUERY, {"cursor": cursor, "q": product_query or None})
        block = data["products"]
        for p in block.get("nodes") or []:
            if p.get("status") != "ACTIVE":
                continue
            studio = ((p.get("studio") or {}).get("value") or "").strip()
            if not _contains_studio(studio, studio_match):
                continue
            matching_studios.add(studio or "(blank)")

            for v in (p.get("variants") or {}).get("nodes") or []:
                price = _f(v.get("price"))
                uc = _f((((v.get("inventoryItem") or {}).get("unitCost") or {}).get("amount")))
                cc = (((v.get("inventoryItem") or {}).get("unitCost") or {}).get("currencyCode") or "")

                skip = False
                reason = ""
                if uc is None or uc <= 0:
                    skip = True
                    reason = "missing_or_zero_cost"
                elif cc and cc != "AUD":
                    skip = True
                    reason = f"non_aud_cost_{cc}"
                elif price is None or price <= 0:
                    skip = True
                    reason = "missing_or_zero_price"

                current_gp = None
                current_margin = None
                floor_28 = None
                proposed = None
                proposed_margin = None
                inc = None
                inc_pct = None
                would_change = False

                if not skip and price is not None and uc is not None:
                    ex_gst = price / GST_RATE
                    current_gp = ex_gst - uc
                    current_margin = _calc_margin_pct(price, uc)
                    floor_28 = _min_28_price(uc)
                    proposed = max(price, floor_28)
                    proposed_margin = _calc_margin_pct(proposed, uc)
                    would_change = abs(proposed - price) > 0.005
                    inc = round(proposed - price, 2) if would_change else 0.0
                    inc_pct = round((inc / price) * 100, 2) if would_change and price else 0.0

                rows.append(
                    AuditRow(
                        product_title=p.get("title", ""),
                        variant_title=v.get("title", ""),
                        barcode=v.get("barcode") or "",
                        sku=v.get("sku") or "",
                        shopify_product_id=p.get("id", ""),
                        shopify_variant_id=v.get("id", ""),
                        studio=studio,
                        unit_cost=uc,
                        cost_currency=cc,
                        cost_source="shopify_inventory_item_unit_cost",
                        current_price=price,
                        current_gp=round(current_gp, 2) if current_gp is not None else None,
                        current_margin_pct=round(current_margin, 2) if current_margin is not None else None,
                        floor_price_28=floor_28,
                        proposed_price=proposed,
                        proposed_margin_pct=round(proposed_margin, 2) if proposed_margin is not None else None,
                        price_increase=inc,
                        price_increase_pct=inc_pct,
                        would_change=would_change,
                        skip=skip,
                        skip_reason=reason,
                    )
                )
        pi = block.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        time.sleep(0.15)

    return rows, sorted(matching_studios)


def sales_weighted_analysis(client: ShopifyClient, rows: list[AuditRow], studio_match: str) -> dict:
    row_by_vid = {r.shopify_variant_id: r for r in rows if not r.skip}
    units = 0
    current_gp_total = 0.0
    proposed_gp_total = 0.0
    current_ex_gst_revenue = 0.0
    proposed_ex_gst_revenue = 0.0
    sku_delta: dict[str, float] = {}
    cursor = None
    pages = 0

    while pages < 20:  # same recent window behavior as Arrow work
        pages += 1
        data = _gql(client, ORDERS_QUERY, {"cursor": cursor})
        block = data["orders"]
        for order in block.get("nodes") or []:
            for li in (order.get("lineItems") or {}).get("nodes") or []:
                studio = (((li.get("product") or {}).get("studio") or {}).get("value") or "").strip()
                if not _contains_studio(studio, studio_match):
                    continue
                var = li.get("variant") or {}
                vid = var.get("id") or ""
                q = int(li.get("quantity") or 0)
                r = row_by_vid.get(vid)
                if not r or q <= 0:
                    continue
                if r.current_price is None or r.proposed_price is None or r.unit_cost is None:
                    continue
                units += q
                old_gp_per_unit = (r.current_price / GST_RATE) - r.unit_cost
                new_gp_per_unit = (r.proposed_price / GST_RATE) - r.unit_cost
                current_gp_total += old_gp_per_unit * q
                proposed_gp_total += new_gp_per_unit * q
                current_ex_gst_revenue += (r.current_price / GST_RATE) * q
                proposed_ex_gst_revenue += (r.proposed_price / GST_RATE) * q
                sku_key = r.sku or r.barcode or r.product_title
                sku_delta[sku_key] = sku_delta.get(sku_key, 0.0) + (new_gp_per_unit - old_gp_per_unit) * q
        pi = block.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        time.sleep(0.12)

    additional = proposed_gp_total - current_gp_total
    top = sorted(sku_delta.items(), key=lambda x: x[1], reverse=True)[:10]
    return {
        "units_sold": units,
        "sales_weighted_margin_current_pct": round((current_gp_total / current_ex_gst_revenue) * 100, 2)
        if current_ex_gst_revenue > 0
        else None,
        "sales_weighted_margin_proposed_pct": round((proposed_gp_total / proposed_ex_gst_revenue) * 100, 2)
        if proposed_ex_gst_revenue > 0
        else None,
        "estimated_gp_current": round(current_gp_total, 2),
        "estimated_gp_proposed": round(proposed_gp_total, 2),
        "additional_gp": round(additional, 2),
        "additional_gp_pct": round((additional / current_gp_total * 100), 2) if current_gp_total > 0 else None,
        "top_sku_gp_improvement": [{"sku_or_key": k, "additional_gp": round(v, 2)} for k, v in top],
        "note": "Based on recent orders window queried from Shopify (up to 20 pages x 50 orders).",
    }


def build_summary(rows: list[AuditRow]) -> dict:
    total = len(rows)
    valid = [r for r in rows if not r.skip]
    skipped = [r for r in rows if r.skip]
    already = [r for r in valid if not r.would_change]
    below = [r for r in valid if r.would_change]

    margins = [r.current_margin_pct for r in valid if r.current_margin_pct is not None]
    incs = [r.price_increase for r in below if r.price_increase is not None]

    cur_w_num = sum((r.current_margin_pct or 0) * (r.current_price or 0) for r in valid)
    cur_w_den = sum((r.current_price or 0) for r in valid)
    prop_w_num = sum((r.proposed_margin_pct or 0) * (r.proposed_price or 0) for r in valid)
    prop_w_den = sum((r.proposed_price or 0) for r in valid)

    dist = {
        "$0": 0,
        "$0.01-$2": 0,
        "$2.01-$5": 0,
        "$5.01-$10": 0,
        "$10.01-$20": 0,
        "$20+": 0,
    }
    for r in valid:
        inc = r.price_increase or 0.0
        if inc <= 0:
            dist["$0"] += 1
        elif inc <= 2:
            dist["$0.01-$2"] += 1
        elif inc <= 5:
            dist["$2.01-$5"] += 1
        elif inc <= 10:
            dist["$5.01-$10"] += 1
        elif inc <= 20:
            dist["$10.01-$20"] += 1
        else:
            dist["$20+"] += 1

    largest = sorted([r for r in below if r.price_increase is not None], key=lambda x: x.price_increase or 0, reverse=True)[:10]
    large_flags = [
        {
            "title": r.product_title,
            "barcode": r.barcode,
            "sku": r.sku,
            "cost": r.unit_cost,
            "current_price": r.current_price,
            "proposed_price": r.proposed_price,
            "price_increase": r.price_increase,
            "price_increase_pct": r.price_increase_pct,
            "shopify_product_id": r.shopify_product_id,
            "shopify_variant_id": r.shopify_variant_id,
        }
        for r in largest
    ]

    return {
        "total_active_variants": total,
        "valid_cost_variants": len(valid),
        "skipped_missing_or_invalid_cost": len(skipped),
        "variants_already_gte_28": len(already),
        "variants_below_28": len(below),
        "current_weighted_margin_pct": round(cur_w_num / cur_w_den, 2) if cur_w_den else None,
        "modeled_weighted_margin_pct": round(prop_w_num / prop_w_den, 2) if prop_w_den else None,
        "average_current_margin_pct": round(statistics.mean(margins), 2) if margins else None,
        "median_current_margin_pct": round(statistics.median(margins), 2) if margins else None,
        "average_proposed_price_increase": round(statistics.mean(incs), 2) if incs else 0.0,
        "median_proposed_price_increase": round(statistics.median(incs), 2) if incs else 0.0,
        "largest_proposed_price_increase": round(max(incs), 2) if incs else 0.0,
        "price_increase_distribution": dist,
        "large_increase_flags": large_flags,
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Studio margin audit")
    p.add_argument("--env", default=".env.prod")
    p.add_argument("--api-version", default="2026-04")
    p.add_argument("--studio-match", required=True, help='Case-insensitive studio token, e.g. "second sight"')
    p.add_argument("--product-query", default="status:active")
    p.add_argument("--csv", required=True)
    p.add_argument("--json", required=True)
    p.add_argument("--with-sales-analysis", action="store_true")
    args = p.parse_args()

    load_dotenv(args.env, override=True)
    client = ShopifyClient(api_version=args.api_version)

    rows, studio_values = run_audit(client, args.studio_match, args.product_query)
    summary = build_summary(rows)
    sales = sales_weighted_analysis(client, rows, args.studio_match) if args.with_sales_analysis else None

    out_csv = Path(args.csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[f.name for f in fields(AuditRow)])
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))

    out_json = Path(args.json)
    payload = {
        "studio_match": args.studio_match,
        "distinct_matching_studio_values": studio_values,
        "summary": summary,
        "sales_weighted_analysis": sales,
        "rows": [asdict(r) for r in rows],
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("DISTINCT_STUDIOS|" + json.dumps(studio_values))
    print("SUMMARY|" + json.dumps(summary))
    if sales is not None:
        print("SALES|" + json.dumps(sales))
    print(f"CSV={out_csv}")
    print(f"JSON={out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
