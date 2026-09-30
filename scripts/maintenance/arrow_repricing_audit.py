"""
Arrow repricing dry-run audit.

Queries all ACTIVE Arrow Films / Arrow Video products from Shopify,
reads each variant's current price and unit cost (from inventoryItem.unitCost),
then calculates the minimum 28% gross-margin price and proposes repricing.

Safety: READ-ONLY — no Shopify writes.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import time
from dataclasses import dataclass, asdict, fields
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.rules.pricing_rules import round_up_to_99

TARGET_MARGIN = 0.28
GST_RATE = 1.10

PRODUCTS_QUERY = """
query ArrowRepricingAudit($cursor: String, $q: String) {
  products(first: 50, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      handle
      status
      vendor
      studio: metafield(namespace: "custom", key: "studio") { value }
      preOrder: metafield(namespace: "custom", key: "pre_order") { value }
      mediaReleaseDate: metafield(namespace: "custom", key: "media_release_date") { value }
      variants(first: 50) {
        nodes {
          id
          title
          sku
          barcode
          price
          compareAtPrice
          inventoryPolicy
          inventoryQuantity
          inventoryItem {
            unitCost { amount currencyCode }
          }
        }
      }
    }
  }
}
"""


def _parse_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(val)
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


def _is_arrow(studio: Optional[str]) -> bool:
    return "arrow" in (studio or "").casefold()


def calc_min_price_28(unit_cost_aud: float) -> float:
    """GST-inclusive price achieving exactly 28% margin on ex-GST revenue."""
    ex_gst = unit_cost_aud / (1 - TARGET_MARGIN)
    return ex_gst * GST_RATE


def round_up_to_99_safe(value: float) -> float:
    """Round up to .99 ensuring the result is >= value."""
    candidate = round_up_to_99(value)
    if candidate < value - 0.001:
        candidate = round_up_to_99(value + 1.0)
    return candidate


def calc_margin_pct(price_inc_gst: float, cost: float) -> Optional[float]:
    """Gross margin % on ex-GST revenue."""
    if price_inc_gst <= 0:
        return None
    ex_gst = price_inc_gst / GST_RATE
    return (ex_gst - cost) / ex_gst * 100


@dataclass
class VariantAudit:
    product_title: str
    variant_title: str
    barcode: str
    sku: str
    shopify_product_id: str
    shopify_variant_id: str
    studio: str
    vendor: str
    unit_cost: Optional[float]
    cost_currency: str
    cost_source: str
    current_price: Optional[float]
    compare_at_price: Optional[float]
    current_ex_gst: Optional[float]
    current_gross_profit: Optional[float]
    current_margin_pct: Optional[float]
    calc_28_price_raw: Optional[float]
    proposed_price: Optional[float]
    proposed_gross_profit: Optional[float]
    proposed_margin_pct: Optional[float]
    price_increase: Optional[float]
    price_increase_pct: Optional[float]
    inventory_qty: Optional[int]
    inventory_policy: str
    is_preorder: bool
    would_change: bool
    skip: bool
    skip_reason: str
    warning: str


def fetch_arrow_variants(client: ShopifyClient, product_query: str) -> list[VariantAudit]:
    results: list[VariantAudit] = []
    cursor = None
    pages = 0

    while True:
        pages += 1
        tries = 0
        while True:
            tries += 1
            try:
                data = client.graphql(PRODUCTS_QUERY, {"cursor": cursor, "q": product_query or None})
                break
            except Exception as e:
                if "THROTTLED" in str(e) and tries < 6:
                    time.sleep(min(2 * tries, 10))
                    continue
                raise

        block = data["products"]
        for product in block.get("nodes") or []:
            status = product.get("status", "")
            if status != "ACTIVE":
                continue

            studio_raw = (product.get("studio") or {}).get("value") or ""
            if not _is_arrow(studio_raw):
                continue

            product_id = product.get("id", "")
            title = product.get("title", "")
            vendor = product.get("vendor", "")

            pre_order_raw = (product.get("preOrder") or {}).get("value") or ""
            media_release = (product.get("mediaReleaseDate") or {}).get("value") or ""
            is_preorder = pre_order_raw.lower() in ("true", "1", "yes")

            for variant in (product.get("variants") or {}).get("nodes") or []:
                vid = variant.get("id", "")
                vtitle = variant.get("title", "")
                vsku = variant.get("sku", "")
                vbarcode = variant.get("barcode", "")
                vprice = _parse_float(variant.get("price"))
                vcompare = _parse_float(variant.get("compareAtPrice"))
                inv_qty = variant.get("inventoryQuantity")
                inv_policy = variant.get("inventoryPolicy", "")

                cost_obj = (variant.get("inventoryItem") or {}).get("unitCost") or {}
                unit_cost = _parse_float(cost_obj.get("amount"))
                cost_cc = cost_obj.get("currencyCode", "")

                # Determine skip/warning
                skip = False
                skip_reason = ""
                warning = ""

                if unit_cost is None or unit_cost == 0:
                    skip = True
                    skip_reason = "missing_or_zero_cost"
                elif cost_cc and cost_cc != "AUD":
                    skip = True
                    skip_reason = f"non_aud_cost_{cost_cc}"
                elif vprice is None:
                    skip = True
                    skip_reason = "missing_price"

                # Calculations
                current_ex_gst = vprice / GST_RATE if vprice else None
                current_gp = (current_ex_gst - unit_cost) if (current_ex_gst is not None and unit_cost is not None) else None
                current_margin = calc_margin_pct(vprice, unit_cost) if (vprice and unit_cost) else None

                calc_raw = None
                proposed = None
                proposed_gp = None
                proposed_margin = None
                price_inc = None
                price_inc_pct = None
                would_change = False

                if not skip and unit_cost and vprice:
                    calc_raw = round(calc_min_price_28(unit_cost), 2)
                    min_99 = round_up_to_99_safe(calc_raw)

                    # Verify the rounded price actually achieves 28%
                    verify_margin = calc_margin_pct(min_99, unit_cost)
                    if verify_margin is not None and verify_margin < TARGET_MARGIN * 100 - 0.01:
                        min_99 = round_up_to_99_safe(min_99 + 1.0)
                        warning = "double_rounded_to_hit_28pct"

                    proposed = max(vprice, min_99)
                    proposed_ex_gst = proposed / GST_RATE
                    proposed_gp = round(proposed_ex_gst - unit_cost, 2)
                    proposed_margin = calc_margin_pct(proposed, unit_cost)
                    would_change = abs(proposed - vprice) > 0.005
                    price_inc = round(proposed - vprice, 2) if would_change else 0.0
                    price_inc_pct = round(price_inc / vprice * 100, 2) if (would_change and vprice > 0) else 0.0

                results.append(VariantAudit(
                    product_title=title,
                    variant_title=vtitle,
                    barcode=vbarcode,
                    sku=vsku,
                    shopify_product_id=product_id,
                    shopify_variant_id=vid,
                    studio=studio_raw,
                    vendor=vendor,
                    unit_cost=unit_cost,
                    cost_currency=cost_cc,
                    cost_source="shopify_inventory_item_unit_cost",
                    current_price=vprice,
                    compare_at_price=vcompare,
                    current_ex_gst=round(current_ex_gst, 2) if current_ex_gst is not None else None,
                    current_gross_profit=round(current_gp, 2) if current_gp is not None else None,
                    current_margin_pct=round(current_margin, 2) if current_margin is not None else None,
                    calc_28_price_raw=calc_raw,
                    proposed_price=proposed,
                    proposed_gross_profit=proposed_gp,
                    proposed_margin_pct=round(proposed_margin, 2) if proposed_margin is not None else None,
                    price_increase=price_inc,
                    price_increase_pct=price_inc_pct,
                    inventory_qty=inv_qty,
                    inventory_policy=inv_policy,
                    is_preorder=is_preorder,
                    would_change=would_change,
                    skip=skip,
                    skip_reason=skip_reason,
                    warning=warning,
                ))

        pi = block.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        time.sleep(0.15)

    return results


def print_summary(rows: list[VariantAudit]) -> dict:
    total = len(rows)
    skipped = [r for r in rows if r.skip]
    eligible = [r for r in rows if not r.skip]
    already_ok = [r for r in eligible if not r.would_change]
    needs_change = [r for r in eligible if r.would_change]

    # Weighted margins (weighted by current price as proxy for revenue)
    def weighted_margin(items, price_attr, margin_attr):
        total_w = sum(getattr(r, price_attr) or 0 for r in items)
        if total_w == 0:
            return None
        return sum((getattr(r, margin_attr) or 0) * (getattr(r, price_attr) or 0) for r in items) / total_w

    current_wm = weighted_margin(eligible, "current_price", "current_margin_pct")
    proposed_wm = weighted_margin(eligible, "proposed_price", "proposed_margin_pct")

    increases = [r.price_increase for r in needs_change if r.price_increase]
    avg_inc = sum(increases) / len(increases) if increases else 0
    med_inc = sorted(increases)[len(increases) // 2] if increases else 0
    max_inc = max(increases) if increases else 0

    summary = {
        "total_arrow_variants": total,
        "eligible": len(eligible),
        "already_gte_28pct": len(already_ok),
        "below_28pct_need_reprice": len(needs_change),
        "skipped": len(skipped),
        "skip_reasons": {},
        "current_weighted_margin_pct": round(current_wm, 2) if current_wm else None,
        "proposed_weighted_margin_pct": round(proposed_wm, 2) if proposed_wm else None,
        "avg_price_increase": round(avg_inc, 2),
        "median_price_increase": round(med_inc, 2),
        "max_price_increase": round(max_inc, 2),
    }

    for r in skipped:
        summary["skip_reasons"][r.skip_reason] = summary["skip_reasons"].get(r.skip_reason, 0) + 1

    print("\n" + "=" * 60)
    print("ARROW REPRICING DRY-RUN SUMMARY")
    print("=" * 60)
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print("=" * 60)

    # Validation examples
    validation_titles = [
        "Troy Limited Edition 4K Ultra HD",
        "The Frighteners Limited Edition 4K Ultra HD",
        "The Last Boy Scout Limited Edition 4K Ultra HD",
        "Spaceballs Limited Edition 4K Ultra HD",
        "Bullet In The Head Limited Edition 4K Ultra HD",
        "Stranger Things Seasons 1 to 5 Complete Collection Deluxe Limited Edition 4K Ultra HD",
    ]
    print("\nVALIDATION EXAMPLES:")
    for vt in validation_titles:
        matches = [r for r in rows if r.product_title == vt]
        if not matches:
            print(f"  {vt}: NOT FOUND")
            continue
        for m in matches:
            print(f"  {m.product_title}")
            print(f"    cost=${m.unit_cost}  current=${m.current_price}  margin={m.current_margin_pct}%")
            print(f"    calc_28=${m.calc_28_price_raw}  proposed=${m.proposed_price}  new_margin={m.proposed_margin_pct}%")
            print(f"    change={'YES +$' + str(m.price_increase) if m.would_change else 'NO'}")
    print()

    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description="Arrow repricing dry-run audit")
    parser.add_argument("--env", default=".env", help="Env file (default .env)")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument("--csv", default="tmp/arrow_repricing_audit.csv", help="Output CSV path")
    parser.add_argument("--json", default="", help="Optional JSON summary output")
    parser.add_argument("--product-query", default="status:active", help="Shopify product query filter")
    args = parser.parse_args(argv)

    load_dotenv(args.env, override=True)
    client = ShopifyClient(api_version=args.api_version)

    print("Fetching active Arrow products from Shopify...")
    rows = fetch_arrow_variants(client, args.product_query)
    print(f"Found {len(rows)} Arrow variants")

    summary = print_summary(rows)

    # Write CSV
    out = Path(args.csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    field_names = [f.name for f in fields(VariantAudit)]
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=field_names)
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))
    print(f"CSV written: {out} ({len(rows)} rows)")

    if args.json:
        jp = Path(args.json)
        jp.parent.mkdir(parents=True, exist_ok=True)
        jp.write_text(json.dumps({"summary": summary, "rows": [asdict(r) for r in rows]}, indent=2))
        print(f"JSON written: {jp}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
