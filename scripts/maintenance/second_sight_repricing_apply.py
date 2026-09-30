"""
Apply approved Second Sight repricing from completed audit.

Scope:
- Apply only rows where would_change=True and price_increase <= threshold.
- Exclude >threshold rows (commercial exceptions) and already >=28% rows.
- Price-only Shopify updates via productVariantsBulkUpdate.
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient

GST_RATE = 1.10

VARIANT_QUERY = """
query VariantNode($id: ID!) {
  node(id: $id) {
    ... on ProductVariant {
      id
      title
      barcode
      sku
      price
      compareAtPrice
      inventoryPolicy
      inventoryQuantity
      product {
        id
        title
        studio: metafield(namespace: "custom", key: "studio") { value }
      }
      inventoryItem { unitCost { amount currencyCode } }
    }
  }
}
"""

VARIANT_NODES_QUERY = """
query VariantNodes($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on ProductVariant {
      id
      title
      barcode
      sku
      price
      compareAtPrice
      inventoryPolicy
      inventoryQuantity
      product {
        id
        title
        studio: metafield(namespace: "custom", key: "studio") { value }
      }
      inventoryItem { unitCost { amount currencyCode } }
    }
  }
}
"""

MUTATION = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id price }
    userErrors { field message }
  }
}
"""

ORDERS_QUERY = """
query StudioMarginOrders($cursor: String) {
  orders(first: 50, after: $cursor, sortKey: CREATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      lineItems(first: 50) {
        nodes {
          quantity
          variant { id sku barcode }
          product { studio: metafield(namespace: "custom", key: "studio") { value } }
        }
      }
    }
  }
}
"""


def _f(v: Any) -> Optional[float]:
    if v in ("", None, "None"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _b(v: Any) -> bool:
    return str(v).strip().lower() == "true"


def _money(v: Optional[float]) -> str:
    return "" if v is None else f"{v:.2f}"


def _contains_second_sight(studio: str) -> bool:
    return "second sight" in (studio or "").casefold()


def _margin_pct(price_inc_gst: float, cost_aud: float) -> Optional[float]:
    if price_inc_gst <= 0:
        return None
    ex = price_inc_gst / GST_RATE
    return (ex - cost_aud) / ex * 100


def _gql(client: ShopifyClient, query: str, variables: dict, retries: int = 6) -> dict:
    for i in range(retries):
        try:
            return client.graphql(query, variables)
        except Exception as e:  # noqa: BLE001
            if ("THROTTLED" in str(e) or "429" in str(e)) and i < retries - 1:
                time.sleep(min(2 + i, 10))
                continue
            raise
    raise RuntimeError("GraphQL failed")


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
    current_price: Optional[float]
    current_margin_pct: Optional[float]
    proposed_price: Optional[float]
    proposed_margin_pct: Optional[float]
    price_increase: Optional[float]
    price_increase_pct: Optional[float]
    would_change: bool
    skip: bool


def load_rows(csv_path: Path) -> list[AuditRow]:
    out: list[AuditRow] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            out.append(
                AuditRow(
                    product_title=r["product_title"],
                    variant_title=r["variant_title"],
                    barcode=r["barcode"],
                    sku=r["sku"],
                    shopify_product_id=r["shopify_product_id"],
                    shopify_variant_id=r["shopify_variant_id"],
                    studio=r["studio"],
                    unit_cost=_f(r["unit_cost"]),
                    current_price=_f(r["current_price"]),
                    current_margin_pct=_f(r["current_margin_pct"]),
                    proposed_price=_f(r["proposed_price"]),
                    proposed_margin_pct=_f(r["proposed_margin_pct"]),
                    price_increase=_f(r["price_increase"]),
                    price_increase_pct=_f(r["price_increase_pct"]),
                    would_change=_b(r["would_change"]),
                    skip=_b(r["skip"]),
                )
            )
    return out


def chunk(ids: list[str], n: int = 50) -> list[list[str]]:
    return [ids[i : i + n] for i in range(0, len(ids), n)]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--env", default=".env.prod")
    p.add_argument("--api-version", default="2026-04")
    p.add_argument("--audit-csv", default="tmp/second_sight_margin_audit.csv")
    p.add_argument("--rollback-csv", default="tmp/second_sight_repricing_rollback.csv")
    p.add_argument("--report-json", default="tmp/second_sight_repricing_apply_report.json")
    p.add_argument("--max-increase", type=float, default=10.0)
    args = p.parse_args()

    load_dotenv(args.env, override=True)
    client = ShopifyClient(api_version=args.api_version)
    rows = load_rows(Path(args.audit_csv))

    all_rows = [r for r in rows if _contains_second_sight(r.studio)]
    below = [r for r in all_rows if not r.skip and r.would_change and (r.price_increase or 0) > 0]
    approved = [r for r in below if (r.price_increase or 0) <= args.max_increase]
    excluded = [r for r in below if (r.price_increase or 0) > args.max_increase]
    unchanged = [r for r in all_rows if not r.skip and not r.would_change]

    print(
        "COUNTS|"
        + json.dumps(
            {
                "total": len(all_rows),
                "already_gte_28": len(unchanged),
                "below_28": len(below),
                "approved": len(approved),
                "excluded": len(excluded),
            }
        )
    )

    # explicit exception names requested
    explicit_exception_names = {
        "High Tension 4K UHD",
        "The Addiction Limited Edition 4K Ultra HD",
        "Sexy Beast Limited Edition 4K Ultra HD + Blu-Ray",
    }

    now = datetime.now(timezone.utc).isoformat()
    results: list[dict[str, Any]] = []
    rollback_rows: list[dict[str, Any]] = []

    # precheck each approved row individually (strict checks), then update grouped by product
    grouped_updates: dict[str, list[tuple[AuditRow, float, float]]] = {}
    for r in approved:
        node = _gql(client, VARIANT_QUERY, {"id": r.shopify_variant_id}).get("node") or {}
        live_vid = node.get("id")
        live_pid = ((node.get("product") or {}).get("id")) or ""
        live_studio = (((node.get("product") or {}).get("studio") or {}).get("value") or "").strip()
        live_price = _f(node.get("price"))
        live_cost = _f((((node.get("inventoryItem") or {}).get("unitCost") or {}).get("amount")))
        live_cc = (((node.get("inventoryItem") or {}).get("unitCost") or {}).get("currencyCode") or "")

        ok = True
        reason = "ok"
        if live_vid != r.shopify_variant_id or live_pid != r.shopify_product_id:
            ok = False
            reason = "id_mismatch"
        elif not _contains_second_sight(live_studio):
            ok = False
            reason = f"studio_not_second_sight:{live_studio}"
        elif r.current_price is None or live_price is None or abs(live_price - r.current_price) > 0.005:
            ok = False
            reason = "live_price_conflict"
        elif r.unit_cost is None or live_cost is None or abs(live_cost - r.unit_cost) > 0.005:
            ok = False
            reason = "live_unit_cost_conflict"
        elif live_cc and live_cc != "AUD":
            ok = False
            reason = f"non_aud_live_cost:{live_cc}"
        elif r.proposed_price is None or r.proposed_price <= (r.current_price or 0):
            ok = False
            reason = "proposed_not_higher"
        elif (r.price_increase or 0) > args.max_increase:
            ok = False
            reason = "increase_gt_threshold"
        elif (r.proposed_margin_pct or 0) < 28.0:
            ok = False
            reason = "proposed_margin_below_28"

        if not ok:
            results.append(
                {
                    "title": r.product_title,
                    "barcode": r.barcode,
                    "shopify_product_id": r.shopify_product_id,
                    "shopify_variant_id": r.shopify_variant_id,
                    "unit_cost": r.unit_cost,
                    "old_price": live_price,
                    "new_price": r.proposed_price,
                    "old_margin_pct": r.current_margin_pct,
                    "new_margin_pct": r.proposed_margin_pct,
                    "status": "skipped_conflict",
                    "reason": reason,
                    "timestamp_utc": now,
                }
            )
            continue

        grouped_updates.setdefault(r.shopify_product_id, []).append((r, live_price, live_cost))

    # apply grouped mutations
    for pid, entries in grouped_updates.items():
        payload = [{"id": r.shopify_variant_id, "price": _money(r.proposed_price)} for r, _, _ in entries]
        out = _gql(client, MUTATION, {"productId": pid, "variants": payload})
        pld = out.get("productVariantsBulkUpdate") or {}
        errs = pld.get("userErrors") or []
        updated = {x.get("id") for x in (pld.get("productVariants") or []) if x.get("id")}
        err_text = "; ".join(e.get("message", "error") for e in errs) if errs else ""
        for r, old_price, old_cost in entries:
            success = r.shopify_variant_id in updated and not errs
            status = "updated" if success else "failed"
            reason = "ok" if success else (err_text or "mutation_failed")
            results.append(
                {
                    "title": r.product_title,
                    "barcode": r.barcode,
                    "shopify_product_id": r.shopify_product_id,
                    "shopify_variant_id": r.shopify_variant_id,
                    "unit_cost": old_cost,
                    "old_price": old_price,
                    "new_price": r.proposed_price,
                    "old_margin_pct": r.current_margin_pct,
                    "new_margin_pct": r.proposed_margin_pct,
                    "status": status,
                    "reason": reason,
                    "timestamp_utc": now,
                }
            )
            if success:
                rollback_rows.append(
                    {
                        "product_title": r.product_title,
                        "barcode": r.barcode,
                        "shopify_product_id": r.shopify_product_id,
                        "shopify_variant_id": r.shopify_variant_id,
                        "unit_cost": _money(old_cost),
                        "old_price": _money(old_price),
                        "new_price": _money(r.proposed_price),
                        "old_gross_margin_pct": r.current_margin_pct,
                        "new_gross_margin_pct": r.proposed_margin_pct,
                        "timestamp_utc": now,
                    }
                )

    # annotate excluded and unchanged statuses
    for r in excluded:
        results.append(
            {
                "title": r.product_title,
                "barcode": r.barcode,
                "shopify_product_id": r.shopify_product_id,
                "shopify_variant_id": r.shopify_variant_id,
                "unit_cost": r.unit_cost,
                "old_price": r.current_price,
                "new_price": r.current_price,
                "old_margin_pct": r.current_margin_pct,
                "new_margin_pct": r.current_margin_pct,
                "status": "excluded_gt_threshold",
                "reason": f"increase_gt_{args.max_increase}",
                "timestamp_utc": now,
            }
        )
    for r in unchanged:
        results.append(
            {
                "title": r.product_title,
                "barcode": r.barcode,
                "shopify_product_id": r.shopify_product_id,
                "shopify_variant_id": r.shopify_variant_id,
                "unit_cost": r.unit_cost,
                "old_price": r.current_price,
                "new_price": r.current_price,
                "old_margin_pct": r.current_margin_pct,
                "new_margin_pct": r.current_margin_pct,
                "status": "unchanged_gte_28",
                "reason": "already_at_or_above_floor",
                "timestamp_utc": now,
            }
        )

    # reconciliation reread all second sight variants from audit list
    live_by_vid: dict[str, dict] = {}
    ids = [r.shopify_variant_id for r in all_rows]
    for batch in chunk(ids, 50):
        nodes = _gql(client, VARIANT_NODES_QUERY, {"ids": batch}).get("nodes") or []
        for n in nodes:
            if n and n.get("id"):
                live_by_vid[n["id"]] = n

    # verify scoped invariants
    changed_count = 0
    reductions = 0
    approved_at_target = 0
    excluded_unchanged = 0
    unchanged_still_unchanged = 0

    for r in all_rows:
        n = live_by_vid.get(r.shopify_variant_id) or {}
        lp = _f(n.get("price"))
        if lp is None or r.current_price is None:
            continue
        if abs(lp - r.current_price) > 0.005:
            changed_count += 1
        if lp + 0.005 < r.current_price:
            reductions += 1
        if r in approved and r.proposed_price is not None and abs(lp - r.proposed_price) < 0.005:
            approved_at_target += 1
        if r in excluded and abs(lp - r.current_price) < 0.005:
            excluded_unchanged += 1
        if r in unchanged and abs(lp - r.current_price) < 0.005:
            unchanged_still_unchanged += 1

    # weighted margins before and after (actual live)
    valid = [r for r in all_rows if not r.skip and r.unit_cost is not None and r.current_price is not None]
    cur_num = sum((r.current_margin_pct or 0) * (r.current_price or 0) for r in valid)
    cur_den = sum((r.current_price or 0) for r in valid)
    new_num = 0.0
    new_den = 0.0
    for r in valid:
        lp = _f((live_by_vid.get(r.shopify_variant_id) or {}).get("price"))
        if lp is None or r.unit_cost is None:
            continue
        m = _margin_pct(lp, r.unit_cost)
        if m is None:
            continue
        new_num += m * lp
        new_den += lp
    weighted_before = round(cur_num / cur_den, 2) if cur_den else None
    weighted_after = round(new_num / new_den, 2) if new_den else None

    # average/median actual price increase for updated rows
    updated_rows = [x for x in results if x["status"] == "updated"]
    incs = [(x["new_price"] - x["old_price"]) for x in updated_rows if x["new_price"] is not None and x["old_price"] is not None]
    avg_inc = round(statistics.mean(incs), 2) if incs else 0.0
    med_inc = round(statistics.median(incs), 2) if incs else 0.0

    # sales mix economics with actual live prices
    audit_by_vid = {r.shopify_variant_id: r for r in all_rows}
    units = 0
    gp_old = 0.0
    gp_new = 0.0
    ex_old = 0.0
    ex_new = 0.0
    cursor = None
    for _ in range(20):
        data = _gql(client, ORDERS_QUERY, {"cursor": cursor})
        block = data["orders"]
        for o in block.get("nodes") or []:
            for li in (o.get("lineItems") or {}).get("nodes") or []:
                studio = (((li.get("product") or {}).get("studio") or {}).get("value") or "")
                if not _contains_second_sight(studio):
                    continue
                q = int(li.get("quantity") or 0)
                vid = ((li.get("variant") or {}).get("id") or "")
                r = audit_by_vid.get(vid)
                if not r or q <= 0 or r.unit_cost is None or r.current_price is None:
                    continue
                live_price = _f((live_by_vid.get(vid) or {}).get("price"))
                if live_price is None:
                    continue
                units += q
                old_ex = (r.current_price / GST_RATE) * q
                new_ex = (live_price / GST_RATE) * q
                ex_old += old_ex
                ex_new += new_ex
                gp_old += (old_ex - (r.unit_cost * q))
                gp_new += (new_ex - (r.unit_cost * q))
        pi = block.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        time.sleep(0.1)
    additional_gp = gp_new - gp_old

    # explicit exception confirmation live prices
    exceptions_live = []
    for name in sorted(explicit_exception_names):
        match = next((r for r in excluded if r.product_title == name), None)
        if not match:
            exceptions_live.append({"title": name, "found_in_exclusions": False})
            continue
        lp = _f((live_by_vid.get(match.shopify_variant_id) or {}).get("price"))
        exceptions_live.append(
            {
                "title": name,
                "shopify_variant_id": match.shopify_variant_id,
                "current_price_audit": match.current_price,
                "live_price_post_run": lp,
                "unchanged": (lp is not None and match.current_price is not None and abs(lp - match.current_price) < 0.005),
            }
        )

    # write rollback artifact
    rb_path = Path(args.rollback_csv)
    rb_path.parent.mkdir(parents=True, exist_ok=True)
    with rb_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "product_title",
                "barcode",
                "shopify_product_id",
                "shopify_variant_id",
                "unit_cost",
                "old_price",
                "new_price",
                "old_gross_margin_pct",
                "new_gross_margin_pct",
                "timestamp_utc",
            ],
        )
        w.writeheader()
        for row in rollback_rows:
            w.writerow(row)

    summary = {
        "total_second_sight_variants": len(all_rows),
        "approved_variants": len(approved),
        "successfully_updated": len(updated_rows),
        "already_at_target_idempotent": len([x for x in results if x["status"] == "already_target"]),
        "skipped_live_conflict": len([x for x in results if x["status"] == "skipped_conflict"]),
        "failed": len([x for x in results if x["status"] == "failed"]),
        "excluded_gt_10": len(excluded),
        "unchanged_gte_28": len(unchanged),
        "current_weighted_margin_before_pct": weighted_before,
        "resulting_weighted_margin_after_pct": weighted_after,
        "average_price_increase_applied": avg_inc,
        "median_price_increase_applied": med_inc,
        "reconciliation": {
            "approved_now_at_target": approved_at_target,
            "excluded_remain_unchanged": excluded_unchanged,
            "unchanged_gte_28_remain_unchanged": unchanged_still_unchanged,
            "no_price_reductions": reductions == 0,
            "mutation_errors": len([x for x in results if x["status"] == "failed"]),
            "total_live_changes_vs_audit_baseline": changed_count,
        },
        "sales_mix_102_units_recalc": {
            "units_sold": units,
            "previous_gp": round(gp_old, 2),
            "modeled_gp_actual_live_prices": round(gp_new, 2),
            "additional_gp": round(additional_gp, 2),
            "additional_gp_pct": round((additional_gp / gp_old) * 100, 2) if gp_old > 0 else None,
            "resulting_sales_weighted_margin_pct": round((gp_new / ex_new) * 100, 2) if ex_new > 0 else None,
        },
        "explicit_exceptions_live_post_run": exceptions_live,
    }

    report = {
        "summary": summary,
        "results": results,
    }
    rp = Path(args.report_json)
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("SUMMARY|" + json.dumps(summary))
    print(f"ROLLBACK={rb_path}")
    print(f"REPORT={rp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
