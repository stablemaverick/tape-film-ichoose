"""
Apply approved Arrow repricing updates from dry-run audit.

Safety:
- Updates ONLY variants from the audit file.
- Updates ONLY when increase > 0 and <= max increase threshold.
- Live precheck must match audited current price (or already equal proposed).
- Uses Shopify productVariantsBulkUpdate price-only updates.
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.rules.pricing_rules import round_up_to_99

GST_RATE = 1.10

VARIANT_NODES_QUERY = """
query VariantNodes($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on ProductVariant {
      id
      price
      product { id title }
      inventoryItem { unitCost { amount currencyCode } }
    }
  }
}
"""

PRODUCT_VARIANTS_BULK_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id price }
    userErrors { field message }
  }
}
"""


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


@dataclass
class ApplyResult:
    shopify_product_id: str
    shopify_variant_id: str
    barcode: str
    title: str
    old_price: Optional[float]
    new_price: Optional[float]
    status: str
    reason: str
    timestamp_utc: str


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _b(v: Any) -> bool:
    return str(v).strip().lower() == "true"


def _money(v: Optional[float]) -> str:
    if v is None:
        return ""
    return f"{v:.2f}"


def _graphql_with_retry(client: ShopifyClient, query: str, variables: dict, tries: int = 6) -> dict:
    last: Exception | None = None
    for i in range(tries):
        try:
            return client.graphql(query, variables)
        except Exception as e:  # noqa: BLE001
            last = e
            msg = str(e)
            if ("THROTTLED" in msg or "429" in msg) and i < tries - 1:
                time.sleep(min(2 + i, 10))
                continue
            raise
    raise RuntimeError(f"GraphQL failed after retries: {last}")


def load_audit(csv_path: Path) -> list[AuditRow]:
    rows: list[AuditRow] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(
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
    return rows


def compute_margin_pct(price_inc_gst: float, cost_aud: float) -> Optional[float]:
    if not price_inc_gst or price_inc_gst <= 0:
        return None
    ex = price_inc_gst / GST_RATE
    return (ex - cost_aud) / ex * 100


def min_price_for_margin(cost_aud: float, margin_ratio: float) -> float:
    raw = (cost_aud / (1 - margin_ratio)) * GST_RATE
    p = round_up_to_99(raw)
    while compute_margin_pct(p, cost_aud) is not None and compute_margin_pct(p, cost_aud) < margin_ratio * 100 - 0.0001:
        p = round_up_to_99(p + 1.0)
    return p


def chunked(seq: list[str], size: int) -> list[list[str]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply approved Arrow repricing updates")
    parser.add_argument("--env", default=".env.prod")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument("--audit-csv", default="tmp/arrow_repricing_audit.csv")
    parser.add_argument("--sales-csv", default="tmp/arrow_order_lines_financial.csv")
    parser.add_argument("--max-increase", type=float, default=10.0)
    parser.add_argument("--rollback-csv", default="tmp/arrow_repricing_rollback.csv")
    parser.add_argument("--report-json", default="tmp/arrow_repricing_apply_report.json")
    args = parser.parse_args()

    load_dotenv(args.env, override=True)
    client = ShopifyClient(api_version=args.api_version)

    audit_rows = load_audit(Path(args.audit_csv))
    excluded = [
        r
        for r in audit_rows
        if r.would_change and not r.skip and r.proposed_price and r.current_price and (r.proposed_price - r.current_price) > args.max_increase
    ]
    approved = [
        r
        for r in audit_rows
        if r.would_change
        and not r.skip
        and r.proposed_price
        and r.current_price
        and 0 < (r.proposed_price - r.current_price) <= args.max_increase
    ]
    unchanged_ge_28 = [r for r in audit_rows if not r.skip and not r.would_change]

    print(f"excluded_gt_${args.max_increase:.2f}={len(excluded)}")
    for r in sorted(excluded, key=lambda x: x.price_increase or 0, reverse=True):
        print(
            "EXCLUDED|"
            f"title={r.product_title}|barcode={r.barcode}|cost={_money(r.unit_cost)}|"
            f"current={_money(r.current_price)}|cur_margin={r.current_margin_pct}|"
            f"proposed={_money(r.proposed_price)}|proposed_margin={r.proposed_margin_pct}|"
            f"inc={_money(r.price_increase)}|inc_pct={r.price_increase_pct}|"
            f"product_id={r.shopify_product_id}|variant_id={r.shopify_variant_id}"
        )

    # Precheck live states for approved set
    approved_by_vid = {r.shopify_variant_id: r for r in approved}
    live_map: dict[str, dict] = {}
    for id_batch in chunked(list(approved_by_vid.keys()), 50):
        data = _graphql_with_retry(client, VARIANT_NODES_QUERY, {"ids": id_batch})
        for node in data.get("nodes") or []:
            if node and node.get("id"):
                live_map[node["id"]] = node

    timestamp = datetime.now(timezone.utc).isoformat()
    results: list[ApplyResult] = []
    to_update_by_product: dict[str, list[tuple[AuditRow, float]]] = defaultdict(list)

    for vid, row in approved_by_vid.items():
        node = live_map.get(vid)
        if not node:
            results.append(
                ApplyResult(
                    row.shopify_product_id,
                    vid,
                    row.barcode,
                    row.product_title,
                    row.current_price,
                    row.proposed_price,
                    "skipped_conflict",
                    "variant_not_found_live",
                    timestamp,
                )
            )
            continue

        live_product_id = ((node.get("product") or {}).get("id")) or ""
        live_price = _f(node.get("price"))
        if live_product_id != row.shopify_product_id:
            results.append(
                ApplyResult(
                    row.shopify_product_id,
                    vid,
                    row.barcode,
                    row.product_title,
                    live_price,
                    row.proposed_price,
                    "skipped_conflict",
                    f"product_mismatch_live={live_product_id}",
                    timestamp,
                )
            )
            continue

        if live_price is not None and row.proposed_price is not None and abs(live_price - row.proposed_price) < 0.005:
            results.append(
                ApplyResult(
                    row.shopify_product_id,
                    vid,
                    row.barcode,
                    row.product_title,
                    live_price,
                    row.proposed_price,
                    "already_target",
                    "live_price_already_proposed",
                    timestamp,
                )
            )
            continue

        if live_price is None or row.current_price is None or abs(live_price - row.current_price) > 0.005:
            results.append(
                ApplyResult(
                    row.shopify_product_id,
                    vid,
                    row.barcode,
                    row.product_title,
                    live_price,
                    row.proposed_price,
                    "skipped_conflict",
                    "live_price_changed_since_audit",
                    timestamp,
                )
            )
            continue

        to_update_by_product[row.shopify_product_id].append((row, live_price))

    # Apply updates grouped by product
    for product_id, entries in to_update_by_product.items():
        variants_input = [{"id": e[0].shopify_variant_id, "price": _money(e[0].proposed_price)} for e in entries]
        try:
            out = _graphql_with_retry(
                client, PRODUCT_VARIANTS_BULK_UPDATE, {"productId": product_id, "variants": variants_input}
            )
            payload = (out.get("productVariantsBulkUpdate") or {})
            errors = payload.get("userErrors") or []
            updated_ids = {v.get("id") for v in (payload.get("productVariants") or []) if v.get("id")}

            if errors:
                err_txt = "; ".join(e.get("message", "unknown_error") for e in errors)
                for row, old_live in entries:
                    status = "updated" if row.shopify_variant_id in updated_ids else "failed"
                    reason = "partial_success" if status == "updated" else err_txt
                    results.append(
                        ApplyResult(
                            row.shopify_product_id,
                            row.shopify_variant_id,
                            row.barcode,
                            row.product_title,
                            old_live,
                            row.proposed_price,
                            status,
                            reason,
                            timestamp,
                        )
                    )
            else:
                for row, old_live in entries:
                    status = "updated" if row.shopify_variant_id in updated_ids else "failed"
                    results.append(
                        ApplyResult(
                            row.shopify_product_id,
                            row.shopify_variant_id,
                            row.barcode,
                            row.product_title,
                            old_live,
                            row.proposed_price,
                            status,
                            "ok" if status == "updated" else "mutation_missing_variant",
                            timestamp,
                        )
                    )
        except Exception as e:  # noqa: BLE001
            for row, old_live in entries:
                results.append(
                    ApplyResult(
                        row.shopify_product_id,
                        row.shopify_variant_id,
                        row.barcode,
                        row.product_title,
                        old_live,
                        row.proposed_price,
                        "failed",
                        f"mutation_exception:{e}",
                        timestamp,
                    )
                )

    # Add explicit non-applied statuses for excluded and unchanged rows
    for r in excluded:
        results.append(
            ApplyResult(
                r.shopify_product_id,
                r.shopify_variant_id,
                r.barcode,
                r.product_title,
                r.current_price,
                r.current_price,
                "excluded_gt_threshold",
                f"increase_gt_{args.max_increase:.2f}",
                timestamp,
            )
        )
    for r in unchanged_ge_28:
        results.append(
            ApplyResult(
                r.shopify_product_id,
                r.shopify_variant_id,
                r.barcode,
                r.product_title,
                r.current_price,
                r.current_price,
                "unchanged_gte_28",
                "already_at_or_above_floor",
                timestamp,
            )
        )

    # Reconciliation: reread all audit variants live
    all_vids = [r.shopify_variant_id for r in audit_rows]
    recon_live: dict[str, dict] = {}
    for id_batch in chunked(all_vids, 50):
        data = _graphql_with_retry(client, VARIANT_NODES_QUERY, {"ids": id_batch})
        for node in data.get("nodes") or []:
            if node and node.get("id"):
                recon_live[node["id"]] = node

    audit_by_vid = {r.shopify_variant_id: r for r in audit_rows}

    # Validate outcomes and compute resulting weighted margin
    intended_ok = 0
    excluded_unchanged = 0
    unchanged_ge_ok = 0
    price_reductions_detected = 0
    non_arrow_changed = 0
    total_changed_prices = 0

    for vid, row in audit_by_vid.items():
        live_node = recon_live.get(vid) or {}
        live_price = _f(live_node.get("price"))
        if live_price is None or row.current_price is None:
            continue
        if abs(live_price - row.current_price) > 0.005:
            total_changed_prices += 1
        if live_price + 0.005 < row.current_price:
            price_reductions_detected += 1
        if "arrow" not in (row.studio or "").casefold():
            non_arrow_changed += 1

        if row.would_change and not row.skip and row.price_increase and 0 < row.price_increase <= args.max_increase:
            if row.proposed_price is not None and abs(live_price - row.proposed_price) < 0.005:
                intended_ok += 1
        if row.would_change and not row.skip and row.price_increase and row.price_increase > args.max_increase:
            if abs(live_price - row.current_price) < 0.005:
                excluded_unchanged += 1
        if not row.would_change and not row.skip:
            if abs(live_price - row.current_price) < 0.005:
                unchanged_ge_ok += 1

    # weighted margin using live prices + audited costs
    eligible_rows = [r for r in audit_rows if not r.skip and r.unit_cost is not None]
    cur_weight_num = 0.0
    cur_weight_den = 0.0
    new_weight_num = 0.0
    new_weight_den = 0.0
    for r in eligible_rows:
        live = _f((recon_live.get(r.shopify_variant_id) or {}).get("price"))
        if r.current_price is not None and r.current_margin_pct is not None:
            cur_weight_num += r.current_margin_pct * r.current_price
            cur_weight_den += r.current_price
        if live is not None and r.unit_cost is not None:
            m = compute_margin_pct(live, r.unit_cost)
            if m is not None:
                new_weight_num += m * live
                new_weight_den += live
    current_weighted_margin = (cur_weight_num / cur_weight_den) if cur_weight_den else None
    resulting_weighted_margin = (new_weight_num / new_weight_den) if new_weight_den else None

    # recent sales mix uplift
    extra_gp_mix = 0.0
    sales_rows = []
    sales_path = Path(args.sales_csv)
    if sales_path.exists():
        with sales_path.open(newline="", encoding="utf-8") as f:
            sales_rows = list(csv.DictReader(f))
    sales_units_by_sku: dict[str, int] = defaultdict(int)
    for s in sales_rows:
        sku = (s.get("sku") or "").strip()
        try:
            q = int(s.get("quantity") or 0)
        except Exception:
            q = 0
        if sku:
            sales_units_by_sku[sku] += q

    for r in eligible_rows:
        if not r.sku:
            continue
        units = sales_units_by_sku.get(r.sku, 0)
        if units <= 0:
            continue
        live = _f((recon_live.get(r.shopify_variant_id) or {}).get("price"))
        if live is None or r.current_price is None or r.unit_cost is None:
            continue
        old_gp = (r.current_price / GST_RATE) - r.unit_cost
        new_gp = (live / GST_RATE) - r.unit_cost
        extra_gp_mix += (new_gp - old_gp) * units

    # rollback artifact for successful updates
    rollback_rows = [r for r in results if r.status == "updated"]
    rollback_path = Path(args.rollback_csv)
    rollback_path.parent.mkdir(parents=True, exist_ok=True)
    with rollback_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "shopify_product_id",
                "shopify_variant_id",
                "barcode",
                "title",
                "old_price",
                "new_price",
                "timestamp_utc",
            ],
        )
        w.writeheader()
        for r in rollback_rows:
            w.writerow(
                {
                    "shopify_product_id": r.shopify_product_id,
                    "shopify_variant_id": r.shopify_variant_id,
                    "barcode": r.barcode,
                    "title": r.title,
                    "old_price": _money(r.old_price),
                    "new_price": _money(r.new_price),
                    "timestamp_utc": r.timestamp_utc,
                }
            )

    # exception analysis >$10
    exception_rows = []
    for r in sorted(excluded, key=lambda x: x.price_increase or 0, reverse=True):
        if r.unit_cost is None:
            continue
        p25 = min_price_for_margin(r.unit_cost, 0.25)
        p26 = min_price_for_margin(r.unit_cost, 0.26)
        p27 = min_price_for_margin(r.unit_cost, 0.27)
        p28 = min_price_for_margin(r.unit_cost, 0.28)
        exception_rows.append(
            {
                "title": r.product_title,
                "barcode": r.barcode,
                "shopify_product_id": r.shopify_product_id,
                "shopify_variant_id": r.shopify_variant_id,
                "unit_cost": r.unit_cost,
                "current_price": r.current_price,
                "current_margin_pct": r.current_margin_pct,
                "proposed_price_28_floor": r.proposed_price,
                "proposed_margin_28_floor": r.proposed_margin_pct,
                "price_increase": r.price_increase,
                "price_increase_pct": r.price_increase_pct,
                "price_25_margin": p25,
                "margin_at_25_price": compute_margin_pct(p25, r.unit_cost),
                "price_26_margin": p26,
                "margin_at_26_price": compute_margin_pct(p26, r.unit_cost),
                "price_27_margin": p27,
                "margin_at_27_price": compute_margin_pct(p27, r.unit_cost),
                "price_28_margin_min": p28,
                "margin_at_28_price": compute_margin_pct(p28, r.unit_cost),
            }
        )

    summary = {
        "approved_variants": len(approved),
        "successfully_updated": len([r for r in results if r.status == "updated"]),
        "already_at_target_idempotent": len([r for r in results if r.status == "already_target"]),
        "skipped_live_price_conflict": len([r for r in results if r.status == "skipped_conflict"]),
        "failed": len([r for r in results if r.status == "failed"]),
        "excluded_gt_threshold": len(excluded),
        "unchanged_gte_28": len(unchanged_ge_28),
        "total_price_changes_live_vs_audit": total_changed_prices,
        "current_weighted_margin_pct": round(current_weighted_margin, 2) if current_weighted_margin is not None else None,
        "resulting_weighted_margin_pct": round(resulting_weighted_margin, 2) if resulting_weighted_margin is not None else None,
        "estimated_additional_gp_recent_30_mix": round(extra_gp_mix, 2),
        "reconciliation": {
            "intended_variants_now_at_proposed": intended_ok,
            "excluded_gt_threshold_remain_unchanged": excluded_unchanged,
            "unchanged_gte_28_remain_unchanged": unchanged_ge_ok,
            "no_price_reductions_detected": price_reductions_detected == 0,
            "non_arrow_variants_in_scope": non_arrow_changed,
        },
    }

    report = {
        "summary": summary,
        "excluded_gt_threshold": [asdict(r) for r in excluded],
        "apply_results": [asdict(r) for r in results],
        "exceptions_analysis": exception_rows,
        "artifacts": {
            "rollback_csv": str(rollback_path),
            "audit_csv": args.audit_csv,
        },
    }
    report_path = Path(args.report_json)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("APPLY_SUMMARY|" + json.dumps(summary))
    print(f"rollback_csv={rollback_path}")
    print(f"report_json={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
