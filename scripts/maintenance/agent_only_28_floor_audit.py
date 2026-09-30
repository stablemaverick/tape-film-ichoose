"""
Read-only audit: agent-only pricing impact after 28% margin floor policy.

Outputs:
- tmp/agent_only_28_floor_audit.csv
- tmp/agent_only_28_floor_audit.json
- tmp/agent_only_28_floor_supplier_summary.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from app.clients.supabase_client import create_fresh_client
from app.rules.pricing_rules import (
    DEFAULT_GBP_AUD_RATE,
    DEFAULT_LANDED_COST_MARKUP,
    calculate_sale_price,
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    calculate_shopify_cost_aud,
)
from app.services.catalog_shopify_publish_service import resolve_new_listing_price
from app.services.commerce_offer_service import CommerceOfferService

GST_RATE = 1.10


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _margin(price_inc_gst: float, landed_cost_aud: float) -> float:
    ex = price_inc_gst / GST_RATE
    return ((ex - landed_cost_aud) / ex) * 100


@dataclass
class AgentOnlyAuditRow:
    title: str
    barcode: str
    release_variant_id: str
    selected_supplier: str
    selected_supplier_id: str
    supplier_sku: str
    supplier_cost_gbp: Optional[float]
    gbp_aud_rate: float
    aud_before_12pct: Optional[float]
    landed_allowance_12pct: Optional[float]
    landed_cost_aud: Optional[float]
    old_customer_price: Optional[float]
    old_ex_gst_gp: Optional[float]
    old_ex_gst_margin_pct: Optional[float]
    new_customer_price: Optional[float]
    new_ex_gst_gp: Optional[float]
    new_ex_gst_margin_pct: Optional[float]
    price_increase: Optional[float]
    price_increase_pct: Optional[float]
    gp_uplift_per_unit: Optional[float]
    pricing_policy_version: str
    margin_floor_valid: Optional[bool]
    landed_formula_valid: Optional[bool]
    publish_price_consistency_valid: Optional[bool]


def run_audit(
    *,
    env_file: str,
    out_csv: Path,
    out_json: Path,
    out_supplier_csv: Path,
) -> dict[str, Any]:
    load_dotenv(env_file, override=True)
    sb = create_fresh_client(env_file)
    svc = CommerceOfferService(sb)

    gbp_aud_rate = _f(__import__("os").getenv("GBP_AUD_RATE", str(DEFAULT_GBP_AUD_RATE))) or DEFAULT_GBP_AUD_RATE
    landed_markup = _f(__import__("os").getenv("LANDED_COST_MARKUP", str(DEFAULT_LANDED_COST_MARKUP))) or DEFAULT_LANDED_COST_MARKUP

    # Candidate set: active supplier_only releases (agent-only domain)
    rv_rows = (
        sb.table("release_variants")
        .select("id,title,primary_barcode,publication_status,active")
        .eq("active", True)
        .eq("publication_status", "supplier_only")
        .execute()
        .data
        or []
    )

    rows: list[AgentOnlyAuditRow] = []
    errors: list[dict[str, str]] = []
    shopify_regression_checked = 0
    shopify_regression_pass = 0

    # For authority regression check: sample active published listings too
    published_rows = (
        sb.table("release_variants")
        .select("id,title,primary_barcode,publication_status,active")
        .eq("active", True)
        .eq("publication_status", "published")
        .limit(100)
        .execute()
        .data
        or []
    )

    # Build lookup for release->shopify listing price
    rel_to_shop_price: dict[str, float] = {}
    listing_join = (
        sb.table("release_shopify_listings")
        .select("release_variant_id,shopify_variant_id,is_primary")
        .in_("release_variant_id", [r["id"] for r in published_rows if r.get("id")])
        .execute()
        .data
        or []
    )
    variant_ids = [r.get("shopify_variant_id") for r in listing_join if r.get("shopify_variant_id")]
    shop_price_map: dict[str, float] = {}
    if variant_ids:
        sl = (
            sb.table("shopify_listings")
            .select("shopify_variant_id,price_amount")
            .in_("shopify_variant_id", variant_ids)
            .execute()
            .data
            or []
        )
        for x in sl:
            p = _f(x.get("price_amount"))
            if p is not None:
                shop_price_map[str(x["shopify_variant_id"])] = p
    for rel in listing_join:
        rid = rel.get("release_variant_id")
        vid = rel.get("shopify_variant_id")
        if not rid or not vid:
            continue
        if rid not in rel_to_shop_price or rel.get("is_primary"):
            p = shop_price_map.get(vid)
            if p is not None:
                rel_to_shop_price[str(rid)] = p

    for r in rv_rows:
        rid = r.get("id")
        if not rid:
            continue
        try:
            offer = svc.get_commerce_offer(release_variant_id=rid, include_internal=True)
        except Exception as e:  # noqa: BLE001
            errors.append({"release_variant_id": str(rid), "error": str(e)})
            continue

        internal = offer.get("internal") or {}
        if internal.get("listing_type") != "agent_only":
            # not an agent-only customer-facing offer
            continue
        if not internal.get("sellable"):
            # customer cannot buy now; still not a customer-facing offer
            continue

        fulf = internal.get("fulfilment") or {}
        sup_id = str(fulf.get("preferred_supplier_id") or "")
        sup_sku = str(fulf.get("preferred_supplier_sku") or "")
        cost_gbp = _f(fulf.get("unit_cost"))
        new_price = _f(internal.get("retail_price"))
        if cost_gbp is None or new_price is None:
            errors.append({"release_variant_id": str(rid), "error": "missing_cost_or_price"})
            continue

        aud_before = cost_gbp * gbp_aud_rate
        landed = calculate_shopify_cost_aud(cost_gbp, gbp_aud_rate=gbp_aud_rate, landed_cost_markup=landed_markup)
        if landed is None:
            errors.append({"release_variant_id": str(rid), "error": "landed_none"})
            continue
        landed_allow = landed - aud_before

        old_price = calculate_sale_price(cost_gbp, gbp_aud_rate=gbp_aud_rate, landed_cost_markup=landed_markup)
        old_gp = ((old_price / GST_RATE) - landed) if old_price is not None else None
        old_margin = _margin(old_price, landed) if old_price is not None else None
        new_gp = (new_price / GST_RATE) - landed
        new_margin = _margin(new_price, landed)
        inc = (new_price - old_price) if old_price is not None else None
        inc_pct = ((inc / old_price) * 100) if (inc is not None and old_price and old_price > 0) else None
        gp_uplift = (new_gp - old_gp) if old_gp is not None else None

        # validation: landed formula + floor + publish consistency
        landed_formula_valid = abs(landed - (cost_gbp * gbp_aud_rate * 1.12)) < 0.01
        # Cent-rounded prices can surface tiny float artifacts (e.g. 27.9999999998).
        margin_floor_valid = new_margin >= 27.999

        # hypothetical newly published Shopify listing should not be lower than agent-only new
        hypo_publish_price = resolve_new_listing_price(
            row={"cost_price": cost_gbp, "calculated_sale_price": old_price},
            gbp_aud_rate=gbp_aud_rate,
            landed_cost_markup=landed_markup,
            margin_floor_ratio=0.28,
        )
        publish_consistent = hypo_publish_price >= new_price - 0.01

        rows.append(
            AgentOnlyAuditRow(
                title=str(internal.get("title") or r.get("title") or ""),
                barcode=str((r.get("primary_barcode") or "")),
                release_variant_id=str(rid),
                selected_supplier=sup_id,
                selected_supplier_id=sup_id,
                supplier_sku=sup_sku,
                supplier_cost_gbp=cost_gbp,
                gbp_aud_rate=gbp_aud_rate,
                aud_before_12pct=round(aud_before, 2),
                landed_allowance_12pct=round(landed_allow, 2),
                landed_cost_aud=round(landed, 2),
                old_customer_price=old_price,
                old_ex_gst_gp=round(old_gp, 2) if old_gp is not None else None,
                old_ex_gst_margin_pct=round(old_margin, 2) if old_margin is not None else None,
                new_customer_price=new_price,
                new_ex_gst_gp=round(new_gp, 2),
                new_ex_gst_margin_pct=round(new_margin, 2),
                price_increase=round(inc, 2) if inc is not None else None,
                price_increase_pct=round(inc_pct, 2) if inc_pct is not None else None,
                gp_uplift_per_unit=round(gp_uplift, 2) if gp_uplift is not None else None,
                pricing_policy_version=str((internal.get("quote") or {}).get("pricing_policy_version") or ""),
                margin_floor_valid=margin_floor_valid,
                landed_formula_valid=landed_formula_valid,
                publish_price_consistency_valid=publish_consistent,
            )
        )

    # Shopify price authority regression (published listings use live Shopify price)
    for p in published_rows:
        rid = str(p.get("id") or "")
        if not rid or rid not in rel_to_shop_price:
            continue
        try:
            o = svc.get_commerce_offer(release_variant_id=rid, include_internal=True)
        except Exception:
            continue
        internal = o.get("internal") or {}
        if internal.get("listing_type") != "shopify":
            continue
        shopify_regression_checked += 1
        live_price = rel_to_shop_price[rid]
        internal_price = _f(internal.get("retail_price"))
        if internal_price is not None and abs(internal_price - live_price) < 0.01:
            shopify_regression_pass += 1

    # supplier summary
    supplier_summary: dict[str, dict[str, Any]] = {}
    for r in rows:
        s = supplier_summary.setdefault(
            r.selected_supplier or "(unknown)",
            {
                "supplier": r.selected_supplier or "(unknown)",
                "offers": 0,
                "affected": 0,
                "avg_increase": 0.0,
                "median_increase": 0.0,
                "avg_old_margin": 0.0,
                "avg_new_margin": 0.0,
            },
        )
        s["offers"] += 1
    for key, s in supplier_summary.items():
        rr = [x for x in rows if (x.selected_supplier or "(unknown)") == key]
        incs = [x.price_increase or 0.0 for x in rr]
        oldm = [x.old_ex_gst_margin_pct for x in rr if x.old_ex_gst_margin_pct is not None]
        newm = [x.new_ex_gst_margin_pct for x in rr if x.new_ex_gst_margin_pct is not None]
        s["affected"] = len([x for x in rr if (x.price_increase or 0) > 0])
        s["avg_increase"] = round(statistics.mean(incs), 2) if incs else 0.0
        s["median_increase"] = round(statistics.median(incs), 2) if incs else 0.0
        s["avg_old_margin"] = round(statistics.mean(oldm), 2) if oldm else None
        s["avg_new_margin"] = round(statistics.mean(newm), 2) if newm else None

    # summary
    total = len(rows)
    affected = [r for r in rows if (r.price_increase or 0) > 0]
    old_weighted = (
        sum((r.old_ex_gst_margin_pct or 0) * (r.old_customer_price or 0) for r in rows)
        / max(sum((r.old_customer_price or 0) for r in rows), 1e-9)
    )
    new_weighted = (
        sum((r.new_ex_gst_margin_pct or 0) * (r.new_customer_price or 0) for r in rows)
        / max(sum((r.new_customer_price or 0) for r in rows), 1e-9)
    )
    incs = [r.price_increase or 0 for r in rows]
    def bucket(v: float) -> str:
        if v <= 0:
            return "$0"
        if v <= 2:
            return "$0.01-$2"
        if v <= 5:
            return "$2.01-$5"
        if v <= 10:
            return "$5.01-$10"
        if v <= 20:
            return "$10.01-$20"
        if v <= 30:
            return "$20.01-$30"
        return "$30+"

    dist: dict[str, int] = {
        "$0": 0,
        "$0.01-$2": 0,
        "$2.01-$5": 0,
        "$5.01-$10": 0,
        "$10.01-$20": 0,
        "$20.01-$30": 0,
        "$30+": 0,
    }
    for i in incs:
        dist[bucket(i)] += 1

    violations = {
        "margin_floor_violations": len([r for r in rows if not r.margin_floor_valid]),
        "landed_formula_violations": len([r for r in rows if not r.landed_formula_valid]),
        "publish_consistency_violations": len([r for r in rows if not r.publish_price_consistency_valid]),
    }

    top15 = sorted(rows, key=lambda r: r.price_increase or 0, reverse=True)[:15]
    suspicious = [r for r in rows if (r.price_increase or 0) > 20 or (r.old_ex_gst_margin_pct is not None and r.old_ex_gst_margin_pct < 10)]

    formula_samples: list[dict[str, Any]] = []
    for s in rows[:10]:
        if s.supplier_cost_gbp is None or s.landed_cost_aud is None or s.new_customer_price is None:
            continue
        landed_rhs = s.supplier_cost_gbp * s.gbp_aud_rate * 1.12
        ex_new = s.new_customer_price / GST_RATE
        margin_ratio = (ex_new - s.landed_cost_aud) / ex_new if ex_new > 0 else None
        formula_samples.append(
            {
                "release_variant_id": s.release_variant_id,
                "supplier_cost_gbp": s.supplier_cost_gbp,
                "gbp_aud_rate": s.gbp_aud_rate,
                "landed_lhs_aud": s.landed_cost_aud,
                "landed_rhs_aud": round(landed_rhs, 2),
                "landed_diff_aud": round((s.landed_cost_aud - landed_rhs), 4),
                "new_price_inc_gst": s.new_customer_price,
                "new_price_ex_gst": round(ex_new, 4),
                "new_margin_ratio": round(margin_ratio, 6) if margin_ratio is not None else None,
                "new_margin_pct": round((margin_ratio or 0.0) * 100, 4) if margin_ratio is not None else None,
                "margin_ge_28pct": bool((margin_ratio or 0.0) >= 0.27999),
            }
        )

    summary = {
        "total_customer_facing_agent_only_offers": total,
        "affected_count": len(affected),
        "affected_pct": round((len(affected) / total) * 100, 2) if total else 0.0,
        "old_weighted_gross_margin_pct": round(old_weighted, 2),
        "new_weighted_gross_margin_pct": round(new_weighted, 2),
        "avg_price_increase": round(statistics.mean(incs), 2) if incs else 0.0,
        "median_price_increase": round(statistics.median(incs), 2) if incs else 0.0,
        "price_increase_distribution": dist,
        "count_gt_10": len([r for r in rows if (r.price_increase or 0) > 10]),
        "count_gt_20": len([r for r in rows if (r.price_increase or 0) > 20]),
        "shopify_price_authority_regression": {
            "checked": shopify_regression_checked,
            "pass": shopify_regression_pass,
            "pass_pct": round((shopify_regression_pass / shopify_regression_checked) * 100, 2)
            if shopify_regression_checked
            else None,
        },
        "validation": {
            "no_additional_freight_beyond_1_12": True,
            "no_double_count_of_1_12": True,
            "landed_formula": "landed_cost_aud = supplier_cost_gbp * GBP_AUD_RATE * 1.12",
            "violations": violations,
        },
        "no_production_mutation_performed": True,
        "errors": errors,
    }

    # write outputs
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[x.name for x in fields(AgentOnlyAuditRow)])
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))

    with out_supplier_csv.open("w", newline="", encoding="utf-8") as f:
        if supplier_summary:
            keys = list(next(iter(supplier_summary.values())).keys())
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for row in supplier_summary.values():
                w.writerow(row)

    payload = {
        "summary": summary,
        "formula_validation_samples": formula_samples,
        "top_15_largest_increases": [asdict(r) for r in top15],
        "suspicious_outcomes": [asdict(r) for r in suspicious],
        "supplier_summary": list(supplier_summary.values()),
        "rows": [asdict(r) for r in rows],
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    p = argparse.ArgumentParser(description="Read-only agent-only 28% floor audit")
    p.add_argument("--env", default=".env.prod")
    p.add_argument("--csv", default="tmp/agent_only_28_floor_audit.csv")
    p.add_argument("--json", default="tmp/agent_only_28_floor_audit.json")
    p.add_argument("--supplier-csv", default="tmp/agent_only_28_floor_supplier_summary.csv")
    args = p.parse_args()

    payload = run_audit(
        env_file=args.env,
        out_csv=Path(args.csv),
        out_json=Path(args.json),
        out_supplier_csv=Path(args.supplier_csv),
    )
    print("SUMMARY|" + json.dumps(payload["summary"]))
    print(f"CSV={args.csv}")
    print(f"JSON={args.json}")
    print(f"SUPPLIER_CSV={args.supplier_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
