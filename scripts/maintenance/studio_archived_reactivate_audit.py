#!/usr/bin/env python3
"""Read-only audit: archived Arrow / Second Sight / Criterion titles vs supplier stock and price.

Also rebuilds Region-B-scoped inventoryPolicy toggle candidates (active products).
Does not mutate Shopify.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

from app.rules.pricing_rules import (
    DEFAULT_GBP_AUD_RATE,
    DEFAULT_LANDED_COST_MARKUP,
    calculate_sale_price,
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    calculate_shopify_cost_aud,
)
from app.services.arrow_inventory_policy_sync_service import (
    ACTION_SET_CONTINUE,
    ACTION_SET_DENY,
    DEFAULT_ELIGIBLE_STUDIO_LABELS,
    PolicyDecision,
    build_decisions_for_products,
    iter_active_eligible_products,
)
from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client


GST = 1.10


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _margin_pct(price_inc: Optional[float], landed: Optional[float]) -> Optional[float]:
    if price_inc is None or landed is None or price_inc <= 0:
        return None
    ex = price_inc / GST
    return round(((ex - landed) / ex) * 100, 2)


def enrich_pricing(d: PolicyDecision, *, gbp_aud: float, markup: float) -> dict[str, Any]:
    cost = d.supplier_cost_gbp
    landed = calculate_shopify_cost_aud(cost, gbp_aud_rate=gbp_aud, landed_cost_markup=markup)
    old_price = calculate_sale_price(cost, gbp_aud_rate=gbp_aud, landed_cost_markup=markup)
    floor_price = calculate_sale_price_with_margin_floor_from_gbp_cost(
        cost, gbp_aud_rate=gbp_aud, landed_cost_markup=markup
    )
    shopify_price = d.shopify_price
    return {
        **asdict(d),
        "gbp_aud_rate": gbp_aud,
        "landed_cost_markup": markup,
        "landed_cost_aud": landed,
        "old_tiered_price": old_price,
        "floor_28_price": floor_price,
        "current_shopify_margin_pct": _margin_pct(shopify_price, landed),
        "floor_margin_pct": _margin_pct(floor_price, landed),
        "price_delta_shopify_vs_floor": (
            round(shopify_price - floor_price, 2)
            if shopify_price is not None and floor_price is not None
            else None
        ),
        "region_b": d.normalized_region == "B",
        "supplier_available": bool(d.available_suppliers),
        "reactivate_candidate": (
            (d.product_status or "").upper() == "ARCHIVED"
            and d.normalized_region == "B"
            and bool(d.available_suppliers)
        ),
        "toggle_candidate": (
            (d.product_status or "").upper() == "ACTIVE"
            and d.normalized_region == "B"
            and d.action in {ACTION_SET_CONTINUE, ACTION_SET_DENY}
        ),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for r in rows:
        for k in r:
            if k not in fields:
                fields.append(k)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Read-only archived + Region B candidate audit")
    p.add_argument("--env", default=".env.prod")
    p.add_argument("--archived-csv", default="tmp/studio_archived_supplier_pricing_audit.csv")
    p.add_argument("--archived-json", default="tmp/studio_archived_supplier_pricing_audit.json")
    p.add_argument("--approval-csv", default="tmp/studio_region_b_approval_candidates.csv")
    p.add_argument("--approval-json", default="tmp/studio_region_b_approval_candidates.json")
    p.add_argument("--toggle-csv", default="tmp/studio_supplier_inventory_policy_audit.csv")
    p.add_argument("--toggle-json", default="tmp/studio_supplier_inventory_policy_audit.json")
    args = p.parse_args(argv)

    env_path = Path(args.env)
    if not env_path.is_absolute():
        env_path = _REPO / env_path
    load_dotenv(env_path, override=True)

    gbp_aud = _f(os.getenv("GBP_AUD_RATE", str(DEFAULT_GBP_AUD_RATE))) or DEFAULT_GBP_AUD_RATE
    markup = _f(os.getenv("LANDED_COST_MARKUP", str(DEFAULT_LANDED_COST_MARKUP))) or DEFAULT_LANDED_COST_MARKUP

    shopify = ShopifyClient()
    sb = create_fresh_client(str(env_path))

    scanned_arch, archived_nodes = iter_active_eligible_products(
        shopify, product_query="status:archived", labels=DEFAULT_ELIGIBLE_STUDIO_LABELS
    )
    scanned_act, active_nodes = iter_active_eligible_products(
        shopify, product_query="status:active", labels=DEFAULT_ELIGIBLE_STUDIO_LABELS
    )

    archived_decisions = build_decisions_for_products(
        archived_nodes,
        supabase=sb,
        extra_continue_protections=True,
        require_region_b=True,
    )
    active_decisions = build_decisions_for_products(
        active_nodes,
        supabase=sb,
        extra_continue_protections=True,
        require_region_b=True,
    )

    archived_rows = [enrich_pricing(d, gbp_aud=gbp_aud, markup=markup) for d in archived_decisions]
    active_rows = [enrich_pricing(d, gbp_aud=gbp_aud, markup=markup) for d in active_decisions]

    archived_available = [r for r in archived_rows if r["supplier_available"]]
    reactivate = [r for r in archived_rows if r["reactivate_candidate"]]
    toggle = [r for r in active_rows if r["toggle_candidate"]]
    deny_to_continue = [r for r in toggle if r["action"] == ACTION_SET_CONTINUE]
    continue_to_deny = [r for r in toggle if r["action"] == ACTION_SET_DENY]
    region_a_active = [r for r in active_rows if r["normalized_region"] == "A"]
    region_a_archived = [r for r in archived_rows if r["normalized_region"] == "A"]

    write_csv(Path(args.archived_csv), archived_rows)
    write_csv(Path(args.toggle_csv), active_rows)
    approval_rows = [
        {**r, "approval_bucket": "make_active"} for r in reactivate
    ] + [
        {**r, "approval_bucket": "inventory_policy_toggle"} for r in toggle
    ]
    write_csv(Path(args.approval_csv), approval_rows)

    def _short(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "title": r["title"],
            "barcode": r["barcode"],
            "studio": r["studio"],
            "normalized_studio": r["normalized_studio"],
            "product_status": r["product_status"],
            "region_raw": r["region_raw"],
            "normalized_region": r["normalized_region"],
            "shopify_qty": r["shopify_qty"],
            "inventory_policy": r["inventory_policy"],
            "proposed_inventory_policy": r["proposed_inventory_policy"],
            "action": r["action"],
            "reason": r["reason"],
            "available_suppliers": r["available_suppliers"],
            "supplier_qty": r["supplier_qty"],
            "supplier_cost_gbp": r["supplier_cost_gbp"],
            "landed_cost_aud": r["landed_cost_aud"],
            "shopify_price": r["shopify_price"],
            "old_tiered_price": r["old_tiered_price"],
            "floor_28_price": r["floor_28_price"],
            "current_shopify_margin_pct": r["current_shopify_margin_pct"],
            "price_delta_shopify_vs_floor": r["price_delta_shopify_vs_floor"],
            "approval_bucket": r.get("approval_bucket"),
            "product_id": r["product_id"],
            "variant_id": r["variant_id"],
        }

    archived_payload = {
        "scanned_archived_products": scanned_arch,
        "archived_eligible_variants": len(archived_rows),
        "archived_with_supplier_available": len(archived_available),
        "reactivate_region_b_supplier_available": len(reactivate),
        "archived_region_a_excluded": len(region_a_archived),
        "gbp_aud_rate": gbp_aud,
        "landed_cost_markup": markup,
        "no_shopify_mutation": True,
        "archived_available_from_supplier": [_short(r) for r in archived_available],
        "reactivate_candidates": [_short({**r, "approval_bucket": "make_active"}) for r in reactivate],
        "all_archived_rows": archived_rows,
    }
    Path(args.archived_json).write_text(json.dumps(archived_payload, indent=2, default=str), encoding="utf-8")

    approval_payload = {
        "no_shopify_mutation": True,
        "require_region_b": True,
        "scanned_active_products": scanned_act,
        "active_eligible_variants": len(active_rows),
        "active_region_a_excluded": len(region_a_active),
        "toggle_candidates": len(toggle),
        "toggle_deny_to_continue": [_short({**r, "approval_bucket": "inventory_policy_toggle"}) for r in deny_to_continue],
        "toggle_continue_to_deny": [_short({**r, "approval_bucket": "inventory_policy_toggle"}) for r in continue_to_deny],
        "reactivate_candidates": [_short({**r, "approval_bucket": "make_active"}) for r in reactivate],
        "counts": {
            "make_active": len(reactivate),
            "inventory_policy_toggle": len(toggle),
            "deny_to_continue": len(deny_to_continue),
            "continue_to_deny": len(continue_to_deny),
        },
    }
    Path(args.approval_json).write_text(json.dumps(approval_payload, indent=2, default=str), encoding="utf-8")
    Path(args.toggle_json).write_text(
        json.dumps(
            {
                "summary": {
                    "scanned": scanned_act,
                    "variants": len(active_rows),
                    "region_b_toggle_candidates": len(toggle),
                    "region_a_excluded": len(region_a_active),
                    "set_continue": len(deny_to_continue),
                    "set_deny": len(continue_to_deny),
                    "dry_run": True,
                },
                "decisions": active_rows,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    print("ARCHIVED_SCANNED=" + str(scanned_arch))
    print("ARCHIVED_VARIANTS=" + str(len(archived_rows)))
    print("ARCHIVED_SUPPLIER_AVAILABLE=" + str(len(archived_available)))
    print("REACTIVATE_CANDIDATES=" + str(len(reactivate)))
    print("ACTIVE_SCANNED=" + str(scanned_act))
    print("TOGGLE_CANDIDATES=" + str(len(toggle)))
    print("TOGGLE_DENY_TO_CONTINUE=" + str(len(deny_to_continue)))
    print("TOGGLE_CONTINUE_TO_DENY=" + str(len(continue_to_deny)))
    print("ACTIVE_REGION_A_EXCLUDED=" + str(len(region_a_active)))
    print("ARCHIVED_REGION_A_EXCLUDED=" + str(len(region_a_archived)))
    print("NO_SHOPIFY_MUTATION=1")
    print("APPROVAL_JSON=" + args.approval_json)
    print("ARCHIVED_JSON=" + args.archived_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
