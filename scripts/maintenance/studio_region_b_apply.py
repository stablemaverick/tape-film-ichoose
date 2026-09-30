#!/usr/bin/env python3
"""
Region B Arrow / Second Sight / Criterion apply:

1. Dry-run cost + floor + policy + reactivation validation.
2. Optional --apply of independently safe rows only.

Shopify mutations (apply only):
- inventoryPolicy (DENY→CONTINUE)
- product status (ARCHIVED→ACTIVE)
- variant price (raise to 28% floor; never lower)
- inventoryItem.cost (landed AUD, when material)

Never mutates Region A / Region Free / unknown region.
Never changes inventory quantity.
Never lowers price.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Optional

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client
from app.helpers.text_helpers import clean_text
from app.rules.pricing_rules import (
    DEFAULT_GBP_AUD_RATE,
    DEFAULT_LANDED_COST_MARKUP,
    DEFAULT_MARGIN_FLOOR_RATIO,
    GST_RATE,
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    calculate_shopify_cost_aud,
)
from app.services.arrow_inventory_policy_sync_service import (
    ACTION_SET_CONTINUE,
    WHOLESALE_SUPPLIER_IDS,
    _eval_offers,
    _load_supplier_context,
    is_region_b,
    normalize_region,
    resolve_variant_region,
    supplier_is_usable,
)
from app.services.commerce_offer_service import (
    CommerceOfferService,
    assert_public_offer_has_no_supplier_leak,
)
from app.services.stock_availability_service import pick_preferred_supplier

GST_DEC = Decimal(str(GST_RATE))
FLOOR_DEC = Decimal(str(DEFAULT_MARGIN_FLOOR_RATIO))
MATERIAL_ABS_AUD = Decimal("1.00")
MATERIAL_PCT = Decimal("0.05")

APPROVED_TOGGLE_BARCODES = {
    "5028836041672",  # The Witch 4K
    "5028836042563",  # When Evil Lurks 4K
    "5028836041535",  # Lake Mungo
    "5028836042761",  # Re-Animator 4K
    "5028836042860",  # Insomnia 1997 LE 4K
    "5028836042020",  # Mean Streets
    "5028836041757",  # Monster
    "5060952897542",  # Mikey and Nicky UK
    "5061088921248",  # Pee Wees UK
    "5061088922276",  # Yi Yi 4K
    "5061088922283",  # The Dead 4K
    "5060952892523",  # Risky Business 4K
    "5061088921781",  # The Mirror
    "5050629947038",  # Before Trilogy
}
APPROVED_REACTIVATE_BARCODES = {
    "5050629184334",  # Moonrise Kingdom
    "5050629306033",  # Infernal Affairs
    "5027035026466",  # Psycho Story Continues
    "5028836042433",  # Scanners
    "5028836041825",  # TCM 1974
    "5028836042594",  # Innkeepers
    "5028836042266",  # You're Next
    "5028836042662",  # Maxxxine
    "5028836042631",  # Pearl
    "5027035027661",  # Shawscope Vol 4
}

LOCATIONS_QUERY = """
query TapeLocations {
  locations(first: 20, includeLegacy: true) {
    nodes { id isActive fulfillsOnlineOrders }
  }
}
"""

VARIANT_QUERY = """
query RegionBCandidateVariant($id: ID!, $locId: ID!) {
  node(id: $id) {
    ... on ProductVariant {
      id
      title
      sku
      barcode
      price
      compareAtPrice
      inventoryPolicy
      inventoryQuantity
      product {
        id
        title
        handle
        status
        studio: metafield(namespace: "custom", key: "studio") { value }
        region: metafield(namespace: "custom", key: "region") { value }
      }
      region: metafield(namespace: "custom", key: "region") { value }
      inventoryItem {
        id
        unitCost { amount currencyCode }
        inventoryLevel(locationId: $locId) {
          quantities(names: ["available", "on_hand", "committed", "incoming", "reserved"]) {
            name
            quantity
          }
        }
        inventoryLevels(first: 10) {
          nodes {
            location { id }
            quantities(names: ["available", "on_hand", "committed", "incoming", "reserved"]) {
              name
              quantity
            }
          }
        }
      }
    }
  }
}
"""

VARIANT_BULK_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id price inventoryPolicy }
    userErrors { field message }
  }
}
"""

INVENTORY_ITEM_UPDATE = """
mutation InventoryItemUpdate($id: ID!, $input: InventoryItemInput!) {
  inventoryItemUpdate(id: $id, input: $input) {
    inventoryItem { id unitCost { amount currencyCode } }
    userErrors { field message }
  }
}
"""

PRODUCT_CHANGE_STATUS = """
mutation ProductChangeStatus($productId: ID!, $status: ProductStatus!) {
  productChangeStatus(productId: $productId, status: $status) {
    product { id status }
    userErrors { field message }
  }
}
"""


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _qty_map(quantities: list[dict[str, Any]] | None) -> dict[str, int]:
    out: dict[str, int] = {}
    for q in quantities or []:
        name = str(q.get("name") or "")
        try:
            out[name] = int(q.get("quantity") or 0)
        except (TypeError, ValueError):
            out[name] = 0
    return out


def exact_margin_ok(price_inc: Optional[float], landed: Optional[float]) -> bool:
    if price_inc is None or landed is None or price_inc <= 0:
        return False
    price = Decimal(str(price_inc))
    landed_d = Decimal(str(landed)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    ex = price / GST_DEC
    if ex <= 0:
        return False
    return ((ex - landed_d) / ex) >= FLOOR_DEC


def classify_cost_materiality(shopify_cost: Optional[float], landed: Optional[float]) -> str:
    if shopify_cost is None or landed is None:
        return "unable_to_validate"
    diff = Decimal(str(landed)) - Decimal(str(shopify_cost))
    abs_diff = abs(diff)
    pct = abs_diff / Decimal(str(shopify_cost)) if Decimal(str(shopify_cost)) != 0 else Decimal("1")
    if abs_diff < Decimal("0.005"):
        return "MATCH"
    if abs_diff < MATERIAL_ABS_AUD and pct < MATERIAL_PCT:
        return "immaterial_difference"
    if abs_diff >= MATERIAL_ABS_AUD or pct >= MATERIAL_PCT:
        direction = "CURRENT_COST_LOWER" if diff > 0 else "CURRENT_COST_HIGHER"
        return f"MATERIAL_COST_CHANGE|{direction}"
    return "immaterial_difference"


def offer_age_hours(observed_at: str, now: datetime) -> Optional[float]:
    raw = (observed_at or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return round((now - dt).total_seconds() / 3600.0, 2)
    except ValueError:
        return None


def to_preferred_pool(evaluated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pool = []
    for e in evaluated:
        sid = str(e.get("supplier_id") or "").strip().lower()
        if sid not in WHOLESALE_SUPPLIER_IDS:
            continue
        pool.append(
            {
                "supplier_id": sid,
                "supplier": e.get("supplier"),
                "supplier_sku": e.get("supplier_sku"),
                "availability_status": e.get("api_status"),
                "is_stale": str(e.get("freshness") or "") == "stale",
                "unit_cost": e.get("unit_cost"),
                "qty": e.get("qty"),
                "freshness": e.get("freshness"),
                "observed_at": e.get("observed_at"),
            }
        )
    return pool


def load_approved_ids(approval_json: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    payload = json.loads(approval_json.read_text(encoding="utf-8"))
    toggle = [r for r in payload.get("toggle_deny_to_continue") or [] if str(r.get("barcode") or "") in APPROVED_TOGGLE_BARCODES]
    reactivate = [r for r in payload.get("reactivate_candidates") or [] if str(r.get("barcode") or "") in APPROVED_REACTIVATE_BARCODES]
    return toggle, reactivate


def fetch_live_variant(client: ShopifyClient, variant_id: str, location_id: str) -> dict[str, Any]:
    data = client.graphql(VARIANT_QUERY, {"id": variant_id, "locId": location_id})
    node = data.get("node") or {}
    if not node or not node.get("id"):
        raise RuntimeError(f"variant not found: {variant_id}")
    return node


def build_row(
    *,
    approved: dict[str, Any],
    live: dict[str, Any],
    ctx: dict[str, Any],
    gbp_aud: float,
    markup: float,
    now: datetime,
    bucket: str,
) -> dict[str, Any]:
    product = live.get("product") or {}
    inv = live.get("inventoryItem") or {}
    unit_cost_obj = inv.get("unitCost") or {}
    shopify_cost = _f(unit_cost_obj.get("amount"))
    shopify_cost_ccy = unit_cost_obj.get("currencyCode") or ""
    loc_qty = _qty_map(((inv.get("inventoryLevel") or {}).get("quantities")))
    all_levels = []
    for lvl in ((inv.get("inventoryLevels") or {}).get("nodes") or []):
        loc = lvl.get("location") or {}
        qm = _qty_map(lvl.get("quantities"))
        all_levels.append({"location_id": loc.get("id"), **qm})

    product_region = clean_text((product.get("region") or {}).get("value")) or ""
    variant_region = clean_text((live.get("region") or {}).get("value")) or ""
    region_code = resolve_variant_region(product_region=product_region, variant_region=variant_region)
    region_raw = variant_region or product_region
    status = (clean_text(product.get("status")) or "").upper()
    policy = (clean_text(live.get("inventoryPolicy")) or "").upper()
    barcode = clean_text(live.get("barcode")) or clean_text(approved.get("barcode")) or ""
    vid = live.get("id") or ""
    try:
        shopify_qty = int(live["inventoryQuantity"]) if live.get("inventoryQuantity") is not None else None
    except (TypeError, ValueError):
        shopify_qty = None
    tape_available = loc_qty.get("available")
    tape_on_hand = loc_qty.get("on_hand")
    if tape_available is None:
        tape_available = shopify_qty

    rid = ctx["rsl"].get(vid)
    offers = list(ctx["offers_by_release"].get(str(rid), [])) if rid else []
    resolution = "release_shopify_listings" if offers else ""
    if not offers and barcode:
        offers = list(ctx["offers_by_barcode"].get(barcode, []))
        if offers:
            resolution = "supplier_offers_by_barcode"
    evaluated = _eval_offers(offers, suppliers=ctx["suppliers"], now=now)
    usable = supplier_is_usable(evaluated)
    pool = to_preferred_pool(evaluated)
    preferred = pick_preferred_supplier(pool) if pool else None
    selected = preferred if preferred and preferred.get("availability_status") == "available" and not preferred.get("is_stale") else None
    if selected is None and usable:
        selected = next((p for p in pool if p.get("availability_status") == "available" and not p.get("is_stale")), None)

    cost_gbp = _f((selected or {}).get("unit_cost"))
    landed = calculate_shopify_cost_aud(cost_gbp, gbp_aud_rate=gbp_aud, landed_cost_markup=markup)
    freight = None
    if cost_gbp is not None:
        aud_before = round(cost_gbp * gbp_aud, 2)
        freight = round((landed or 0) - aud_before, 2) if landed is not None else None
    floor = calculate_sale_price_with_margin_floor_from_gbp_cost(
        cost_gbp, gbp_aud_rate=gbp_aud, landed_cost_markup=markup
    )
    current_price = _f(live.get("price"))
    proposed_price = current_price
    if floor is not None and (current_price is None or current_price + 1e-9 < floor):
        proposed_price = floor

    cost_class = classify_cost_materiality(shopify_cost, landed)
    exclude: list[str] = []
    if region_code != "B":
        exclude.append("region_a_us_excluded" if region_code == "A" else "region_not_b")

    if barcode not in APPROVED_TOGGLE_BARCODES | APPROVED_REACTIVATE_BARCODES:
        exclude.append("not_in_approved_barcode_set")
    if not selected:
        exclude.append("no_current_usable_supplier")
    elif not usable:
        exclude.append("supplier_not_usable")
    if selected and (selected.get("qty") is None or int(selected.get("qty") or 0) <= 0) and str(selected.get("freshness") or "") == "unknown":
        exclude.append("supplier_qty_not_positive")
    if selected and str(selected.get("freshness") or "") == "stale":
        exclude.append("stale_supplier_offer")
    if cost_gbp is None or landed is None or floor is None:
        exclude.append("cost_or_landed_or_floor_failed")
    if shopify_cost_ccy and shopify_cost_ccy.upper() not in {"AUD", ""}:
        exclude.append("shopify_cost_not_aud")
    if not rid and not barcode:
        exclude.append("ambiguous_identity")
    if bucket == "toggle":
        if status != "ACTIVE":
            exclude.append("not_active")
        if policy != "DENY":
            exclude.append("policy_not_deny")
        if (tape_available or 0) != 0 and (shopify_qty or 0) != 0:
            exclude.append("tape_qty_not_zero")
    if bucket == "reactivate":
        if status != "ARCHIVED":
            exclude.append("not_archived")

    infernal_note = ""
    if barcode == "5050629306033":
        on_hand = int(loc_qty.get("on_hand") or 0)
        available = int(loc_qty.get("available") or 0)
        committed = int(loc_qty.get("committed") or 0)
        incoming = int(loc_qty.get("incoming") or 0)
        infernal_note = json.dumps(
            {
                "canonical_location": loc_qty,
                "all_levels": all_levels,
                "shopify_qty": shopify_qty,
            }
        )
        genuine = on_hand > 0 or available > 0
        mismatched = (shopify_qty or 0) != available and (shopify_qty or 0) != on_hand
        if (shopify_qty or 0) > 0 and not genuine and mismatched:
            exclude.append("infernal_affairs_inventory_unreconciled")
            infernal_note += "|HOLD_unreconciled"
        elif genuine:
            infernal_note += f"|PRESERVE_genuine_tape on_hand={on_hand} available={available} committed={committed} incoming={incoming}"

    proposed_policy = policy
    proposed_status = status
    proposed_cost = shopify_cost
    actions: list[str] = []
    if not exclude:
        if bucket == "toggle":
            proposed_policy = "CONTINUE"
            actions.append("SET_CONTINUE")
        if bucket == "reactivate":
            proposed_status = "ACTIVE"
            actions.append("SET_ACTIVE")
            if (tape_available or 0) == 0 and (shopify_qty or 0) == 0:
                proposed_policy = "CONTINUE"
                actions.append("SET_CONTINUE")
            elif policy != "CONTINUE" and usable:
                proposed_policy = "CONTINUE"
                actions.append("SET_CONTINUE")
        if proposed_price is not None and current_price is not None and proposed_price > current_price + 1e-9:
            actions.append("RAISE_PRICE")
        elif proposed_price is not None and current_price is None:
            actions.append("RAISE_PRICE")
        if cost_class.startswith("MATERIAL_COST_CHANGE") and landed is not None:
            proposed_cost = landed
            actions.append("UPDATE_LANDED_COST")
        elif cost_class == "unable_to_validate":
            exclude.append("unable_to_validate_shopify_cost")
            actions = []
            proposed_policy = policy
            proposed_status = status
            proposed_price = current_price
            proposed_cost = shopify_cost

    # Final region abort for this row
    if region_code != "B":
        actions = []
        proposed_policy = policy
        proposed_status = status
        proposed_price = current_price
        proposed_cost = shopify_cost

    mutate = bool(actions) and not exclude and region_code == "B"
    age = offer_age_hours(str((selected or {}).get("observed_at") or ""), now)
    diff_aud = None
    diff_pct = None
    if shopify_cost is not None and landed is not None:
        diff_aud = round(landed - shopify_cost, 2)
        diff_pct = round(((landed - shopify_cost) / shopify_cost) * 100, 2) if shopify_cost else None

    return {
        "bucket": bucket,
        "title": product.get("title") or approved.get("title"),
        "variant_id": vid,
        "product_id": product.get("id"),
        "inventory_item_id": inv.get("id") or "",
        "barcode": barcode,
        "studio": clean_text((product.get("studio") or {}).get("value")) or approved.get("studio"),
        "region": region_raw,
        "normalized_region": region_code,
        "current_status": status,
        "current_inventory_policy": policy,
        "TAPE_qty": shopify_qty,
        "tape_available": tape_available,
        "tape_on_hand": tape_on_hand,
        "tape_committed": loc_qty.get("committed"),
        "tape_incoming": loc_qty.get("incoming"),
        "tape_levels_json": json.dumps(all_levels),
        "selected_supplier": (selected or {}).get("supplier_id") or "",
        "supplier_sku": (selected or {}).get("supplier_sku") or "",
        "supplier_qty": (selected or {}).get("qty"),
        "supplier_GBP_cost": cost_gbp,
        "supplier_freshness": (selected or {}).get("freshness") or "",
        "supplier_offer_age_hours": age,
        "all_supplier_states": "; ".join(
            f"{e.get('supplier_id')}:{e.get('api_status')}:{e.get('freshness')}:qty={e.get('qty')}:gbp={e.get('unit_cost')}"
            for e in evaluated
        ),
        "gbp_aud_rate": gbp_aud,
        "landed_markup": markup,
        "freight_AUD": freight,
        "calculated_landed_cost_AUD": landed,
        "current_Shopify_cost_AUD": shopify_cost,
        "shopify_cost_currency": shopify_cost_ccy,
        "cost_difference_AUD": diff_aud,
        "cost_difference_pct": diff_pct,
        "cost_materiality": cost_class,
        "current_price": current_price,
        "new_28pct_floor": floor,
        "proposed_price": proposed_price if mutate or (not exclude and proposed_price) else current_price,
        "proposed_cost": proposed_cost if mutate else shopify_cost,
        "proposed_status": proposed_status if mutate else status,
        "proposed_inventory_policy": proposed_policy if mutate else policy,
        "exact_margin_ok_at_proposed": exact_margin_ok(proposed_price if mutate else current_price, landed),
        "exact_margin_ok_at_current": exact_margin_ok(current_price, landed),
        "action": "|".join(actions) if mutate else "EXCLUDE",
        "reason": "ok" if mutate else ";".join(exclude) or "no_action",
        "mutate": mutate,
        "resolution_method": resolution,
        "release_variant_id": rid or "",
        "infernal_note": infernal_note,
        "compare_at_unchanged": live.get("compareAtPrice"),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for r in rows:
        for k in r:
            if k not in fields:
                fields.append(k)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields or ["empty"])
        w.writeheader()
        for r in rows:
            w.writerow(r)


def graphql_ok(client: ShopifyClient, query: str, variables: dict[str, Any], key: str) -> dict[str, Any]:
    data = client.graphql(query, variables)
    block = data.get(key) or {}
    errs = block.get("userErrors") or []
    if errs:
        raise RuntimeError(str(errs))
    return block


def apply_row(client: ShopifyClient, row: dict[str, Any], *, sleep_s: float = 0.12) -> dict[str, str]:
    result = {"status_ok": "", "variant_ok": "", "cost_ok": "", "error": ""}
    try:
        if "SET_ACTIVE" in row["action"]:
            graphql_ok(
                client,
                PRODUCT_CHANGE_STATUS,
                {"productId": row["product_id"], "status": "ACTIVE"},
                "productChangeStatus",
            )
            result["status_ok"] = "ACTIVE"
            time.sleep(sleep_s)
        variant_input: dict[str, Any] = {"id": row["variant_id"]}
        changed = False
        if "SET_CONTINUE" in row["action"]:
            variant_input["inventoryPolicy"] = "CONTINUE"
            changed = True
        if "RAISE_PRICE" in row["action"]:
            variant_input["price"] = f"{float(row['proposed_price']):.2f}"
            changed = True
        if changed:
            graphql_ok(
                client,
                VARIANT_BULK_UPDATE,
                {"productId": row["product_id"], "variants": [variant_input]},
                "productVariantsBulkUpdate",
            )
            result["variant_ok"] = "ok"
            time.sleep(sleep_s)
        if "UPDATE_LANDED_COST" in row["action"] and row.get("inventory_item_id"):
            graphql_ok(
                client,
                INVENTORY_ITEM_UPDATE,
                {
                    "id": row["inventory_item_id"],
                    "input": {"cost": float(f"{float(row['proposed_cost']):.2f}")},
                },
                "inventoryItemUpdate",
            )
            result["cost_ok"] = "ok"
            time.sleep(sleep_s)
    except Exception as exc:
        result["error"] = str(exc)
    return result


def post_validate(client: ShopifyClient, row: dict[str, Any], location_id: str, landed: Optional[float]) -> dict[str, Any]:
    live = fetch_live_variant(client, row["variant_id"], location_id)
    product = live.get("product") or {}
    region_code = resolve_variant_region(
        product_region=clean_text((product.get("region") or {}).get("value")) or "",
        variant_region=clean_text((live.get("region") or {}).get("value")) or "",
    )
    price = _f(live.get("price"))
    cost = _f(((live.get("inventoryItem") or {}).get("unitCost") or {}).get("amount"))
    return {
        "live_region": region_code,
        "live_status": (product.get("status") or "").upper(),
        "live_policy": (live.get("inventoryPolicy") or "").upper(),
        "live_price": price,
        "live_cost": cost,
        "live_qty": live.get("inventoryQuantity"),
        "live_margin_ok": exact_margin_ok(price, landed if landed is not None else cost),
        "region_b": region_code == "B",
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Region B cost/policy/reactivate dry-run and apply")
    p.add_argument("--env", default=".env.prod")
    p.add_argument("--approval-json", default="tmp/studio_region_b_approval_candidates.json")
    p.add_argument("--apply", action="store_true", help="Mutate Shopify for independently safe rows")
    p.add_argument("--validation-csv", default="tmp/studio_region_b_cost_validation.csv")
    p.add_argument("--apply-csv", default="tmp/studio_region_b_apply_results.csv")
    p.add_argument("--exclusions-csv", default="tmp/studio_region_b_exclusions.csv")
    args = p.parse_args(argv)

    env_path = Path(args.env)
    if not env_path.is_absolute():
        env_path = _REPO / env_path
    load_dotenv(env_path, override=True)

    gbp_aud = _f(os.getenv("GBP_AUD_RATE", str(DEFAULT_GBP_AUD_RATE))) or DEFAULT_GBP_AUD_RATE
    markup = _f(os.getenv("LANDED_COST_MARKUP", str(DEFAULT_LANDED_COST_MARKUP))) or DEFAULT_LANDED_COST_MARKUP
    client = ShopifyClient()
    location_id = clean_text(os.getenv("SHOPIFY_INVENTORY_LOCATION_ID")) or ""
    if not location_id:
        try:
            locs = (client.graphql(LOCATIONS_QUERY, {}) or {}).get("locations", {}).get("nodes") or []
            online = [x for x in locs if x.get("fulfillsOnlineOrders") and x.get("isActive")]
            chosen = (online[0] if len(online) == 1 else None) or (locs[0] if len(locs) == 1 else None)
        except Exception:
            chosen = None
        if not chosen:
            location_id = "gid://shopify/Location/78213775584"
            print(f"LOCATION_FALLBACK_DOCS|{location_id}")
        else:
            location_id = str(chosen["id"])
            print(f"LOCATION_RESOLVED|{location_id}")

    toggle, reactivate = load_approved_ids(Path(args.approval_json))
    sb = create_fresh_client(str(env_path))
    now = datetime.now(timezone.utc)

    approved_all = [(r, "toggle") for r in toggle] + [(r, "reactivate") for r in reactivate]
    variant_ids = [str(r.get("variant_id") or "") for r, _ in approved_all if r.get("variant_id")]
    barcodes = [str(r.get("barcode") or "") for r, _ in approved_all if r.get("barcode")]
    ctx = _load_supplier_context(sb, variant_ids=variant_ids, barcodes=barcodes)

    rows: list[dict[str, Any]] = []
    for approved, bucket in approved_all:
        live = fetch_live_variant(client, str(approved["variant_id"]), location_id)
        time.sleep(0.08)
        rows.append(
            build_row(
                approved=approved,
                live=live,
                ctx=ctx,
                gbp_aud=gbp_aud,
                markup=markup,
                now=now,
                bucket=bucket,
            )
        )

    region_counts = {"B": 0, "A": 0, "Free": 0, "missing_unknown": 0}
    for r in rows:
        code = r["normalized_region"]
        raw = (r["region"] or "").casefold()
        if code == "B":
            region_counts["B"] += 1
        elif code == "A":
            region_counts["A"] += 1
        elif "free" in raw:
            region_counts["Free"] += 1
        else:
            region_counts["missing_unknown"] += 1

    mutate_rows = [r for r in rows if r["mutate"]]
    excluded = [r for r in rows if not r["mutate"]]
    non_b_mutate = [r for r in mutate_rows if r["normalized_region"] != "B"]
    abort = bool(non_b_mutate)
    if abort:
        for r in mutate_rows:
            r["mutate"] = False
            r["action"] = "ABORT_NON_REGION_B"
        mutate_rows = []

    write_csv(Path(args.validation_csv), rows)
    write_csv(Path(args.exclusions_csv), excluded)

    apply_results: list[dict[str, Any]] = []
    if args.apply and not abort:
        for r in mutate_rows:
            applied = apply_row(client, r)
            time.sleep(0.15)
            post = post_validate(client, r, location_id, r.get("calculated_landed_cost_AUD"))
            apply_results.append({**r, **applied, **post, "applied": not applied.get("error")})
            time.sleep(0.08)
        # commerce sample
        commerce = CommerceOfferService(sb)
        for r in apply_results:
            if r.get("error") or not r.get("release_variant_id"):
                continue
            try:
                offer = commerce.get_commerce_offer(release_variant_id=r["release_variant_id"], include_internal=True)
                pub = offer.get("public") or {}
                assert_public_offer_has_no_supplier_leak(pub)
                r["commerce_price"] = pub.get("price")
                r["commerce_availability"] = pub.get("availability")
                r["commerce_listing_type"] = (offer.get("internal") or {}).get("listing_type")
                r["commerce_public_safe"] = True
            except Exception as exc:
                r["commerce_error"] = str(exc)
                r["commerce_public_safe"] = False
        write_csv(Path(args.apply_csv), apply_results)
    else:
        write_csv(Path(args.apply_csv), [])

    summary = {
        "dry_run": not args.apply,
        "apply_requested": bool(args.apply),
        "aborted_non_region_b": abort,
        "gbp_aud_rate": gbp_aud,
        "landed_cost_markup": markup,
        "materiality": "5% OR A$1.00, whichever exceeded first",
        "shopify_cost_semantics": "landed_aud = gbp * GBP_AUD_RATE * 1.12 via calculate_shopify_cost_aud",
        "region_counts_candidates": region_counts,
        "approved_toggle": len(toggle),
        "approved_reactivate": len(reactivate),
        "mutate_count": len(mutate_rows),
        "excluded_count": len(excluded),
        "region_a_in_mutation_set": sum(1 for r in mutate_rows if r["normalized_region"] == "A"),
        "no_continue_to_deny": all("SET_DENY" not in (r.get("action") or "") for r in rows),
        "mutate_barcodes": [r["barcode"] for r in mutate_rows],
        "excluded_reasons": {r["barcode"]: r["reason"] for r in excluded},
    }
    print("SUMMARY|" + json.dumps(summary, default=str))
    return 2 if abort else 0


if __name__ == "__main__":
    raise SystemExit(main())
