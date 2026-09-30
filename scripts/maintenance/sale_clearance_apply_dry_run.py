#!/usr/bin/env python3
"""
Clearance sale apply — DRY-RUN ONLY.

Uses ONLY SALE_10 / SALE_15 / SALE_20 rows from
tmp/sale_candidates_20260922/clearance_sale_executable_snapshot.json

Intended eventual writes (NOT enabled):
  - price = proposed_sale_price
  - compareAtPrice = original current_price (snapshot)
  - tagsAdd "Sale" (preserve existing tags)

Safety:
  - MUTATIONS_ENABLED = False (hard)
  - Mutation GraphQL commented out
  - --apply is refused
  - SKIP if live price drifted, compare-at populated, or no free stock
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text
from app.services.catalog_shopify_publish_service import shopify_inventory_location_id

MUTATIONS_ENABLED = False
SALE_TAG = "Sale"

VARIANT_NODES_QUERY = """
query ClearanceSaleDryRun($ids: [ID!]!, $locId: ID!) {
  nodes(ids: $ids) {
    ... on ProductVariant {
      id
      title
      barcode
      sku
      price
      compareAtPrice
      product {
        id
        title
        status
        tags
      }
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
"""

# DISABLED — do not uncomment without explicit merchant approval.
# Future mutation (see arrow_inventory_policy_sync_service.py):
# PRODUCT_VARIANTS_BULK_UPDATE = """
# mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
#   productVariantsBulkUpdate(productId: $productId, variants: $variants) {
#     productVariants { id price compareAtPrice inventoryPolicy }
#     userErrors { field message }
#   }
# }
# """
# TAGS_ADD = """
# mutation TagsAdd($id: ID!, $tags: [String!]!) {
#   tagsAdd(id: $id, tags: $tags) {
#     node { ... on Product { id tags } }
#     userErrors { field message }
#   }
# }
# """
# Eventual variant input MUST include inventoryPolicy: DENY (plus price/compareAt as approved).
# Rollback snapshot MUST capture: price, compareAtPrice, tags[], inventoryPolicy.


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


def chunked(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot",
        default=str(
            _REPO / "tmp/sale_candidates_20260922/clearance_sale_executable_snapshot.json"
        ),
    )
    parser.add_argument("--env", default=str(_REPO / ".env.prod"))
    parser.add_argument(
        "--out",
        default=str(
            _REPO / "tmp/sale_candidates_20260922/clearance_sale_apply_dry_run_log.json"
        ),
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    load_dotenv(args.env)
    if args.apply or MUTATIONS_ENABLED:
        print(
            "REFUSING WRITES: MUTATIONS_ENABLED=False. No Shopify mutations will run.",
            file=sys.stderr,
        )

    location_id = shopify_inventory_location_id() or "gid://shopify/Location/78213775584"
    snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
    # Hard filter: only SALE_10/15/20
    snapshot = [r for r in snapshot if r.get("decision") in {"SALE_10", "SALE_15", "SALE_20"}]
    print(f"Snapshot sale rows: {len(snapshot)}; mutations_enabled={MUTATIONS_ENABLED}")

    client = ShopifyClient()
    live_map: Dict[str, Dict[str, Any]] = {}
    ids = [r["shopify_variant_id"] for r in snapshot]
    for batch in chunked(ids, 40):
        tries = 0
        while True:
            tries += 1
            try:
                data = client.graphql(
                    VARIANT_NODES_QUERY, {"ids": batch, "locId": location_id}
                )
                break
            except Exception as exc:  # noqa: BLE001
                if ("THROTTLED" in str(exc) or "429" in str(exc)) and tries < 8:
                    time.sleep(min(2 * tries, 12))
                    continue
                raise
        for node in data.get("nodes") or []:
            if not node or not node.get("id"):
                continue
            product = node.get("product") or {}
            tags_raw = product.get("tags") or []
            if isinstance(tags_raw, str):
                tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
            else:
                tags = [clean_text(t) for t in tags_raw if clean_text(t)]
            qmap = _qty_map((node.get("inventoryItem") or {}).get("inventoryLevel"))
            live_map[node["id"]] = {
                "product_id": product.get("id"),
                "product_title": product.get("title"),
                "status": product.get("status"),
                "tags": tags,
                "price": _f(node.get("price")),
                "compare_at_price": _f(node.get("compareAtPrice")),
                "available": qmap.get("available"),
                "committed": qmap.get("committed"),
                "on_hand": qmap.get("on_hand"),
                "unit_cost": _f(
                    ((node.get("inventoryItem") or {}).get("unitCost") or {}).get("amount")
                ),
            }
        time.sleep(0.2)

    results = []
    for snap in snapshot:
        live = live_map.get(snap["shopify_variant_id"])
        row = {
            "decision": snap["decision"],
            "product_title": snap["product_title"],
            "shopify_product_id": snap["shopify_product_id"],
            "shopify_variant_id": snap["shopify_variant_id"],
            "barcode": snap.get("barcode"),
            "sku": snap.get("sku"),
            "snapshot_price": snap["current_price"],
            "proposed_sale_price": snap["proposed_sale_price"],
            "status": "",
            "reason": "",
            "old_price": None,
            "new_price": None,
            "old_compare_at": None,
            "new_compare_at": None,
            "existing_tags": "",
            "proposed_tags": "",
            "live_available": None,
            "would_mutate": False,
            "mutations_enabled": MUTATIONS_ENABLED,
        }
        if live is None:
            row["status"] = "skip"
            row["reason"] = "variant_not_found"
            results.append(row)
            continue

        tags = list(live.get("tags") or [])
        proposed_tags = tags if SALE_TAG in tags else tags + [SALE_TAG]
        row.update(
            {
                "old_price": live.get("price"),
                "old_compare_at": live.get("compare_at_price"),
                "existing_tags": ",".join(tags),
                "proposed_tags": ",".join(proposed_tags),
                "live_available": live.get("available"),
            }
        )

        if live.get("product_id") != snap["shopify_product_id"]:
            row["status"] = "skip"
            row["reason"] = "product_id_mismatch"
        elif live.get("status") and str(live.get("status")).upper() not in {"ACTIVE", "DRAFT"}:
            row["status"] = "skip"
            row["reason"] = f"product_status_{live.get('status')}"
        elif live.get("price") is None:
            row["status"] = "skip"
            row["reason"] = "live_price_missing"
        elif abs(float(live["price"]) - float(snap["current_price"])) > 0.005:
            row["status"] = "skip"
            row["reason"] = (
                f"price_drift_live_{live['price']}_snapshot_{snap['current_price']}"
            )
        elif live.get("compare_at_price") is not None and float(live["compare_at_price"]) > 0:
            row["status"] = "skip"
            row["reason"] = f"compare_at_now_populated:{live['compare_at_price']}"
        elif live.get("available") is None:
            row["status"] = "skip"
            row["reason"] = "available_unknown"
        elif int(live["available"]) <= 0:
            row["status"] = "skip"
            row["reason"] = "no_free_stock"
        else:
            row.update(
                {
                    "status": "would_update",
                    "reason": "ok",
                    "new_price": snap["proposed_sale_price"],
                    "new_compare_at": snap["current_price"],
                    "would_mutate": True,
                }
            )
        results.append(row)

    # WRITE PATH intentionally unreachable.
    # if MUTATIONS_ENABLED and args.apply:
    #     ... productVariantsBulkUpdate / tagsAdd ...

    would = [r for r in results if r["status"] == "would_update"]
    skipped = [r for r in results if r["status"] == "skip"]
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mutations_enabled": MUTATIONS_ENABLED,
        "apply_requested": bool(args.apply),
        "snapshot_rows": len(snapshot),
        "would_update": len(would),
        "skipped": len(skipped),
        "example_changes": would[:8],
        "results": results,
    }
    Path(args.out).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"would_update={len(would)} skipped={len(skipped)}")
    for r in would[:5]:
        print(
            f"  {r['product_title'][:50]} | {r['old_price']}->{r['new_price']} | "
            f"compareAt {r['old_compare_at']}->{r['new_compare_at']} | tag Sale"
        )
    print(f"Wrote {args.out}")
    print("CLEARANCE SALE DRY RUN READY — NO SHOPIFY CHANGES MADE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
