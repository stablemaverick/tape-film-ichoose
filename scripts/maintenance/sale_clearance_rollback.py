#!/usr/bin/env python3
"""
Clearance sale ROLLBACK — DRY RUN by default.

Restores pre-sale price / compareAtPrice / Sale-tag state / inventoryPolicy
from the immutable pre-sale rollback manifest.

DOES NOT restore inventory quantities.

Usage (dry-run default):
  ./venv/bin/python scripts/maintenance/sale_clearance_rollback.py \
      --env .env.prod \
      --rollback-manifest tmp/sale_candidates_20260922/clearance_sale_rollback_manifest.json

Explicit mutations (ONLY when authorised next week):
  ./venv/bin/python scripts/maintenance/sale_clearance_rollback.py \
      --env .env.prod \
      --rollback-manifest tmp/sale_candidates_20260922/clearance_sale_rollback_manifest.json \
      --apply-rollback \
      --i-understand-this-restores-pre-sale-state
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

MUTATIONS_ENABLED_DEFAULT = False
SALE_TAG = "Sale"

VARIANT_QUERY = """
query RollbackLive($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on ProductVariant {
      id
      price
      compareAtPrice
      inventoryPolicy
      product { id title status tags }
    }
  }
}
"""

BULK_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id price compareAtPrice inventoryPolicy }
    userErrors { field message }
  }
}
"""

TAGS_ADD = """
mutation TagsAdd($id: ID!, $tags: [String!]!) {
  tagsAdd(id: $id, tags: $tags) {
    userErrors { field message }
  }
}
"""

TAGS_REMOVE = """
mutation TagsRemove($id: ID!, $tags: [String!]!) {
  tagsRemove(id: $id, tags: $tags) {
    userErrors { field message }
  }
}
"""


def _f(v):
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _tags(raw):
    if isinstance(raw, str):
        return [t.strip() for t in raw.split(",") if t.strip()]
    if isinstance(raw, list):
        return [clean_text(t) for t in raw if clean_text(t)]
    return []


def match_price(a, b, tol=0.005):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--env", default=str(_REPO / ".env.prod"))
    p.add_argument(
        "--rollback-manifest",
        default=str(_REPO / "tmp/sale_candidates_20260922/clearance_sale_rollback_manifest.json"),
    )
    p.add_argument("--apply-rollback", action="store_true")
    p.add_argument(
        "--i-understand-this-restores-pre-sale-state",
        action="store_true",
        help="Required together with --apply-rollback to enable writes",
    )
    p.add_argument(
        "--out",
        default=str(_REPO / "tmp/sale_candidates_20260922/clearance_sale_rollback_dry_run_log.json"),
    )
    args = p.parse_args()
    load_dotenv(args.env)

    apply = bool(args.apply_rollback and args.i_understand_this_restores_pre_sale_state)
    if args.apply_rollback and not args.i_understand_this_restores_pre_sale_state:
        print("REFUSING: --apply-rollback requires --i-understand-this-restores-pre-sale-state", file=sys.stderr)
        return 2

    rb = json.loads(Path(args.rollback_manifest).read_text(encoding="utf-8"))
    records = rb.get("records") or []
    print(f"Loaded {len(records)} rollback records; apply={apply}")

    client = ShopifyClient()
    ids = [r["shopify_variant_id"] for r in records]
    live_map = {}
    for i in range(0, len(ids), 40):
        batch = ids[i : i + 40]
        data = client.graphql(VARIANT_QUERY, {"ids": batch})
        for node in data.get("nodes") or []:
            if not node or not node.get("id"):
                continue
            product = node.get("product") or {}
            live_map[node["id"]] = {
                "variant_id": node["id"],
                "product_id": product.get("id"),
                "title": product.get("title"),
                "status": product.get("status"),
                "price": _f(node.get("price")),
                "compare_at_price": _f(node.get("compareAtPrice")),
                "inventory_policy": (clean_text(node.get("inventoryPolicy")) or "").upper(),
                "tags": _tags(product.get("tags")),
            }
        time.sleep(0.2)

    results = []
    for rec in records:
        live = live_map.get(rec["shopify_variant_id"])
        row = {
            "title": rec.get("product_title"),
            "shopify_variant_id": rec["shopify_variant_id"],
            "status": "",
            "reason": "",
            "would_restore_price": rec.get("original_price"),
            "would_restore_compare_at": rec.get("original_compare_at"),
            "would_restore_inventory_policy": rec.get("original_inventory_policy"),
            "would_remove_sale_tag": not bool(rec.get("sale_tag_existed_before_clearance")),
            "inventory_qty_restore": "NEVER",
        }
        if live is None:
            row["status"] = "ROLLBACK_STATE_CHANGED_REVIEW"
            row["reason"] = "variant_missing"
            results.append(row)
            continue

        # Expected clearance state: sale price / compare-at / DENY / Sale tag present
        # If live no longer matches expected clearance, hold for review
        expected_sale_price = _f(rec.get("clearance_target_price"))
        expected_compare = _f(rec.get("clearance_target_compare_at"))
        expected_policy = "DENY"
        clearance_ok = (
            match_price(live.get("price"), expected_sale_price)
            and match_price(live.get("compare_at_price"), expected_compare)
            and (live.get("inventory_policy") == expected_policy)
            and (SALE_TAG in (live.get("tags") or []))
        )
        # For EXISTING_SALE_KEEP_PRICE, clearance price == original price
        if not clearance_ok:
            # Still allow restore if only policy/tag drifted but price matches clearance expectation loosely
            row["status"] = "ROLLBACK_STATE_CHANGED_REVIEW"
            row["reason"] = (
                f"live_not_expected_clearance price={live.get('price')} "
                f"compare={live.get('compare_at_price')} policy={live.get('inventory_policy')} "
                f"tags_has_Sale={SALE_TAG in (live.get('tags') or [])}"
            )
            results.append(row)
            continue

        row["status"] = "would_restore" if not apply else "restored_pending_verify"
        results.append(row)

        if not apply:
            continue

        variant_input = {
            "id": rec["shopify_variant_id"],
            "price": f"{float(rec['original_price']):.2f}",
            "inventoryPolicy": rec["original_inventory_policy"],
        }
        if rec.get("original_compare_at") in (None, "", "None"):
            variant_input["compareAtPrice"] = None
        else:
            variant_input["compareAtPrice"] = f"{float(rec['original_compare_at']):.2f}"
        data = client.graphql(
            BULK_UPDATE,
            {"productId": rec["shopify_product_id"], "variants": [variant_input]},
        )
        errs = (data.get("productVariantsBulkUpdate") or {}).get("userErrors") or []
        if errs:
            row["status"] = "failed"
            row["reason"] = str(errs)
            continue
        if not rec.get("sale_tag_existed_before_clearance") and SALE_TAG in (live.get("tags") or []):
            data = client.graphql(
                TAGS_REMOVE, {"id": rec["shopify_product_id"], "tags": [SALE_TAG]}
            )
            errs = (data.get("tagsRemove") or {}).get("userErrors") or []
            if errs:
                row["status"] = "failed"
                row["reason"] = f"tags_remove:{errs}"
                continue
        time.sleep(0.1)

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "apply": apply,
        "mutations_enabled": apply,
        "results": results,
        "note": "Inventory quantities are NEVER restored by this script.",
    }
    Path(args.out).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({"apply": apply, "rows": len(results), "out": args.out}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
