#!/usr/bin/env python3
"""
Execute approved Tape! Film clearance sale against live Shopify.

Safety:
  - Live Shopify is authoritative for inventory and commercial-state checks.
  - Immutable pre-sale rollback required BEFORE first mutation.
  - Mutations only after all global safeguards pass.
  - Post-mutation re-fetch verification required.

Usage:
  ./venv/bin/python scripts/maintenance/sale_clearance_execute.py --env .env.prod --execute
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text
from app.rules.pricing_rules import GST_RATE
from app.services.catalog_shopify_publish_service import shopify_inventory_location_id
from app.services.shopify_inventory_settings_audit import parse_shopify_bool_metafield
from app.services.supplier_orders_report_service import parse_release_date

TODAY = date(2026, 9, 23)
SALE_TAG = "Sale"
TARGET_POLICY = "DENY"
FORBIDDEN_INFERNAL_UNLISTED = "gid://shopify/ProductVariant/51368090140896"
APPROVED_INFERNAL = "gid://shopify/ProductVariant/46374098895072"
LAST_OF_US_TITLE = "The Last Of Us Season 2 Limited Edition Steelbook 4K Ultra HD"
LAST_OF_US_EXPECTED_PRICE = 79.99
LAST_OF_US_EXPECTED_COMPARE = 115.00
LAST_OF_US_TARGET_PRICE = 69.99
LAST_OF_US_TARGET_COMPARE = 115.00

EXECUTABLE_GROUPS = {
    "SALE_20",
    "SALE_15",
    "SALE_10",
    "SMALL_LOSS_CLEARANCE",
    "EXISTING_SALE_KEEP_PRICE",
    "MANUAL_CLEARANCE_EXCEPTION",
}

KNOWN_MOVED_TITLES = {
    "the virgin suicides 4k ultra hd + blu-ray",
    "the horror of frankenstein limited collectors edition 4k ultra hd",
    "falling down limited edition 4k ultra hd",
}

VARIANT_LIVE_QUERY = """
query ClearanceExecuteLive($ids: [ID!]!, $locId: ID!) {
  nodes(ids: $ids) {
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
        status
        tags
        preOrder: metafield(namespace: "custom", key: "pre_order") { value }
        preorderAlt: metafield(namespace: "custom", key: "preorder") { value }
        mediaRelease: metafield(namespace: "custom", key: "media_release_date") { value }
      }
      inventoryItem {
        tracked
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

PRODUCT_VARIANTS_BULK_UPDATE = """
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
    node { ... on Product { id tags } }
    userErrors { field message }
  }
}
"""

SEARCH_BY_TITLE = """
query ClearanceTitleSearch($q: String, $locId: ID!) {
  products(first: 10, query: $q) {
    nodes {
      id
      title
      status
      tags
      variants(first: 10) {
        nodes {
          id
          sku
          barcode
          price
          compareAtPrice
          inventoryPolicy
          inventoryItem {
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


def _i(v: Any) -> Optional[int]:
    if v in (None, "", "None"):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _money_str(v: Optional[float]) -> Optional[str]:
    if v is None:
        return None
    return f"{float(v):.2f}"


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


def _tags(raw: Any) -> List[str]:
    if isinstance(raw, str):
        return [t.strip() for t in raw.split(",") if t.strip()]
    if isinstance(raw, list):
        return [clean_text(t) for t in raw if clean_text(t)]
    return []


def _metafield_bool(product: Dict[str, Any], *keys: str) -> bool:
    for key in keys:
        mf = product.get(key)
        if isinstance(mf, dict) and mf.get("value") is not None:
            return bool(parse_shopify_bool_metafield(mf.get("value")))
    return False


def _norm_title(s: str) -> str:
    return " ".join((s or "").casefold().split())


def chunked(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


def graphql_retry(client: ShopifyClient, query: str, variables: dict, tries: int = 8) -> dict:
    last: Exception | None = None
    for i in range(tries):
        try:
            return client.graphql(query, variables)
        except Exception as exc:  # noqa: BLE001
            last = exc
            msg = str(exc)
            if ("THROTTLED" in msg or "429" in msg) and i < tries - 1:
                time.sleep(min(2 * (i + 1), 12))
                continue
            raise
    raise RuntimeError(f"GraphQL failed: {last}")


def fetch_live(
    client: ShopifyClient, location_id: str, variant_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for batch in chunked(variant_ids, 25):
        data = graphql_retry(
            client, VARIANT_LIVE_QUERY, {"ids": batch, "locId": location_id}
        )
        for node in data.get("nodes") or []:
            if not node or not node.get("id"):
                continue
            product = node.get("product") or {}
            inv = node.get("inventoryItem") or {}
            qmap = _qty_map(inv.get("inventoryLevel"))
            levels = bool(inv.get("inventoryLevel"))
            available = qmap.get("available") if levels else None
            committed = qmap.get("committed") if levels else None
            on_hand = qmap.get("on_hand") if levels else None
            free = None
            inventory_ok = False
            if levels and available is not None and committed is not None and on_hand is not None:
                if int(available) == int(on_hand) - int(committed):
                    free = max(0, int(available))
                    inventory_ok = True
            release_raw = clean_text((product.get("mediaRelease") or {}).get("value")) or ""
            out[node["id"]] = {
                "variant_id": node["id"],
                "variant_title": clean_text(node.get("title")) or "",
                "sku": clean_text(node.get("sku")) or "",
                "barcode": clean_text(node.get("barcode")) or "",
                "price": _f(node.get("price")),
                "compare_at_price": _f(node.get("compareAtPrice")),
                "inventory_policy": (clean_text(node.get("inventoryPolicy")) or "").upper(),
                "inventory_quantity": _i(node.get("inventoryQuantity")),
                "product_id": clean_text(product.get("id")) or "",
                "product_title": clean_text(product.get("title")) or "",
                "status": (clean_text(product.get("status")) or "").upper(),
                "tags": _tags(product.get("tags")),
                "pre_order": _metafield_bool(product, "preOrder", "preorderAlt"),
                "media_release_date": release_raw,
                "tracked": inv.get("tracked"),
                "unit_cost": _f((inv.get("unitCost") or {}).get("amount")),
                "available": available,
                "committed": committed,
                "on_hand": on_hand,
                "free_inventory": free,
                "inventory_ok": inventory_ok,
                "levels_present": levels,
            }
        time.sleep(0.2)
    return out


def search_title_status(
    client: ShopifyClient, location_id: str, title_query: str
) -> List[Dict[str, Any]]:
    data = graphql_retry(
        client, SEARCH_BY_TITLE, {"q": f"title:{title_query}", "locId": location_id}
    )
    rows = []
    for p in (data.get("products") or {}).get("nodes") or []:
        for v in (p.get("variants") or {}).get("nodes") or []:
            qmap = _qty_map((v.get("inventoryItem") or {}).get("inventoryLevel"))
            rows.append(
                {
                    "product_title": p.get("title"),
                    "product_id": p.get("id"),
                    "status": p.get("status"),
                    "variant_id": v.get("id"),
                    "sku": v.get("sku"),
                    "barcode": v.get("barcode"),
                    "price": v.get("price"),
                    "compare_at": v.get("compareAtPrice"),
                    "inventory_policy": v.get("inventoryPolicy"),
                    "available": qmap.get("available"),
                    "committed": qmap.get("committed"),
                    "on_hand": qmap.get("on_hand"),
                    "tags": _tags(p.get("tags")),
                }
            )
    return rows


def prices_match(a: Optional[float], b: Optional[float], tol: float = 0.005) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def classify(
    plan: Dict[str, Any], live: Optional[Dict[str, Any]]
) -> Tuple[str, str]:
    """Return (result_class, reason). MUTATE means eligible for mutation."""
    vid = plan.get("shopify_variant_id") or ""
    group = plan.get("group") or ""

    if vid == FORBIDDEN_INFERNAL_UNLISTED:
        return "NON_ACTIVE_SKIPPED", "forbidden_unlisted_infernal_affairs"
    if group not in EXECUTABLE_GROUPS:
        return "OTHER_FAILURE", f"group_not_authorised:{group}"
    if "Infernal Affairs" in (plan.get("title") or "") and vid != APPROVED_INFERNAL:
        return "OTHER_FAILURE", "infernal_affairs_unapproved_variant_id"
    if live is None:
        return "INVENTORY_REVIEW", "variant_not_found_live"
    if live.get("status") != "ACTIVE":
        return "NON_ACTIVE_SKIPPED", f"status_{live.get('status')}"

    release = parse_release_date(live.get("media_release_date"))
    if live.get("pre_order") or (release and release > TODAY):
        return "FUTURE_PREORDER_SKIPPED", f"release={live.get('media_release_date')}"

    if not live.get("inventory_ok") or live.get("free_inventory") is None:
        return "INVENTORY_REVIEW", "cannot_establish_free_inventory"
    if int(live["free_inventory"]) <= 0:
        return "SOLD_SINCE_SNAPSHOT", "free_inventory_zero"
    if live.get("tracked") is False:
        return "INVENTORY_POLICY_REVIEW", "inventory_not_tracked"
    policy = live.get("inventory_policy") or ""
    if policy not in {"DENY", "CONTINUE"}:
        return "INVENTORY_POLICY_REVIEW", f"unreadable_policy:{policy!r}"

    # Commercial snapshot drift
    plan_price = _f(plan.get("current_price"))
    plan_compare = _f(plan.get("current_compare_at")) if plan.get("current_compare_at") not in ("", None) else None
    live_price = live.get("price")
    live_compare = live.get("compare_at_price")

    title = plan.get("title") or live.get("product_title") or ""
    if title == LAST_OF_US_TITLE or group == "MANUAL_CLEARANCE_EXCEPTION":
        if not prices_match(live_price, LAST_OF_US_EXPECTED_PRICE):
            return "SNAPSHOT_CHANGED_REVIEW", f"last_of_us_price_live_{live_price}_expected_{LAST_OF_US_EXPECTED_PRICE}"
        if not prices_match(live_compare, LAST_OF_US_EXPECTED_COMPARE):
            return "SNAPSHOT_CHANGED_REVIEW", f"last_of_us_compare_live_{live_compare}_expected_{LAST_OF_US_EXPECTED_COMPARE}"
        return "MUTATE", "manual_clearance_exception_ok"

    if group == "EXISTING_SALE_KEEP_PRICE":
        if not prices_match(live_price, plan_price):
            return "SNAPSHOT_CHANGED_REVIEW", f"price_drift_live_{live_price}_plan_{plan_price}"
        if not prices_match(live_compare, plan_compare):
            return "SNAPSHOT_CHANGED_REVIEW", f"compare_drift_live_{live_compare}_plan_{plan_compare}"
        return "MUTATE", "existing_sale_keep_price_ok"

    # Normal sale groups: price must match plan original; compare-at must still be blank/null
    if not prices_match(live_price, plan_price):
        return "SNAPSHOT_CHANGED_REVIEW", f"price_drift_live_{live_price}_plan_{plan_price}"
    # Expected pre-sale compare-at for normal groups is blank
    if live_compare is not None and live_compare > 0:
        # If plan expected blank and live has compare-at, drift
        if plan_compare is None:
            return "SNAPSHOT_CHANGED_REVIEW", f"compare_at_now_populated_{live_compare}"
        if not prices_match(live_compare, plan_compare):
            return "SNAPSHOT_CHANGED_REVIEW", f"compare_drift_live_{live_compare}_plan_{plan_compare}"

    return "MUTATE", "ok"


def build_target(plan: Dict[str, Any], live: Dict[str, Any]) -> Dict[str, Any]:
    group = plan.get("group") or ""
    tags = list(live.get("tags") or [])
    sale_existed = SALE_TAG in tags
    target_tags = tags if sale_existed else tags + [SALE_TAG]

    if group == "MANUAL_CLEARANCE_EXCEPTION" or (plan.get("title") == LAST_OF_US_TITLE):
        return {
            "target_price": LAST_OF_US_TARGET_PRICE,
            "target_compare_at": LAST_OF_US_TARGET_COMPARE,
            "target_inventory_policy": TARGET_POLICY,
            "target_tags": target_tags,
            "sale_tag_existed_before_clearance": sale_existed,
            "mutate_price": True,
            "mutate_compare_at": True,
            "mutate_policy": True,
            "mutate_tags": not sale_existed,
        }
    if group == "EXISTING_SALE_KEEP_PRICE":
        return {
            "target_price": live.get("price"),
            "target_compare_at": live.get("compare_at_price"),
            "target_inventory_policy": TARGET_POLICY,
            "target_tags": target_tags,
            "sale_tag_existed_before_clearance": sale_existed,
            "mutate_price": False,
            "mutate_compare_at": False,
            "mutate_policy": True,
            "mutate_tags": not sale_existed,
        }
    # Normal sale
    return {
        "target_price": _f(plan.get("target_sale_price")),
        "target_compare_at": _f(plan.get("current_price")),  # original pre-sale price
        "target_inventory_policy": TARGET_POLICY,
        "target_tags": target_tags,
        "sale_tag_existed_before_clearance": sale_existed,
        "mutate_price": True,
        "mutate_compare_at": True,
        "mutate_policy": True,
        "mutate_tags": not sale_existed,
    }


def margin_parts(price_inc_gst: float, unit_cost: float) -> Tuple[float, float]:
    net = price_inc_gst / GST_RATE
    gp = net - unit_cost
    margin = (gp / net * 100.0) if net else float("nan")
    return gp, margin


def apply_variant(
    client: ShopifyClient, live: Dict[str, Any], target: Dict[str, Any]
) -> Tuple[bool, str]:
    variant_input: Dict[str, Any] = {"id": live["variant_id"]}
    if target["mutate_price"]:
        variant_input["price"] = _money_str(target["target_price"])
    if target["mutate_compare_at"]:
        # Shopify accepts string money; null clears — we set explicit value
        variant_input["compareAtPrice"] = _money_str(target["target_compare_at"])
    if target["mutate_policy"]:
        variant_input["inventoryPolicy"] = TARGET_POLICY

    needs_variant_update = any(
        [
            target["mutate_price"],
            target["mutate_compare_at"],
            target["mutate_policy"],
        ]
    )
    if needs_variant_update:
        data = graphql_retry(
            client,
            PRODUCT_VARIANTS_BULK_UPDATE,
            {"productId": live["product_id"], "variants": [variant_input]},
        )
        block = data.get("productVariantsBulkUpdate") or {}
        errs = block.get("userErrors") or []
        if errs:
            return False, f"variant_update_errors:{errs}"

    if target["mutate_tags"]:
        data = graphql_retry(
            client,
            TAGS_ADD,
            {"id": live["product_id"], "tags": [SALE_TAG]},
        )
        block = data.get("tagsAdd") or {}
        errs = block.get("userErrors") or []
        if errs:
            return False, f"tags_add_errors:{errs}"
    return True, "ok"


def verify_variant(
    live_after: Dict[str, Any],
    live_before: Dict[str, Any],
    target: Dict[str, Any],
    group: str,
) -> Tuple[bool, str]:
    if not prices_match(live_after.get("price"), target.get("target_price")):
        return False, f"price_got_{live_after.get('price')}_want_{target.get('target_price')}"
    if not prices_match(live_after.get("compare_at_price"), target.get("target_compare_at")):
        return False, f"compare_got_{live_after.get('compare_at_price')}_want_{target.get('target_compare_at')}"
    if (live_after.get("inventory_policy") or "").upper() != TARGET_POLICY:
        return False, f"policy_got_{live_after.get('inventory_policy')}"
    tags_after = set(live_after.get("tags") or [])
    if SALE_TAG not in tags_after:
        return False, "sale_tag_missing"
    # previous tags preserved
    before_tags = set(live_before.get("tags") or [])
    missing = before_tags - tags_after
    if missing:
        return False, f"tags_lost:{sorted(missing)}"
    return True, "verified"


def write_rollback_script(path: Path) -> None:
    path.write_text(
        '''#!/usr/bin/env python3
"""
Clearance sale ROLLBACK — DRY RUN by default.

Restores pre-sale price / compareAtPrice / Sale-tag state / inventoryPolicy
from the immutable pre-sale rollback manifest.

DOES NOT restore inventory quantities.

Usage (dry-run default):
  ./venv/bin/python scripts/maintenance/sale_clearance_rollback.py \\
      --env .env.prod \\
      --rollback-manifest tmp/sale_candidates_20260922/clearance_sale_rollback_manifest.json

Explicit mutations (ONLY when authorised next week):
  ./venv/bin/python scripts/maintenance/sale_clearance_rollback.py \\
      --env .env.prod \\
      --rollback-manifest tmp/sale_candidates_20260922/clearance_sale_rollback_manifest.json \\
      --apply-rollback \\
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
''',
        encoding="utf-8",
    )
    path.chmod(0o755)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=str(_REPO / ".env.prod"))
    parser.add_argument(
        "--manifest",
        default=str(
            _REPO / "tmp/sale_candidates_20260922/clearance_sale_final_manifest_with_policy.json"
        ),
    )
    parser.add_argument("--out-dir", default=str(_REPO / "tmp/sale_candidates_20260922"))
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Enable mutations AFTER all global safeguards pass",
    )
    args = parser.parse_args()

    load_dotenv(args.env)
    out_dir = Path(args.out_dir)
    archive_dir = out_dir / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    payload = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    plan_rows = [
        r
        for r in (payload.get("executable") or [])
        if (r.get("group") in EXECUTABLE_GROUPS)
    ]
    original_proposed = len(plan_rows)
    print(f"Loaded approved plan executable rows: {original_proposed}")

    location_id = shopify_inventory_location_id() or "gid://shopify/Location/78213775584"
    client = ShopifyClient()

    # Live fetch all plan variants + force-check known moved titles via search
    vids = [r["shopify_variant_id"] for r in plan_rows]
    live_map = fetch_live(client, location_id, vids)

    # Explicit live status report for named products
    special_report = {}
    for label, q in [
        ("The Virgin Suicides", "The Virgin Suicides"),
        ("The Horror of Frankenstein", "Horror Of Frankenstein"),
        ("Falling Down", "Falling Down Limited Edition"),
        ("The Last Of Us Season 2", "Last Of Us Season 2"),
        ("Wicked UPSESB0197", "Wicked - For Good"),
        ("Wicked UPSESB0206", "Wicked - For Good"),
        ("Infernal Affairs ACTIVE", "Infernal Affairs Trilogy"),
    ]:
        special_report[label] = search_title_status(client, location_id, q)
        time.sleep(0.15)

    print("\n===== LIVE STATUS (named products) =====")
    for label, rows in special_report.items():
        print(f"\n--- {label} ---")
        # Filter wicked by sku when needed
        shown = rows
        if label.endswith("0197"):
            shown = [r for r in rows if (r.get("sku") or "") == "UPSESB0197"]
        elif label.endswith("0206"):
            shown = [r for r in rows if (r.get("sku") or "") == "UPSESB0206"]
        elif label.startswith("Infernal"):
            shown = [r for r in rows if r.get("variant_id") == APPROVED_INFERNAL]
        for r in shown[:5]:
            print(
                json.dumps(
                    {
                        "title": r.get("product_title"),
                        "status": r.get("status"),
                        "variant_id": r.get("variant_id"),
                        "sku": r.get("sku"),
                        "price": r.get("price"),
                        "compare_at": r.get("compare_at"),
                        "policy": r.get("inventory_policy"),
                        "available": r.get("available"),
                        "committed": r.get("committed"),
                        "on_hand": r.get("on_hand"),
                    },
                    indent=2,
                )
            )

    # Classify each plan row
    classified: List[Dict[str, Any]] = []
    mutate_candidates: List[Dict[str, Any]] = []
    counts = Counter()

    for plan in plan_rows:
        live = live_map.get(plan["shopify_variant_id"])
        result, reason = classify(plan, live)
        counts[result] += 1
        row = {
            "plan": plan,
            "live": live,
            "result_class": result,
            "reason": reason,
        }
        if result == "MUTATE" and live is not None:
            target = build_target(plan, live)
            row["target"] = target
            mutate_candidates.append(row)
        classified.append(row)

    # Global assertions on mutation set
    mutate_vids = [r["plan"]["shopify_variant_id"] for r in mutate_candidates]
    unique_ok = len(mutate_vids) == len(set(mutate_vids))
    print("\n===== PREFLIGHT SUMMARY =====")
    print(f"Original proposed executable variants: {original_proposed}")
    print(f"Still eligible now (MUTATE): {len(mutate_candidates)}")
    print(f"SOLD_SINCE_SNAPSHOT: {counts['SOLD_SINCE_SNAPSHOT']}")
    print(f"SNAPSHOT_CHANGED_REVIEW: {counts['SNAPSHOT_CHANGED_REVIEW']}")
    print(f"INVENTORY_REVIEW: {counts['INVENTORY_REVIEW']}")
    print(f"INVENTORY_POLICY_REVIEW: {counts['INVENTORY_POLICY_REVIEW']}")
    print(f"NON_ACTIVE_SKIPPED: {counts['NON_ACTIVE_SKIPPED']}")
    print(f"FUTURE_PREORDER_SKIPPED: {counts['FUTURE_PREORDER_SKIPPED']}")
    print(f"OTHER_FAILURE: {counts['OTHER_FAILURE']}")
    print(f"Final variants proposed for mutation: {len(mutate_candidates)}")
    print(f"FINAL MUTATION VARIANT IDS UNIQUE: {'PASS' if unique_ok else 'FAIL'}")

    # Ensure no forbidden / held IDs
    held_forbidden = []
    for vid in mutate_vids:
        if vid == FORBIDDEN_INFERNAL_UNLISTED:
            held_forbidden.append(vid)
    # Virgin Suicides must not be in mutate set
    for r in mutate_candidates:
        if "virgin suicides" in _norm_title(r["plan"].get("title") or ""):
            held_forbidden.append(r["plan"]["shopify_variant_id"])

    if not unique_ok or held_forbidden:
        print("GLOBAL SAFETY ASSERTION FAILED — STOPPING. NO MUTATIONS.")
        print("held_forbidden:", held_forbidden)
        return 2

    if not args.execute:
        print("\n--execute not passed; stopping after preflight (no mutations).")
        return 0

    if not mutate_candidates:
        print("No variants eligible for mutation after preflight. STOPPING.")
        return 0

    # ===== Immutable rollback =====
    ts = datetime.now(timezone.utc)
    ts_label = ts.strftime("%Y%m%dT%H%M%SZ")
    archive_name = f"clearance_sale_rollback_manifest_PRE_SALE_20260923_{ts_label}.json"
    archive_path = archive_dir / archive_name
    if archive_path.exists():
        print(f"FATAL: archival rollback already exists: {archive_path}")
        return 2

    rollback_records = []
    for r in mutate_candidates:
        plan, live, target = r["plan"], r["live"], r["target"]
        assert live is not None
        rollback_records.append(
            {
                "shopify_product_id": live["product_id"],
                "shopify_variant_id": live["variant_id"],
                "product_title": live["product_title"],
                "variant_title": live["variant_title"],
                "sku": live.get("sku") or "",
                "barcode": live.get("barcode") or "",
                "group": plan.get("group"),
                "original_price": live.get("price"),
                "original_compare_at": live.get("compare_at_price"),
                "original_tags": list(live.get("tags") or []),
                "sale_tag_existed_before_clearance": bool(target.get("sale_tag_existed_before_clearance")),
                "original_inventory_policy": live.get("inventory_policy"),
                "product_status": live.get("status"),
                "inventory_tracked": live.get("tracked"),
                "free_inventory_at_capture": live.get("free_inventory"),
                "on_hand_at_capture": live.get("on_hand"),
                "committed_at_capture": live.get("committed"),
                "clearance_target_price": target.get("target_price"),
                "clearance_target_compare_at": target.get("target_compare_at"),
                "clearance_target_inventory_policy": TARGET_POLICY,
                "capture_timestamp": ts.isoformat(),
                "inventory_quantities_excluded_from_rollback_writes": True,
            }
        )

    rb_vids = [r["shopify_variant_id"] for r in rollback_records]
    id_match = set(rb_vids) == set(mutate_vids) and len(rb_vids) == len(mutate_vids)
    count_match = len(rollback_records) == len(set(mutate_vids))

    pre_mutation_snapshot = {
        "generated_at": ts.isoformat(),
        "purpose": "immutable_pre_sale_live_state_before_clearance_mutations",
        "location_id": location_id,
        "mutation_variant_ids": mutate_vids,
        "records": rollback_records,
    }
    rollback_manifest = {
        "generated_at": ts.isoformat(),
        "purpose": "temporary_clearance_rollback_manifest",
        "mutations_enabled_at_creation": False,
        "inventory_quantities_excluded_from_rollback_writes": True,
        "restore_fields": [
            "original_price",
            "original_compare_at",
            "original_inventory_policy",
            "sale_tag_existed_before_clearance",
        ],
        "records": rollback_records,
        "mutation_variant_ids": mutate_vids,
    }

    pre_path = out_dir / "clearance_sale_pre_mutation_snapshot.json"
    rb_path = out_dir / "clearance_sale_rollback_manifest.json"
    pre_path.write_text(json.dumps(pre_mutation_snapshot, indent=2, default=str), encoding="utf-8")
    rb_path.write_text(json.dumps(rollback_manifest, indent=2, default=str), encoding="utf-8")
    shutil.copy2(rb_path, archive_path)

    # Readback validation
    rb_read = json.loads(rb_path.read_text(encoding="utf-8"))
    pre_read = json.loads(pre_path.read_text(encoding="utf-8"))
    arch_read = json.loads(archive_path.read_text(encoding="utf-8"))
    readback_ok = (
        len(rb_read.get("records") or []) == len(rollback_records)
        and len(pre_read.get("records") or []) == len(rollback_records)
        and len(arch_read.get("records") or []) == len(rollback_records)
        and set(rb_read.get("mutation_variant_ids") or []) == set(mutate_vids)
    )

    # Prepare rollback script (dry-run default)
    rollback_script = _REPO / "scripts/maintenance/sale_clearance_rollback.py"
    write_rollback_script(rollback_script)
    rollback_script_ready = rollback_script.exists()

    print("\n===== PRE-MUTATION ROLLBACK CONFIRMATION =====")
    print(f"ROLLBACK MANIFEST READY: {'YES' if rb_path.exists() else 'NO'}")
    print(f"ROLLBACK RECORDS: {len(rollback_records)}")
    print(f"MUTATION VARIANTS: {len(mutate_vids)}")
    print(f"VARIANT ID SET MATCH: {'PASS' if id_match and count_match else 'FAIL'}")
    print(f"ARCHIVAL ROLLBACK COPY CREATED: {'YES' if archive_path.exists() else 'NO'}")
    print(f"ROLLBACK FILE READBACK VALIDATION: {'PASS' if readback_ok else 'FAIL'}")
    print("INVENTORY QUANTITIES EXCLUDED FROM ROLLBACK WRITES: YES")
    print(f"ROLLBACK SCRIPT PREPARED IN DRY-RUN MODE: {'YES' if rollback_script_ready else 'NO'}")

    if not (id_match and count_match and readback_ok and archive_path.exists() and rollback_script_ready):
        print("GLOBAL ROLLBACK SAFEGUARD FAILED — STOPPING. NO MUTATIONS.")
        return 2

    # ===== Execute mutations =====
    print("\n===== EXECUTING MUTATIONS =====")
    results: List[Dict[str, Any]] = []
    success_by_group = Counter()
    success_rows: List[Dict[str, Any]] = []

    # Include skipped rows in results too
    for r in classified:
        if r["result_class"] != "MUTATE":
            plan = r["plan"]
            live = r["live"] or {}
            results.append(
                {
                    "product": plan.get("title"),
                    "sku": plan.get("sku") or live.get("sku"),
                    "barcode": plan.get("barcode") or live.get("barcode"),
                    "product_id": plan.get("shopify_product_id") or live.get("product_id"),
                    "variant_id": plan.get("shopify_variant_id"),
                    "group": plan.get("group"),
                    "free_inventory_at_execution": (live or {}).get("free_inventory"),
                    "original_price": (live or {}).get("price"),
                    "resulting_price": (live or {}).get("price"),
                    "original_compare_at": (live or {}).get("compare_at_price"),
                    "resulting_compare_at": (live or {}).get("compare_at_price"),
                    "original_inventory_policy": (live or {}).get("inventory_policy"),
                    "resulting_inventory_policy": (live or {}).get("inventory_policy"),
                    "sale_tag_existed_before_clearance": "",
                    "sale_tag_present_after_execution": "",
                    "previous_tags_preserved": "",
                    "result": r["result_class"],
                    "error_review_reason": r["reason"],
                }
            )

    for idx, r in enumerate(mutate_candidates, start=1):
        plan, live, target = r["plan"], r["live"], r["target"]
        assert live is not None
        print(
            f"[{idx}/{len(mutate_candidates)}] {plan.get('group')} "
            f"{(plan.get('title') or '')[:55]} ..."
        )
        ok, msg = apply_variant(client, live, target)
        if not ok:
            results.append(
                {
                    "product": plan.get("title"),
                    "sku": live.get("sku"),
                    "barcode": live.get("barcode"),
                    "product_id": live.get("product_id"),
                    "variant_id": live.get("variant_id"),
                    "group": plan.get("group"),
                    "free_inventory_at_execution": live.get("free_inventory"),
                    "original_price": live.get("price"),
                    "resulting_price": "",
                    "original_compare_at": live.get("compare_at_price"),
                    "resulting_compare_at": "",
                    "original_inventory_policy": live.get("inventory_policy"),
                    "resulting_inventory_policy": "",
                    "sale_tag_existed_before_clearance": target.get("sale_tag_existed_before_clearance"),
                    "sale_tag_present_after_execution": "",
                    "previous_tags_preserved": "",
                    "result": "OTHER_FAILURE",
                    "error_review_reason": msg,
                }
            )
            time.sleep(0.15)
            continue

        # Re-fetch verify
        after_map = fetch_live(client, location_id, [live["variant_id"]])
        after = after_map.get(live["variant_id"])
        if after is None:
            verified, vmsg = False, "post_fetch_missing"
        else:
            verified, vmsg = verify_variant(after, live, target, plan.get("group") or "")

        if verified:
            success_by_group[plan.get("group") or ""] += 1
            success_rows.append({"plan": plan, "live_before": live, "live_after": after, "target": target})
            result_code = "SUCCESS"
        else:
            result_code = "POST_UPDATE_VERIFICATION_FAILED"

        results.append(
            {
                "product": plan.get("title"),
                "sku": live.get("sku"),
                "barcode": live.get("barcode"),
                "product_id": live.get("product_id"),
                "variant_id": live.get("variant_id"),
                "group": plan.get("group"),
                "free_inventory_at_execution": live.get("free_inventory"),
                "original_price": live.get("price"),
                "resulting_price": (after or {}).get("price"),
                "original_compare_at": live.get("compare_at_price"),
                "resulting_compare_at": (after or {}).get("compare_at_price"),
                "original_inventory_policy": live.get("inventory_policy"),
                "resulting_inventory_policy": (after or {}).get("inventory_policy"),
                "sale_tag_existed_before_clearance": target.get("sale_tag_existed_before_clearance"),
                "sale_tag_present_after_execution": SALE_TAG in ((after or {}).get("tags") or []),
                "previous_tags_preserved": (
                    set(live.get("tags") or []).issubset(set((after or {}).get("tags") or []))
                    if after
                    else False
                ),
                "result": result_code,
                "error_review_reason": "" if verified else vmsg,
            }
        )
        time.sleep(0.12)

    # Actual economics from successful rows using free inventory at execution
    units = 0
    cost_v = 0.0
    orig_retail = 0.0
    sale_retail = 0.0
    gp_total = 0.0
    for s in success_rows:
        qty = int(s["live_before"].get("free_inventory") or 0)
        cost = float(s["live_before"].get("unit_cost") or s["plan"].get("cost") or 0)
        op = float(s["live_before"].get("price") or 0)
        sp = float(s["target"].get("target_price") or 0)
        units += qty
        cost_v += qty * cost
        orig_retail += qty * op
        sale_retail += qty * sp
        if cost and sp:
            gp, _ = margin_parts(sp, cost)
            gp_total += qty * gp
    rev_ex = sale_retail / GST_RATE if sale_retail else 0.0

    # Write results
    result_fields = [
        "product",
        "sku",
        "barcode",
        "product_id",
        "variant_id",
        "group",
        "free_inventory_at_execution",
        "original_price",
        "resulting_price",
        "original_compare_at",
        "resulting_compare_at",
        "original_inventory_policy",
        "resulting_inventory_policy",
        "sale_tag_existed_before_clearance",
        "sale_tag_present_after_execution",
        "previous_tags_preserved",
        "result",
        "error_review_reason",
    ]
    with (out_dir / "clearance_sale_execution_results.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=result_fields, extrasaction="ignore")
        w.writeheader()
        for row in results:
            w.writerow(row)

    result_counts = Counter(r["result"] for r in results)
    mutated_success_vids = [r["variant_id"] for r in results if r["result"] == "SUCCESS"]
    exec_json = {
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "original_proposed_executable_variants": original_proposed,
        "final_eligible_variants": len(mutate_candidates),
        "result_counts": dict(result_counts),
        "success_by_group": dict(success_by_group),
        "actual_economics": {
            "variants": len(success_rows),
            "units": units,
            "inventory_cost": round(cost_v, 2),
            "original_retail": round(orig_retail, 2),
            "clearance_retail": round(sale_retail, 2),
            "markdown_dollars": round(orig_retail - sale_retail, 2),
            "markdown_pct": round((1 - sale_retail / orig_retail) * 100, 2) if orig_retail else 0,
            "revenue_ex_gst": round(rev_ex, 2),
            "gross_profit_loss": round(gp_total, 2),
            "weighted_gross_margin_pct": round((gp_total / rev_ex) * 100, 2) if rev_ex else 0,
            "cash_if_all_free_units_sell_inc_gst": round(sale_retail, 2),
        },
        "rollback_manifest": str(rb_path),
        "archival_rollback": str(archive_path),
        "rollback_dry_run_command": (
            "./venv/bin/python scripts/maintenance/sale_clearance_rollback.py "
            "--env .env.prod "
            "--rollback-manifest tmp/sale_candidates_20260922/clearance_sale_rollback_manifest.json"
        ),
        "rollback_apply_command": (
            "./venv/bin/python scripts/maintenance/sale_clearance_rollback.py "
            "--env .env.prod "
            "--rollback-manifest tmp/sale_candidates_20260922/clearance_sale_rollback_manifest.json "
            "--apply-rollback --i-understand-this-restores-pre-sale-state"
        ),
        "safety_audit": {
            "no_duplicate_variant_mutated": len(mutated_success_vids) == len(set(mutated_success_vids)),
            "no_unlisted_mutated": FORBIDDEN_INFERNAL_UNLISTED not in mutated_success_vids,
            "infernal_unlisted_untouched": True,
            "rollback_ids_match_success": set(mutated_success_vids).issubset(set(rb_vids)),
            "inventory_qty_excluded_from_rollback": True,
        },
        "results": results,
        "special_live_status": special_report,
        "preflight_counts": dict(counts),
    }
    (out_dir / "clearance_sale_execution_results.json").write_text(
        json.dumps(exec_json, indent=2, default=str), encoding="utf-8"
    )

    # Final output
    print("\nCLEARANCE SALE EXECUTION COMPLETE")
    print()
    print(f"Successfully updated: {result_counts['SUCCESS']}")
    print(f"Skipped — sold since snapshot: {result_counts['SOLD_SINCE_SNAPSHOT']}")
    print(f"Skipped — snapshot changed: {result_counts['SNAPSHOT_CHANGED_REVIEW']}")
    print(f"Skipped — inventory review: {result_counts['INVENTORY_REVIEW']}")
    print(f"Skipped — inventory policy review: {result_counts['INVENTORY_POLICY_REVIEW']}")
    print(f"Post-update verification failures: {result_counts['POST_UPDATE_VERIFICATION_FAILED']}")
    other = (
        result_counts["OTHER_FAILURE"]
        + result_counts["NON_ACTIVE_SKIPPED"]
        + result_counts["FUTURE_PREORDER_SKIPPED"]
    )
    print(f"Other failures/skips: {other}")
    print()
    print(f"Final sale variants: {len(success_rows)}")
    print(f"Final sale units: {units}")
    print(f"Clearance retail value: ${sale_retail:,.2f}")
    print(f"Expected cash if all remaining sale inventory sells: ${sale_retail:,.2f}")
    print()
    print(f"Rollback manifest: {rb_path}")
    print(f"Archival rollback: {archive_path}")
    print(
        "Rollback dry-run command: ./venv/bin/python scripts/maintenance/sale_clearance_rollback.py "
        "--env .env.prod --rollback-manifest tmp/sale_candidates_20260922/clearance_sale_rollback_manifest.json"
    )
    print()
    print("Success by group:", dict(success_by_group))
    fails = [r for r in results if r["result"] in {
        "POST_UPDATE_VERIFICATION_FAILED", "OTHER_FAILURE", "SNAPSHOT_CHANGED_REVIEW",
        "INVENTORY_REVIEW", "INVENTORY_POLICY_REVIEW"
    }]
    if fails:
        print("\nExceptions requiring manual review:")
        for r in fails:
            print(f"  [{r['result']}] {r['product']}: {r['error_review_reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
