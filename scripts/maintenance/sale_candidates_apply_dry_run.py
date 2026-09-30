#!/usr/bin/env python3
"""
Sale price apply script — DRY-RUN ONLY in this initial version.

Intended eventual behaviour (AFTER explicit approval):
  1. Verify live Shopify price still matches the analysis snapshot.
  2. Set compareAtPrice to the ORIGINAL normal selling price (snapshot current_price),
     unless an existing compare-at requires safe handling.
  3. Set price to the approved proposed_sale_price.
  4. Add product tag ``Sale`` without removing existing tags.

SAFETY (v1 — this file):
  - Default mode is dry-run.
  - Mutations are COMPLETELY DISABLED / commented out.
  - Even with --apply, this script will refuse to write.
  - Do not enable writes until the analysis report is explicitly approved.

Usage:
  ./venv/bin/python scripts/maintenance/sale_candidates_apply_dry_run.py \\
      --snapshot tmp/sale_candidates_20260922/eligible_snapshot.json \\
      --env .env.prod
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
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

# ---------------------------------------------------------------------------
# GraphQL — READ paths used by dry-run verification.
# WRITE mutations are intentionally commented out and not referenced.
# ---------------------------------------------------------------------------

VARIANT_NODES_QUERY = """
query SaleApplyVariantNodes($ids: [ID!]!, $locId: ID!) {
  nodes(ids: $ids) {
    ... on ProductVariant {
      id
      title
      barcode
      sku
      price
      compareAtPrice
      inventoryQuantity
      product {
        id
        title
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

# DISABLED — do not uncomment without explicit approval to enable Shopify writes.
# PRODUCT_VARIANTS_BULK_UPDATE = """
# mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
#   productVariantsBulkUpdate(productId: $productId, variants: $variants) {
#     productVariants { id price compareAtPrice }
#     userErrors { field message }
#   }
# }
# """
#
# TAGS_ADD = """
# mutation TagsAdd($id: ID!, $tags: [String!]!) {
#   tagsAdd(id: $id, tags: $tags) {
#     node { ... on Product { id tags } }
#     userErrors { field message }
#   }
# }
# """

MUTATIONS_ENABLED = False  # hard off
SALE_TAG = "Sale"


@dataclass
class SnapshotRow:
    product_title: str
    variant_title: str
    barcode: str
    sku: str
    shopify_product_id: str
    shopify_variant_id: str
    current_price: float
    compare_at_price: Optional[float]
    proposed_sale_price: float
    discount_pct: int
    available: int
    committed: int
    on_hand: int
    tags: str
    flags: str


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


def load_snapshot(path: Path) -> List[SnapshotRow]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    rows: List[SnapshotRow] = []
    for r in raw:
        rows.append(
            SnapshotRow(
                product_title=clean_text(r.get("product_title")) or "",
                variant_title=clean_text(r.get("variant_title")) or "",
                barcode=clean_text(r.get("barcode")) or "",
                sku=clean_text(r.get("sku")) or "",
                shopify_product_id=clean_text(r.get("shopify_product_id")) or "",
                shopify_variant_id=clean_text(r.get("shopify_variant_id")) or "",
                current_price=float(r["current_price"]),
                compare_at_price=_f(r.get("compare_at_price")),
                proposed_sale_price=float(r["proposed_sale_price"]),
                discount_pct=int(r["discount_pct"]),
                available=int(r["available"]),
                committed=int(r["committed"]),
                on_hand=int(r["on_hand"]),
                tags=clean_text(r.get("tags")) or "",
                flags=clean_text(r.get("flags")) or "",
            )
        )
    return rows


def chunked(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


def fetch_live(
    client: ShopifyClient, location_id: str, variant_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for batch in chunked(variant_ids, 40):
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
            qmap = _qty_map((node.get("inventoryItem") or {}).get("inventoryLevel"))
            product = node.get("product") or {}
            tags_raw = product.get("tags") or []
            if isinstance(tags_raw, str):
                tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
            else:
                tags = [clean_text(t) for t in tags_raw if clean_text(t)]
            out[node["id"]] = {
                "variant_id": node["id"],
                "product_id": product.get("id"),
                "product_title": product.get("title"),
                "variant_title": node.get("title"),
                "barcode": clean_text(node.get("barcode")) or "",
                "sku": clean_text(node.get("sku")) or "",
                "price": _f(node.get("price")),
                "compare_at_price": _f(node.get("compareAtPrice")),
                "inventory_quantity": _i(node.get("inventoryQuantity")),
                "available": qmap.get("available"),
                "committed": qmap.get("committed"),
                "on_hand": qmap.get("on_hand"),
                "tags": tags,
            }
        time.sleep(0.2)
    return out


def propose_compare_at(snap: SnapshotRow, live_compare: Optional[float]) -> tuple[Optional[float], str]:
    """
    Safe compare-at policy:
      - If blank: set to snapshot original retail (current_price at analysis time).
      - If existing equals snapshot original: keep.
      - If existing differs: skip compare-at change (manual review).
    """
    desired = snap.current_price
    if live_compare is None:
        return desired, "set_from_blank_to_original_retail"
    if abs(live_compare - desired) < 0.005:
        return live_compare, "keep_existing_matches_original"
    return None, f"skip_existing_compare_at_differs:{live_compare}"


def evaluate_row(snap: SnapshotRow, live: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ts = datetime.now(timezone.utc).isoformat()
    base = {
        "timestamp_utc": ts,
        "status": "",
        "reason": "",
        "product_title": snap.product_title,
        "barcode": snap.barcode,
        "sku": snap.sku,
        "shopify_product_id": snap.shopify_product_id,
        "shopify_variant_id": snap.shopify_variant_id,
        "snapshot_price": snap.current_price,
        "snapshot_compare_at": snap.compare_at_price,
        "snapshot_available": snap.available,
        "snapshot_committed": snap.committed,
        "proposed_sale_price": snap.proposed_sale_price,
        "discount_pct": snap.discount_pct,
        "live_price": None,
        "live_compare_at": None,
        "live_available": None,
        "live_committed": None,
        "old_price": None,
        "new_price": None,
        "old_compare_at": None,
        "new_compare_at": None,
        "existing_tags": "",
        "proposed_tags": "",
        "would_mutate": False,
        "mutations_enabled": MUTATIONS_ENABLED,
    }
    if live is None:
        base["status"] = "skip"
        base["reason"] = "variant_not_found_live"
        return base

    live_price = live.get("price")
    live_compare = live.get("compare_at_price")
    live_avail = live.get("available")
    live_committed = live.get("committed")
    tags = list(live.get("tags") or [])
    proposed_tags = tags if SALE_TAG in tags else tags + [SALE_TAG]

    base.update(
        {
            "live_price": live_price,
            "live_compare_at": live_compare,
            "live_available": live_avail,
            "live_committed": live_committed,
            "old_price": live_price,
            "old_compare_at": live_compare,
            "existing_tags": ",".join(tags),
            "proposed_tags": ",".join(proposed_tags),
        }
    )

    if live.get("product_id") != snap.shopify_product_id:
        base["status"] = "skip"
        base["reason"] = "product_id_mismatch"
        return base
    if live_price is None:
        base["status"] = "skip"
        base["reason"] = "live_price_missing"
        return base
    if abs(float(live_price) - float(snap.current_price)) > 0.005:
        base["status"] = "skip"
        base["reason"] = f"price_drift_live_{live_price}_snapshot_{snap.current_price}"
        return base
    if live_avail is None or live_committed is None:
        base["status"] = "skip"
        base["reason"] = "live_commitment_unavailable"
        return base
    if int(live_avail) <= 0:
        base["status"] = "skip"
        base["reason"] = "no_uncommitted_stock_live"
        return base
    # Material inventory / commitment change vs snapshot.
    if abs(int(live_avail) - int(snap.available)) > 0 or abs(int(live_committed) - int(snap.committed)) > 0:
        base["status"] = "skip"
        base["reason"] = (
            f"inventory_commitment_changed "
            f"avail {snap.available}->{live_avail} committed {snap.committed}->{live_committed}"
        )
        return base

    new_compare, compare_note = propose_compare_at(snap, live_compare)
    if new_compare is None:
        base["status"] = "skip"
        base["reason"] = compare_note
        return base

    base.update(
        {
            "status": "would_update",
            "reason": compare_note,
            "new_price": snap.proposed_sale_price,
            "new_compare_at": new_compare,
            "would_mutate": True,
        }
    )
    return base


def main() -> int:
    parser = argparse.ArgumentParser(description="Sale apply dry-run (mutations disabled)")
    parser.add_argument(
        "--snapshot",
        default=str(_REPO / "tmp/sale_candidates_20260922/eligible_snapshot.json"),
    )
    parser.add_argument("--env", default=str(_REPO / ".env.prod"))
    parser.add_argument(
        "--out",
        default=str(_REPO / "tmp/sale_candidates_20260922/apply_dry_run_log.json"),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Ignored in v1: mutations are hard-disabled.",
    )
    args = parser.parse_args()

    load_dotenv(args.env)
    if args.apply:
        print(
            "REFUSING: --apply was passed but MUTATIONS_ENABLED=False. "
            "No Shopify writes will occur.",
            file=sys.stderr,
        )

    location_id = shopify_inventory_location_id() or "gid://shopify/Location/78213775584"
    snapshot = load_snapshot(Path(args.snapshot))
    print(f"Loaded {len(snapshot)} snapshot rows from {args.snapshot}")
    print(f"Location {location_id}; mutations_enabled={MUTATIONS_ENABLED}")

    client = ShopifyClient()
    live_map = fetch_live(
        client, location_id, [r.shopify_variant_id for r in snapshot]
    )

    results = [evaluate_row(row, live_map.get(row.shopify_variant_id)) for row in snapshot]

    # -------------------------------------------------------------------------
    # WRITE PATH — intentionally unreachable / commented.
    # When eventually approved, implementation would:
    #   - group would_update rows by product_id
    #   - call productVariantsBulkUpdate with {id, price, compareAtPrice}
    #   - call tagsAdd with tag "Sale"
    # Those calls must remain commented until MUTATIONS_ENABLED is deliberately
    # flipped after human approval.
    # -------------------------------------------------------------------------
    # if MUTATIONS_ENABLED and args.apply:
    #     for ...:
    #         client.graphql(PRODUCT_VARIANTS_BULK_UPDATE, {...})
    #         client.graphql(TAGS_ADD, {...})
    # else:
    #     pass

    would = [r for r in results if r["status"] == "would_update"]
    skipped = [r for r in results if r["status"] == "skip"]
    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mutations_enabled": MUTATIONS_ENABLED,
        "apply_requested": bool(args.apply),
        "snapshot_rows": len(snapshot),
        "would_update": len(would),
        "skipped": len(skipped),
        "results": results,
        "example_changes": would[:5],
    }
    Path(args.out).write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(f"would_update={len(would)} skipped={len(skipped)}")
    print("Example dry-run changes (first 5):")
    for r in would[:5]:
        print(
            f"  {r['product_title'][:55]} | "
            f"price {r['old_price']} -> {r['new_price']} | "
            f"compareAt {r['old_compare_at']} -> {r['new_compare_at']} | "
            f"tags {r['existing_tags']!r} -> {r['proposed_tags']!r}"
        )
    print(f"Wrote {args.out}")
    print("NO SHOPIFY MUTATIONS EXECUTED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
