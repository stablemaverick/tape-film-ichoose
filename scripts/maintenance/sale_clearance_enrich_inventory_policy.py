#!/usr/bin/env python3
"""
Enrich clearance-sale final dry-run manifest with live inventoryPolicy.

READ-ONLY. MUTATIONS_ENABLED = False. No Shopify writes.

Shopify field (Admin GraphQL, API 2026-04 as used by ShopifyClient):
  ProductVariant.inventoryPolicy  enum: DENY | CONTINUE

Future mutation (already used in app/services/arrow_inventory_policy_sync_service.py):
  mutation ProductVariantsBulkUpdate(
    $productId: ID!,
    $variants: [ProductVariantsBulkInput!]!
  ) {
    productVariantsBulkUpdate(productId: $productId, variants: $variants) {
      productVariants { id inventoryPolicy price compareAtPrice }
      userErrors { field message }
    }
  }
  variables.variants[] may include:
    { id, price, compareAtPrice, inventoryPolicy: DENY }

Tags via separate tagsAdd (preserve existing; add "Sale").
"""
from __future__ import annotations

import argparse
import csv
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

MUTATIONS_ENABLED = False
TARGET_POLICY = "DENY"
SALE_TAG = "Sale"

EXECUTABLE_GROUPS = {
    "SALE_20",
    "SALE_15",
    "SALE_10",
    "SMALL_LOSS_CLEARANCE",
    "EXISTING_SALE_KEEP_PRICE",
}

VARIANT_NODES_QUERY = """
query ClearancePolicyEnrich($ids: [ID!]!) {
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
      }
      inventoryItem {
        tracked
        unitCost { amount currencyCode }
      }
    }
  }
}
"""

# Documented future mutation — NOT executed.
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


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def chunked(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


def tags_list(raw: Any) -> List[str]:
    if isinstance(raw, str):
        return [t.strip() for t in raw.split(",") if t.strip()]
    if isinstance(raw, list):
        return [clean_text(t) for t in raw if clean_text(t)]
    return []


def fetch_live_policies(
    client: ShopifyClient, variant_ids: List[str]
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for batch in chunked(variant_ids, 40):
        tries = 0
        while True:
            tries += 1
            try:
                data = client.graphql(VARIANT_NODES_QUERY, {"ids": batch})
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
            policy = (clean_text(node.get("inventoryPolicy")) or "").upper()
            out[node["id"]] = {
                "variant_id": node["id"],
                "product_id": product.get("id"),
                "product_title": product.get("title"),
                "status": (clean_text(product.get("status")) or "").upper(),
                "tags": tags_list(product.get("tags")),
                "sku": clean_text(node.get("sku")) or "",
                "barcode": clean_text(node.get("barcode")) or "",
                "price": _f(node.get("price")),
                "compare_at_price": _f(node.get("compareAtPrice")),
                "inventory_policy": policy,
                "inventory_quantity": node.get("inventoryQuantity"),
                "tracked": ((node.get("inventoryItem") or {}).get("tracked")),
            }
        time.sleep(0.2)
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=str(_REPO / ".env.prod"))
    parser.add_argument(
        "--manifest",
        default=str(
            _REPO / "tmp/sale_candidates_20260922/clearance_sale_final_manifest.json"
        ),
    )
    parser.add_argument(
        "--out-dir",
        default=str(_REPO / "tmp/sale_candidates_20260922"),
    )
    args = parser.parse_args()

    load_dotenv(args.env)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    executable = [
        r for r in (payload.get("executable") or []) if r.get("group") in EXECUTABLE_GROUPS
    ]
    print(f"Loaded {len(executable)} executable rows; MUTATIONS_ENABLED={MUTATIONS_ENABLED}")

    # Hard excludes that must never be touched
    forbidden_vids = {
        "gid://shopify/ProductVariant/51368090140896",  # Infernal Affairs UNLISTED
    }
    for r in executable:
        if r.get("shopify_variant_id") in forbidden_vids:
            raise SystemExit(f"FATAL: forbidden variant in executable: {r}")

    client = ShopifyClient()
    live_map = fetch_live_policies(
        client, [r["shopify_variant_id"] for r in executable]
    )

    enriched: List[Dict[str, Any]] = []
    policy_review: List[Dict[str, Any]] = []
    currently_deny = 0
    currently_continue = 0
    unknown_policy = 0

    price_changes = 0
    compare_at_changes = 0
    sale_tag_required = 0
    policy_changes = 0

    for row in executable:
        vid = row["shopify_variant_id"]
        live = live_map.get(vid)
        out = dict(row)
        out["target_inventory_policy"] = TARGET_POLICY
        out["mutations_enabled"] = MUTATIONS_ENABLED
        out["policy_enriched_at"] = datetime.now(timezone.utc).isoformat()

        if live is None:
            out["current_inventory_policy"] = ""
            out["inventory_policy_change_required"] = "UNKNOWN"
            out["inventory_policy_status"] = "INVENTORY_POLICY_REVIEW"
            out["inventory_policy_notes"] = "variant_not_found_on_live_refetch"
            out["executable"] = False
            policy_review.append(out)
            unknown_policy += 1
            continue

        policy = live.get("inventory_policy") or ""
        out["current_inventory_policy"] = policy
        out["live_product_status"] = live.get("status")
        out["live_tracked"] = live.get("tracked")
        out["live_price"] = live.get("price")
        out["live_compare_at"] = live.get("compare_at_price")
        out["live_tags"] = ",".join(live.get("tags") or [])

        # Rollback snapshot fields (capture only — no write)
        out["rollback_original_price"] = live.get("price")
        out["rollback_original_compare_at"] = live.get("compare_at_price")
        out["rollback_original_tags"] = list(live.get("tags") or [])
        out["rollback_original_inventory_policy"] = policy

        if policy not in {"DENY", "CONTINUE"}:
            out["inventory_policy_change_required"] = "UNKNOWN"
            out["inventory_policy_status"] = "INVENTORY_POLICY_REVIEW"
            out["inventory_policy_notes"] = f"unrecognised_or_missing_policy:{policy!r}"
            out["executable"] = False
            policy_review.append(out)
            unknown_policy += 1
            continue

        if live.get("status") != "ACTIVE":
            out["inventory_policy_change_required"] = "YES" if policy != TARGET_POLICY else "NO"
            out["inventory_policy_status"] = "INVENTORY_POLICY_REVIEW"
            out["inventory_policy_notes"] = f"product_status_{live.get('status')}"
            out["executable"] = False
            policy_review.append(out)
            continue

        if live.get("tracked") is False:
            # Untracked inventory: DENY/CONTINUE semantics unsafe for clearance.
            out["inventory_policy_change_required"] = "UNKNOWN"
            out["inventory_policy_status"] = "INVENTORY_POLICY_REVIEW"
            out["inventory_policy_notes"] = "inventory_item_not_tracked"
            out["executable"] = False
            policy_review.append(out)
            unknown_policy += 1
            continue

        if policy == "DENY":
            currently_deny += 1
            out["inventory_policy_change_required"] = "NO"
        else:
            currently_continue += 1
            out["inventory_policy_change_required"] = "YES"
            policy_changes += 1

        out["inventory_policy_status"] = "OK"
        out["inventory_policy_notes"] = ""

        # Eventual change tallies (dry-run intent)
        group = row.get("group")
        cur_price = _f(row.get("current_price"))
        tgt_price = _f(row.get("target_sale_price"))
        cur_cmp = _f(row.get("current_compare_at")) if row.get("current_compare_at") not in ("", None) else None
        tgt_cmp = _f(row.get("target_compare_at")) if row.get("target_compare_at") not in ("", None) else None

        if group in {"SALE_20", "SALE_15", "SALE_10", "SMALL_LOSS_CLEARANCE"}:
            if cur_price is not None and tgt_price is not None and abs(cur_price - tgt_price) > 0.005:
                price_changes += 1
                out["price_change_required"] = "YES"
            else:
                out["price_change_required"] = "NO"
            # compare-at: blank -> original price
            if cur_cmp is None and tgt_cmp is not None:
                compare_at_changes += 1
                out["compare_at_change_required"] = "YES"
            elif cur_cmp is not None and tgt_cmp is not None and abs(cur_cmp - tgt_cmp) > 0.005:
                compare_at_changes += 1
                out["compare_at_change_required"] = "YES"
            else:
                out["compare_at_change_required"] = "NO"
        else:
            # EXISTING_SALE_KEEP_PRICE
            out["price_change_required"] = "NO"
            out["compare_at_change_required"] = "NO"

        tags = live.get("tags") or []
        if SALE_TAG not in tags:
            sale_tag_required += 1
            out["sale_tag_change_required"] = "YES"
        else:
            out["sale_tag_change_required"] = "NO"

        # Future eventual actions (documented, not executed)
        if group == "EXISTING_SALE_KEEP_PRICE":
            out["eventual_actions"] = [
                "preserve_price",
                "preserve_compare_at",
                "tagsAdd:Sale_if_missing",
                "inventoryPolicy:DENY",
            ]
        else:
            out["eventual_actions"] = [
                "set_price:approved_sale_price",
                "set_compareAtPrice:original_current_price",
                "tagsAdd:Sale_if_missing",
                "inventoryPolicy:DENY",
            ]

        enriched.append(out)

    # Still include policy-review rows in enrichment output separately
    all_rows = enriched + policy_review

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mutations_enabled": MUTATIONS_ENABLED,
        "source_manifest": str(args.manifest),
        "executable_input_count": len(executable),
        "executable_after_policy_ok": len(enriched),
        "inventory_policy_review_count": len(policy_review),
        "currently_deny": currently_deny,
        "currently_continue": currently_continue,
        "unknown_or_unverified_policy": unknown_policy,
        "price_changes_eventually_required": price_changes,
        "compare_at_changes_eventually_required": compare_at_changes,
        "sale_tags_eventually_required": sale_tag_required,
        "inventory_policy_changes_eventually_required": policy_changes,
        "shopify_api_fields": {
            "read": "ProductVariant.inventoryPolicy",
            "write_mutation": "productVariantsBulkUpdate",
            "write_input": "ProductVariantsBulkInput.inventoryPolicy",
            "allowed_values": ["DENY", "CONTINUE"],
            "codebase_reference": "app/services/arrow_inventory_policy_sync_service.py",
            "tags_mutation": "tagsAdd(id, tags) — additive; never replace full tag set",
        },
        "future_execution_sequence": [
            "re-fetch live variant",
            "verify ACTIVE",
            "verify price matches snapshot",
            "verify compare-at matches expected snapshot",
            "verify free inventory > 0",
            "verify not future/preorder",
            "verify inventoryPolicy readable",
            "capture rollback: price, compareAt, tags[], inventoryPolicy",
            "apply approved price/compareAt/tagsAdd/inventoryPolicy=DENY",
            "post-mutation re-fetch verification of all fields",
        ],
        "rollback_snapshot_fields": [
            "rollback_original_price",
            "rollback_original_compare_at",
            "rollback_original_tags",
            "rollback_original_inventory_policy",
        ],
        "held_groups_untouched": [
            "LARGE_LOSS_REVIEW",
            "FUTURE_RELEASE_OR_PREORDER",
            "UNLISTED_EXCLUDED",
            "SNAPSHOT_CHANGED_REVIEW",
            "AMBIGUOUS_VARIANT_REVIEW",
            "INVENTORY_POLICY_REVIEW",
        ],
        "forbidden_variants_never_touch": list(forbidden_vids),
        "inventory_policy_review": policy_review,
        "executable_enriched": enriched,
    }

    # Write enriched JSON (merge into final manifest structure)
    enriched_payload = dict(payload)
    enriched_payload["policy_enrichment"] = {
        k: v
        for k, v in summary.items()
        if k not in {"executable_enriched", "inventory_policy_review"}
    }
    enriched_payload["executable"] = enriched
    enriched_payload["inventory_policy_review"] = policy_review
    enriched_payload["mutations_enabled"] = False
    (out_dir / "clearance_sale_final_manifest_with_policy.json").write_text(
        json.dumps(enriched_payload, indent=2, default=str), encoding="utf-8"
    )
    (out_dir / "clearance_sale_policy_enrichment_summary.json").write_text(
        json.dumps(
            {k: v for k, v in summary.items() if k not in {"executable_enriched"}},
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    # CSV
    fields = [
        "group",
        "action",
        "shopify_product_id",
        "shopify_variant_id",
        "title",
        "sku",
        "barcode",
        "available_free_qty",
        "cost",
        "current_price",
        "target_sale_price",
        "current_compare_at",
        "target_compare_at",
        "current_tags",
        "target_tags",
        "current_inventory_policy",
        "target_inventory_policy",
        "inventory_policy_change_required",
        "price_change_required",
        "compare_at_change_required",
        "sale_tag_change_required",
        "inventory_policy_status",
        "inventory_policy_notes",
        "rollback_original_price",
        "rollback_original_compare_at",
        "rollback_original_inventory_policy",
        "effective_discount_pct",
        "gp_per_unit_after_sale",
        "product_status",
        "notes",
        "executable",
    ]
    with (out_dir / "clearance_sale_final_manifest_with_policy.csv").open(
        "w", newline="", encoding="utf-8"
    ) as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in enriched:
            # serialize rollback tags list if present in csv as joined
            row = dict(r)
            if isinstance(row.get("rollback_original_tags"), list):
                row["rollback_original_tags"] = ",".join(row["rollback_original_tags"])
            w.writerow(row)

    if policy_review:
        with (out_dir / "clearance_sale_inventory_policy_review.csv").open(
            "w", newline="", encoding="utf-8"
        ) as fh:
            w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            for r in policy_review:
                row = dict(r)
                if isinstance(row.get("rollback_original_tags"), list):
                    row["rollback_original_tags"] = ",".join(row["rollback_original_tags"])
                w.writerow(row)

    # Markdown summary snippet
    md = []
    md.append("# Clearance Sale — Inventory Policy Enrichment")
    md.append("")
    md.append("**MUTATIONS_ENABLED=`False` — NO SHOPIFY CHANGES MADE**")
    md.append("")
    md.append("## Shopify API")
    md.append("")
    md.append("- **Read:** `ProductVariant.inventoryPolicy` (`DENY` | `CONTINUE`)")
    md.append("- **Write (future):** `productVariantsBulkUpdate` → `ProductVariantsBulkInput.inventoryPolicy`")
    md.append("- **Codebase reference:** `app/services/arrow_inventory_policy_sync_service.py`")
    md.append("- **Tags (future):** `tagsAdd` (additive only)")
    md.append("")
    md.append("## Counts")
    md.append("")
    md.append(f"- Executable variants (input): **{len(executable)}**")
    md.append(f"- Currently DENY: **{currently_deny}**")
    md.append(f"- Currently CONTINUE: **{currently_continue}**")
    md.append(f"- Inventory policy review: **{len(policy_review)}**")
    md.append(f"- Price changes eventually required: **{price_changes}**")
    md.append(f"- Compare-at changes eventually required: **{compare_at_changes}**")
    md.append(f"- Sale tags eventually required: **{sale_tag_required}**")
    md.append(f"- Inventory policy changes eventually required: **{policy_changes}**")
    md.append("")
    md.append("Target for every executable variant: `inventoryPolicy = DENY`.")
    md.append("")
    md.append("Rollback snapshot now includes original `inventoryPolicy`.")
    md.append("")
    md.append("CLEARANCE SALE PREPARATION COMPLETE — NO SHOPIFY CHANGES MADE")
    (out_dir / "CLEARANCE_SALE_POLICY_ENRICHMENT.md").write_text(
        "\n".join(md), encoding="utf-8"
    )

    print("\nCLEARANCE SALE PREPARATION COMPLETE — NO SHOPIFY CHANGES MADE")
    print()
    print(f"Executable variants: {len(executable)}")
    print(f"Currently DENY: {currently_deny}")
    print(f"Currently CONTINUE: {currently_continue}")
    print(f"Inventory policy review: {len(policy_review)}")
    print(f"Price changes eventually required: {price_changes}")
    print(f"Compare-at changes eventually required: {compare_at_changes}")
    print(f"Sale tags eventually required: {sale_tag_required}")
    print(f"Inventory policy changes eventually required: {policy_changes}")
    if policy_review:
        print("\nINVENTORY_POLICY_REVIEW rows:")
        for r in policy_review:
            print(
                f"  {r.get('title')} | vid={r.get('shopify_variant_id')} | "
                f"{r.get('inventory_policy_notes')}"
            )
    print(f"\nWrote {out_dir / 'clearance_sale_final_manifest_with_policy.csv'}")
    print(f"Wrote {out_dir / 'clearance_sale_final_manifest_with_policy.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
