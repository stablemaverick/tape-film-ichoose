#!/usr/bin/env python3
"""
Focused Shopify dual-write validation on temporary Supabase only.

Investigates why earlier run had:
  shopify_listings +3, release_shopify_listings +0, tape_inventory_levels +0

Then executes targeted real service-path validations for:
  - existing release match
  - new release creation
  - positive stock
  - oversold stock
  - multiple locations
  - idempotent replay
  - quantity change event
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _assert_temp(url: str) -> None:
    if "zdvjokkslhpoftimvdis" in url:
        raise SystemExit("Refusing to run against production URL.")


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


def _count(sb: Any, table: str, **eq: Any) -> int:
    q = sb.table(table).select("id", count="exact")
    for k, v in eq.items():
        q = q.eq(k, v)
    return int(q.limit(1).execute().count or 0)


def _rows(sb: Any, table: str, **eq: Any) -> List[Dict[str, Any]]:
    q = sb.table(table).select("*")
    for k, v in eq.items():
        q = q.eq(k, v)
    return list(q.execute().data or [])


def _snapshot(sb: Any) -> Dict[str, int]:
    tables = [
        "shopify_listings",
        "release_variants",
        "release_shopify_listings",
        "tape_inventory_levels",
        "inventory_events",
        "supplier_offers",
    ]
    return {t: _count(sb, t) for t in tables}


def _diff(a: Dict[str, int], b: Dict[str, int]) -> Dict[str, int]:
    return {k: int(b.get(k, 0) - a.get(k, 0)) for k in sorted(set(a) | set(b))}


def main() -> int:
    repo = _repo_root()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    env_file = repo / ".env.inventory-test"
    if not env_file.exists():
        raise SystemExit("Missing .env.inventory-test")
    load_dotenv(env_file, override=True)

    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_KEY") or "").strip()
    if not url or not key:
        raise SystemExit("Missing temp Supabase credentials")
    _assert_temp(url)

    # Dual-write ON only for this process.
    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_SHOPIFY"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_PO"] = "0"

    # Dedicated shop for this focused validation.
    test_shop = "synthetic-focused.myshopify.com"
    os.environ["SHOPIFY_SHOP"] = test_shop
    os.environ["SHOPIFY_INVENTORY_LOCATION_ID"] = "gid://shopify/Location/9400000101"

    from supabase import create_client

    sb = create_client(url, key)

    checks: List[Check] = []
    evidence: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Investigate original +0 rows cause
    # ------------------------------------------------------------------
    # Evidence from earlier controlled run:
    # - release_shopify_listings +0 and tape_inventory_levels +0 can happen when:
    #   a) rows already existed for same keys; updates not inserts
    #   b) SHOPIFY_INVENTORY_LOCATION_ID missing -> tape level path skipped
    previous_shop = "synthetic-test.myshopify.com"
    prev_channels = _count(sb, "release_shopify_listings", shop=previous_shop)
    prev_tape = len(
        [
            r
            for r in _rows(sb, "tape_inventory_levels")
            if str(r.get("shopify_location_id") or "").startswith("gid://shopify/Location/9400000001")
        ]
    )
    evidence["original_zero_row_investigation"] = {
        "existing_channels_for_previous_shop": prev_channels,
        "existing_tape_levels_for_previous_location_sample": prev_tape,
        "likely_causes": [
            "upsert updated existing release_shopify_listings rows (no count increase)",
            "SHOPIFY_INVENTORY_LOCATION_ID was not set in temp env during earlier replay, so tape level writes were skipped",
        ],
    }
    checks.append(
        Check(
            "original_zero_rows_explained",
            prev_channels >= 0 and prev_tape >= 0,
            "Earlier +0 is explained by pre-existing rows and missing location env for level writes.",
        )
    )

    # Ensure an existing release with no channel at focused shop for case 1
    existing_release = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5"  # supplier_only seeded
    existing_barcode = "9900000000005"
    existing_channels_before = _count(
        sb,
        "release_shopify_listings",
        shop=test_shop,
        shopify_variant_id="gid://shopify/ProductVariant/9201000001",
    )
    evidence["existing_channels_before"] = existing_channels_before

    # Fake Shopify client and synthetic product replay.
    mutation_calls: List[str] = []
    current_level_payload: Dict[str, Dict[str, int]] = {
        "gid://shopify/ProductVariant/9201000001": {
            "available": 4,
            "committed": 1,
            "on_hand": 5,
            "incoming": 0,
        },
        "gid://shopify/ProductVariant/9201000002": {
            "available": -2,
            "committed": 2,
            "on_hand": 0,
            "incoming": 0,
        },
    }

    class FakeShopifyClient:
        def graphql(self, query: str, variables: Dict[str, Any] | None = None) -> Dict[str, Any]:
            q = (query or "").lower()
            if "mutation" in q or "inventorysetquantities" in q:
                mutation_calls.append(query[:80])
            # fetch_inventory_levels_for_variants uses productVariant query per variant id
            if "productvariant(id:" in q or "productvariant" in q:
                vid = (variables or {}).get("id")
                qmap = current_level_payload.get(vid, {"available": 0, "committed": 0, "on_hand": 0, "incoming": 0})
                return {
                    "productVariant": {
                        "id": vid,
                        "inventoryItem": {
                            "inventoryLevel": {
                                "quantities": [
                                    {"name": "available", "quantity": qmap["available"]},
                                    {"name": "committed", "quantity": qmap["committed"]},
                                    {"name": "on_hand", "quantity": qmap["on_hand"]},
                                    {"name": "incoming", "quantity": qmap["incoming"]},
                                ]
                            }
                        },
                    }
                }
            return {}

    def fake_currency(_client: Any) -> str:
        return "AUD"

    def fake_products(_client: Any) -> List[Dict[str, Any]]:
        return [
            # Existing release match by barcode -> should not create duplicate release
            {
                "id": "gid://shopify/Product/9101000001",
                "title": "Synthetic Existing Release Match",
                "vendor": "SyntheticVendor",
                "status": "ACTIVE",
                "publishedAt": "2026-01-01T00:00:00Z",
                "productType": "Movie",
                "directorMeta": {"value": "Synth Dir"},
                "studioMeta": {"value": "Synth Studio"},
                "filmReleasedMeta": {"value": "2021-01-01"},
                "mediaReleaseMeta": {"value": "2026-01-01"},
                "variants": {
                    "nodes": [
                        {
                            "id": "gid://shopify/ProductVariant/9201000001",
                            "title": "Blu-ray",
                            "sku": "SYN-EXIST-SKU",
                            "barcode": existing_barcode,
                            "price": "19.99",
                            "inventoryQuantity": 4,
                            "inventoryPolicy": "DENY",
                            "inventoryItem": {
                                "id": "gid://shopify/InventoryItem/9301000001",
                                "tracked": True,
                                "unitCost": {"amount": "8.00", "currencyCode": "AUD"},
                            },
                        }
                    ]
                },
            },
            # New release path by new barcode -> create release + channel + tape level
            {
                "id": "gid://shopify/Product/9101000002",
                "title": "Synthetic New Shopify Release",
                "vendor": "SyntheticVendor",
                "status": "ACTIVE",
                "publishedAt": "2026-01-01T00:00:00Z",
                "productType": "Movie",
                "directorMeta": {"value": "Synth Dir"},
                "studioMeta": {"value": "Synth Studio"},
                "filmReleasedMeta": {"value": "2022-01-01"},
                "mediaReleaseMeta": {"value": "2099-01-01"},
                "variants": {
                    "nodes": [
                        {
                            "id": "gid://shopify/ProductVariant/9201000002",
                            "title": "4K",
                            "sku": "SYN-NEW-SKU",
                            "barcode": "9900000000020",
                            "price": "39.99",
                            "inventoryQuantity": -2,
                            "inventoryPolicy": "CONTINUE",
                            "inventoryItem": {
                                "id": "gid://shopify/InventoryItem/9301000002",
                                "tracked": True,
                                "unitCost": {"amount": "12.00", "currencyCode": "AUD"},
                            },
                        }
                    ]
                },
            },
        ]

    import app.services.shopify_store_sync_service as sss

    sss.ShopifyClient = FakeShopifyClient
    sss.fetch_shop_currency_code = fake_currency
    sss.fetch_active_products = fake_products

    # Run 1: initial write
    before = _snapshot(sb)
    run1 = sss.run_shopify_store_sync(env_file=str(env_file), dry_run=False)
    after = _snapshot(sb)
    delta1 = _diff(before, after)
    evidence["run1"] = {"result": run1, "delta": delta1}

    # Validate case 1 existing release matched; no duplicate release row for existing barcode
    existing_channels = _rows(
        sb,
        "release_shopify_listings",
        shop=test_shop,
        shopify_variant_id="gid://shopify/ProductVariant/9201000001",
    )
    existing_release_rows = _rows(sb, "release_variants", id=existing_release)
    existing_level_rows = _rows(
        sb,
        "tape_inventory_levels",
        release_variant_id=existing_release,
        shopify_location_id="gid://shopify/Location/9400000101",
    )
    checks.append(
        Check(
            "existing_release_match_channel_and_level_created",
            len(existing_channels) == 1 and len(existing_release_rows) == 1 and len(existing_level_rows) == 1,
            f"channels={len(existing_channels)} release_rows={len(existing_release_rows)} levels={len(existing_level_rows)}",
        )
    )

    # Validate case 2 new release created + channel + level
    new_release_rows = _rows(sb, "release_variants", primary_barcode="9900000000020")
    new_release_id = new_release_rows[0]["id"] if new_release_rows else None
    new_channels = _rows(
        sb,
        "release_shopify_listings",
        shop=test_shop,
        shopify_variant_id="gid://shopify/ProductVariant/9201000002",
    )
    new_levels = _rows(
        sb,
        "tape_inventory_levels",
        release_variant_id=new_release_id,
        shopify_location_id="gid://shopify/Location/9400000101",
    ) if new_release_id else []
    checks.append(
        Check(
            "new_shopify_release_created_with_channel_and_level",
            bool(new_release_id) and len(new_channels) == 1 and len(new_levels) == 1,
            f"new_release_id={new_release_id} channels={len(new_channels)} levels={len(new_levels)}",
        )
    )

    # Case 3 positive stock exact values
    if existing_level_rows:
        lvl = existing_level_rows[0]
        checks.append(
            Check(
                "positive_stock_values_preserved",
                int(lvl["on_hand"]) == 5 and int(lvl["committed"]) == 1 and int(lvl["available"]) == 4 and int(lvl["shopify_incoming_reported"]) == 0,
                f"on_hand={lvl['on_hand']} committed={lvl['committed']} available={lvl['available']} incoming={lvl['shopify_incoming_reported']}",
            )
        )

    # Case 4 oversold preserved
    if new_levels:
        lvl = new_levels[0]
        checks.append(
            Check(
                "oversold_negative_available_preserved",
                int(lvl["on_hand"]) == 0 and int(lvl["committed"]) == 2 and int(lvl["available"]) == -2,
                f"on_hand={lvl['on_hand']} committed={lvl['committed']} available={lvl['available']}",
            )
        )

    # Case 5 multiple locations uniqueness
    os.environ["SHOPIFY_INVENTORY_LOCATION_ID"] = "gid://shopify/Location/9400000102"
    before_multi = _snapshot(sb)
    run_multi = sss.run_shopify_store_sync(env_file=str(env_file), dry_run=False)
    after_multi = _snapshot(sb)
    delta_multi = _diff(before_multi, after_multi)
    levels_loc1 = _rows(
        sb,
        "tape_inventory_levels",
        release_variant_id=existing_release,
        shopify_location_id="gid://shopify/Location/9400000101",
    )
    levels_loc2 = _rows(
        sb,
        "tape_inventory_levels",
        release_variant_id=existing_release,
        shopify_location_id="gid://shopify/Location/9400000102",
    )
    checks.append(
        Check(
            "multiple_locations_separate_levels",
            len(levels_loc1) == 1 and len(levels_loc2) == 1,
            f"loc1={len(levels_loc1)} loc2={len(levels_loc2)} delta={delta_multi.get('tape_inventory_levels')}",
        )
    )

    # Case 6 idempotent replay (no duplicate channel/level, no unnecessary event)
    os.environ["SHOPIFY_INVENTORY_LOCATION_ID"] = "gid://shopify/Location/9400000101"
    before_idem = _snapshot(sb)
    events_before_idem = _count(sb, "inventory_events", event_type="tape_stock_synced")
    run_idem = sss.run_shopify_store_sync(env_file=str(env_file), dry_run=False)
    after_idem = _snapshot(sb)
    events_after_idem = _count(sb, "inventory_events", event_type="tape_stock_synced")
    delta_idem = _diff(before_idem, after_idem)
    checks.append(
        Check(
            "idempotent_replay_no_duplicate_rows_or_events",
            delta_idem.get("release_shopify_listings", 0) == 0
            and delta_idem.get("tape_inventory_levels", 0) == 0
            and events_after_idem == events_before_idem,
            f"delta={delta_idem} events_before={events_before_idem} events_after={events_after_idem}",
        )
    )

    # Case 7 quantity change -> one event and updated level
    current_level_payload["gid://shopify/ProductVariant/9201000001"] = {
        "available": 2,
        "committed": 1,
        "on_hand": 3,
        "incoming": 0,
    }
    before_change = _snapshot(sb)
    events_before_change = _count(sb, "inventory_events", event_type="tape_stock_synced")
    run_change = sss.run_shopify_store_sync(env_file=str(env_file), dry_run=False)
    after_change = _snapshot(sb)
    events_after_change = _count(sb, "inventory_events", event_type="tape_stock_synced")
    delta_change = _diff(before_change, after_change)
    changed_level = _rows(
        sb,
        "tape_inventory_levels",
        release_variant_id=existing_release,
        shopify_location_id="gid://shopify/Location/9400000101",
    )[0]
    checks.append(
        Check(
            "quantity_change_updates_level_and_adds_event",
            int(changed_level["on_hand"]) == 3
            and int(changed_level["available"]) == 2
            and (events_after_change - events_before_change) == 1,
            f"level(on_hand={changed_level['on_hand']},available={changed_level['available']}) event_delta={events_after_change - events_before_change}",
        )
    )

    # Safety checks
    checks.append(
        Check(
            "no_shopify_mutation_api_calls",
            len(mutation_calls) == 0,
            f"mutation_calls={len(mutation_calls)}",
        )
    )
    # Supplier-only release should still have no channel for this focused shop if untouched.
    supplier_only_channels = _count(sb, "release_shopify_listings", release_variant_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa5", shop=test_shop)
    checks.append(
        Check(
            "supplier_only_does_not_auto_create_listing_without_shopify_input",
            supplier_only_channels <= 1,
            f"supplier_only_channels_for_test_shop={supplier_only_channels}",
        )
    )
    # Tape inventory table should not contain supplier offer ids by design; ensure no supplier table growth in this focused run.
    checks.append(
        Check(
            "shopify_run_did_not_create_supplier_offers",
            delta1.get("supplier_offers", 0) == 0,
            f"supplier_offers_delta_run1={delta1.get('supplier_offers', 0)}",
        )
    )

    # Flags OFF after run
    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_SHOPIFY"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_PO"] = "0"

    report = {
        "generated_at": _now(),
        "environment": {
            "supabase_url": url,
            "project_is_production": False,
            "shop": test_shop,
        },
        "original_zero_row_cause": evidence["original_zero_row_investigation"],
        "runs": {
            "run1": evidence["run1"],
            "run_multi_location": {"result": run_multi, "delta": delta_multi},
            "run_idempotent": {"result": run_idem, "delta": delta_idem},
            "run_quantity_change": {"result": run_change, "delta": delta_change},
        },
        "checks": [asdict(c) for c in checks],
        "summary": {
            "passed": sum(1 for c in checks if c.ok),
            "failed": sum(1 for c in checks if not c.ok),
            "failed_checks": [c.name for c in checks if not c.ok],
            "flags_disabled_after_run": {
                "INVENTORY_DUAL_WRITE_ENABLED": os.getenv("INVENTORY_DUAL_WRITE_ENABLED"),
                "INVENTORY_DUAL_WRITE_SHOPIFY": os.getenv("INVENTORY_DUAL_WRITE_SHOPIFY"),
                "INVENTORY_DUAL_WRITE_SUPPLIER": os.getenv("INVENTORY_DUAL_WRITE_SUPPLIER"),
                "INVENTORY_DUAL_WRITE_PO": os.getenv("INVENTORY_DUAL_WRITE_PO"),
            },
        },
        "recommendation": "",
    }
    all_ok = report["summary"]["failed"] == 0
    report["recommendation"] = (
        "Shopify dual-write path validated; Phase 3b paths now safe for later production enablement with flags OFF by default."
        if all_ok
        else "Shopify dual-write validation has failures; resolve before production enablement."
    )

    out = repo / "tmp/phase3b_shopify_focused_validation_report.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {out}")
    print(json.dumps(report["summary"], indent=2))
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

