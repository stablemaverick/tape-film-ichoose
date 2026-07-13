"""Tests for Shopify inventory settings audit helpers."""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.services.shopify_inventory_settings_audit import (
    ISSUE_MISSING_INVENTORY_ITEM,
    ISSUE_MISSING_LOCATION_INVENTORY,
    ISSUE_POLICY_SHOULD_BE_CONTINUE,
    ISSUE_POLICY_SHOULD_BE_DENY,
    ISSUE_TRACKING_DISABLED,
    apply_repairs,
    classify_variant_issues,
    expected_inventory_policy,
    parse_shopify_bool_metafield,
    proposed_repairs_for_row,
    row_from_product_variant,
    run_audit,
)


def test_parse_shopify_bool_metafield_strings_and_missing():
    assert parse_shopify_bool_metafield(None) is False
    assert parse_shopify_bool_metafield("") is False
    assert parse_shopify_bool_metafield("true") is True
    assert parse_shopify_bool_metafield("TRUE") is True
    assert parse_shopify_bool_metafield("false") is False
    assert parse_shopify_bool_metafield("False") is False
    assert parse_shopify_bool_metafield(True) is True
    assert parse_shopify_bool_metafield(False) is False


def test_expected_policy_preorder_or_backorder_continue_else_deny():
    assert expected_inventory_policy(pre_order=True, backorder=False) == "CONTINUE"
    assert expected_inventory_policy(pre_order=False, backorder=True) == "CONTINUE"
    assert expected_inventory_policy(pre_order=True, backorder=True) == "CONTINUE"
    assert expected_inventory_policy(pre_order=False, backorder=False) == "DENY"


def test_preorder_with_deny_flagged():
    issues = classify_variant_issues(
        tracked=True,
        inventory_policy="DENY",
        inventory_item_id="gid://shopify/InventoryItem/1",
        has_location_level=True,
        expected_policy="CONTINUE",
        sku="SKU1",
        barcode="111",
    )
    assert ISSUE_POLICY_SHOULD_BE_CONTINUE in issues
    assert ISSUE_POLICY_SHOULD_BE_DENY not in issues


def test_backorder_with_deny_flagged():
    issues = classify_variant_issues(
        tracked=True,
        inventory_policy="DENY",
        inventory_item_id="gid://shopify/InventoryItem/1",
        has_location_level=True,
        expected_policy=expected_inventory_policy(pre_order=False, backorder=True),
        sku="SKU1",
        barcode="111",
    )
    assert ISSUE_POLICY_SHOULD_BE_CONTINUE in issues


def test_normal_instock_with_stale_continue_flagged():
    issues = classify_variant_issues(
        tracked=True,
        inventory_policy="CONTINUE",
        inventory_item_id="gid://shopify/InventoryItem/1",
        has_location_level=True,
        expected_policy="DENY",
        sku="SKU1",
        barcode="111",
    )
    assert ISSUE_POLICY_SHOULD_BE_DENY in issues


def test_normal_product_with_deny_is_correct():
    issues = classify_variant_issues(
        tracked=True,
        inventory_policy="DENY",
        inventory_item_id="gid://shopify/InventoryItem/1",
        has_location_level=True,
        expected_policy="DENY",
        sku="SKU1",
        barcode="111",
    )
    assert ISSUE_POLICY_SHOULD_BE_DENY not in issues
    assert ISSUE_POLICY_SHOULD_BE_CONTINUE not in issues
    assert ISSUE_TRACKING_DISABLED not in issues


def test_tracking_disabled_flagged():
    issues = classify_variant_issues(
        tracked=False,
        inventory_policy="DENY",
        inventory_item_id="gid://shopify/InventoryItem/1",
        has_location_level=True,
        expected_policy="DENY",
        sku="SKU1",
        barcode="111",
    )
    assert ISSUE_TRACKING_DISABLED in issues


def test_missing_metafields_default_false_via_row_parser():
    product = {
        "id": "gid://shopify/Product/1",
        "title": "Test",
        "status": "ACTIVE",
        "productType": "",
        "preOrder": None,
        "preorderAlt": None,
        "backorder": None,
    }
    variant = {
        "id": "gid://shopify/ProductVariant/1",
        "title": "Default Title",
        "sku": "S1",
        "barcode": "999",
        "inventoryPolicy": "DENY",
        "inventoryItem": {
            "id": "gid://shopify/InventoryItem/1",
            "tracked": True,
            "inventoryLevel": {"quantities": [{"name": "available", "quantity": 3}]},
        },
    }
    row = row_from_product_variant(product, variant)
    assert row.pre_order is False
    assert row.backorder is False
    assert row.expected_policy == "DENY"
    assert not row.has_issues


def test_preorder_alt_key_without_underscore_accepted():
    product = {
        "id": "gid://shopify/Product/1",
        "title": "PO",
        "status": "ACTIVE",
        "productType": "",
        "preOrder": {"value": "false"},
        "preorderAlt": {"value": "true"},
        "backorder": {"value": "false"},
    }
    variant = {
        "id": "gid://shopify/ProductVariant/1",
        "title": "Default Title",
        "sku": "S1",
        "barcode": "999",
        "inventoryPolicy": "DENY",
        "inventoryItem": {
            "id": "gid://shopify/InventoryItem/1",
            "tracked": True,
            "inventoryLevel": {"quantities": [{"name": "available", "quantity": 0}]},
        },
    }
    row = row_from_product_variant(product, variant)
    assert row.pre_order is True
    assert row.expected_policy == "CONTINUE"
    assert ISSUE_POLICY_SHOULD_BE_CONTINUE in row.issues


def test_missing_inventory_item_and_location_flagged():
    issues = classify_variant_issues(
        tracked=None,
        inventory_policy="DENY",
        inventory_item_id=None,
        has_location_level=False,
        expected_policy="DENY",
        sku="S",
        barcode="B",
    )
    assert ISSUE_MISSING_INVENTORY_ITEM in issues

    issues2 = classify_variant_issues(
        tracked=True,
        inventory_policy="DENY",
        inventory_item_id="gid://shopify/InventoryItem/1",
        has_location_level=False,
        expected_policy="DENY",
        sku="S",
        barcode="B",
    )
    assert ISSUE_MISSING_LOCATION_INVENTORY in issues2


def test_dry_run_makes_no_mutation_calls():
    client = MagicMock()
    from app.services.shopify_inventory_settings_audit import ProposedRepair

    repairs = [
        ProposedRepair(
            product_id="gid://shopify/Product/1",
            variant_id="gid://shopify/ProductVariant/1",
            barcode="111",
            title="Film",
            enable_tracking=True,
            inventory_item_id="gid://shopify/InventoryItem/1",
            set_policy="DENY",
            current_policy="CONTINUE",
        )
    ]
    applied, failed = apply_repairs(client, repairs, dry_run=True)
    assert applied == 0
    assert failed == 0
    client.graphql.assert_not_called()


def test_default_audit_mode_makes_no_mutation_calls(tmp_path, monkeypatch):
    """run_audit without --fix must not call graphql mutations (only product query)."""
    calls: List[str] = []

    class FakeClient:
        def graphql(self, query: str, variables: Optional[dict] = None) -> dict:
            calls.append(query)
            if "InventorySettingsAudit" in query:
                return {
                    "products": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            {
                                "id": "gid://shopify/Product/1",
                                "title": "Normal Film",
                                "status": "ACTIVE",
                                "productType": "",
                                "preOrder": {"value": "false"},
                                "preorderAlt": None,
                                "backorder": {"value": "false"},
                                "variants": {
                                    "nodes": [
                                        {
                                            "id": "gid://shopify/ProductVariant/1",
                                            "title": "Default Title",
                                            "sku": "S1",
                                            "barcode": "123",
                                            "inventoryPolicy": "CONTINUE",
                                            "inventoryItem": {
                                                "id": "gid://shopify/InventoryItem/1",
                                                "tracked": True,
                                                "inventoryLevel": {
                                                    "quantities": [
                                                        {"name": "available", "quantity": 5}
                                                    ]
                                                },
                                            },
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                }
            raise AssertionError(f"Unexpected GraphQL call: {query[:80]}")

    monkeypatch.setenv("SHOPIFY_INVENTORY_LOCATION_ID", "gid://shopify/Location/1")
    monkeypatch.setenv("SHOPIFY_SHOP", "example.myshopify.com")
    monkeypatch.setenv("SHOPIFY_CLIENT_ID", "cid")
    monkeypatch.setenv("SHOPIFY_CLIENT_SECRET", "csec")

    csv_path = tmp_path / "audit.csv"
    rows, summary = run_audit(
        env_file=str(tmp_path / "missing.env"),  # load_dotenv no-op if missing
        csv_path=csv_path,
        fix=False,
        dry_run=False,
        client=FakeClient(),  # type: ignore[arg-type]
    )
    assert summary.variants_checked == 1
    assert summary.variants_with_issues == 1
    assert ISSUE_POLICY_SHOULD_BE_DENY in rows[0].issues
    assert all("mutation" not in c.lower() or "InventorySettingsAudit" in c for c in calls)
    assert not any("inventoryItemUpdate" in c or "productVariantsBulkUpdate" in c for c in calls)
    assert csv_path.is_file()


def test_proposed_repair_does_not_seed_or_change_qty():
    product = {
        "id": "gid://shopify/Product/1",
        "title": "Stale CONTINUE",
        "status": "ACTIVE",
        "productType": "",
        "preOrder": {"value": "false"},
        "preorderAlt": None,
        "backorder": {"value": "false"},
    }
    variant = {
        "id": "gid://shopify/ProductVariant/1",
        "title": "Default Title",
        "sku": "S1",
        "barcode": "123",
        "inventoryPolicy": "CONTINUE",
        "inventoryItem": {
            "id": "gid://shopify/InventoryItem/1",
            "tracked": True,
            "inventoryLevel": {"quantities": [{"name": "available", "quantity": 5}]},
        },
    }
    row = row_from_product_variant(product, variant)
    rep = proposed_repairs_for_row(row)
    assert rep is not None
    assert rep.set_policy == "DENY"
    assert rep.enable_tracking is False
