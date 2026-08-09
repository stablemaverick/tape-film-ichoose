"""Regression: Shopify II dual-write excludes gift cards and explicit test products."""

from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.config.inventory_dual_write import InventoryDualWriteFlags
from app.services.shopify_release_dual_write_service import (
    SHOPIFY_II_EXACT_EXCLUDED_PRODUCT_TITLES,
    dual_write_shopify_listings_to_releases,
    shopify_ii_dual_write_exclusion_reason,
)


ON_SHOPIFY = InventoryDualWriteFlags(
    enabled=True,
    shopify=True,
    supplier=False,
    purchase_orders=False,
    auto_accept_min_confidence=0.95,
    fresh_max_hours=36,
    aging_max_hours=72,
    create_supplier_only_releases=True,
    supplier_batch_size=500,
    supplier_in_chunk_size=150,
)


@pytest.mark.parametrize(
    "row,reason",
    [
        ({"product_type": "Gift Card", "product_title": "Anything"}, "gift_card_product_type"),
        ({"product_type": "gift_card", "product_title": "X"}, "gift_card_product_type"),
        (
            {"product_type": "", "product_title": "TAPE! Film Gift Card"},
            "gift_card_exact_title",
        ),
        (
            {
                "product_type": None,
                "product_title": "TEST Film - Hell or High Water (Not for Sale)",
            },
            "test_product_exact_title",
        ),
    ],
)
def test_exclusion_reasons_for_known_non_release_rows(row, reason):
    assert shopify_ii_dual_write_exclusion_reason(row) == reason


def test_legitimate_release_titles_are_not_excluded():
    # Must not use broad "gift" / "test" substring filtering.
    assert (
        shopify_ii_dual_write_exclusion_reason(
            {
                "product_type": "Blu-ray",
                "product_title": "The Gift 4K Ultra HD",
            }
        )
        is None
    )
    assert (
        shopify_ii_dual_write_exclusion_reason(
            {
                "product_type": "",
                "product_title": "Testament of Youth Blu-Ray",
            }
        )
        is None
    )
    assert "tape! film gift card" in SHOPIFY_II_EXACT_EXCLUDED_PRODUCT_TITLES


def test_dual_write_skips_gift_card_and_test_without_creating_releases():
    from tests.services.test_inventory_dual_write_behaviour import FakeSupabase

    sb = FakeSupabase()
    rows = [
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/gift1",
            "shopify_product_id": "gid://shopify/Product/g1",
            "product_title": "TAPE! Film Gift Card",
            "variant_title": "$25.00",
            "product_type": "",
            "barcode": None,
            "tracks_inventory": True,
            "shopify_inventory_item_id": "gid://shopify/InventoryItem/1",
            "inventory_quantity": 0,
        },
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/test1",
            "shopify_product_id": "gid://shopify/Product/t1",
            "product_title": "TEST Film - Hell or High Water (Not for Sale)",
            "variant_title": "Default Title",
            "product_type": "",
            "barcode": None,
            "tracks_inventory": True,
            "shopify_inventory_item_id": "gid://shopify/InventoryItem/2",
            "inventory_quantity": 6,
        },
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/real1",
            "shopify_product_id": "gid://shopify/Product/r1",
            "product_title": "Hush Limited Edition 4K Ultra HD",
            "variant_title": "Default Title",
            "product_type": "",
            "barcode": "5027035030333",
            "tracks_inventory": True,
            "shopify_inventory_item_id": "gid://shopify/InventoryItem/3",
            "inventory_quantity": 7,
        },
    ]
    stats = dual_write_shopify_listings_to_releases(
        sb,
        rows,
        shop="a61446-1c.myshopify.com",
        location_id="gid://shopify/Location/78213775584",
        inventory_levels_by_variant={
            "gid://shopify/ProductVariant/real1": {
                "available": 7,
                "committed": 0,
                "on_hand": 7,
                "incoming": 0,
            }
        },
        flags=ON_SHOPIFY,
    )
    assert stats["skipped_non_release"] == 2
    assert stats["skipped_non_release_by_reason"]["gift_card_exact_title"] == 1
    assert stats["skipped_non_release_by_reason"]["test_product_exact_title"] == 1
    assert stats["channels_upserted"] == 1
    assert stats["created_new_release"] == 1
    assert stats["tape_levels_upserted"] == 1
    channels = sb.store.get("release_shopify_listings", [])
    assert len(channels) == 1
    assert channels[0]["shopify_variant_id"] == "gid://shopify/ProductVariant/real1"
    titles = {r.get("title") for r in sb.store.get("release_variants", [])}
    assert "TAPE! Film Gift Card" not in titles
    assert "TEST Film - Hell or High Water (Not for Sale)" not in titles
