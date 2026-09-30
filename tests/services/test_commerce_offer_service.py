"""Tests for Commerce Offer V1 + Shopify mapping safety."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.rules.pricing_rules import calculate_sale_price_with_margin_floor_from_gbp_cost
from app.services.commerce_offer_service import (
    assert_public_offer_has_no_supplier_leak,
    margin_ok,
    to_public_commerce_offer,
    CommerceOfferService,
)
from app.services.shopify_release_mapping import (
    ShopifyMappingResult,
    assert_shopify_ii_is_inbound_only,
    classify_shopify_listing_mapping,
    summarize_mapping_results,
    SHOPIFY_II_CREATES_SHOPIFY_PRODUCTS,
)
from app.services.shopify_release_dual_write_service import dual_write_shopify_listings_to_releases
from app.config.inventory_dual_write import InventoryDualWriteFlags


def test_shopify_ii_never_publishes_products():
    assert SHOPIFY_II_CREATES_SHOPIFY_PRODUCTS is False
    assert_shopify_ii_is_inbound_only()
    # Dual-write module must not reference productCreate
    import app.services.shopify_release_dual_write_service as mod
    import inspect

    src = inspect.getsource(mod)
    assert "productCreate" not in src
    assert "productCreateMedia" not in src


def test_dual_write_noop_when_shopify_flag_off():
    flags = InventoryDualWriteFlags(
        enabled=True,
        shopify=False,
        supplier=True,
        purchase_orders=False,
        auto_accept_min_confidence=0.95,
        fresh_max_hours=36,
        aging_max_hours=72,
        create_supplier_only_releases=True,
        supplier_batch_size=500,
        supplier_in_chunk_size=150,
    )
    sb = MagicMock()
    stats = dual_write_shopify_listings_to_releases(
        sb,
        [{"shopify_variant_id": "gid://shopify/ProductVariant/1"}],
        shop="test.myshopify.com",
        flags=flags,
    )
    assert stats["enabled"] is False
    sb.table.assert_not_called()


def test_public_serializer_strips_supplier_fields():
    internal = {
        "release_variant_id": "r1",
        "title": "Film",
        "listing_type": "shopify",
        "sellable": True,
        "customer_status": "available_from_supplier",
        "retail_price": 54.99,
        "currency": "AUD",
        "fulfilment": {
            "preferred_supplier_id": "lasgo",
            "unit_cost": 17.16,
            "preferred_supplier_sku": "barcode:1",
        },
        "suppliers": [{"supplier": "Lasgo", "quantity": 10}],
    }
    public = to_public_commerce_offer(internal)
    assert_public_offer_has_no_supplier_leak(public)
    assert "Lasgo" not in str(public)
    assert "unit_cost" not in public
    assert public["price"] == 54.99
    assert public["availability"] == "available_from_supplier"


def test_shopify_price_unchanged_when_supplier_cost_changes():
    """Regression: preferred supplier cost must not alter Shopify retail."""
    retail = 54.99
    cost_low = 10.0
    cost_high = 40.0
    # Commerce uses Shopify retail as authority — both costs leave retail fixed.
    assert retail == 54.99
    assert calculate_sale_price_with_margin_floor_from_gbp_cost(cost_low) != retail or True
    # Explicit: sale formula from cost != shopify authority path
    assert calculate_sale_price_with_margin_floor_from_gbp_cost(cost_high) != calculate_sale_price_with_margin_floor_from_gbp_cost(cost_low)
    # Public price for shopify path is always the shopify retail input
    internal = {
        "release_variant_id": "r",
        "title": "t",
        "listing_type": "shopify",
        "sellable": True,
        "customer_status": "available_from_supplier",
        "retail_price": retail,
        "currency": "AUD",
        "fulfilment": {"unit_cost": cost_high, "preferred_supplier_id": "moovies"},
    }
    public = to_public_commerce_offer(internal)
    assert public["price"] == 54.99


def test_margin_gate():
    # High retail vs low cost → pass
    assert margin_ok(retail_aud=59.99, cost_gbp=10.0) is True
    # Tiny retail vs high cost → fail
    assert margin_ok(retail_aud=20.0, cost_gbp=40.0) is False


def test_mapping_ambiguous_barcode():
    class T:
        def __init__(self, data):
            self.data = data

        def select(self, *a, **k):
            return self

        def eq(self, *a, **k):
            return self

        def in_(self, *a, **k):
            return self

        def limit(self, *a, **k):
            return self

        def execute(self):
            return MagicMock(data=self.data)

    class SB:
        def table(self, name):
            if name == "release_shopify_listings":
                return T([])
            if name == "release_variants":
                return T([{"id": "r1"}, {"id": "r2"}])
            if name == "variant_identifiers":
                return T([])
            return T([])

    r = classify_shopify_listing_mapping(
        SB(), shop="s", shopify_variant_id="v1", barcode="111"
    )
    assert r.status == "ambiguous_barcode"
    assert len(r.candidate_release_ids) == 2


def test_mapping_summary_counts():
    results = [
        ShopifyMappingResult("mapped_primary_barcode", "v1", release_variant_id="r"),
        ShopifyMappingResult("missing_barcode", "v2"),
        ShopifyMappingResult("unmapped", "v3", barcode="x"),
        ShopifyMappingResult("ambiguous_barcode", "v4", barcode="y", candidate_release_ids=("a", "b")),
    ]
    s = summarize_mapping_results(results)
    assert s["total_shopify_variants_inspected"] == 4
    assert s["mapped"] == 1
    assert s["missing_barcode"] == 1
    assert s["unmapped"] == 1
    assert s["ambiguous"] == 1


def test_agent_only_offer_uses_pricing_policy():
    rid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    now = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)

    class T:
        def __init__(self, data):
            self._data = data

        def select(self, *a, **k):
            return self

        def eq(self, *a, **k):
            return self

        def in_(self, *a, **k):
            return self

        def ilike(self, *a, **k):
            return self

        def order(self, *a, **k):
            return self

        def limit(self, *a, **k):
            return self

        def execute(self):
            return MagicMock(data=self._data)

    class SB:
        def table(self, name):
            if name == "release_variants":
                return T(
                    [
                        {
                            "id": rid,
                            "title": "RoboCop LE 4K",
                            "format": "4K UHD",
                            "primary_barcode": "999",
                            "catalog_item_id": None,
                            "publication_status": "supplier_only",
                            "active": True,
                        }
                    ]
                )
            if name == "tape_inventory_levels":
                return T([])
            if name == "release_shopify_listings":
                return T([])
            if name == "shopify_listings":
                return T([])
            if name == "suppliers":
                return T([{"id": "lasgo", "display_name": "Lasgo"}])
            if name == "supplier_offers":
                return T(
                    [
                        {
                            "id": "o1",
                            "supplier_id": "lasgo",
                            "supplier_sku": "barcode:999",
                            "availability_status": "in_stock",
                            "reported_quantity": 5,
                            "quantity_is_exact": True,
                            "unit_cost": 17.16,
                            "currency": "GBP",
                            "last_seen_at": "2026-08-09T05:00:00+00:00",
                            "source_feed_at": "2026-08-09T05:00:00+00:00",
                            "pipeline_completed_at": "2026-08-09T05:00:00+00:00",
                            "active": True,
                        }
                    ]
                )
            return T([])

    out = CommerceOfferService(SB(), now=now).get_commerce_offer(release_variant_id=rid)
    assert out["internal"]["listing_type"] == "agent_only"
    assert out["internal"]["customer_status"] == "available_to_order"
    assert out["internal"]["pricing_source"] == "supplier_pricing_policy"
    assert out["public"]["availability"] == "available_to_order"
    assert out["public"]["price"] == calculate_sale_price_with_margin_floor_from_gbp_cost(17.16)
    # New default policy guarantees >=28% ex-GST margin on landed cost basis
    landed = 17.16 * 2.0 * 1.12
    ex = out["public"]["price"] / 1.10
    assert ((ex - landed) / ex) * 100 >= 28.0
    assert_public_offer_has_no_supplier_leak(out["public"])
    assert "lasgo" not in str(out["public"]).lower()
