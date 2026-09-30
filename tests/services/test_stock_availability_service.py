"""Unit tests for Stock Availability V1 (pure logic + mocked Supabase)."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.services.stock_availability_service import (
    AmbiguousIdentifier,
    InvalidIdentifier,
    ReleaseNotFound,
    StockAvailabilityService,
    assert_no_combined_quantity,
    derive_tape_status,
    map_supplier_api_availability,
    pick_preferred_supplier,
    quantity_type_for_offer,
)


def test_no_combined_quantity_in_summary():
    assert_no_combined_quantity({"tape_available": 2, "supplier_available": True})
    with pytest.raises(AssertionError):
        assert_no_combined_quantity({"total_available": 12})
    with pytest.raises(AssertionError):
        assert_no_combined_quantity({"combined_available": 5})


def test_map_supplier_api_availability_states():
    assert map_supplier_api_availability(offer_status="in_stock", feed_freshness="fresh") == (
        "available",
        None,
    )
    assert map_supplier_api_availability(offer_status="unavailable", feed_freshness="fresh") == (
        "unavailable",
        None,
    )
    assert map_supplier_api_availability(offer_status="unknown", feed_freshness="unknown") == (
        "unknown",
        None,
    )
    assert map_supplier_api_availability(offer_status="in_stock", feed_freshness="stale") == (
        "stale",
        "available",
    )
    assert map_supplier_api_availability(offer_status="unavailable", feed_freshness="stale") == (
        "stale",
        "unavailable",
    )


def test_quantity_types():
    assert quantity_type_for_offer(reported_quantity=10, quantity_is_exact=True) == "exact"
    assert quantity_type_for_offer(reported_quantity=10, quantity_is_exact=False) == "capped"
    assert quantity_type_for_offer(reported_quantity=None, quantity_is_exact=False) == "boolean_only"


def test_tape_status_allows_negative_available():
    assert derive_tape_status(on_hand=0, committed=4, available=-4, is_stale=False) == "oversold"
    assert derive_tape_status(on_hand=3, committed=1, available=2, is_stale=False) == "available"
    assert derive_tape_status(on_hand=0, committed=0, available=0, is_stale=False) == "sold_out"
    assert derive_tape_status(on_hand=2, committed=0, available=2, is_stale=True) == "stale"


def test_preferred_supplier_ranks_fresh_available_lowest_cost():
    suppliers = [
        {
            "supplier_id": "lasgo",
            "supplier": "Lasgo",
            "supplier_sku": "L1",
            "availability_status": "available",
            "is_stale": False,
            "unit_cost": 20.0,
        },
        {
            "supplier_id": "moovies",
            "supplier": "Moovies",
            "supplier_sku": "M1",
            "availability_status": "available",
            "is_stale": False,
            "unit_cost": 12.0,
        },
        {
            "supplier_id": "lasgo",
            "supplier": "Lasgo",
            "supplier_sku": "L2",
            "availability_status": "stale",
            "is_stale": True,
            "unit_cost": 5.0,
        },
    ]
    pref = pick_preferred_supplier(suppliers)
    assert pref is not None
    assert pref["supplier_id"] == "moovies"
    assert pref["unit_cost"] == 12.0


def test_preferred_does_not_rank_stale_ahead_of_fresh():
    suppliers = [
        {
            "supplier_id": "lasgo",
            "supplier_sku": "A",
            "availability_status": "stale",
            "is_stale": True,
            "unit_cost": 1.0,
        },
        {
            "supplier_id": "moovies",
            "supplier_sku": "B",
            "availability_status": "available",
            "is_stale": False,
            "unit_cost": 99.0,
        },
    ]
    pref = pick_preferred_supplier(suppliers)
    assert pref["supplier_id"] == "moovies"


class _Table:
    def __init__(self, data=None, multi=None):
        self._data = data or []
        self._multi = multi or {}
        self._filters = {}
        self._name = None

    def select(self, *_a, **_k):
        return self

    def eq(self, k, v):
        self._filters[k] = v
        return self

    def in_(self, k, vals):
        self._filters[k] = ("in", vals)
        return self

    def ilike(self, *_a, **_k):
        return self

    def order(self, *_a, **_k):
        return self

    def limit(self, *_a, **_k):
        return self

    def execute(self):
        key = tuple(sorted((k, str(v)) for k, v in self._filters.items()))
        if key in self._multi:
            return MagicMock(data=self._multi[key])
        return MagicMock(data=self._data)


class _SB:
    def __init__(self, tables: dict):
        self.tables = tables

    def table(self, name):
        t = self.tables.get(name)
        if t is None:
            return _Table([])
        if callable(t):
            return t()
        return t


def test_resolve_barcode_ambiguous():
    sb = _SB(
        {
            "variant_identifiers": _Table(
                [
                    {"release_variant_id": "r1"},
                    {"release_variant_id": "r2"},
                ]
            ),
            "release_variants": _Table([]),
        }
    )
    svc = StockAvailabilityService(sb)
    with pytest.raises(AmbiguousIdentifier):
        svc.resolve_release_variant_id(barcode="123")


def test_resolve_barcode_not_found():
    sb = _SB(
        {
            "variant_identifiers": _Table([]),
            "release_variants": _Table([]),
        }
    )
    svc = StockAvailabilityService(sb)
    with pytest.raises(ReleaseNotFound):
        svc.resolve_release_variant_id(barcode="999")


def test_get_stock_availability_supplier_only_no_combined_qty():
    rid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    now = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)

    def offers_table():
        return _Table(
            [
                {
                    "id": "o1",
                    "supplier_id": "lasgo",
                    "supplier_sku": "barcode:111",
                    "availability_status": "in_stock",
                    "reported_quantity": 14,
                    "quantity_is_exact": True,
                    "supplier_can_supply": True,
                    "unit_cost": 17.16,
                    "currency": "GBP",
                    "last_seen_at": "2026-08-09T05:00:00+00:00",
                    "source_feed_at": "2026-08-09T05:00:00+00:00",
                    "pipeline_completed_at": "2026-08-09T05:05:00+00:00",
                    "active": True,
                }
            ]
        )

    sb = _SB(
        {
            "release_variants": _Table(
                [
                    {
                        "id": rid,
                        "title": "The Frighteners",
                        "format": "4K UHD",
                        "primary_barcode": "111",
                        "catalog_item_id": None,
                        "publication_status": "supplier_only",
                        "active": True,
                    }
                ]
            ),
            "tape_inventory_levels": _Table([]),
            "supplier_offers": offers_table,
            "suppliers": _Table([{"id": "lasgo", "display_name": "Lasgo"}]),
        }
    )
    # resolve path uses release_variant_id directly
    svc = StockAvailabilityService(sb, now=now)
    out = svc.get_stock_availability(release_variant_id=rid)
    assert out["tape"]["status"] == "unknown"
    assert out["tape"]["present"] is False
    assert out["suppliers"][0]["availability_status"] == "available"
    assert out["suppliers"][0]["quantity"] == 14
    assert out["summary"]["tape_available"] is None
    assert out["summary"]["supplier_available"] is True
    assert "total_available" not in out["summary"]
    # must not invent tape+supplier
    assert out["summary"].get("tape_available") != (
        (out["tape"].get("available") or 0) + (out["suppliers"][0].get("quantity") or 0)
    )


def test_invalid_identifier_multiple():
    svc = StockAvailabilityService(_SB({}))
    with pytest.raises(InvalidIdentifier):
        svc.resolve_release_variant_id(barcode="1", shopify_variant_id="2")


def test_tape_oversold_not_clamped():
    rid = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
    now = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
    sb = _SB(
        {
            "release_variants": _Table(
                [
                    {
                        "id": rid,
                        "title": "Preorder",
                        "format": "BD",
                        "primary_barcode": "x",
                        "catalog_item_id": None,
                        "publication_status": "published",
                        "active": True,
                    }
                ]
            ),
            "tape_inventory_levels": _Table(
                [
                    {
                        "on_hand": 0,
                        "committed": 4,
                        "available": -4,
                        "po_incoming_confirmed": 0,
                        "shopify_incoming_reported": 0,
                        "damaged_or_unavailable": 0,
                        "last_synced_at": "2026-08-09T07:00:00+00:00",
                        "shopify_location_id": "gid://shopify/Location/1",
                    }
                ]
            ),
            "supplier_offers": _Table([]),
            "suppliers": _Table([]),
        }
    )
    out = StockAvailabilityService(sb, now=now).get_stock_availability(release_variant_id=rid)
    assert out["tape"]["available"] == -4
    assert out["tape"]["status"] == "oversold"
