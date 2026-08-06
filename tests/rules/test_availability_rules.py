"""Tests for supplier availability normalisation and freshness derivation."""

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.availability_rules import (
    AVAILABILITY_CONFIDENCE_VERSION,
    build_observation_dedupe_key,
    compute_availability_confidence_v1,
    derive_feed_freshness,
    legacy_catalog_status_to_controlled,
    map_supplier_availability_status,
    normalise_supplier_availability,
    observation_material_fingerprint,
    supplier_can_supply_from_status,
)


class TestMapStatus:
    def test_numeric_qty_in_stock(self):
        assert map_supplier_availability_status(reported_quantity=14) == "in_stock"

    def test_numeric_qty_low_stock(self):
        assert map_supplier_availability_status(reported_quantity=2) == "low_stock"

    def test_numeric_qty_zero(self):
        assert map_supplier_availability_status(reported_quantity=0) == "unavailable"

    def test_legacy_supplier_stock(self):
        assert map_supplier_availability_status(raw_status="supplier_stock") == "in_stock"
        assert map_supplier_availability_status(raw_status="supplier_out") == "unavailable"

    def test_text_in_stock_without_qty_stays_in_stock_qty_null(self):
        result = normalise_supplier_availability(raw_status="In stock")
        assert result.availability_status == "in_stock"
        assert result.reported_quantity is None
        assert result.quantity_is_exact is False

    def test_never_invent_qty_for_text_status(self):
        result = normalise_supplier_availability(raw_status="Available")
        assert result.reported_quantity is None
        assert result.reported_quantity != 999

    def test_preorder_from_future_release(self):
        assert (
            map_supplier_availability_status(
                reported_quantity=0,
                release_date_is_future=True,
            )
            == "preorder"
        )

    def test_discontinued_not_overridden_by_future_release(self):
        assert (
            map_supplier_availability_status(
                raw_status="discontinued",
                release_date_is_future=True,
            )
            == "discontinued"
        )

    def test_unknown_when_empty(self):
        assert map_supplier_availability_status() == "unknown"


class TestSupplierCanSupply:
    def test_supply_true_for_orderable_statuses(self):
        for status in ("in_stock", "low_stock", "preorder", "backorder"):
            assert supplier_can_supply_from_status(status) is True

    def test_supply_false_for_unavailable(self):
        assert supplier_can_supply_from_status("unavailable") is False
        assert supplier_can_supply_from_status("discontinued") is False

    def test_supply_unknown(self):
        assert supplier_can_supply_from_status("unknown") is None


class TestFreshness:
    def test_fresh_aging_stale(self):
        now = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)
        fresh = derive_feed_freshness(
            last_seen_at=now - timedelta(hours=10),
            now=now,
        )
        assert fresh.status == "fresh"
        aging = derive_feed_freshness(
            last_seen_at=now - timedelta(hours=48),
            now=now,
        )
        assert aging.status == "aging"
        stale = derive_feed_freshness(
            last_seen_at=now - timedelta(hours=100),
            now=now,
        )
        assert stale.status == "stale"

    def test_unknown_without_timestamps(self):
        result = derive_feed_freshness()
        assert result.status == "unknown"
        assert result.age_hours is None

    def test_failed_pipeline_without_prior_timestamps_is_unknown(self):
        result = derive_feed_freshness(pipeline_failed=True)
        assert result.status == "unknown"


class TestConfidence:
    def test_version_constant(self):
        assert AVAILABILITY_CONFIDENCE_VERSION == "v1"

    def test_exact_fresh_in_stock_high(self):
        score = compute_availability_confidence_v1(
            status="in_stock",
            quantity_is_exact=True,
            feed_freshness="fresh",
            match_confidence=1.0,
        )
        # 0.55 + 0.20 + 0.15 + 0.05 = 0.95
        assert score == 0.95

    def test_stale_lowers_confidence(self):
        fresh = compute_availability_confidence_v1(
            status="in_stock",
            quantity_is_exact=True,
            feed_freshness="fresh",
        )
        stale = compute_availability_confidence_v1(
            status="in_stock",
            quantity_is_exact=True,
            feed_freshness="stale",
        )
        assert stale < fresh

    def test_normalise_includes_version_when_confidence_set(self):
        result = normalise_supplier_availability(
            reported_quantity=5,
            feed_freshness="fresh",
        )
        assert result.availability_confidence is not None
        assert result.availability_confidence_version == "v1"


class TestObservationDedupe:
    def test_unchanged_fingerprint_stable(self):
        a = observation_material_fingerprint(
            availability_status="in_stock",
            reported_quantity=14,
            quantity_is_exact=True,
            supplier_can_supply=True,
            unit_cost=18.5,
            currency="GBP",
        )
        b = observation_material_fingerprint(
            availability_status="in_stock",
            reported_quantity=14,
            quantity_is_exact=True,
            supplier_can_supply=True,
            unit_cost=18.5,
            currency="GBP",
        )
        assert a == b
        assert build_observation_dedupe_key("offer-1", a) == build_observation_dedupe_key(
            "offer-1", b
        )

    def test_qty_change_changes_fingerprint(self):
        a = observation_material_fingerprint(
            availability_status="in_stock",
            reported_quantity=14,
            quantity_is_exact=True,
            supplier_can_supply=True,
        )
        b = observation_material_fingerprint(
            availability_status="in_stock",
            reported_quantity=10,
            quantity_is_exact=True,
            supplier_can_supply=True,
        )
        assert a != b


class TestLegacyMapping:
    def test_catalog_values(self):
        assert legacy_catalog_status_to_controlled("store_stock") == "in_stock"
        assert legacy_catalog_status_to_controlled("preorder") == "preorder"
        assert legacy_catalog_status_to_controlled("store_out") == "unavailable"
