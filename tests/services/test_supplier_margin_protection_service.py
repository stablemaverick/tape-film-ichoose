"""Tests for supplier margin monitoring (existing catalogue is read-only by default)."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.rules.pricing_rules import (
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    classify_supplier_gbp_cost_movement,
    exact_ex_gst_margin_ok,
    replacement_landed_cost_aud,
)
from app.services.supplier_margin_protection_service import (
    BLOCKED_NO_AUTHORITY,
    MONITOR_COST_DOWN,
    MONITOR_MARGIN_SAFE,
    MONITOR_PRICE_INCREASE_INDICATED,
    OUT_OF_SCOPE,
    REVIEW_COST_ANOMALY,
    ApplyAllowlist,
    classify_studio_region_eligibility,
    evaluate_variant_row,
    parse_apply_allowlist,
    run_supplier_margin_protection,
)


def _cfg():
    return {
        "gbp_aud_rate": 2.0,
        "landed_cost_markup": 1.12,
        "margin_floor_ratio": 0.28,
        "supplier_cost_alert_gbp": 1.0,
        "supplier_cost_alert_pct": 0.05,
        "supplier_cost_anomaly_pct": 0.25,
    }


def _listing(**kw):
    base = {
        "shopify_variant_id": "gid://shopify/ProductVariant/1",
        "shopify_product_id": "gid://shopify/Product/1",
        "product_title": "Test Film 4K",
        "barcode": "5028836041672",
        "price_amount": 85.99,
        "inventory_policy": "CONTINUE",
        "inventory_quantity": 0,
        "product_status": "ACTIVE",
        "product_type": "Film",
        "match_status": "matched",
        "studio_text": "Arrow Video",
        "_studio_raw": "Arrow Video",
        "_studio_norm": "Arrow",
        "_region_raw": "Region B",
        "_region_norm": "B",
    }
    base.update(kw)
    return base


def _ctx(offers, rsl=None):
    return {
        "rsl": rsl or {"gid://shopify/ProductVariant/1": "release-1"},
        "offers_by_release": {"release-1": offers},
        "offers_by_barcode": {},
        "suppliers": {"moovies": "Moovies", "lasgo": "Lasgo"},
        "tape_by_release": {},
    }


def _offer(sid="moovies", cost=28.0, qty=5, offer_id="offer-1"):
    return {
        "id": offer_id,
        "supplier_id": sid,
        "supplier_sku": f"sku-{sid}",
        "availability_status": "in_stock",
        "reported_quantity": qty,
        "quantity_is_exact": True,
        "unit_cost": cost,
        "currency": "GBP",
        "last_seen_at": datetime.now(timezone.utc).isoformat(),
        "source_feed_at": datetime.now(timezone.utc).isoformat(),
        "pipeline_completed_at": datetime.now(timezone.utc).isoformat(),
    }


class TestMonitoringActions:
    NOW = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)

    def test_below_floor_is_monitoring_only(self):
        offers = [_offer(cost=32.0)]
        prev_obs = {
            "offer-1": {
                "unit_cost": 28.0,
                "availability_status": "in_stock",
                "reported_quantity": 5,
                "observed_at": "2026-08-18T00:00:00+00:00",
            }
        }
        old_price = calculate_sale_price_with_margin_floor_from_gbp_cost(28.0)
        row = evaluate_variant_row(
            listing=_listing(price_amount=old_price),
            ctx=_ctx(offers),
            prev_obs_by_offer=prev_obs,
            cfg=_cfg(),
            now=self.NOW,
        )
        assert row["monitoring_action"] == MONITOR_PRICE_INCREASE_INDICATED
        assert row["theoretical_28pct_floor"] > old_price

    def test_above_floor_is_monitor_only(self):
        floor_32 = calculate_sale_price_with_margin_floor_from_gbp_cost(32.0)
        offers = [_offer(cost=32.0)]
        prev_obs = {
            "offer-1": {
                "unit_cost": 28.0,
                "availability_status": "in_stock",
                "reported_quantity": 5,
                "observed_at": "2026-08-18T00:00:00+00:00",
            }
        }
        row = evaluate_variant_row(
            listing=_listing(price_amount=floor_32 + 10),
            ctx=_ctx(offers),
            prev_obs_by_offer=prev_obs,
            cfg=_cfg(),
            now=self.NOW,
        )
        assert row["monitoring_action"] == MONITOR_MARGIN_SAFE

    def test_cost_down_never_proposes_price_cut(self):
        offers = [_offer(cost=26.0)]
        prev_obs = {
            "offer-1": {
                "unit_cost": 28.0,
                "availability_status": "in_stock",
                "reported_quantity": 5,
                "observed_at": "2026-08-18T00:00:00+00:00",
            }
        }
        price = calculate_sale_price_with_margin_floor_from_gbp_cost(28.0) or 85.99
        row = evaluate_variant_row(
            listing=_listing(price_amount=price),
            ctx=_ctx(offers),
            prev_obs_by_offer=prev_obs,
            cfg=_cfg(),
            now=self.NOW,
        )
        assert row["monitoring_action"] == MONITOR_COST_DOWN
        assert (row.get("theoretical_price_delta") or 0) <= 0

    def test_small_move_below_floor_still_monitored(self):
        offers = [_offer(cost=28.01)]
        prev_obs = {
            "offer-1": {
                "unit_cost": 28.0,
                "availability_status": "in_stock",
                "reported_quantity": 5,
                "observed_at": "2026-08-18T00:00:00+00:00",
            }
        }
        floor = calculate_sale_price_with_margin_floor_from_gbp_cost(28.01)
        assert not classify_supplier_gbp_cost_movement(28.0, 28.01)["significant"]
        row = evaluate_variant_row(
            listing=_listing(price_amount=floor - 1.0),
            ctx=_ctx(offers),
            prev_obs_by_offer=prev_obs,
            cfg=_cfg(),
            now=self.NOW,
        )
        assert row["monitoring_action"] == MONITOR_PRICE_INCREASE_INDICATED

    def test_extreme_anomaly_review_only(self):
        offers = [_offer(cost=280.0)]
        prev_obs = {
            "offer-1": {
                "unit_cost": 28.0,
                "availability_status": "in_stock",
                "reported_quantity": 5,
                "observed_at": "2026-08-18T00:00:00+00:00",
            }
        }
        old_price = calculate_sale_price_with_margin_floor_from_gbp_cost(28.0)
        row = evaluate_variant_row(
            listing=_listing(price_amount=old_price),
            ctx=_ctx(offers),
            prev_obs_by_offer=prev_obs,
            cfg=_cfg(),
            now=self.NOW,
        )
        assert row["monitoring_action"] == REVIEW_COST_ANOMALY


class TestMonitoringOnlyRun:
    @patch("app.services.supplier_margin_protection_service._write_csv")
    @patch("app.services.supplier_margin_protection_service.create_fresh_client")
    @patch("app.services.supplier_margin_protection_service.load_eligible_listings")
    @patch("app.services.supplier_margin_protection_service._load_supplier_context")
    @patch("app.services.supplier_margin_protection_service.fetch_previous_observations")
    @patch("app.services.supplier_margin_protection_service.ShopifyClient")
    def test_default_run_no_shopify_client(
        self,
        mock_shopify,
        mock_prev,
        mock_ctx,
        mock_listings,
        mock_sb,
        _csv,
    ):
        mock_listings.return_value = []
        mock_sb.return_value = MagicMock()
        mock_ctx.return_value = {
            "rsl": {},
            "offers_by_release": {},
            "offers_by_barcode": {},
            "suppliers": {},
            "tape_by_release": {},
        }
        mock_prev.return_value = {}
        _rows, summary = run_supplier_margin_protection(env_file=".env", apply=False)
        mock_shopify.assert_not_called()
        assert summary.monitoring_only is True
        assert summary.price_increases_applied == 0

    @patch("app.services.supplier_margin_protection_service._apply_price_updates")
    @patch("app.services.supplier_margin_protection_service._write_csv")
    @patch("app.services.supplier_margin_protection_service.create_fresh_client")
    @patch("app.services.supplier_margin_protection_service.load_eligible_listings")
    @patch("app.services.supplier_margin_protection_service.enrich_listings_with_region")
    @patch("app.services.supplier_margin_protection_service._load_supplier_context")
    @patch("app.services.supplier_margin_protection_service.fetch_previous_observations")
    @patch("app.services.supplier_margin_protection_service.evaluate_variant_row")
    def test_apply_without_allowlist_never_mutates(
        self,
        mock_eval,
        mock_prev,
        mock_ctx,
        mock_enrich,
        mock_listings,
        mock_sb,
        _csv,
        mock_apply,
    ):
        mock_eval.return_value = {
            "variant_id": "v1",
            "barcode": "bc1",
            "monitoring_action": MONITOR_PRICE_INCREASE_INDICATED,
            "anomaly_status": "OK",
            "exact_current_margin_pct": 10.0,
        }
        mock_listings.return_value = [_listing()]
        mock_sb.return_value = MagicMock()
        mock_ctx.return_value = {
            "rsl": {},
            "offers_by_release": {},
            "offers_by_barcode": {},
            "suppliers": {},
            "tape_by_release": {},
        }
        mock_prev.return_value = {}
        _rows, summary = run_supplier_margin_protection(
            env_file=".env", apply=True, allowlist=ApplyAllowlist()
        )
        mock_apply.assert_not_called()
        assert summary.price_increases_applied == 0
        mock_enrich.assert_called_once()

    def test_allowlist_parsing(self):
        al = parse_apply_allowlist(barcodes=["5028836041672", "5050629184334"])
        assert al.matches({"barcode": "5028836041672", "variant_id": "x"})
        assert not al.matches({"barcode": "000", "variant_id": "x"})


class TestNewProductPricingUnchanged:
    def test_catalog_publish_still_uses_floor(self):
        from app.services.catalog_shopify_publish_service import resolve_new_listing_price

        row = {"cost_price": 17.16, "calculated_sale_price": 10.99}
        out = resolve_new_listing_price(row=row, gbp_aud_rate=2.0, landed_cost_markup=1.12)
        assert out == 58.99

    def test_exact_28_boundary(self):
        cost = 17.16
        floor = calculate_sale_price_with_margin_floor_from_gbp_cost(cost)
        landed = replacement_landed_cost_aud(cost)
        assert floor and landed
        assert exact_ex_gst_margin_ok(floor, landed)


class TestStudioRegionAuthority:
    NOW = datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc)

    def test_criterion_region_a_blocked_no_authority(self):
        listing = _listing(
            studio_text="Criterion Collection",
            _studio_raw="Criterion Collection",
            _studio_norm="Criterion Collection",
            _region_raw="Region A",
            _region_norm="A",
        )
        row = evaluate_variant_row(
            listing=listing,
            ctx=_ctx([_offer()]),
            prev_obs_by_offer={},
            cfg=_cfg(),
            now=self.NOW,
        )
        assert row["monitoring_action"] == BLOCKED_NO_AUTHORITY
        assert "region_a" in row["reason"]

    def test_criterion_region_b_below_floor(self):
        floor = calculate_sale_price_with_margin_floor_from_gbp_cost(40.0)
        listing = _listing(
            studio_text="Criterion Collection",
            _studio_raw="Criterion Collection",
            _studio_norm="Criterion Collection",
            _region_norm="B",
            price_amount=round(floor - 5.0, 2),
        )
        row = evaluate_variant_row(
            listing=listing,
            ctx=_ctx([_offer(cost=40.0)]),
            prev_obs_by_offer={},
            cfg=_cfg(),
            now=self.NOW,
        )
        assert row["monitoring_action"] == MONITOR_PRICE_INCREASE_INDICATED

    def test_second_sight_region_b_safe(self):
        floor = calculate_sale_price_with_margin_floor_from_gbp_cost(20.0)
        listing = _listing(
            studio_text="Second Sight",
            _studio_raw="Second Sight",
            _studio_norm="Second Sight",
            _region_norm="B",
            price_amount=floor + 10.0,
        )
        row = evaluate_variant_row(
            listing=listing,
            ctx=_ctx([_offer(cost=20.0)]),
            prev_obs_by_offer={},
            cfg=_cfg(),
            now=self.NOW,
        )
        assert row["monitoring_action"] in {MONITOR_MARGIN_SAFE, MONITOR_COST_DOWN}

    def test_warner_excluded_by_classifier(self):
        action, reason = classify_studio_region_eligibility(
            studio_norm="Warner Bros", region_norm="B"
        )
        assert action == OUT_OF_SCOPE

