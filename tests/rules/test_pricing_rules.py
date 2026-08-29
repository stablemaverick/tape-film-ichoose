"""
Tests for pricing rules.

Verifies margin tiers, GBP→AUD conversion, .99 rounding, and cost calculation.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.pricing_rules import (
    calculate_sale_price,
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    calculate_sale_price_with_margin_floor_from_landed_cost,
    calculate_shopify_cost_aud,
    classify_supplier_gbp_cost_movement,
    get_margin,
    pricing_source_for_supplier,
    replacement_landed_cost_aud,
    round_up_to_99,
)


class TestMarginTiers:
    def test_low_cost_32_percent(self):
        assert get_margin(10.0) == 0.32
        assert get_margin(15.0) == 0.32

    def test_mid_cost_28_percent(self):
        assert get_margin(20.0) == 0.28
        assert get_margin(30.0) == 0.28

    def test_high_cost_24_percent(self):
        assert get_margin(35.0) == 0.24
        assert get_margin(40.0) == 0.24

    def test_premium_cost_20_percent(self):
        assert get_margin(50.0) == 0.20
        assert get_margin(100.0) == 0.20


class TestRoundUpTo99:
    def test_rounds_up(self):
        assert round_up_to_99(25.50) == 25.99
        assert round_up_to_99(30.01) == 30.99

    def test_already_at_99(self):
        assert round_up_to_99(25.99) == 25.99

    def test_just_over_99(self):
        assert round_up_to_99(26.00) == 26.99


class TestCalculateSalePrice:
    def test_none_returns_none(self):
        assert calculate_sale_price(None) is None

    def test_basic_calculation(self):
        result = calculate_sale_price(10.0)
        assert result is not None
        assert str(result).endswith("99")
        assert result > 10.0

    def test_ends_in_99(self):
        for cost in [5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 40.0, 50.0]:
            result = calculate_sale_price(cost)
            if result is not None:
                cents = round(result * 100) % 100
                assert cents == 99, f"cost={cost} gave price={result}"


class TestCalculateShopifyCostAud:
    def test_none_returns_none(self):
        assert calculate_shopify_cost_aud(None) is None

    def test_basic_conversion(self):
        result = calculate_shopify_cost_aud(10.0, gbp_aud_rate=2.0, landed_cost_markup=1.12)
        assert result == 22.40

    def test_custom_rate(self):
        result = calculate_shopify_cost_aud(10.0, gbp_aud_rate=1.95, landed_cost_markup=1.12)
        assert result == 21.84


class TestSupplierGbpCostMovement:
    def test_replacement_landed_alias(self):
        assert replacement_landed_cost_aud(28.0) == calculate_shopify_cost_aud(28.0)
        assert replacement_landed_cost_aud(28.0) == 62.72

    def test_significant_increase(self):
        m = classify_supplier_gbp_cost_movement(28.0, 32.0)
        assert m["significant"] is True
        assert m["direction"] == "UP"

    def test_decrease_not_auto_pricing_gate(self):
        m = classify_supplier_gbp_cost_movement(28.0, 26.0)
        assert m["direction"] == "DOWN"


class TestPricingSource:
    def test_tape_film_source(self):
        assert pricing_source_for_supplier("Tape Film") == "shopify_live"

    def test_other_supplier_source(self):
        assert pricing_source_for_supplier("Moovies") == "gbp_formula_v1"
        assert pricing_source_for_supplier("Lasgo") == "gbp_formula_v1"


class TestMarginFloorPricing:
    def _margin_pct(self, retail_inc_gst: float, landed_cost: float) -> float:
        ex = retail_inc_gst / 1.10
        return ((ex - landed_cost) / ex) * 100

    def test_landed_cost_floor_calculation(self):
        # standard 4K-like landed cost
        p = calculate_sale_price_with_margin_floor_from_landed_cost(38.46)
        assert p is not None
        assert p == 58.99
        assert self._margin_pct(p, 38.46) >= 28.0

    def test_rounding_boundary_exact_99(self):
        p = calculate_sale_price_with_margin_floor_from_landed_cost(31.92)
        assert p is not None
        assert round(p * 100) % 100 == 99
        assert self._margin_pct(p, 31.92) >= 28.0

    def test_just_above_99_boundary_rounds_up(self):
        # picked to force rounding above .99 boundary
        p = calculate_sale_price_with_margin_floor_from_landed_cost(35.71)
        assert p == 54.99
        assert self._margin_pct(p, 35.71) >= 28.0

    def test_low_cost_bluray(self):
        p = calculate_sale_price_with_margin_floor_from_landed_cost(23.93)
        assert p == 36.99
        assert self._margin_pct(p, 23.93) >= 28.0

    def test_premium_limited_edition(self):
        p = calculate_sale_price_with_margin_floor_from_landed_cost(63.32)
        assert p == 96.99
        assert self._margin_pct(p, 63.32) >= 28.0

    def test_high_value_box_set(self):
        p = calculate_sale_price_with_margin_floor_from_landed_cost(302.29)
        assert p == 461.99
        assert self._margin_pct(p, 302.29) >= 28.0

    def test_very_low_cost(self):
        p = calculate_sale_price_with_margin_floor_from_landed_cost(1.11)
        assert p is not None
        assert p >= 1.99
        assert self._margin_pct(p, 1.11) >= 28.0

    def test_missing_zero_negative_invalid_cost(self):
        assert calculate_sale_price_with_margin_floor_from_landed_cost(None) is None
        assert calculate_sale_price_with_margin_floor_from_landed_cost(0) is None
        assert calculate_sale_price_with_margin_floor_from_landed_cost(-1) is None

    def test_from_gbp_cost_uses_same_landed_basis(self):
        # landed = 17.16 * 2.0 * 1.12 = 38.44
        p = calculate_sale_price_with_margin_floor_from_gbp_cost(17.16)
        assert p == 58.99
        landed = calculate_shopify_cost_aud(17.16)
        assert landed == 38.44
        assert landed is not None
        assert self._margin_pct(p, landed) >= 28.0

    def test_floor_price_is_never_below_old_tiered_price(self):
        for cost_gbp in [3.10, 5.55, 8.69, 17.16, 34.17, 116.25, 199.99]:
            old = calculate_sale_price(cost_gbp)
            new = calculate_sale_price_with_margin_floor_from_gbp_cost(cost_gbp)
            assert old is not None and new is not None
            assert new >= old

    def test_boundary_violation_regression_next_99_selected(self):
        # Representative from audit: cost 5.55 (landed 12.43) was at boundary with 18.99.
        cost_gbp = 5.55
        landed = calculate_shopify_cost_aud(cost_gbp)
        new = calculate_sale_price_with_margin_floor_from_gbp_cost(cost_gbp)
        assert landed == 12.43
        assert new is not None
        assert round(new * 100) % 100 == 99
        assert new == 19.99
        assert self._margin_pct(new, landed) >= 28.0

    def test_representative_audit_boundary_samples_all_pass_28(self):
        # Samples taken from prior audit boundary cases and adjacent price bands.
        for cost_gbp in [5.55, 20.16, 3.10, 11.58, 15.61, 34.17, 199.99]:
            landed = calculate_shopify_cost_aud(cost_gbp)
            price = calculate_sale_price_with_margin_floor_from_gbp_cost(cost_gbp)
            assert landed is not None and price is not None
            assert self._margin_pct(price, landed) >= 28.0
