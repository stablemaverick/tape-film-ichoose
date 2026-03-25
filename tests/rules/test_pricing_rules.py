"""
Tests for pricing rules.

Verifies margin tiers, GBP→AUD conversion, .99 rounding, and cost calculation.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.pricing_rules import (
    calculate_sale_price,
    calculate_shopify_cost_aud,
    get_margin,
    pricing_source_for_supplier,
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


class TestPricingSource:
    def test_tape_film_source(self):
        assert pricing_source_for_supplier("Tape Film") == "shopify_live"

    def test_other_supplier_source(self):
        assert pricing_source_for_supplier("Moovies") == "gbp_formula_v1"
        assert pricing_source_for_supplier("Lasgo") == "gbp_formula_v1"
