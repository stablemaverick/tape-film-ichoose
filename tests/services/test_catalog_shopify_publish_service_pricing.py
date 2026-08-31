from __future__ import annotations

import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.services.catalog_shopify_publish_service import resolve_new_listing_price


def _margin_pct(retail_inc_gst: float, landed_cost: float) -> float:
    ex = retail_inc_gst / 1.10
    return ((ex - landed_cost) / ex) * 100


def test_new_listing_price_uses_floor_when_upstream_is_too_low():
    row = {"cost_price": 17.16, "calculated_sale_price": 54.99}
    # landed from this cost/rates is 38.44, floor requires 58.99
    out = resolve_new_listing_price(row=row, gbp_aud_rate=2.0, landed_cost_markup=1.12)
    assert out == 58.99


def test_new_listing_price_preserves_higher_upstream_override():
    row = {"cost_price": 17.16, "calculated_sale_price": 62.99}
    out = resolve_new_listing_price(row=row, gbp_aud_rate=2.0, landed_cost_markup=1.12)
    assert out == 62.99


def test_new_listing_price_uses_row_price_when_cost_missing():
    row = {"cost_price": None, "calculated_sale_price": 49.99}
    out = resolve_new_listing_price(row=row)
    assert out == 49.99


def test_new_listing_price_returns_zero_when_no_inputs():
    row = {"cost_price": None, "calculated_sale_price": None}
    out = resolve_new_listing_price(row=row)
    assert out == 0.0


def test_floor_margin_is_at_least_28_percent():
    row = {"cost_price": 17.16, "calculated_sale_price": 10.99}
    out = resolve_new_listing_price(row=row, gbp_aud_rate=2.0, landed_cost_markup=1.12)
    landed = 17.16 * 2.0 * 1.12
    assert _margin_pct(out, landed) >= 28.0
