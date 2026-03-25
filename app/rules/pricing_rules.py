"""
Pricing rules for supplier cost → sale price calculation.

Conversion pipeline:
  1. cost_gbp (supplier invoice price in GBP)
  2. × GBP_AUD_RATE (default 2.0) → AUD base cost
  3. × LANDED_COST_MARKUP (default 1.12, i.e. +12%) → landed cost
  4. × (1 + margin_for_tier) → pre-GST sale price
  5. × 1.10 (+10% GST) → final sale price
  6. round_up_to_99 → consumer-friendly .99 price

Margin tiers (based on GBP cost):
  ≤ £15   -> 32%
  ≤ £30   -> 28%
  ≤ £40   -> 24%
  > £40   -> 20%

Shopify cost conversion (for variant cost field):
  cost_gbp × GBP_AUD_RATE × LANDED_COST_MARKUP
  (separate from sale price; used in publish_selected_barcodes_to_shopify)
"""

from typing import Optional

DEFAULT_GBP_AUD_RATE = 2.0
DEFAULT_LANDED_COST_MARKUP = 1.12
GST_RATE = 1.10


def get_margin(cost_gbp: float) -> float:
    """Return the margin percentage based on GBP cost tier."""
    if cost_gbp <= 15:
        return 0.32
    if cost_gbp <= 30:
        return 0.28
    if cost_gbp <= 40:
        return 0.24
    return 0.20


def round_up_to_99(value: float) -> float:
    """Round a price up to the nearest .99 ending."""
    rounded = round(value, 2)
    whole = int(rounded)
    if rounded <= whole + 0.99:
        return round(whole + 0.99, 2)
    return round((whole + 1) + 0.99, 2)


def calculate_sale_price(
    cost_gbp: Optional[float],
    gbp_aud_rate: float = DEFAULT_GBP_AUD_RATE,
    landed_cost_markup: float = DEFAULT_LANDED_COST_MARKUP,
) -> Optional[float]:
    """
    Full sale price calculation from GBP supplier cost.
    Returns the GST-inclusive, .99-rounded consumer price in AUD.
    """
    if cost_gbp is None:
        return None
    aud_base = cost_gbp * gbp_aud_rate
    total_cost = aud_base * landed_cost_markup
    pre_gst_sale = total_cost * (1 + get_margin(cost_gbp))
    return round_up_to_99(pre_gst_sale * GST_RATE)


def calculate_shopify_cost_aud(
    cost_gbp: Optional[float],
    gbp_aud_rate: float = DEFAULT_GBP_AUD_RATE,
    landed_cost_markup: float = DEFAULT_LANDED_COST_MARKUP,
) -> Optional[float]:
    """
    Convert GBP cost to AUD landed cost for Shopify variant cost field.
    Does NOT apply margin or GST — this is the internal cost, not the sale price.
    """
    if cost_gbp is None:
        return None
    return round(cost_gbp * gbp_aud_rate * landed_cost_markup, 2)


def pricing_source_for_supplier(supplier: str) -> str:
    """Determine pricing source label based on supplier origin."""
    return "shopify_live" if supplier == "Tape Film" else "gbp_formula_v1"
