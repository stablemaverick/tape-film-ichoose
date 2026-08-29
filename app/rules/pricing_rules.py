"""
Pricing rules for supplier cost → sale price calculation.

Canonical Region B implementation — do not duplicate this formula elsewhere.
Policy: docs/inventory-intelligence/region-b-film-pricing-policy.md

For existing catalogue repricing, DEFAULT_MARGIN_FLOOR_RATIO (28%) is a floor,
not a target: raise under-floor retail to the minimum valid .99 price; never
auto-reduce prices that already clear the floor. Competitor prices are not an
input to this calculator.

Conversion pipeline:
  1. cost_gbp (supplier invoice price in GBP)
  2. × GBP_AUD_RATE (default 2.0) → AUD base cost
  3. × LANDED_COST_MARKUP (default 1.12, i.e. +12%) → landed cost
  4. × (1 + margin_for_tier) → pre-GST sale price
  5. × 1.10 (+10% GST) → final sale price
  6. round_up_to_99 → consumer-friendly .99 price

Margin tiers (based on GBP cost) for new/publish paths:
  ≤ £15   -> 32%
  ≤ £30   -> 28%
  ≤ £40   -> 24%
  > £40   -> 20%

Existing Region B film repricing uses the 28% replacement-cost floor path
(calculate_sale_price_with_margin_floor_from_gbp_cost), not tier reduction.

Shopify cost conversion (for variant cost field):
  cost_gbp × GBP_AUD_RATE × LANDED_COST_MARKUP
  (separate from sale price; used in publish_selected_barcodes_to_shopify)

Region A / USD products are out of scope for this GBP calculator.
"""

import logging
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

DEFAULT_GBP_AUD_RATE = 2.0
DEFAULT_LANDED_COST_MARKUP = 1.12
GST_RATE = 1.10
DEFAULT_MARGIN_FLOOR_RATIO = 0.28
DEFAULT_SUPPLIER_COST_ALERT_GBP = 1.00
DEFAULT_SUPPLIER_COST_ALERT_PCT = 0.05
DEFAULT_SUPPLIER_COST_ANOMALY_PCT = 0.25
_ONE_CENT = Decimal("0.01")
_ONE_DOLLAR = Decimal("1.00")
_GST_RATE_DECIMAL = Decimal(str(GST_RATE))
_HALF_CENT = Decimal("0.005")


def _float_env(name: str, default: float) -> tuple[float, str]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default, "default"
    try:
        return float(raw), "env"
    except ValueError:
        return default, "default_invalid_env"


def effective_pricing_assumptions() -> dict[str, Any]:
    """
    Effective commercial assumptions from the single env configuration source.

    Does not log secrets. Values match code defaults when env keys are unset.
    """
    gbp_aud, gbp_src = _float_env("GBP_AUD_RATE", DEFAULT_GBP_AUD_RATE)
    markup, markup_src = _float_env("LANDED_COST_MARKUP", DEFAULT_LANDED_COST_MARKUP)
    floor, floor_src = _float_env("DEFAULT_MARGIN_FLOOR_RATIO", DEFAULT_MARGIN_FLOOR_RATIO)
    gst, gst_src = _float_env("GST_RATE", GST_RATE)
    alert_gbp, alert_gbp_src = _float_env(
        "SUPPLIER_COST_ALERT_GBP", DEFAULT_SUPPLIER_COST_ALERT_GBP
    )
    alert_pct, alert_pct_src = _float_env(
        "SUPPLIER_COST_ALERT_PCT", DEFAULT_SUPPLIER_COST_ALERT_PCT
    )
    anomaly_pct, anomaly_pct_src = _float_env(
        "SUPPLIER_COST_ANOMALY_PCT", DEFAULT_SUPPLIER_COST_ANOMALY_PCT
    )
    return {
        "gbp_aud_rate": gbp_aud,
        "gbp_aud_rate_source": gbp_src,
        "landed_cost_markup": markup,
        "landed_cost_markup_source": markup_src,
        "gst_rate": gst,
        "gst_rate_source": gst_src,
        "margin_floor_ratio": floor,
        "margin_floor_ratio_source": floor_src,
        "supplier_cost_alert_gbp": alert_gbp,
        "supplier_cost_alert_gbp_source": alert_gbp_src,
        "supplier_cost_alert_pct": alert_pct,
        "supplier_cost_alert_pct_source": alert_pct_src,
        "supplier_cost_anomaly_pct": anomaly_pct,
        "supplier_cost_anomaly_pct_source": anomaly_pct_src,
    }


def log_pricing_assumptions(logger: Optional[logging.Logger] = None) -> dict[str, Any]:
    cfg = effective_pricing_assumptions()
    msg = (
        "PRICING_ASSUMPTIONS "
        f"gbp_aud_rate={cfg['gbp_aud_rate']} source={cfg['gbp_aud_rate_source']} "
        f"landed_cost_markup={cfg['landed_cost_markup']} source={cfg['landed_cost_markup_source']} "
        f"gst_rate={cfg['gst_rate']} source={cfg['gst_rate_source']} "
        f"margin_floor_ratio={cfg['margin_floor_ratio']} source={cfg['margin_floor_ratio_source']} "
        f"supplier_cost_alert_gbp={cfg['supplier_cost_alert_gbp']} "
        f"supplier_cost_alert_pct={cfg['supplier_cost_alert_pct']} "
        f"supplier_cost_anomaly_pct={cfg['supplier_cost_anomaly_pct']}"
    )
    if logger is not None:
        logger.info(msg)
    else:
        print(msg, flush=True)
    return cfg


def replacement_landed_cost_aud(
    cost_gbp: Optional[float],
    gbp_aud_rate: float = DEFAULT_GBP_AUD_RATE,
    landed_cost_markup: float = DEFAULT_LANDED_COST_MARKUP,
) -> Optional[float]:
    """
    Current supplier replacement landed AUD.

    Canonical formula lives in calculate_shopify_cost_aud; this name exists so
    callers do not treat the result as Shopify inventoryItem.cost / acquisition cost.
    """
    return calculate_shopify_cost_aud(
        cost_gbp, gbp_aud_rate=gbp_aud_rate, landed_cost_markup=landed_cost_markup
    )


def exact_ex_gst_margin_ratio(
    price_inc_gst: Optional[float],
    landed_cost_aud: Optional[float],
) -> Optional[Decimal]:
    """((price / GST) - landed) / (price / GST) using exact decimal arithmetic."""
    if price_inc_gst is None or landed_cost_aud is None:
        return None
    if price_inc_gst <= 0:
        return None
    price = Decimal(str(price_inc_gst))
    landed = Decimal(str(landed_cost_aud)).quantize(_ONE_CENT, rounding=ROUND_HALF_UP)
    ex_gst = price / _GST_RATE_DECIMAL
    if ex_gst <= 0:
        return None
    return (ex_gst - landed) / ex_gst


def exact_ex_gst_margin_ok(
    price_inc_gst: Optional[float],
    landed_cost_aud: Optional[float],
    *,
    margin_floor_ratio: float = DEFAULT_MARGIN_FLOOR_RATIO,
) -> bool:
    ratio = exact_ex_gst_margin_ratio(price_inc_gst, landed_cost_aud)
    if ratio is None:
        return False
    return ratio >= Decimal(str(margin_floor_ratio))


def classify_supplier_gbp_cost_movement(
    previous_gbp: Optional[float],
    current_gbp: Optional[float],
    *,
    alert_gbp: float = DEFAULT_SUPPLIER_COST_ALERT_GBP,
    alert_pct: float = DEFAULT_SUPPLIER_COST_ALERT_PCT,
    anomaly_pct: float = DEFAULT_SUPPLIER_COST_ANOMALY_PCT,
) -> dict[str, Any]:
    """
    Observability-only supplier GBP cost movement.

    Significant when |delta| >= alert_gbp OR |delta|/previous >= alert_pct.
    Anomalous when |delta|/previous >= anomaly_pct (suspicious feed data).
    Not a gate for margin-protection price increases.
    """
    if previous_gbp is None or current_gbp is None:
        return {
            "classification": "unable_to_validate",
            "significant": False,
            "anomalous": False,
            "previous_gbp": previous_gbp,
            "current_gbp": current_gbp,
            "gbp_delta": None,
            "pct_delta": None,
            "direction": None,
        }
    prev = Decimal(str(previous_gbp))
    curr = Decimal(str(current_gbp))
    delta = curr - prev
    abs_diff = abs(delta)
    pct = abs_diff / abs(prev) if prev != 0 else (Decimal("1") if abs_diff > 0 else Decimal("0"))
    if abs_diff < Decimal("0.0005"):
        direction = "UNCHANGED"
        classification = "UNCHANGED"
        significant = False
        anomalous = False
    else:
        direction = "UP" if delta > 0 else "DOWN"
        significant = abs_diff >= Decimal(str(alert_gbp)) or pct >= Decimal(str(alert_pct))
        anomalous = pct >= Decimal(str(anomaly_pct))
        classification = "SIGNIFICANT_" + direction if significant else direction
    return {
        "classification": classification,
        "significant": significant,
        "anomalous": anomalous,
        "previous_gbp": float(prev),
        "current_gbp": float(curr),
        "gbp_delta": float(delta.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "pct_delta": float((pct * Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        "direction": direction,
    }


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


def calculate_sale_price_with_margin_floor_from_landed_cost(
    landed_cost_aud: Optional[float],
    *,
    margin_floor_ratio: float = DEFAULT_MARGIN_FLOOR_RATIO,
) -> Optional[float]:
    """
    Calculate GST-inclusive retail price from AUD landed cost using a margin floor.

    Margin floor is applied on ex-GST revenue:
      ex_gst_price = landed_cost_aud / (1 - margin_floor_ratio)
      inc_gst_price = ex_gst_price * GST_RATE
      final_price = round_up_to_99(inc_gst_price)

    Returns None when cost is missing/invalid.
    """
    if landed_cost_aud is None:
        return None
    if landed_cost_aud <= 0:
        return None
    if not (0 < margin_floor_ratio < 1):
        raise ValueError("margin_floor_ratio must be between 0 and 1")

    landed_dec = Decimal(str(landed_cost_aud)).quantize(_ONE_CENT, rounding=ROUND_HALF_UP)
    floor_dec = Decimal(str(margin_floor_ratio))
    one = Decimal("1")

    min_inc_gst = (landed_dec / (one - floor_dec)) * _GST_RATE_DECIMAL
    candidate = Decimal(str(round_up_to_99(float(min_inc_gst)))).quantize(
        _ONE_CENT, rounding=ROUND_HALF_UP
    )

    # Enforce strict mathematical floor on exact decimal arithmetic.
    # If boundary math fails for a .99 candidate, advance to next .99.
    for _ in range(100):
        ex_gst = candidate / _GST_RATE_DECIMAL
        margin = (ex_gst - landed_dec) / ex_gst
        if margin >= floor_dec:
            return float(candidate)
        candidate = (candidate + _ONE_DOLLAR).quantize(_ONE_CENT, rounding=ROUND_HALF_UP)

    raise RuntimeError("Unable to satisfy margin floor within 100 .99 increments")


def calculate_sale_price_with_margin_floor_from_gbp_cost(
    cost_gbp: Optional[float],
    *,
    gbp_aud_rate: float = DEFAULT_GBP_AUD_RATE,
    landed_cost_markup: float = DEFAULT_LANDED_COST_MARKUP,
    margin_floor_ratio: float = DEFAULT_MARGIN_FLOOR_RATIO,
) -> Optional[float]:
    """
    Calculate GST-inclusive, .99-rounded retail price from GBP supplier cost
    using landed AUD cost plus a margin floor.
    """
    landed = calculate_shopify_cost_aud(
        cost_gbp, gbp_aud_rate=gbp_aud_rate, landed_cost_markup=landed_cost_markup
    )
    return calculate_sale_price_with_margin_floor_from_landed_cost(
        landed, margin_floor_ratio=margin_floor_ratio
    )


def pricing_source_for_supplier(supplier: str) -> str:
    """Determine pricing source label based on supplier origin."""
    return "shopify_live" if supplier == "Tape Film" else "gbp_formula_v1"
