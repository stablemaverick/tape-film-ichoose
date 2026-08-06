"""
Supplier availability normalisation and freshness derivation.

Controlled internal availability statuses:
  in_stock | low_stock | preorder | backorder | unavailable | discontinued | unknown

Rules:
  - Never invent numeric quantities for non-numeric supplier statuses.
  - Persist freshness *inputs* as facts; derive fresh/aging/stale/unknown.
  - If availability_confidence is persisted, include AVAILABILITY_CONFIDENCE_VERSION.
  - supplier_can_supply is distinct from customer_can_purchase.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

# ---------------------------------------------------------------------------
# Controlled vocabularies
# ---------------------------------------------------------------------------

SUPPLIER_AVAILABILITY_STATUSES = frozenset(
    {
        "in_stock",
        "low_stock",
        "preorder",
        "backorder",
        "unavailable",
        "discontinued",
        "unknown",
    }
)

FEED_FRESHNESS_STATUSES = frozenset({"fresh", "aging", "stale", "unknown"})

# Structured derived statuses for the future unified availability contract.
# Customer-facing prose stays in the application layer.
DERIVED_AVAILABILITY_STATUSES = frozenset(
    {
        "in_stock_at_tape",
        "low_stock_at_tape",
        "incoming_to_tape",
        "available_from_supplier",
        "preorder",
        "backorder",
        "temporarily_unavailable",
        "discontinued",
        "unknown",
    }
)

# Confidence formula version — bump when compute_availability_confidence_v1 changes.
AVAILABILITY_CONFIDENCE_VERSION = "v1"

# Default freshness windows (hours). Override via config at call sites / env in Phase 3b+.
DEFAULT_FRESH_MAX_HOURS = 36
DEFAULT_AGING_MAX_HOURS = 72
DEFAULT_LOW_STOCK_MAX_QTY = 3

# Legacy / operational catalog strings → controlled status.
_STATUS_ALIASES: dict[str, str] = {
    "supplier_stock": "in_stock",
    "store_stock": "in_stock",
    "in stock": "in_stock",
    "instock": "in_stock",
    "available": "in_stock",
    "yes": "in_stock",
    "low stock": "low_stock",
    "low_stock": "low_stock",
    "lowstock": "low_stock",
    "supplier_out": "unavailable",
    "store_out": "unavailable",
    "out of stock": "unavailable",
    "out": "unavailable",
    "oos": "unavailable",
    "unavailable": "unavailable",
    "none": "unavailable",
    "n/a": "unavailable",
    "preorder": "preorder",
    "pre-order": "preorder",
    "pre_order": "preorder",
    "supplier_preorder": "preorder",
    "backorder": "backorder",
    "back-order": "backorder",
    "on order": "backorder",
    "on_order": "backorder",
    "discontinued": "discontinued",
    "inactive": "discontinued",
    "unknown": "unknown",
}


@dataclass(frozen=True)
class NormalisedSupplierAvailability:
    """Normalised supplier availability facts (no customer purchase eligibility)."""

    availability_status: str
    reported_quantity: Optional[int]
    quantity_is_exact: bool
    supplier_can_supply: Optional[bool]
    raw_status_text: Optional[str]
    availability_confidence: Optional[float]
    availability_confidence_version: Optional[str]


@dataclass(frozen=True)
class FreshnessAssessment:
    """Derived freshness — not a source-of-truth label to persist as sole fact."""

    status: str  # fresh | aging | stale | unknown
    age_hours: Optional[float]
    reference_timestamp: Optional[datetime]


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _parse_nonneg_int(value: Any) -> Optional[int]:
    """Parse an exact non-negative integer quantity. Never invent from status text."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        if value < 0 or not value.is_integer():
            return None
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        n = int(text)
        return n if n >= 0 else None
    return None


def map_supplier_availability_status(
    *,
    raw_status: Any = None,
    reported_quantity: Any = None,
    release_date_is_future: bool = False,
    low_stock_max_qty: int = DEFAULT_LOW_STOCK_MAX_QTY,
) -> str:
    """
    Map feed status / qty into the controlled availability vocabulary.

    Quantity alone never invents stock for textual "in stock" without a number.
    Future release date can elevate to preorder when status is otherwise unknown/unavailable.
    """
    qty = _parse_nonneg_int(reported_quantity)
    raw = _clean_text(raw_status)
    mapped: Optional[str] = None
    if raw:
        key = raw.casefold().replace("_", " ").strip()
        key_compact = key.replace(" ", "_")
        mapped = _STATUS_ALIASES.get(key) or _STATUS_ALIASES.get(key_compact) or _STATUS_ALIASES.get(
            raw.casefold()
        )
        if mapped is None and raw.casefold() in SUPPLIER_AVAILABILITY_STATUSES:
            mapped = raw.casefold()

    if mapped in SUPPLIER_AVAILABILITY_STATUSES:
        status = mapped
    elif qty is not None:
        if qty == 0:
            status = "unavailable"
        elif qty <= max(0, low_stock_max_qty):
            status = "low_stock"
        else:
            status = "in_stock"
    else:
        status = "unknown"

    if release_date_is_future and status in {"unknown", "unavailable", "in_stock", "low_stock"}:
        # Do not override discontinued.
        if status != "discontinued":
            status = "preorder"

    return status


def supplier_can_supply_from_status(status: str) -> Optional[bool]:
    """
    Whether the supplier can supply this SKU based on normalised status.

    This is NOT customer_can_purchase.
    """
    if status in {"in_stock", "low_stock", "preorder", "backorder"}:
        return True
    if status in {"unavailable", "discontinued"}:
        return False
    if status == "unknown":
        return None
    return None


def normalise_supplier_availability(
    *,
    raw_status: Any = None,
    reported_quantity: Any = None,
    release_date_is_future: bool = False,
    low_stock_max_qty: int = DEFAULT_LOW_STOCK_MAX_QTY,
    feed_freshness: Optional[str] = None,
    match_confidence: Optional[float] = None,
) -> NormalisedSupplierAvailability:
    """
    Build normalised supplier availability facts.

    reported_quantity is set only when the source provides a parseable non-negative integer.
    Textual statuses such as \"In stock\" never become 999 (or any invented qty).
    """
    qty = _parse_nonneg_int(reported_quantity)
    quantity_is_exact = qty is not None
    status = map_supplier_availability_status(
        raw_status=raw_status,
        reported_quantity=qty if quantity_is_exact else None,
        release_date_is_future=release_date_is_future,
        low_stock_max_qty=low_stock_max_qty,
    )
    # If only a textual status was provided (no numeric qty), keep qty null.
    if not quantity_is_exact:
        qty = None

    can_supply = supplier_can_supply_from_status(status)
    confidence = compute_availability_confidence_v1(
        status=status,
        quantity_is_exact=quantity_is_exact,
        feed_freshness=feed_freshness,
        match_confidence=match_confidence,
    )
    return NormalisedSupplierAvailability(
        availability_status=status,
        reported_quantity=qty,
        quantity_is_exact=quantity_is_exact,
        supplier_can_supply=can_supply,
        raw_status_text=_clean_text(raw_status),
        availability_confidence=confidence,
        availability_confidence_version=AVAILABILITY_CONFIDENCE_VERSION if confidence is not None else None,
    )


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    text = _clean_text(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _as_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def pick_freshness_reference(
    *,
    last_seen_at: Any = None,
    source_feed_at: Any = None,
    pipeline_completed_at: Any = None,
) -> Optional[datetime]:
    """
    Prefer the most recent reliable freshness input.

    Order of preference for *age calculation*: newest among available inputs.
    Missing all inputs → unknown freshness.
    """
    candidates = [
        _parse_datetime(last_seen_at),
        _parse_datetime(source_feed_at),
        _parse_datetime(pipeline_completed_at),
    ]
    present = [c for c in candidates if c is not None]
    if not present:
        return None
    return max(present)


def derive_feed_freshness(
    *,
    last_seen_at: Any = None,
    source_feed_at: Any = None,
    pipeline_completed_at: Any = None,
    now: Optional[datetime] = None,
    fresh_max_hours: float = DEFAULT_FRESH_MAX_HOURS,
    aging_max_hours: float = DEFAULT_AGING_MAX_HOURS,
    pipeline_failed: bool = False,
) -> FreshnessAssessment:
    """
    Derive fresh | aging | stale | unknown from timestamp facts.

    A failed pipeline must not force unavailable — callers should keep last-known
    offer status and only age freshness / lower confidence.
    """
    if pipeline_failed and not any(
        [_parse_datetime(last_seen_at), _parse_datetime(source_feed_at), _parse_datetime(pipeline_completed_at)]
    ):
        return FreshnessAssessment(status="unknown", age_hours=None, reference_timestamp=None)

    ref = pick_freshness_reference(
        last_seen_at=last_seen_at,
        source_feed_at=source_feed_at,
        pipeline_completed_at=pipeline_completed_at,
    )
    if ref is None:
        return FreshnessAssessment(status="unknown", age_hours=None, reference_timestamp=None)

    now_dt = _as_utc(now or datetime.now(timezone.utc))
    age_hours = max(0.0, (now_dt - ref).total_seconds() / 3600.0)
    if age_hours <= fresh_max_hours:
        status = "fresh"
    elif age_hours <= aging_max_hours:
        status = "aging"
    else:
        status = "stale"
    return FreshnessAssessment(status=status, age_hours=age_hours, reference_timestamp=ref)


def compute_availability_confidence_v1(
    *,
    status: str,
    quantity_is_exact: bool,
    feed_freshness: Optional[str] = None,
    match_confidence: Optional[float] = None,
) -> float:
    """
    Reproducible confidence score (version AVAILABILITY_CONFIDENCE_VERSION).

    Formula v1:
      base = 0.55
      + 0.20 if quantity_is_exact
      + freshness: fresh +0.15, aging +0.05, stale -0.20, unknown -0.10
      + status: in_stock/low_stock +0.05, preorder/backorder +0.00,
                unavailable +0.00, discontinued +0.05, unknown -0.15
      * match_confidence if provided (else treat as 1.0)
      clamp to [0, 1]
    """
    base = 0.55
    if quantity_is_exact:
        base += 0.20

    freshness = (feed_freshness or "unknown").casefold()
    if freshness == "fresh":
        base += 0.15
    elif freshness == "aging":
        base += 0.05
    elif freshness == "stale":
        base -= 0.20
    else:
        base -= 0.10

    st = (status or "unknown").casefold()
    if st in {"in_stock", "low_stock"}:
        base += 0.05
    elif st == "discontinued":
        base += 0.05
    elif st == "unknown":
        base -= 0.15

    mc = 1.0 if match_confidence is None else max(0.0, min(1.0, float(match_confidence)))
    score = base * mc
    return round(max(0.0, min(1.0, score)), 4)


def observation_material_fingerprint(
    *,
    availability_status: str,
    reported_quantity: Optional[int],
    quantity_is_exact: bool,
    supplier_can_supply: Optional[bool],
    unit_cost: Any = None,
    currency: Optional[str] = None,
) -> str:
    """
    Stable fingerprint of material supplier observation fields.

    Used to avoid duplicate observations/events when source data is unchanged.
    """
    cost = ""
    if unit_cost is not None and str(unit_cost).strip() != "":
        try:
            cost = f"{float(unit_cost):.4f}"
        except (TypeError, ValueError):
            cost = str(unit_cost).strip()
    can = {True: "1", False: "0", None: ""}[supplier_can_supply]
    qty = "" if reported_quantity is None else str(int(reported_quantity))
    cur = (_clean_text(currency) or "").upper()
    return "|".join(
        [
            (availability_status or "unknown").casefold(),
            qty,
            "1" if quantity_is_exact else "0",
            can,
            cost,
            cur,
        ]
    )


def build_observation_dedupe_key(supplier_offer_id: str, fingerprint: str) -> str:
    """Dedupe key unique per offer + material fingerprint (unchanged input → same key)."""
    return f"obs:{supplier_offer_id}:{fingerprint}"


def legacy_catalog_status_to_controlled(availability_status: Any) -> str:
    """Map existing catalog_items.availability_status values into the controlled set."""
    return map_supplier_availability_status(raw_status=availability_status)


def supplier_offer_row_for_persist(
    normalised: NormalisedSupplierAvailability,
    freshness_inputs: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Shape a supplier_offers write payload from normalised facts + freshness inputs.

    Does not include a persisted feed_freshness label — callers derive it.
    """
    return {
        "availability_status": normalised.availability_status,
        "reported_quantity": normalised.reported_quantity,
        "quantity_is_exact": normalised.quantity_is_exact,
        "supplier_can_supply": normalised.supplier_can_supply,
        "raw_status_text": normalised.raw_status_text,
        "availability_confidence": normalised.availability_confidence,
        "availability_confidence_version": normalised.availability_confidence_version,
        "last_seen_at": freshness_inputs.get("last_seen_at"),
        "source_feed_at": freshness_inputs.get("source_feed_at"),
        "pipeline_completed_at": freshness_inputs.get("pipeline_completed_at"),
        "latest_successful_pipeline_run_id": freshness_inputs.get(
            "latest_successful_pipeline_run_id"
        ),
    }
