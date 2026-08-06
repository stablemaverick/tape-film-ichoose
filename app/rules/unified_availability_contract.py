"""
Future unified availability contract (Phase 3a — definition only).

No consumers are switched to this contract yet. Agent, admin, and storefront
continue to use existing catalog ranking / lookup paths.

Customer-facing prose stays in the application layer. This module only defines
structured statuses and the JSON-shaped read model.
"""

from __future__ import annotations

from typing import Any, Optional, TypedDict

from app.rules.availability_rules import DERIVED_AVAILABILITY_STATUSES


class TapeInventoryBlock(TypedDict, total=False):
    on_hand: int
    committed: int
    available: int
    po_incoming_confirmed: int
    shopify_incoming_reported: int
    # Derived only — never a stored SoT; null when both channels present and unresolved.
    incoming_confirmed_derived: Optional[int]
    incoming_reconciliation_rationale: str
    damaged_or_unavailable: int
    last_synced_at: Optional[str]
    shopify_location_id: str


class SupplierAvailabilityBlock(TypedDict, total=False):
    supplier_id: str
    supplier_sku: Optional[str]
    status: str  # controlled supplier availability status
    reported_quantity: Optional[int]
    quantity_is_exact: bool
    supplier_can_supply: Optional[bool]
    # Explicitly separate from supplier_can_supply — application/policy decides.
    customer_can_purchase: Optional[bool]
    unit_cost: Optional[float]
    currency: Optional[str]
    last_seen_at: Optional[str]
    source_feed_at: Optional[str]
    pipeline_completed_at: Optional[str]
    # Derived from timestamp facts — not the sole SoT.
    feed_freshness: str
    availability_confidence: Optional[float]
    availability_confidence_version: Optional[str]


class AvailabilitySummaryBlock(TypedDict, total=False):
    sellable_from_tape_stock: bool
    supplier_replenishment_available: bool
    preferred_fulfilment_source: Optional[str]  # tape_stock | supplier | none
    # Structured status — NOT customer prose.
    customer_availability_status: str
    confidence: Optional[float]


class VariantAvailabilityContract(TypedDict, total=False):
    release_variant_id: str
    barcode: Optional[str]
    publication_status: str
    tape_inventory: Optional[TapeInventoryBlock]  # null for supplier-only unsstocked releases
    supplier_availability: list[SupplierAvailabilityBlock]
    availability: AvailabilitySummaryBlock


UNIFIED_AVAILABILITY_CONTRACT_VERSION = "2026-08-06.phase3b"

EXAMPLE_VARIANT_AVAILABILITY: dict[str, Any] = {
    "contract_version": UNIFIED_AVAILABILITY_CONTRACT_VERSION,
    "release_variant_id": "rv_123",
    "publication_status": "published",
    "barcode": "5055201851234",
    "tape_inventory": {
        "on_hand": 4,
        "committed": 1,
        "available": 3,
        "po_incoming_confirmed": 6,
        "shopify_incoming_reported": 6,
        "incoming_confirmed_derived": 6,
        "incoming_reconciliation_rationale": "both_present_prefer_po",
        "damaged_or_unavailable": 0,
        "last_synced_at": "2026-08-06T00:00:00+00:00",
        "shopify_location_id": "gid://shopify/Location/example",
    },
    "supplier_availability": [
        {
            "supplier_id": "moovies",
            "supplier_sku": "ABC123",
            "status": "in_stock",
            "reported_quantity": 14,
            "quantity_is_exact": True,
            "supplier_can_supply": True,
            "customer_can_purchase": None,
            "unit_cost": 18.5,
            "currency": "GBP",
            "last_seen_at": "2026-08-06T00:30:00+00:00",
            "source_feed_at": "2026-08-05T22:00:00+00:00",
            "pipeline_completed_at": "2026-08-06T00:35:00+00:00",
            "feed_freshness": "fresh",
            "availability_confidence": 0.95,
            "availability_confidence_version": "v1",
        }
    ],
    "availability": {
        "sellable_from_tape_stock": True,
        "supplier_replenishment_available": True,
        "preferred_fulfilment_source": "tape_stock",
        "customer_availability_status": "in_stock_at_tape",
        "confidence": 0.95,
    },
}


def is_structured_customer_availability_status(status: str) -> bool:
    return status in DERIVED_AVAILABILITY_STATUSES
