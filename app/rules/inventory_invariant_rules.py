"""
Inventory ownership rules and reconciliation invariants.

Source ownership (enforced by writers in Phase 3b+; validated here as pure rules):

  * Shopify owns current TAPE inventory facts (tape_inventory_levels).
  * Purchase orders own confirmed ordered / po_incoming_confirmed quantities.
  * Supplier feeds own supplier availability observations (supplier_offers /
    supplier_offer_observations).
  * Supplier quantities must never update Shopify inventory or tape on_hand.
  * Preorder / supplier availability must never create positive TAPE on-hand.
  * Derived values and recommendations must never overwrite source facts.

Shopify quantity relationships are validated as warnings — production data
includes available < 0 when committed > on_hand (CONTINUE / preorder oversolds).
Do not treat available <= on_hand as a hard DB constraint in Phase 3a.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Mapping, Optional, Sequence

from app.rules.availability_rules import (
    DERIVED_AVAILABILITY_STATUSES,
    observation_material_fingerprint,
)

# Writers allowed to mutate each fact class (documentation + guard helpers).
OWNER_WRITERS = {
    "tape_inventory_levels": frozenset({"shopify_store_sync", "tape_inventory_receipt"}),
    "supplier_offers": frozenset({"supplier_normalize", "supplier_stock_sync"}),
    "supplier_offer_observations": frozenset({"supplier_normalize", "supplier_stock_sync"}),
    "purchase_orders": frozenset({"purchase_order_ingest"}),
    "purchase_order_lines": frozenset({"purchase_order_ingest", "purchase_order_receipt"}),
    "inventory_events": frozenset(
        {
            "shopify_store_sync",
            "supplier_normalize",
            "supplier_stock_sync",
            "purchase_order_ingest",
            "purchase_order_receipt",
        }
    ),
}

# Fields that are source facts vs derived (must not be written back as facts).
TAPE_SOURCE_FIELDS = frozenset(
    {
        "on_hand",
        "committed",
        "available",
        "shopify_incoming_reported",
        "damaged_or_unavailable",
        "last_synced_at",
    }
)
TAPE_PO_OWNED_FIELDS = frozenset({"po_incoming_confirmed"})
SUPPLIER_SOURCE_FIELDS = frozenset(
    {
        "availability_status",
        "reported_quantity",
        "quantity_is_exact",
        "supplier_can_supply",
        "unit_cost",
        "currency",
        "last_seen_at",
        "source_feed_at",
        "pipeline_completed_at",
        "latest_successful_pipeline_run_id",
        "raw_status_text",
        "raw_payload",
    }
)
DERIVED_ONLY_FIELDS = frozenset(
    {
        "feed_freshness",
        "customer_can_purchase",
        "customer_availability_status",
        "preferred_fulfilment_source",
        "sellable_from_tape_stock",
        "supplier_replenishment_available",
        "incoming_confirmed",  # combined derived — not a stored SoT
        "reorder_quantity",
        "stockout_risk",
        "days_of_cover",
    }
)


@dataclass(frozen=True)
class InvariantViolation:
    code: str
    message: str
    severity: str  # error | warning
    context: Mapping[str, Any]


def _as_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None:
        return default
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def writer_may_mutate(table: str, writer: str) -> bool:
    allowed = OWNER_WRITERS.get(table)
    if allowed is None:
        return False
    return writer in allowed


def reject_derived_fields_in_fact_payload(payload: Mapping[str, Any]) -> List[InvariantViolation]:
    """Derived / recommendation fields must not be written onto source-fact tables."""
    hits = [k for k in payload.keys() if k in DERIVED_ONLY_FIELDS]
    if not hits:
        return []
    return [
        InvariantViolation(
            code="derived_field_on_fact_table",
            message=f"Derived fields must not overwrite source facts: {sorted(hits)}",
            severity="error",
            context={"fields": sorted(hits)},
        )
    ]


def validate_tape_inventory_levels(row: Mapping[str, Any]) -> List[InvariantViolation]:
    """
    Validate TAPE inventory facts.

    Hard errors: non-integer-coercible junk handled by callers; negative damaged qty.
    Warnings: Shopify-semantic relationships that can be legitimate in production.
    """
    violations: List[InvariantViolation] = []
    on_hand = _as_int(row.get("on_hand"), 0)
    committed = _as_int(row.get("committed"), 0)
    available = _as_int(row.get("available"), 0)
    po_incoming = _as_int(row.get("po_incoming_confirmed"), 0)
    shopify_incoming = _as_int(row.get("shopify_incoming_reported"), 0)
    damaged = _as_int(row.get("damaged_or_unavailable"), 0)

    assert on_hand is not None and committed is not None and available is not None
    assert po_incoming is not None and shopify_incoming is not None and damaged is not None

    if damaged < 0:
        violations.append(
            InvariantViolation(
                code="damaged_negative",
                message="damaged_or_unavailable cannot be negative",
                severity="error",
                context={"damaged_or_unavailable": damaged},
            )
        )
    if po_incoming < 0:
        violations.append(
            InvariantViolation(
                code="po_incoming_negative",
                message="po_incoming_confirmed cannot be negative",
                severity="error",
                context={"po_incoming_confirmed": po_incoming},
            )
        )
    if shopify_incoming < 0:
        violations.append(
            InvariantViolation(
                code="shopify_incoming_negative",
                message="shopify_incoming_reported cannot be negative",
                severity="error",
                context={"shopify_incoming_reported": shopify_incoming},
            )
        )

    # Production: available can be negative; committed can exceed on_hand.
    if available > on_hand:
        violations.append(
            InvariantViolation(
                code="available_exceeds_on_hand",
                message="available exceeds on_hand (unexpected under Shopify identity available=on_hand-committed)",
                severity="warning",
                context={"available": available, "on_hand": on_hand, "committed": committed},
            )
        )
    if committed < 0:
        violations.append(
            InvariantViolation(
                code="committed_negative",
                message="committed is negative (not observed in production sample)",
                severity="warning",
                context={"committed": committed},
            )
        )
    if on_hand < 0:
        violations.append(
            InvariantViolation(
                code="on_hand_negative",
                message="on_hand is negative (not observed in production sample)",
                severity="warning",
                context={"on_hand": on_hand},
            )
        )
    if available != on_hand - committed:
        violations.append(
            InvariantViolation(
                code="available_identity_mismatch",
                message="available != on_hand - committed (Shopify usually enforces this identity)",
                severity="warning",
                context={"available": available, "on_hand": on_hand, "committed": committed},
            )
        )
    if committed > max(on_hand, 0):
        violations.append(
            InvariantViolation(
                code="committed_exceeds_on_hand",
                message="committed exceeds on_hand (common for CONTINUE/preorder oversolds)",
                severity="warning",
                context={"committed": committed, "on_hand": on_hand, "available": available},
            )
        )

    return violations


def validate_incoming_not_auto_summed(row: Mapping[str, Any]) -> List[InvariantViolation]:
    """
    Guard: a combined incoming_confirmed must not be persisted as if it were a fact
    equal to po_incoming + shopify_incoming without explicit reconciliation.
    """
    if "incoming_confirmed" not in row:
        return []
    combined = _as_int(row.get("incoming_confirmed"))
    po_incoming = _as_int(row.get("po_incoming_confirmed"), 0) or 0
    shopify_incoming = _as_int(row.get("shopify_incoming_reported"), 0) or 0
    if combined is None:
        return []
    if combined == po_incoming + shopify_incoming and po_incoming > 0 and shopify_incoming > 0:
        return [
            InvariantViolation(
                code="incoming_auto_sum_forbidden",
                message=(
                    "incoming_confirmed must not be auto-summed from po_incoming_confirmed + "
                    "shopify_incoming_reported without reconciliation (possible double-count)"
                ),
                severity="error",
                context={
                    "incoming_confirmed": combined,
                    "po_incoming_confirmed": po_incoming,
                    "shopify_incoming_reported": shopify_incoming,
                },
            )
        ]
    return []


def reconcile_incoming_confirmed(
    *,
    po_incoming_confirmed: int,
    shopify_incoming_reported: int,
    strategy: str = "prefer_po_when_both",
) -> dict[str, Any]:
    """
    Documented reconciliation for a *derived* incoming figure (read model only).

    Strategies:
      prefer_po_when_both — if both > 0, use PO only (Shopify may already mirror POs).
      prefer_shopify_when_both — opposite.
      max_when_both — max(po, shopify) when both > 0.
      separate_only — never combine; derived value is null.

    Phase 3a does not expose this on consumers; dual-write/read model may use it later.
    """
    po = max(0, int(po_incoming_confirmed))
    shop = max(0, int(shopify_incoming_reported))
    if po == 0 and shop == 0:
        derived = 0
        rationale = "both_zero"
    elif po > 0 and shop == 0:
        derived = po
        rationale = "po_only"
    elif shop > 0 and po == 0:
        derived = shop
        rationale = "shopify_only"
    elif strategy == "prefer_po_when_both":
        derived = po
        rationale = "both_present_prefer_po"
    elif strategy == "prefer_shopify_when_both":
        derived = shop
        rationale = "both_present_prefer_shopify"
    elif strategy == "max_when_both":
        derived = max(po, shop)
        rationale = "both_present_use_max"
    elif strategy == "separate_only":
        derived = None
        rationale = "both_present_left_uncombined"
    else:
        raise ValueError(f"Unknown incoming reconciliation strategy: {strategy}")

    return {
        "po_incoming_confirmed": po,
        "shopify_incoming_reported": shop,
        "incoming_confirmed_derived": derived,
        "strategy": strategy,
        "rationale": rationale,
        "possible_double_count": po > 0 and shop > 0,
    }


def validate_supplier_must_not_update_tape(
    *,
    tape_mutation_fields: Iterable[str],
    source: str,
) -> List[InvariantViolation]:
    """Supplier feed writers must not mutate TAPE on-hand / available / committed."""
    if source not in {"supplier_normalize", "supplier_stock_sync", "supplier_feed"}:
        return []
    forbidden = set(tape_mutation_fields) & {
        "on_hand",
        "committed",
        "available",
        "shopify_incoming_reported",
        "damaged_or_unavailable",
    }
    if not forbidden:
        return []
    return [
        InvariantViolation(
            code="supplier_updates_tape_inventory",
            message="Supplier quantities must never update TAPE / Shopify inventory facts",
            severity="error",
            context={"fields": sorted(forbidden), "source": source},
        )
    ]


def validate_preorder_no_positive_tape_on_hand(
    *,
    is_preorder: bool,
    on_hand: int,
    source: str,
) -> List[InvariantViolation]:
    """
    Preorder or supplier availability must never *create* positive TAPE on-hand.

    Checking: a writer that is not Shopify must not set on_hand > 0 for preorders.
    Shopify-synced positive on_hand on a preorder metafield product is a warning
    (seen rarely in production, sometimes test products).
    """
    if not is_preorder or on_hand <= 0:
        return []
    if source in {"supplier_normalize", "supplier_stock_sync", "supplier_feed", "purchase_order_ingest"}:
        return [
            InvariantViolation(
                code="preorder_positive_on_hand_from_non_shopify",
                message="Preorder/supplier paths must not create positive TAPE on_hand",
                severity="error",
                context={"on_hand": on_hand, "source": source},
            )
        ]
    return [
        InvariantViolation(
            code="preorder_positive_on_hand_shopify",
            message="Shopify reports positive on_hand for a preorder product",
            severity="warning",
            context={"on_hand": on_hand, "source": source},
        )
    ]


def validate_purchase_order_line(line: Mapping[str, Any]) -> List[InvariantViolation]:
    violations: List[InvariantViolation] = []
    ordered = _as_int(line.get("quantity_ordered"), 0) or 0
    confirmed = _as_int(line.get("quantity_confirmed"), 0) or 0
    received = _as_int(line.get("quantity_received"), 0) or 0
    cancelled = _as_int(line.get("quantity_cancelled"), 0) or 0
    over = bool(line.get("over_receipt_adjustment"))

    for name, val in (
        ("quantity_ordered", ordered),
        ("quantity_confirmed", confirmed),
        ("quantity_received", received),
        ("quantity_cancelled", cancelled),
    ):
        if val < 0:
            violations.append(
                InvariantViolation(
                    code="po_quantity_negative",
                    message=f"{name} cannot be negative",
                    severity="error",
                    context={name: val},
                )
            )
    if received > ordered and not over:
        violations.append(
            InvariantViolation(
                code="po_received_exceeds_ordered",
                message="quantity_received exceeds quantity_ordered without over_receipt_adjustment",
                severity="error",
                context={"quantity_received": received, "quantity_ordered": ordered},
            )
        )
    if confirmed > ordered and ordered >= 0:
        violations.append(
            InvariantViolation(
                code="po_confirmed_exceeds_ordered",
                message="quantity_confirmed exceeds quantity_ordered",
                severity="warning",
                context={"quantity_confirmed": confirmed, "quantity_ordered": ordered},
            )
        )
    return violations


def validate_resolution_uniqueness(
    active_resolutions: Sequence[Mapping[str, Any]],
) -> List[InvariantViolation]:
    """One supplier SKU should resolve to one release_variant at a time (active rows)."""
    violations: List[InvariantViolation] = []
    by_sku: dict[tuple[str, str], List[Any]] = {}
    for row in active_resolutions:
        if not row.get("active", True):
            continue
        supplier_id = str(row.get("supplier_id") or "")
        sku = str(row.get("supplier_sku") or "").strip()
        if not supplier_id or not sku:
            continue
        resolved = row.get("resolved_release_variant_id")
        if resolved is None:
            resolved = row.get("resolved_variant_id")  # legacy alias in tests
        by_sku.setdefault((supplier_id, sku), []).append(resolved)
    for key, releases in by_sku.items():
        distinct = {v for v in releases if v}
        if len(releases) > 1:
            violations.append(
                InvariantViolation(
                    code="supplier_sku_multiple_active_resolutions",
                    message="One supplier SKU has multiple active resolutions",
                    severity="error",
                    context={"supplier_id": key[0], "supplier_sku": key[1], "count": len(releases)},
                )
            )
        if len(distinct) > 1:
            violations.append(
                InvariantViolation(
                    code="supplier_sku_multiple_variants",
                    message="One supplier SKU resolves to multiple release_variants",
                    severity="error",
                    context={
                        "supplier_id": key[0],
                        "supplier_sku": key[1],
                        "release_variant_ids": sorted(str(v) for v in distinct),
                    },
                )
            )
    return violations


def validate_barcode_variant_conflicts(
    identifiers: Sequence[Mapping[str, Any]],
) -> List[InvariantViolation]:
    """One barcode mapping to multiple active release_variants should be flagged."""
    by_barcode: dict[str, set[str]] = {}
    for row in identifiers:
        if (row.get("id_type") or "").casefold() not in {"barcode", "ean", "upc"}:
            continue
        if row.get("is_valid") is False:
            continue
        value = str(row.get("id_value") or "").strip()
        release_id = str(
            row.get("release_variant_id") or row.get("variant_id") or ""
        ).strip()
        if not value or not release_id:
            continue
        by_barcode.setdefault(value, set()).add(release_id)
    violations: List[InvariantViolation] = []
    for barcode, releases in by_barcode.items():
        if len(releases) > 1:
            violations.append(
                InvariantViolation(
                    code="barcode_maps_multiple_variants",
                    message="Barcode maps to multiple active release_variants",
                    severity="warning",
                    context={"barcode": barcode, "release_variant_ids": sorted(releases)},
                )
            )
    return violations


def validate_stale_feed_no_mass_unavailable(
    *,
    pipeline_failed_or_stale: bool,
    proposed_status_updates: Sequence[str],
    mass_unavailable_threshold: int = 50,
) -> List[InvariantViolation]:
    """Failed/stale feeds must not flip large sets of offers to unavailable."""
    if not pipeline_failed_or_stale:
        return []
    unavailable_writes = sum(
        1 for s in proposed_status_updates if (s or "").casefold() == "unavailable"
    )
    if unavailable_writes >= mass_unavailable_threshold:
        return [
            InvariantViolation(
                code="stale_feed_mass_unavailable",
                message="Failed/stale feed attempted mass unavailable updates",
                severity="error",
                context={
                    "unavailable_writes": unavailable_writes,
                    "threshold": mass_unavailable_threshold,
                },
            )
        ]
    return []


def should_emit_observation(
    *,
    previous_fingerprint: Optional[str],
    new_fingerprint: str,
) -> bool:
    """Unchanged supplier input must not create duplicate observations."""
    if previous_fingerprint is None:
        return True
    return previous_fingerprint != new_fingerprint


def should_emit_inventory_event(
    *,
    previous_fingerprint: Optional[str],
    new_fingerprint: str,
) -> bool:
    """Unchanged material state must not create duplicate inventory events."""
    return should_emit_observation(
        previous_fingerprint=previous_fingerprint,
        new_fingerprint=new_fingerprint,
    )


def supplier_observation_fingerprint_from_row(row: Mapping[str, Any]) -> str:
    return observation_material_fingerprint(
        availability_status=str(row.get("availability_status") or "unknown"),
        reported_quantity=_as_int(row.get("reported_quantity")),
        quantity_is_exact=bool(row.get("quantity_is_exact")),
        supplier_can_supply=row.get("supplier_can_supply"),
        unit_cost=row.get("unit_cost"),
        currency=row.get("currency"),
    )


def validate_supplier_only_has_no_tape_requirement(
    *,
    publication_status: str,
    has_tape_inventory_row: bool,
) -> List[InvariantViolation]:
    """
    Supplier-only releases may exist without tape_inventory_levels.

    Having a tape row for supplier_only is allowed only after physical stocking
    (warning if present while still supplier_only — ops should promote publication_status).
    """
    status = (publication_status or "").casefold()
    if status == "supplier_only" and has_tape_inventory_row:
        return [
            InvariantViolation(
                code="supplier_only_with_tape_inventory",
                message=(
                    "release_variant is supplier_only but has tape_inventory_levels; "
                    "promote publication_status when TAPE physically stocks it"
                ),
                severity="warning",
                context={"publication_status": publication_status},
            )
        ]
    return []


def derive_structured_availability_status(
    *,
    tape_on_hand: int = 0,
    tape_available: int = 0,
    tape_committed: int = 0,
    po_incoming_confirmed: int = 0,
    shopify_incoming_reported: int = 0,
    supplier_statuses: Sequence[str] = (),
    supplier_can_supply_flags: Sequence[Optional[bool]] = (),
    low_stock_at_tape_max: int = 2,
) -> str:
    """
    Future unified-contract structured status (not customer prose).

    Does not auto-sum incoming channels for decisioning beyond \"any incoming > 0\".
    """
    if tape_available > low_stock_at_tape_max:
        return "in_stock_at_tape"
    if tape_available > 0:
        return "low_stock_at_tape"
    if po_incoming_confirmed > 0 or shopify_incoming_reported > 0:
        return "incoming_to_tape"

    statuses = [(s or "").casefold() for s in supplier_statuses]
    can_flags = list(supplier_can_supply_flags)
    if any(s == "preorder" for s in statuses):
        return "preorder"
    if any(s == "backorder" for s in statuses):
        return "backorder"
    if any(s == "discontinued" for s in statuses) and not any(
        f is True for f in can_flags
    ):
        return "discontinued"
    if any(f is True for f in can_flags) or any(
        s in {"in_stock", "low_stock"} for s in statuses
    ):
        return "available_from_supplier"
    if statuses and all(s in {"unavailable", "unknown"} for s in statuses):
        return "temporarily_unavailable"
    if tape_on_hand == 0 and tape_committed > 0:
        return "temporarily_unavailable"
    return "unknown"


def assert_derived_status_known(status: str) -> bool:
    return status in DERIVED_AVAILABILITY_STATUSES
