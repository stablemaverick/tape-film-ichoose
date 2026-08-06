"""Tests for inventory ownership and invariant rules."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.inventory_invariant_rules import (
    assert_derived_status_known,
    derive_structured_availability_status,
    reconcile_incoming_confirmed,
    reject_derived_fields_in_fact_payload,
    should_emit_observation,
    supplier_observation_fingerprint_from_row,
    validate_barcode_variant_conflicts,
    validate_incoming_not_auto_summed,
    validate_preorder_no_positive_tape_on_hand,
    validate_purchase_order_line,
    validate_resolution_uniqueness,
    validate_stale_feed_no_mass_unavailable,
    validate_supplier_must_not_update_tape,
    validate_tape_inventory_levels,
    writer_may_mutate,
)
from app.rules.unified_availability_contract import (
    EXAMPLE_VARIANT_AVAILABILITY,
    is_structured_customer_availability_status,
)


class TestOwnership:
    def test_shopify_writer_may_mutate_tape(self):
        assert writer_may_mutate("tape_inventory_levels", "shopify_store_sync")

    def test_supplier_writer_may_not_mutate_tape_table(self):
        assert not writer_may_mutate("tape_inventory_levels", "supplier_stock_sync")

    def test_reject_derived_on_fact_payload(self):
        violations = reject_derived_fields_in_fact_payload(
            {"on_hand": 1, "customer_can_purchase": True, "feed_freshness": "fresh"}
        )
        assert any(v.code == "derived_field_on_fact_table" for v in violations)


class TestTapeInventoryWarnings:
    def test_normal_identity_ok(self):
        # available = on_hand - committed
        violations = validate_tape_inventory_levels(
            {"on_hand": 4, "committed": 1, "available": 3}
        )
        assert violations == []

    def test_negative_available_is_warning_not_identity_error_when_consistent(self):
        # Production CONTINUE/preorder pattern
        violations = validate_tape_inventory_levels(
            {"on_hand": 0, "committed": 3, "available": -3}
        )
        codes = {v.code for v in violations}
        assert "available_identity_mismatch" not in codes
        assert "committed_exceeds_on_hand" in codes
        assert all(v.severity == "warning" for v in violations if v.code == "committed_exceeds_on_hand")

    def test_available_exceeds_on_hand_warning(self):
        violations = validate_tape_inventory_levels(
            {"on_hand": 1, "committed": 0, "available": 5}
        )
        assert any(v.code == "available_exceeds_on_hand" for v in violations)

    def test_negative_po_incoming_error(self):
        violations = validate_tape_inventory_levels(
            {"on_hand": 0, "committed": 0, "available": 0, "po_incoming_confirmed": -1}
        )
        assert any(v.code == "po_incoming_negative" and v.severity == "error" for v in violations)


class TestIncomingReconciliation:
    def test_prefer_po_when_both(self):
        result = reconcile_incoming_confirmed(
            po_incoming_confirmed=6,
            shopify_incoming_reported=6,
            strategy="prefer_po_when_both",
        )
        assert result["incoming_confirmed_derived"] == 6
        assert result["possible_double_count"] is True
        assert result["rationale"] == "both_present_prefer_po"

    def test_separate_only_leaves_null(self):
        result = reconcile_incoming_confirmed(
            po_incoming_confirmed=5,
            shopify_incoming_reported=3,
            strategy="separate_only",
        )
        assert result["incoming_confirmed_derived"] is None

    def test_auto_sum_payload_rejected(self):
        violations = validate_incoming_not_auto_summed(
            {
                "po_incoming_confirmed": 4,
                "shopify_incoming_reported": 4,
                "incoming_confirmed": 8,
            }
        )
        assert any(v.code == "incoming_auto_sum_forbidden" for v in violations)


class TestSupplierNeverUpdatesTape:
    def test_supplier_cannot_set_on_hand(self):
        violations = validate_supplier_must_not_update_tape(
            tape_mutation_fields=["on_hand", "title"],
            source="supplier_stock_sync",
        )
        assert any(v.code == "supplier_updates_tape_inventory" for v in violations)

    def test_shopify_source_ignored_here(self):
        assert (
            validate_supplier_must_not_update_tape(
                tape_mutation_fields=["on_hand"],
                source="shopify_store_sync",
            )
            == []
        )


class TestPreorderOnHand:
    def test_supplier_cannot_create_positive_on_hand(self):
        violations = validate_preorder_no_positive_tape_on_hand(
            is_preorder=True,
            on_hand=2,
            source="supplier_feed",
        )
        assert any(v.severity == "error" for v in violations)

    def test_shopify_positive_preorder_is_warning(self):
        violations = validate_preorder_no_positive_tape_on_hand(
            is_preorder=True,
            on_hand=2,
            source="shopify_store_sync",
        )
        assert any(v.code == "preorder_positive_on_hand_shopify" for v in violations)


class TestPurchaseOrders:
    def test_received_cannot_exceed_ordered(self):
        violations = validate_purchase_order_line(
            {
                "quantity_ordered": 5,
                "quantity_received": 6,
                "over_receipt_adjustment": False,
            }
        )
        assert any(v.code == "po_received_exceeds_ordered" for v in violations)

    def test_over_receipt_allowed_with_flag(self):
        violations = validate_purchase_order_line(
            {
                "quantity_ordered": 5,
                "quantity_received": 6,
                "over_receipt_adjustment": True,
            }
        )
        assert not any(v.code == "po_received_exceeds_ordered" for v in violations)


class TestResolutionAndBarcode:
    def test_one_sku_one_variant(self):
        violations = validate_resolution_uniqueness(
            [
                {
                    "supplier_id": "moovies",
                    "supplier_sku": "A",
                    "resolved_release_variant_id": "v1",
                    "active": True,
                },
                {
                    "supplier_id": "moovies",
                    "supplier_sku": "A",
                    "resolved_release_variant_id": "v2",
                    "active": True,
                },
            ]
        )
        assert any(v.code == "supplier_sku_multiple_variants" for v in violations)

    def test_barcode_conflict_flagged(self):
        violations = validate_barcode_variant_conflicts(
            [
                {
                    "id_type": "barcode",
                    "id_value": "123",
                    "release_variant_id": "v1",
                    "is_valid": True,
                },
                {
                    "id_type": "barcode",
                    "id_value": "123",
                    "release_variant_id": "v2",
                    "is_valid": True,
                },
            ]
        )
        assert any(v.code == "barcode_maps_multiple_variants" for v in violations)


class TestStaleFeed:
    def test_mass_unavailable_blocked(self):
        violations = validate_stale_feed_no_mass_unavailable(
            pipeline_failed_or_stale=True,
            proposed_status_updates=["unavailable"] * 50,
            mass_unavailable_threshold=50,
        )
        assert any(v.code == "stale_feed_mass_unavailable" for v in violations)

    def test_healthy_feed_allows_unavailable_updates(self):
        assert (
            validate_stale_feed_no_mass_unavailable(
                pipeline_failed_or_stale=False,
                proposed_status_updates=["unavailable"] * 100,
            )
            == []
        )


class TestObservationDedupe:
    def test_unchanged_skips_emit(self):
        fp = supplier_observation_fingerprint_from_row(
            {
                "availability_status": "in_stock",
                "reported_quantity": 3,
                "quantity_is_exact": True,
                "supplier_can_supply": True,
                "unit_cost": 10,
                "currency": "GBP",
            }
        )
        assert should_emit_observation(previous_fingerprint=fp, new_fingerprint=fp) is False

    def test_changed_emits(self):
        assert should_emit_observation(previous_fingerprint="a", new_fingerprint="b") is True


class TestStructuredStatus:
    def test_tape_preferred(self):
        status = derive_structured_availability_status(tape_available=5)
        assert status == "in_stock_at_tape"
        assert assert_derived_status_known(status)

    def test_incoming_before_supplier(self):
        status = derive_structured_availability_status(
            tape_available=0,
            po_incoming_confirmed=2,
            supplier_statuses=["in_stock"],
            supplier_can_supply_flags=[True],
        )
        assert status == "incoming_to_tape"

    def test_supplier_available(self):
        status = derive_structured_availability_status(
            tape_available=0,
            supplier_statuses=["in_stock"],
            supplier_can_supply_flags=[True],
        )
        assert status == "available_from_supplier"

    def test_example_contract_status_is_structured(self):
        status = EXAMPLE_VARIANT_AVAILABILITY["availability"]["customer_availability_status"]
        assert is_structured_customer_availability_status(status)
