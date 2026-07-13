"""Unit tests for inventory sync apply gating (no Shopify / Supabase I/O)."""

from __future__ import annotations

from app.services.shopify_inventory_sync_service import (
    desired_qty_from_catalog_item,
    should_queue_inventory_apply,
)


def test_desired_qty_preorder_is_zero_for_reporting_only():
    assert desired_qty_from_catalog_item({"availability_status": "preorder"}) == 0


def test_never_queue_preorder_catalog_row():
    assert (
        should_queue_inventory_apply(
            cat={"availability_status": "preorder"},
            drift_classification="expected_preorder_negative_inventory",
            has_inventory_item=True,
        )
        is False
    )


def test_never_queue_expected_backorder_classification():
    assert (
        should_queue_inventory_apply(
            cat={"availability_status": "supplier_stock"},
            drift_classification="expected_backorder_negative_inventory",
            has_inventory_item=True,
        )
        is False
    )


def test_queue_true_mismatch_when_not_preorder():
    assert (
        should_queue_inventory_apply(
            cat={"availability_status": "supplier_stock"},
            drift_classification="true_inventory_mismatch",
            has_inventory_item=True,
        )
        is True
    )


def test_never_queue_without_inventory_item():
    assert (
        should_queue_inventory_apply(
            cat={"availability_status": "supplier_stock"},
            drift_classification="true_inventory_mismatch",
            has_inventory_item=False,
        )
        is False
    )
