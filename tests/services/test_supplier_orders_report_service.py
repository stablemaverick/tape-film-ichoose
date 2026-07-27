"""Unit tests for supplier order qty / classification / delta helpers."""

from __future__ import annotations

from datetime import date

from app.services.supplier_orders_report_service import (
    SupplierOrderRow,
    classify_supplier_need,
    compute_daily_delta,
    parse_release_date,
    qty_to_order,
)


def _row(
    vid: str,
    qty: int,
    *,
    title: str = "Title",
    reason: str = "preorder",
) -> SupplierOrderRow:
    return SupplierOrderRow(
        product_title=title,
        barcode="123",
        sku="SKU",
        qty_to_order=qty,
        shopify_need=qty,
        open_po_qty=0,
        po_match="",
        po_order_ids="",
        committed=qty,
        available=-qty,
        on_hand=0,
        incoming=0,
        inventory_policy="CONTINUE",
        reason=reason,
        media_release_date="2026-12-01",
        pre_order_metafield=True,
        backorder_metafield=False,
        product_status="ACTIVE",
        shopify_product_id="gid://shopify/Product/1",
        shopify_variant_id=vid,
    )


def test_qty_to_order_preorder_books():
    assert qty_to_order(available=-6, committed=6, on_hand=0, incoming=0) == 6


def test_qty_to_order_subtracts_on_hand_and_incoming():
    assert qty_to_order(available=-2, committed=5, on_hand=2, incoming=1) == 2


def test_qty_to_order_falls_back_to_negative_available():
    assert qty_to_order(available=-1, committed=0, on_hand=-1, incoming=0) == 1


def test_classify_preorder():
    assert (
        classify_supplier_need(
            is_preorder=True,
            policy="CONTINUE",
            backorder=False,
            available=-3,
            committed=3,
            on_hand=0,
            need=3,
        )
        == "preorder"
    )


def test_classify_continue_oos():
    assert (
        classify_supplier_need(
            is_preorder=False,
            policy="CONTINUE",
            backorder=False,
            available=-2,
            committed=2,
            on_hand=0,
            need=2,
        )
        == "continue_oos"
    )


def test_classify_deny_oversell():
    assert (
        classify_supplier_need(
            is_preorder=False,
            policy="DENY",
            backorder=False,
            available=-1,
            committed=1,
            on_hand=0,
            need=1,
        )
        == "oversell_uncovered_committed"
    )


def test_classify_skips_zero_need():
    assert (
        classify_supplier_need(
            is_preorder=True,
            policy="CONTINUE",
            backorder=False,
            available=0,
            committed=0,
            on_hand=0,
            need=0,
        )
        is None
    )


def test_parse_release_iso():
    assert parse_release_date("2026-12-31") == date(2026, 12, 31)


def test_compute_daily_delta_new_increased_cleared_sorted_desc():
    previous = {
        "gid://shopify/ProductVariant/1": {"qty_to_order": 2, "product_title": "A"},
        "gid://shopify/ProductVariant/2": {"qty_to_order": 5, "product_title": "B"},
        "gid://shopify/ProductVariant/3": {"qty_to_order": 1, "product_title": "C"},
    }
    current = [
        _row("gid://shopify/ProductVariant/1", 5, title="A"),  # +3 increased
        _row("gid://shopify/ProductVariant/4", 10, title="D"),  # +10 new
        # 2 and 3 cleared
    ]
    deltas = compute_daily_delta(current, previous)
    assert {(d.change, d.qty_delta) for d in deltas} == {
        ("new", 10),
        ("increased", 3),
        ("cleared", -5),
        ("cleared", -1),
    }
    # report order: largest qty_delta first
    ordered = sorted(deltas, key=lambda x: -x.qty_delta)
    assert [d.qty_delta for d in ordered] == [10, 3, -1, -5]
