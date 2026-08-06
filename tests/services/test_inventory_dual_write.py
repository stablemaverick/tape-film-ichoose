"""Unit tests for inventory dual-write flags and pure helpers."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.config.inventory_dual_write import (
    load_inventory_dual_write_flags,
    normalize_supplier_id,
    supplier_sku_identity,
)
from app.services.inventory_events_service import build_event_dedupe_key
from app.services.purchase_order_dual_write_service import _map_po_status
from app.services.supplier_offer_dual_write_service import _event_type_for_change


class TestFlagsDefaultOff:
    def test_defaults_disabled(self, monkeypatch):
        for key in (
            "INVENTORY_DUAL_WRITE_ENABLED",
            "INVENTORY_DUAL_WRITE_SHOPIFY",
            "INVENTORY_DUAL_WRITE_SUPPLIER",
            "INVENTORY_DUAL_WRITE_PO",
        ):
            monkeypatch.delenv(key, raising=False)
        flags = load_inventory_dual_write_flags()
        assert flags.enabled is False
        assert flags.shopify_enabled is False
        assert flags.supplier_enabled is False
        assert flags.po_enabled is False

    def test_master_required(self, monkeypatch):
        monkeypatch.setenv("INVENTORY_DUAL_WRITE_ENABLED", "0")
        monkeypatch.setenv("INVENTORY_DUAL_WRITE_SUPPLIER", "1")
        flags = load_inventory_dual_write_flags()
        assert flags.supplier_enabled is False

    def test_all_on(self, monkeypatch):
        monkeypatch.setenv("INVENTORY_DUAL_WRITE_ENABLED", "1")
        monkeypatch.setenv("INVENTORY_DUAL_WRITE_SHOPIFY", "1")
        monkeypatch.setenv("INVENTORY_DUAL_WRITE_SUPPLIER", "1")
        monkeypatch.setenv("INVENTORY_DUAL_WRITE_PO", "1")
        flags = load_inventory_dual_write_flags()
        assert flags.shopify_enabled and flags.supplier_enabled and flags.po_enabled


class TestIdentityHelpers:
    def test_normalize_supplier(self):
        assert normalize_supplier_id("Tape Film") == "tape_film"
        assert normalize_supplier_id("moovies") == "moovies"
        assert normalize_supplier_id("Lasgo") == "lasgo"

    def test_sku_fallback_to_barcode(self):
        assert supplier_sku_identity(supplier_sku=None, raw_barcode="123") == "barcode:123"
        assert supplier_sku_identity(supplier_sku="SKU1", raw_barcode="123") == "SKU1"
        assert supplier_sku_identity(supplier_sku=None, raw_barcode=None) is None


class TestEventTyping:
    def test_became_available(self):
        assert (
            _event_type_for_change(
                {"availability_status": "unavailable", "reported_quantity": 0},
                {"availability_status": "in_stock", "reported_quantity": 5},
            )
            == "supplier_became_available"
        )

    def test_stock_decreased(self):
        assert (
            _event_type_for_change(
                {"availability_status": "in_stock", "reported_quantity": 10},
                {"availability_status": "in_stock", "reported_quantity": 3},
            )
            == "supplier_stock_decreased"
        )

    def test_dedupe_key_stable(self):
        a = build_event_dedupe_key("tape_stock_synced", release_variant_id="r1", fingerprint="1|0|1|0")
        b = build_event_dedupe_key("tape_stock_synced", release_variant_id="r1", fingerprint="1|0|1|0")
        assert a == b


class TestPoStatusMap:
    def test_preorder_confirmed(self):
        assert _map_po_status("pre-order") == "confirmed"
        assert _map_po_status("picking") == "confirmed"
