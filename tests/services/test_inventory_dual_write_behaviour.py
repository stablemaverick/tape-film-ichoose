"""
Mocked dual-write behaviour: flags off = no DB mutation; supplier path never touches tape.
"""

import os
import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.config.inventory_dual_write import InventoryDualWriteFlags
from app.services.supplier_offer_dual_write_service import dual_write_supplier_offers
from app.services.shopify_release_dual_write_service import dual_write_shopify_listings_to_releases


class FakeTable:
    def __init__(self, name: str, store: Dict[str, List[Dict[str, Any]]]):
        self.name = name
        self.store = store
        self._filters: List[tuple] = []
        self._payload: Optional[Dict[str, Any]] = None
        self._op = "select"
        self._limit_n = 100

    def select(self, *_a, **_k):
        self._op = "select"
        return self

    def insert(self, payload):
        self._op = "insert"
        self._payload = payload
        return self

    def update(self, payload):
        self._op = "update"
        self._payload = payload
        return self

    def upsert(self, payload, on_conflict=None):
        self._op = "upsert"
        self._payload = payload
        return self

    def eq(self, col, val):
        self._filters.append((col, val))
        return self

    def limit(self, n):
        self._limit_n = n
        return self

    def execute(self):
        rows = self.store.setdefault(self.name, [])
        if self._op == "select":
            out = rows
            for col, val in self._filters:
                out = [r for r in out if r.get(col) == val]
            return type("R", (), {"data": out[: self._limit_n]})()
        if self._op == "insert":
            row = dict(self._payload)
            row.setdefault("id", f"{self.name}-{len(rows)+1}")
            rows.append(row)
            return type("R", (), {"data": [row]})()
        if self._op == "update":
            updated = []
            for r in rows:
                if all(r.get(c) == v for c, v in self._filters):
                    r.update(self._payload or {})
                    updated.append(r)
            return type("R", (), {"data": updated})()
        if self._op == "upsert":
            row = dict(self._payload)
            row.setdefault("id", f"{self.name}-{len(rows)+1}")
            rows.append(row)
            return type("R", (), {"data": [row]})()
        return type("R", (), {"data": []})()


class FakeSupabase:
    def __init__(self):
        self.store: Dict[str, List[Dict[str, Any]]] = {}
        self.tables_touched: List[str] = []

    def table(self, name: str):
        self.tables_touched.append(name)
        return FakeTable(name, self.store)


OFF_FLAGS = InventoryDualWriteFlags(
    enabled=False,
    shopify=True,
    supplier=True,
    purchase_orders=True,
    auto_accept_min_confidence=0.95,
    fresh_max_hours=36,
    aging_max_hours=72,
    create_supplier_only_releases=True,
)

ON_SUPPLIER = InventoryDualWriteFlags(
    enabled=True,
    shopify=False,
    supplier=True,
    purchase_orders=False,
    auto_accept_min_confidence=0.95,
    fresh_max_hours=36,
    aging_max_hours=72,
    create_supplier_only_releases=True,
)


def test_supplier_dual_write_noop_when_flag_off():
    sb = FakeSupabase()
    stats = dual_write_supplier_offers(
        sb,
        [{"supplier": "moovies", "supplier_sku": "A", "barcode": "1", "supplier_stock_status": 2}],
        flags=OFF_FLAGS,
    )
    assert stats["enabled"] is False
    assert "supplier_offers" not in sb.tables_touched


def test_supplier_dual_write_never_touches_tape_inventory():
    sb = FakeSupabase()
    # Seed empty — resolution will create release_variant
    stats = dual_write_supplier_offers(
        sb,
        [
            {
                "supplier": "moovies",
                "supplier_sku": "SKU99",
                "barcode": "5055201999999",
                "title": "Test Film",
                "format": "4K",
                "supplier_stock_status": 4,
                "availability_status": "supplier_stock",
                "cost_price": 10.0,
                "supplier_currency": "GBP",
            }
        ],
        flags=ON_SUPPLIER,
    )
    assert stats["enabled"] is True
    assert "tape_inventory_levels" not in sb.tables_touched
    assert "supplier_offers" in sb.tables_touched
    assert stats["upserted"] >= 1


def test_shopify_dual_write_noop_when_flag_off():
    sb = FakeSupabase()
    stats = dual_write_shopify_listings_to_releases(
        sb,
        [{"shopify_variant_id": "gid://shopify/ProductVariant/1", "inventory_quantity": 3}],
        shop="test.myshopify.com",
        flags=OFF_FLAGS,
    )
    assert stats["enabled"] is False
    assert not sb.tables_touched
