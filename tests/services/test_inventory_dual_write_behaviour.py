"""
Mocked dual-write behaviour: flags off = no DB mutation; supplier path never touches tape.
Also covers batching, idempotency, and baseline event semantics.
"""

import os
import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.config.inventory_dual_write import InventoryDualWriteFlags
from app.services.supplier_offer_dual_write_service import (
    _event_type_for_change,
    dual_write_supplier_offers,
)
from app.services.shopify_release_dual_write_service import dual_write_shopify_listings_to_releases


class FakeTable:
    def __init__(self, name: str, store: Dict[str, List[Dict[str, Any]]], counters: Dict[str, int]):
        self.name = name
        self.store = store
        self.counters = counters
        self._filters: List[tuple] = []
        self._in_filters: List[tuple] = []
        self._payload: Any = None
        self._op = "select"
        self._limit_n = 100000
        self._on_conflict = None

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
        self._on_conflict = on_conflict
        return self

    def eq(self, col, val):
        self._filters.append(("eq", col, val))
        return self

    def in_(self, col, vals):
        self._in_filters.append((col, list(vals)))
        return self

    def limit(self, n):
        self._limit_n = n
        return self

    def _match(self, row: Dict[str, Any]) -> bool:
        for kind, col, val in self._filters:
            if kind == "eq" and row.get(col) != val:
                return False
        for col, vals in self._in_filters:
            if row.get(col) not in vals:
                return False
        return True

    def execute(self):
        self.counters["execute"] = self.counters.get("execute", 0) + 1
        rows = self.store.setdefault(self.name, [])
        if self._op == "select":
            out = [r for r in rows if self._match(r)]
            return type("R", (), {"data": out[: self._limit_n]})()
        if self._op == "insert":
            payloads = self._payload if isinstance(self._payload, list) else [self._payload]
            inserted = []
            for p in payloads:
                row = dict(p)
                row.setdefault("id", f"{self.name}-{len(rows)+1}")
                rows.append(row)
                inserted.append(row)
            return type("R", (), {"data": inserted})()
        if self._op == "update":
            updated = []
            for r in rows:
                if self._match(r):
                    r.update(self._payload or {})
                    updated.append(r)
            return type("R", (), {"data": updated})()
        if self._op == "upsert":
            payloads = self._payload if isinstance(self._payload, list) else [self._payload]
            conflict_cols = []
            if self._on_conflict:
                conflict_cols = [c.strip() for c in self._on_conflict.split(",") if c.strip()]
            out = []
            for p in payloads:
                row = dict(p)
                matched = None
                if conflict_cols:
                    for existing in rows:
                        if all(existing.get(c) == row.get(c) for c in conflict_cols):
                            matched = existing
                            break
                if matched:
                    matched.update(row)
                    out.append(matched)
                else:
                    row.setdefault("id", f"{self.name}-{len(rows)+1}")
                    rows.append(row)
                    out.append(row)
            return type("R", (), {"data": out})()
        return type("R", (), {"data": []})()


class FakeSupabase:
    def __init__(self):
        self.store: Dict[str, List[Dict[str, Any]]] = {}
        self.tables_touched: List[str] = []
        self.counters: Dict[str, int] = {"execute": 0}

    def table(self, name: str):
        self.tables_touched.append(name)
        return FakeTable(name, self.store, self.counters)


OFF_FLAGS = InventoryDualWriteFlags(
    enabled=False,
    shopify=True,
    supplier=True,
    purchase_orders=True,
    auto_accept_min_confidence=0.95,
    fresh_max_hours=36,
    aging_max_hours=72,
    create_supplier_only_releases=True,
    supplier_batch_size=500,
    supplier_in_chunk_size=150,
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
    supplier_batch_size=50,
    supplier_in_chunk_size=25,
)


def _offer(sku: str, barcode: str, qty: int = 2, cost: float = 10.0, supplier: str = "moovies"):
    return {
        "supplier": supplier,
        "supplier_sku": sku,
        "barcode": barcode,
        "title": f"Title {sku}",
        "format": "4K",
        "supplier_stock_status": qty,
        "availability_status": "supplier_stock",
        "cost_price": cost,
        "supplier_currency": "GBP",
    }


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


def test_baseline_first_insert_emits_observation_but_no_price_event():
    assert _event_type_for_change(None, {"unit_cost": 10, "availability_status": "in_stock"}) is None
    assert _event_type_for_change({}, {"unit_cost": 10, "availability_status": "in_stock"}) is None
    assert (
        _event_type_for_change(
            {"unit_cost": 10, "availability_status": "in_stock", "reported_quantity": 1},
            {"unit_cost": 12, "availability_status": "in_stock", "reported_quantity": 1},
        )
        == "supplier_price_changed"
    )
    assert (
        _event_type_for_change(
            {"unit_cost": None, "availability_status": "in_stock", "reported_quantity": 1},
            {"unit_cost": 12, "availability_status": "in_stock", "reported_quantity": 1},
        )
        is None
    )

    sb = FakeSupabase()
    stats = dual_write_supplier_offers(sb, [_offer("S1", "111")], flags=ON_SUPPLIER)
    assert stats["observations_inserted"] == 1
    assert stats["events_inserted"] == 0
    assert len(sb.store.get("inventory_events", [])) == 0


def test_batch_dual_write_uses_bounded_db_calls():
    sb = FakeSupabase()
    rows = [_offer(f"SKU{i}", f"5000000000{i:03d}", qty=i % 5) for i in range(120)]
    stats = dual_write_supplier_offers(sb, rows, flags=ON_SUPPLIER)
    assert stats["upserted"] == 120
    assert stats["offers_inserted"] == 120
    # Row-by-row path was ~10+ executes/row (~1200+). Batched must be far lower.
    assert sb.counters["execute"] < 200
    assert stats["db_requests"] < 200


def test_idempotent_replay_skips_observations_and_events():
    sb = FakeSupabase()
    rows = [_offer("S1", "222", qty=3, cost=9.5), _offer("S2", "333", qty=1, cost=8.0)]
    first = dual_write_supplier_offers(sb, rows, flags=ON_SUPPLIER)
    assert first["observations_inserted"] == 2
    assert first["events_inserted"] == 0
    obs_before = len(sb.store.get("supplier_offer_observations", []))
    events_before = len(sb.store.get("inventory_events", []))
    releases_before = len(sb.store.get("release_variants", []))
    offers_before = len(sb.store.get("supplier_offers", []))

    second = dual_write_supplier_offers(sb, rows, flags=ON_SUPPLIER)
    assert second["observations_inserted"] == 0
    assert second["events_inserted"] == 0
    assert len(sb.store.get("supplier_offer_observations", [])) == obs_before
    assert len(sb.store.get("inventory_events", [])) == events_before
    assert len(sb.store.get("release_variants", [])) == releases_before
    assert len(sb.store.get("supplier_offers", [])) == offers_before


def test_changed_input_emits_observation_and_stock_event():
    sb = FakeSupabase()
    dual_write_supplier_offers(sb, [_offer("S1", "444", qty=2, cost=10.0)], flags=ON_SUPPLIER)
    changed = dual_write_supplier_offers(
        sb, [_offer("S1", "444", qty=5, cost=10.0)], flags=ON_SUPPLIER
    )
    assert changed["observations_inserted"] == 1
    assert changed["events_inserted"] == 1
    assert sb.store["inventory_events"][-1]["event_type"] == "supplier_stock_increased"


def test_cross_supplier_same_barcode_reuses_release():
    sb = FakeSupabase()
    dual_write_supplier_offers(
        sb, [_offer("M1", "555", supplier="moovies")], flags=ON_SUPPLIER
    )
    dual_write_supplier_offers(
        sb, [_offer("L1", "555", supplier="lasgo")], flags=ON_SUPPLIER
    )
    releases = sb.store.get("release_variants", [])
    offers = sb.store.get("supplier_offers", [])
    assert len(releases) == 1
    assert len(offers) == 2
    assert offers[0]["release_variant_id"] == offers[1]["release_variant_id"]


def test_lasgo_then_moovies_same_barcode_reuses_release():
    sb = FakeSupabase()
    dual_write_supplier_offers(
        sb, [_offer("L1", "556", supplier="lasgo")], flags=ON_SUPPLIER
    )
    dual_write_supplier_offers(
        sb, [_offer("M1", "556", supplier="moovies")], flags=ON_SUPPLIER
    )
    assert len(sb.store.get("release_variants", [])) == 1
    ids = {o["release_variant_id"] for o in sb.store["supplier_offers"]}
    assert len(ids) == 1


def test_same_barcode_conflicting_format_keeps_separate_releases():
    sb = FakeSupabase()
    dual_write_supplier_offers(
        sb,
        [
            {
                **_offer("M1", "557", supplier="moovies"),
                "format": "4K UHD",
            }
        ],
        flags=ON_SUPPLIER,
    )
    dual_write_supplier_offers(
        sb,
        [
            {
                **_offer("L1", "557", supplier="lasgo"),
                "format": "Blu-ray",
            }
        ],
        flags=ON_SUPPLIER,
    )
    releases = sb.store.get("release_variants", [])
    assert len(releases) == 2
    families = {r.get("format") for r in releases}
    assert "4K UHD" in families and "Blu-ray" in families
    offer_rids = {o["release_variant_id"] for o in sb.store["supplier_offers"]}
    assert len(offer_rids) == 2


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
