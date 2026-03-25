"""
Stock-sync write path: update-only, existence check, payload whitelist behaviour.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.catalog_update_rules import IDENTITY_FIELDS, filter_update_payload, get_update_whitelist
from app.services.catalog_upsert_service import (
    RetryStats,
    apply_stock_sync_row_updates,
    fetch_existing_catalog_item_ids,
)


@dataclass
class _FakeExecResult:
    data: List[Dict[str, Any]]


@dataclass
class _FakeChain:
    table: str
    client: "FakeSupabase"
    op: str = "select"
    _cols: Optional[str] = None
    _in_field: Optional[str] = None
    _in_vals: Optional[List[str]] = None
    _update_payload: Optional[Dict[str, Any]] = None
    _eq_field: Optional[str] = None
    _eq_val: Optional[str] = None
    _upsert_rows: Optional[List[Any]] = None

    def select(self, cols: str):
        self._cols = cols
        self.op = "select"
        return self

    def in_(self, field: str, vals: List[str]):
        self._in_field = field
        self._in_vals = list(vals)
        return self

    def update(self, payload: Dict[str, Any]):
        self.op = "update"
        self._update_payload = dict(payload)
        return self

    def eq(self, field: str, val: str):
        self._eq_field = field
        self._eq_val = val
        return self

    def upsert(self, rows: List[Any], on_conflict: Optional[str] = None):
        self.op = "upsert"
        self._upsert_rows = rows
        return self

    def execute(self) -> _FakeExecResult:
        c = self.client
        c.executed_chains.append(self)
        if self.table != "catalog_items":
            return _FakeExecResult([])
        if self.op == "select" and self._cols == "id" and self._in_field == "id" and self._in_vals:
            rows = [{"id": x} for x in self._in_vals if x in c.existing_catalog_ids]
            return _FakeExecResult(rows)
        if self.op == "update":
            c.update_calls.append((self._eq_val, dict(self._update_payload or {})))
            return _FakeExecResult([])
        if self.op == "upsert":
            c.upsert_calls.append(self._upsert_rows)
            return _FakeExecResult([])
        return _FakeExecResult([])


@dataclass
class FakeSupabase:
    existing_catalog_ids: set[str] = field(default_factory=set)
    executed_chains: List[_FakeChain] = field(default_factory=list)
    update_calls: List[Tuple[Optional[str], Dict[str, Any]]] = field(default_factory=list)
    upsert_calls: List[Any] = field(default_factory=list)

    def table(self, name: str) -> _FakeChain:
        return _FakeChain(table=name, client=self)


def test_fetch_existing_catalog_item_ids_batches_and_filters():
    sb = FakeSupabase(existing_catalog_ids={"a", "c", "d"})
    stats = RetryStats()
    out = fetch_existing_catalog_item_ids(sb, ["a", "b", "c", "d"], chunk_size=2, stats=stats)
    assert out == {"a", "c", "d"}
    select_ops = [c for c in sb.executed_chains if c.op == "select"]
    assert len(select_ops) == 2
    assert set(select_ops[0]._in_vals or []) | set(select_ops[1]._in_vals or []) == {"a", "b", "c", "d"}


def test_missing_target_id_never_triggers_upsert_only_update_for_verified_rows():
    """After existence filter, stock path must only PATCH rows that exist — no upsert."""
    sb = FakeSupabase(existing_catalog_ids={"keep-id"})
    stats = RetryStats()
    pending = [
        ("keep-id", {"cost_price": 9.99, "supplier_stock_status": 1}),
        ("missing-id", {"cost_price": 1.0, "supplier_stock_status": 0}),
    ]
    want = [cid for cid, _ in pending]
    existing = fetch_existing_catalog_item_ids(sb, want, chunk_size=500, stats=stats)
    verified = [(cid, pl) for cid, pl in pending if cid in existing]

    assert verified == [("keep-id", {"cost_price": 9.99, "supplier_stock_status": 1})]

    n = apply_stock_sync_row_updates(sb, verified, stats=stats, progress_every=10_000)
    assert n == 1
    assert len(sb.upsert_calls) == 0
    assert sb.update_calls == [
        ("keep-id", {"cost_price": 9.99, "supplier_stock_status": 1}),
    ]


def test_stock_sync_filtered_payload_excludes_identity_even_when_offer_has_null_title():
    """Whitelist must drop title/format/director/studio so PATCH cannot null NOT NULL identity columns."""
    wl = get_update_whitelist(existing_only=True)
    offer_like = {
        "cost_price": 10.0,
        "calculated_sale_price": 20.0,
        "supplier_stock_status": 3,
        "availability_status": "In Stock",
        "media_release_date": "2025-01-01",
        "supplier_last_seen_at": "2026-01-01T00:00:00+00:00",
        "title": None,
        "format": None,
        "director": None,
        "studio": None,
        "harmonized_title": None,
    }
    update_payload = {
        "cost_price": offer_like.get("cost_price"),
        "calculated_sale_price": offer_like.get("calculated_sale_price"),
        "supplier_stock_status": offer_like.get("supplier_stock_status") or 0,
        "availability_status": offer_like.get("availability_status"),
        "media_release_date": offer_like.get("media_release_date"),
        "supplier_last_seen_at": offer_like.get("supplier_last_seen_at"),
        "title": offer_like.get("title"),
        "format": offer_like.get("format"),
        "director": offer_like.get("director"),
        "studio": offer_like.get("studio"),
    }
    filtered = filter_update_payload(update_payload, wl)
    assert not (set(filtered) & IDENTITY_FIELDS)
    assert "media_release_date" not in filtered
    assert "cost_price" in filtered


def test_apply_stock_sync_row_updates_uses_execute_with_retry(monkeypatch):
    """Regression: row updates go through execute_with_retry like production."""
    calls: list[str] = []

    def fake_execute(q, max_retries: int = 6, label: str = "", stats=None):
        calls.append(label)
        return q.execute()

    monkeypatch.setattr(
        "app.services.catalog_upsert_service.execute_with_retry",
        fake_execute,
    )
    sb = FakeSupabase(existing_catalog_ids={"x"})
    stats = RetryStats()
    apply_stock_sync_row_updates(sb, [("x", {"cost_price": 1})], stats=stats, progress_every=10_000)
    assert calls == ["stock catalog_items update 1/1"]
