"""Tests for post-catalog supplier intelligence projection."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from app.services.supplier_intelligence_projection_service import (
    format_projection_status_line,
    project_supplier_intelligence_from_batches,
)


class _FakeResult:
    def __init__(self, data):
        self.data = data


class _FakeQuery:
    def __init__(self, client: "FakeSB", table: str):
        self.client = client
        self.table = table
        self._batch = None
        self._range = None

    def select(self, _cols: str):
        return self

    def eq(self, field: str, value: str):
        if field == "import_batch_id":
            self._batch = value
        return self

    def range(self, start: int, end: int):
        self._range = (start, end)
        return self

    def execute(self):
        rows = self.client.staging_by_batch.get(self._batch or "", [])
        if self._range:
            start, end = self._range
            rows = rows[start : end + 1]
        return _FakeResult(rows)


class FakeSB:
    def __init__(self, staging_by_batch: Dict[str, List[Dict[str, Any]]]):
        self.staging_by_batch = staging_by_batch

    def table(self, name: str) -> _FakeQuery:
        assert name == "staging_supplier_offers"
        return _FakeQuery(self, name)


def test_projection_skipped_when_flags_off(monkeypatch):
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_ENABLED", "0")
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_SUPPLIER", "0")
    sb = FakeSB({"b1": [{"supplier": "moovies", "barcode": "1"}]})
    result = project_supplier_intelligence_from_batches(sb, moovies_batch="b1")
    assert result["status"] == "skipped"
    assert result["enabled"] is False


def test_projection_success_calls_dual_write(monkeypatch):
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_ENABLED", "1")
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_SUPPLIER", "1")
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_SHOPIFY", "0")
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_PO", "0")

    called = {}

    def fake_dual_write(sb, rows, **kwargs):
        called["rows"] = list(rows)
        called["kwargs"] = kwargs
        return {"errors": 0, "failed_batches": 0, "upserted": len(list(rows))}

    monkeypatch.setattr(
        "app.services.supplier_intelligence_projection_service.dual_write_supplier_offers",
        fake_dual_write,
    )
    rows = [
        {"supplier": "moovies", "barcode": "111", "supplier_sku": "M1"},
        {"supplier": "lasgo", "barcode": "222", "supplier_sku": "L1"},
    ]
    sb = FakeSB({"m-batch": [rows[0]], "l-batch": [rows[1]]})
    result = project_supplier_intelligence_from_batches(
        sb,
        moovies_batch="m-batch",
        lasgo_batch="l-batch",
        pipeline_run_id="run-1",
    )
    assert result["status"] == "success"
    assert result["offers_loaded"] == 2
    assert len(called["rows"]) == 2
    assert called["kwargs"]["pipeline_run_id"] == "run-1"
    line = format_projection_status_line(result)
    assert line.startswith("INVENTORY_INTELLIGENCE_PROJECTION_STATUS=success")


def test_projection_failure_isolated(monkeypatch):
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_ENABLED", "1")
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_SUPPLIER", "1")

    def boom(*_a, **_k):
        raise RuntimeError("projection exploded")

    monkeypatch.setattr(
        "app.services.supplier_intelligence_projection_service.dual_write_supplier_offers",
        boom,
    )
    sb = FakeSB({"m-batch": [{"supplier": "moovies", "barcode": "1", "supplier_sku": "x"}]})
    result = project_supplier_intelligence_from_batches(sb, moovies_batch="m-batch")
    assert result["status"] == "failed"
    assert "exploded" in (result.get("error") or "")
