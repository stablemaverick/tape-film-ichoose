"""Tests for Lasgo raw retention and catalog batch handoff."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.config.inventory_dual_write import load_inventory_dual_write_flags
from app.services.lasgo_raw_retention import (
    BatchRegistryEntry,
    plan_retention,
    prune_lasgo_raw_batches,
    record_completed_batch,
)


CATALOG_SYNC = Path(ROOT) / "pipeline" / "run_catalog_sync.sh"
STOCK_SYNC = Path(ROOT) / "pipeline" / "run_stock_sync.sh"


def test_catalog_captures_lasgo_batch_from_import_log_like_stock():
    catalog = CATALOG_SYNC.read_text(encoding="utf-8")
    stock = STOCK_SYNC.read_text(encoding="utf-8")

    assert "sed -n 's/^LASGO_BATCH=//p'" in catalog
    assert "sed -n 's/^LASGO_BATCH=//p'" in stock
    assert "LASGO_IMPORT_LOG=" in catalog
    assert "refusing latest_batch table scan" in catalog

    # Must not resolve Lasgo batch via ORDER BY imported_at on the raw table.
    assert 'latest_batch("staging_lasgo_raw")' not in catalog
    assert "order(\"imported_at\", desc=True)" not in catalog
    assert re.search(
        r"ORDER BY\s+imported_at\s+DESC", catalog, flags=re.IGNORECASE
    ) is None


def test_catalog_passes_captured_lasgo_batch_to_normalize():
    catalog = CATALOG_SYNC.read_text(encoding="utf-8")
    assert '--lasgo-batch "${LASGO_BATCH}"' in catalog
    assert "03_normalize_supplier_products.py" in catalog


def test_plan_retention_keeps_newest_two_deletes_older():
    entries = [
        BatchRegistryEntry("b1", "2026-01-01T00:00:00Z", 100),
        BatchRegistryEntry("b2", "2026-01-02T00:00:00Z", 100),
        BatchRegistryEntry("b3", "2026-01-03T00:00:00Z", 100),
    ]
    keep, delete = plan_retention(entries, keep_newest=2, completed_batch_id="b3")
    assert keep == ["b2", "b3"]
    assert delete == ["b1"]


def test_plan_retention_idempotent_when_only_two():
    entries = [
        BatchRegistryEntry("b2", "2026-01-02T00:00:00Z", 100),
        BatchRegistryEntry("b3", "2026-01-03T00:00:00Z", 100),
    ]
    keep, delete = plan_retention(entries, keep_newest=2, completed_batch_id="b3")
    assert keep == ["b2", "b3"]
    assert delete == []


def test_prune_deletes_third_batch_by_import_batch_id(tmp_path, monkeypatch):
    monkeypatch.setenv("LASGO_RAW_RETENTION_ENABLED", "1")
    monkeypatch.setenv("LASGO_RAW_RETENTION_KEEP", "2")
    registry = tmp_path / "registry.json"

    # Seed two prior successful batches in registry
    record_completed_batch(registry, import_batch_id="old-a", row_count=10)
    record_completed_batch(registry, import_batch_id="old-b", row_count=10)

    deleted_ids: list[str] = []
    table = MagicMock()

    def delete_side_effect():
        chain = MagicMock()

        def eq_side(col, val):
            assert col == "import_batch_id"
            deleted_ids.append(val)
            ret = MagicMock()
            ret.execute.return_value = None
            return ret

        chain.eq.side_effect = eq_side
        return chain

    table.delete.side_effect = delete_side_effect

    # count for completed batch
    count_resp = MagicMock()
    count_resp.count = 50
    select_chain = MagicMock()
    select_chain.eq.return_value.limit.return_value.execute.return_value = count_resp
    table.select.return_value = select_chain

    sb = MagicMock()
    sb.table.return_value = table

    result = prune_lasgo_raw_batches(
        sb,
        completed_batch_id="new-c",
        row_count=50,
        mode="stock_cost",
        table="staging_lasgo_raw",
        registry_path=registry,
        keep_newest=2,
    )

    assert result.keep == ["old-b", "new-c"]
    assert result.deleted == ["old-a"]
    assert deleted_ids == ["old-a"]
    # Only delete() for obsolete batches; never touches other tables
    assert all(c.args[0] == "staging_lasgo_raw" for c in sb.table.call_args_list)


def test_failed_or_empty_import_does_not_purge(tmp_path, monkeypatch):
    monkeypatch.setenv("LASGO_RAW_RETENTION_ENABLED", "1")
    registry = tmp_path / "registry.json"
    record_completed_batch(registry, import_batch_id="keep-a", row_count=10)
    record_completed_batch(registry, import_batch_id="keep-b", row_count=10)

    sb = MagicMock()
    result = prune_lasgo_raw_batches(
        sb,
        completed_batch_id="failed-c",
        row_count=0,
        registry_path=registry,
    )
    assert result.skipped_reason == "empty_import"
    assert result.deleted == []
    sb.table.assert_not_called()

    # Registry unchanged
    text = registry.read_text(encoding="utf-8")
    assert "keep-a" in text and "keep-b" in text
    assert "failed-c" not in text


def test_prune_skipped_when_completed_batch_missing_in_db(tmp_path, monkeypatch):
    monkeypatch.setenv("LASGO_RAW_RETENTION_ENABLED", "1")
    registry = tmp_path / "registry.json"
    record_completed_batch(registry, import_batch_id="keep-a", row_count=10)

    table = MagicMock()
    count_resp = MagicMock()
    count_resp.count = 0
    table.select.return_value.eq.return_value.limit.return_value.execute.return_value = (
        count_resp
    )
    sb = MagicMock()
    sb.table.return_value = table

    result = prune_lasgo_raw_batches(
        sb,
        completed_batch_id="ghost",
        row_count=99,
        registry_path=registry,
    )
    assert result.skipped_reason == "completed_batch_not_found"
    assert result.deleted == []
    table.delete.assert_not_called()


def test_import_calls_retention_only_after_successful_insert(tmp_path, monkeypatch):
    monkeypatch.setenv("LASGO_RAW_RETENTION_ENABLED", "1")
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")

    # Minimal blu-ray CSV
    csv_path = tmp_path / "LASGO_test.csv"
    csv_path.write_text(
        "TITLE,EAN/Barcode,Format L2,Selling Price Sterling,Free Stock,Label,RELEASE,Artist\n"
        "Film A,1234567890123,Blu-ray,9.99,2,Studio,2026-01-01,Director\n",
        encoding="utf-8",
    )

    insert_calls: list[list] = []
    prune_calls: list[dict] = []

    table = MagicMock()

    def insert_side(batch):
        insert_calls.append(batch)
        ret = MagicMock()
        ret.execute.return_value = None
        return ret

    table.insert.side_effect = insert_side
    sb = MagicMock()
    sb.table.return_value = table

    def fake_prune(supabase, **kwargs):
        prune_calls.append(kwargs)
        return MagicMock(deleted=[], keep=[kwargs["completed_batch_id"]])

    with patch("app.services.lasgo_import_service.create_client", return_value=sb), patch(
        "app.services.lasgo_import_service.load_dotenv"
    ), patch(
        "app.services.lasgo_import_service.prune_lasgo_raw_batches", side_effect=fake_prune
    ):
        from app.services.lasgo_import_service import import_lasgo_raw

        batch_id = import_lasgo_raw(
            str(csv_path),
            table="staging_lasgo_raw",
            mode="full",
            env_file=str(tmp_path / "no.env"),
        )

    assert batch_id
    assert insert_calls and len(insert_calls[0]) == 1
    assert len(prune_calls) == 1
    assert prune_calls[0]["completed_batch_id"] == batch_id
    assert prune_calls[0]["row_count"] == 1


def test_import_failure_before_complete_does_not_prune(tmp_path, monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")

    csv_path = tmp_path / "LASGO_test.csv"
    csv_path.write_text(
        "TITLE,EAN/Barcode,Format L2,Selling Price Sterling,Free Stock\n"
        "Film A,123,Blu-ray,1,1\n",
        encoding="utf-8",
    )

    table = MagicMock()
    table.insert.side_effect = RuntimeError("insert failed")
    sb = MagicMock()
    sb.table.return_value = table

    with patch("app.services.lasgo_import_service.create_client", return_value=sb), patch(
        "app.services.lasgo_import_service.load_dotenv"
    ), patch(
        "app.services.lasgo_import_service.prune_lasgo_raw_batches"
    ) as prune:
        from app.services.lasgo_import_service import import_lasgo_raw

        with pytest.raises(RuntimeError, match="insert failed"):
            import_lasgo_raw(str(csv_path), mode="full", env_file=str(tmp_path / "x.env"))
        prune.assert_not_called()


def test_production_inventory_flags_supplier_on_shopify_po_off(monkeypatch):
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_ENABLED", "1")
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_SUPPLIER", "1")
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_SHOPIFY", "0")
    monkeypatch.setenv("INVENTORY_DUAL_WRITE_PO", "0")
    flags = load_inventory_dual_write_flags()
    assert flags.enabled is True
    assert flags.supplier_enabled is True
    assert flags.shopify_enabled is False
    assert flags.po_enabled is False


def test_supplier_intelligence_module_untouched_by_retention_imports():
    # Retention must not import projection / dual-write services (no coupling).
    import app.services.lasgo_raw_retention as ret
    import inspect

    src = inspect.getsource(ret)
    assert "supplier_intelligence" not in src
    assert "dual_write" not in src
    assert "catalog_items" not in src
