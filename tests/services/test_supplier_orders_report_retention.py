"""Tests for supplier_orders FTP report D-5 retention."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from app.services.supplier_orders_report_retention import (
    apply_retention,
    classify_report_file,
    parse_report_date_from_filename,
    plan_retention,
    sydney_today,
)


AS_OF = date(2026, 9, 7)


def test_parse_dated_filename():
    assert parse_report_date_from_filename("supplier_orders_needed_20260907.csv") == date(2026, 9, 7)
    assert parse_report_date_from_filename("supplier_orders_needed.csv") is None
    assert parse_report_date_from_filename("orders-090826.csv") is None


def test_keep_today_through_d5_delete_d6():
    # Today + D-1..D-5 keep; D-6 delete
    keep_dates = [
        "20260907",  # today
        "20260906",
        "20260905",
        "20260904",
        "20260903",
        "20260902",  # D-5
    ]
    for ymd in keep_dates:
        d = classify_report_file(f"supplier_orders_needed_{ymd}.csv", as_of=AS_OF)
        assert d.action == "KEEP", ymd
    d6 = classify_report_file("supplier_orders_needed_20260901.csv", as_of=AS_OF)
    assert d6.action == "DELETE"
    older = classify_report_file("supplier_orders_delta_20260815.csv", as_of=AS_OF)
    assert older.action == "DELETE"


def test_latest_alias_and_inbound_retained():
    assert classify_report_file("supplier_orders_needed.csv", as_of=AS_OF).action == "KEEP"
    assert classify_report_file("inbound", as_of=AS_OF).action == "IGNORE"


def test_unrelated_and_malformed_kept():
    assert classify_report_file("readme.txt", as_of=AS_OF).action == "IGNORE"
    d = classify_report_file("supplier_orders_needed_notadate.csv", as_of=AS_OF)
    assert d.action == "UNCLASSIFIED"


def test_dry_run_deletes_nothing(tmp_path: Path):
    keep = tmp_path / "supplier_orders_needed_20260907.csv"
    drop = tmp_path / "supplier_orders_needed_20260801.csv"
    keep.write_text("a\n", encoding="utf-8")
    drop.write_text("b\n", encoding="utf-8")
    summary = apply_retention(tmp_path, as_of=AS_OF, dry_run=True)
    assert drop.exists()
    assert "supplier_orders_needed_20260801.csv" in summary.would_delete
    assert summary.deleted == []


def test_apply_deletes_only_series_files(tmp_path: Path):
    keep = tmp_path / "supplier_orders_needed_20260907.csv"
    drop = tmp_path / "supplier_orders_preorder_20260801.csv"
    other = tmp_path / "notes.txt"
    inbound = tmp_path / "inbound"
    inbound.mkdir()
    (inbound / "orders.csv").write_text("x\n", encoding="utf-8")
    keep.write_text("a\n", encoding="utf-8")
    drop.write_text("b\n", encoding="utf-8")
    other.write_text("c\n", encoding="utf-8")
    summary = apply_retention(tmp_path, as_of=AS_OF, dry_run=False)
    assert keep.exists()
    assert other.exists()
    assert inbound.exists()
    assert (inbound / "orders.csv").exists()
    assert not drop.exists()
    assert "supplier_orders_preorder_20260801.csv" in summary.deleted


def test_plan_retention_window():
    names = [
        "supplier_orders_needed.csv",
        "supplier_orders_needed_20260907.csv",
        "supplier_orders_needed_20260902.csv",
        "supplier_orders_needed_20260901.csv",
        "unrelated.csv",
    ]
    summary = plan_retention("/tmp/unused", as_of=AS_OF, filenames=names)
    actions = {d.filename: d.action for d in summary.decisions}
    assert actions["supplier_orders_needed.csv"] == "KEEP"
    assert actions["supplier_orders_needed_20260907.csv"] == "KEEP"
    assert actions["supplier_orders_needed_20260902.csv"] == "KEEP"
    assert actions["supplier_orders_needed_20260901.csv"] == "DELETE"
    assert actions["unrelated.csv"] == "IGNORE"
