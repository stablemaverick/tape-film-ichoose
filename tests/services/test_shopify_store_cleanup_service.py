"""Tests for Shopify store cleanup classification and helpers."""

from __future__ import annotations

import csv
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.services.shopify_store_cleanup_service import (
    classify_archive_candidate,
    find_new_like_metafields,
    parse_iso_date,
    product_node_to_row,
    qty_map_from_level,
    read_product_ids_from_csv,
    write_rows_csv,
    CANDIDATE_CSV_FIELDS,
    ProductCleanupRow,
)


def test_parse_iso_date():
    assert parse_iso_date(None) is None
    assert parse_iso_date("") is None
    assert parse_iso_date("2024-01-15") == date(2024, 1, 15)
    assert parse_iso_date("2024-01-15T12:00:00Z") == date(2024, 1, 15)


def test_qty_map_from_level():
    level = {
        "quantities": [
            {"name": "available", "quantity": -2},
            {"name": "committed", "quantity": 3},
            {"name": "on_hand", "quantity": 1},
        ]
    }
    assert qty_map_from_level(level) == {"available": -2, "committed": 3, "on_hand": 1}
    assert qty_map_from_level(None) == {"available": 0, "committed": 0, "on_hand": 0}


def test_classify_candidate_happy_path():
    ok, reason, excl = classify_archive_candidate(
        status="ACTIVE",
        product_type="Movie",
        available=0,
        committed=0,
        pre_order=False,
        media_release=date(2020, 1, 1),
        as_of=date(2026, 8, 2),
        release_lookback_days=60,
    )
    assert ok is True
    assert excl == ""
    assert "oos" in reason


def test_classify_excludes_in_stock():
    ok, _, excl = classify_archive_candidate(
        status="ACTIVE",
        product_type="Movie",
        available=1,
        committed=0,
        pre_order=False,
        media_release=None,
        as_of=date(2026, 8, 2),
    )
    assert ok is False
    assert excl == "in_stock"


def test_classify_excludes_committed():
    ok, _, excl = classify_archive_candidate(
        status="ACTIVE",
        product_type="Movie",
        available=0,
        committed=2,
        pre_order=False,
        media_release=None,
        as_of=date(2026, 8, 2),
    )
    assert ok is False
    assert excl == "has_committed_demand"


def test_classify_excludes_recent_and_future_release():
    as_of = date(2026, 8, 2)
    ok, _, excl = classify_archive_candidate(
        status="ACTIVE",
        product_type="Movie",
        available=0,
        committed=0,
        pre_order=False,
        media_release=date(2026, 7, 20),
        as_of=as_of,
        release_lookback_days=60,
    )
    assert ok is False
    assert excl.startswith("released_within_")

    ok2, _, excl2 = classify_archive_candidate(
        status="ACTIVE",
        product_type="Movie",
        available=0,
        committed=0,
        pre_order=False,
        media_release=date(2026, 12, 1),
        as_of=as_of,
    )
    assert ok2 is False
    assert excl2 == "future_release"


def test_classify_excludes_preorder_and_gift_card():
    ok, _, excl = classify_archive_candidate(
        status="ACTIVE",
        product_type="Movie",
        available=0,
        committed=0,
        pre_order=True,
        media_release=None,
        as_of=date(2026, 8, 2),
    )
    assert ok is False
    assert excl == "pre_order"

    ok2, _, excl2 = classify_archive_candidate(
        status="ACTIVE",
        product_type="Gift Card",
        available=0,
        committed=0,
        pre_order=False,
        media_release=None,
        as_of=date(2026, 8, 2),
    )
    assert ok2 is False
    assert excl2 == "gift_card_product_type"


def test_find_new_like_metafields():
    nodes = [
        {"namespace": "custom", "key": "new", "value": "true"},
        {"namespace": "custom", "key": "studio", "value": "Arrow"},
        {"namespace": "custom", "key": "badge", "value": "New"},
    ]
    hits = find_new_like_metafields(nodes)
    assert any(h.startswith("custom.new=") for h in hits)
    assert any("badge" in h for h in hits)
    assert not any("studio" in h for h in hits)


def test_product_node_to_row_candidate():
    product = {
        "id": "gid://shopify/Product/1",
        "handle": "old-title",
        "title": "Old Title",
        "status": "ACTIVE",
        "productType": "Blu-ray",
        "tags": ["auto-sync"],
        "mediaReleaseDate": {"value": "2019-05-01"},
        "preOrder": {"value": "false"},
        "preorderAlt": {"value": None},
        "backorder": {"value": "false"},
        "poFlag": {"value": None},
        "metafields": {"nodes": [{"namespace": "custom", "key": "new", "value": "true"}]},
        "variants": {
            "nodes": [
                {
                    "id": "gid://shopify/ProductVariant/1",
                    "sku": "SKU1",
                    "barcode": "123",
                    "inventoryItem": {
                        "inventoryLevel": {
                            "quantities": [
                                {"name": "available", "quantity": 0},
                                {"name": "committed", "quantity": 0},
                                {"name": "on_hand", "quantity": 0},
                            ]
                        }
                    },
                }
            ]
        },
    }
    row = product_node_to_row(product, as_of=date(2026, 8, 2))
    assert row.is_candidate is True
    assert row.barcode == "123"
    assert "custom.new=true" in row.new_like_metafields


def test_read_and_write_csv(tmp_path: Path):
    rows = [
        ProductCleanupRow(
            product_id="gid://shopify/Product/9",
            handle="h",
            title="T",
            barcode="b",
            sku="s",
            available=0,
            committed=0,
            on_hand=0,
            media_release_date="",
            pre_order=False,
            backorder=False,
            new_like_metafields="",
            tags="",
            reason="active_oos_no_committed_not_recent_release",
            is_candidate=True,
        )
    ]
    path = tmp_path / "c.csv"
    write_rows_csv(rows, path, fieldnames=CANDIDATE_CSV_FIELDS)
    ids = read_product_ids_from_csv(path)
    assert ids == ["gid://shopify/Product/9"]
    with path.open() as f:
        reader = csv.DictReader(f)
        got = list(reader)
    assert got[0]["title"] == "T"
