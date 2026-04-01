from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.services.shopify_store_sync_service import (
    _resolve_catalog_matches,
    derive_catalog_availability_from_listing,
)


def test_store_sync_writeback_marks_sellable_variant_store_stock():
    future = (date.today() + timedelta(days=30)).isoformat()
    av, qty = derive_catalog_availability_from_listing(5, "DENY", future)
    assert av == "preorder"
    assert qty == 5

    av2, qty2 = derive_catalog_availability_from_listing(12, "DENY", None)
    assert av2 == "store_stock"
    assert qty2 == 12


def test_store_sync_writeback_marks_zero_qty_non_future_store_out():
    av, qty = derive_catalog_availability_from_listing(0, "DENY", None)
    assert av == "store_out"
    assert qty == 0


@dataclass
class _ExecResult:
    data: List[Dict[str, Any]]


@dataclass
class _Chain:
    client: "_FakeSupabase"
    _select: Optional[str] = None
    _in_field: Optional[str] = None
    _in_vals: Optional[List[str]] = None

    def select(self, cols: str):
        self._select = cols
        return self

    def in_(self, field: str, vals: List[str]):
        self._in_field = field
        self._in_vals = list(vals)
        return self

    def eq(self, _field: str, _val: Any):
        return self

    def ilike(self, _field: str, _pat: str):
        return self

    def execute(self):
        if self._select == "id,shopify_variant_id":
            return _ExecResult([])
        if self._select == "id,barcode,title,edition_title,source_type,shopify_variant_id":
            out = []
            for r in self.client.catalog_rows:
                if self._in_field == "barcode" and self._in_vals is not None:
                    if str(r.get("barcode") or "") in self._in_vals:
                        out.append(r)
            return _ExecResult(out)
        if self._select == "id,title,edition_title":
            return _ExecResult([])
        return _ExecResult([])


@dataclass
class _FakeSupabase:
    catalog_rows: List[Dict[str, Any]]

    def table(self, _name: str):
        return _Chain(client=self)


def test_resolve_catalog_matches_barcode_title_breaks_ambiguity_when_single_strong_title():
    sb = _FakeSupabase(
        catalog_rows=[
            {"id": "c1", "barcode": "5027035029634", "title": "Hard Boiled", "edition_title": None},
            {"id": "c2", "barcode": "5027035029634", "title": "Another Film", "edition_title": None},
        ]
    )
    flat_variants = [
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/1",
            "barcode": "5027035029634",
            "_match_display_title": "Hard Boiled",
            "product_type": "Movie",
        }
    ]
    matches = _resolve_catalog_matches(sb, flat_variants)
    cid, method, status, _ = matches["gid://shopify/ProductVariant/1"]
    assert cid == "c1"
    assert method == "barcode_title"
    assert status == "matched"


def test_resolve_catalog_matches_barcode_title_keeps_ambiguous_when_multiple_strong_titles():
    sb = _FakeSupabase(
        catalog_rows=[
            {"id": "c1", "barcode": "111", "title": "City On Fire", "edition_title": None},
            {"id": "c2", "barcode": "111", "title": "City On Fire", "edition_title": None},
        ]
    )
    flat_variants = [
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/2",
            "barcode": "111",
            "_match_display_title": "City On Fire",
            "product_type": "Movie",
        }
    ]
    matches = _resolve_catalog_matches(sb, flat_variants)
    cid, method, status, val = matches["gid://shopify/ProductVariant/2"]
    assert cid is None
    assert method == "barcode"
    assert status == "ambiguous"
    assert "barcode:111:n=2" in val


def test_resolve_catalog_matches_barcode_title_matches_core_title_after_suffix_strip():
    sb = _FakeSupabase(
        catalog_rows=[
            {
                "id": "c1",
                "barcode": "5027035029634",
                "title": "Ben Hur",
                "edition_title": None,
            },
            {
                "id": "c2",
                "barcode": "5027035029634",
                "title": "Some Other Title",
                "edition_title": None,
            },
        ]
    )
    flat_variants = [
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/3",
            "barcode": "5027035029634",
            "_match_display_title": "Ben Hur (1959) Limited Edition Steelbook 4K Ultra HD + Blu-Ray",
            "product_type": "Movie",
        }
    ]
    matches = _resolve_catalog_matches(sb, flat_variants)
    cid, method, status, _ = matches["gid://shopify/ProductVariant/3"]
    assert cid == "c1"
    assert method == "barcode_title"
    assert status == "matched"
