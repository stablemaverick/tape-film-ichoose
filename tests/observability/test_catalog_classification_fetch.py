"""
Classification fetch must paginate past PostgREST's default ~1000 row cap.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.observability import catalog_metrics as cm  # noqa: E402


class _ExecResult:
    __slots__ = ("data",)

    def __init__(self, data):
        self.data = data


class _FakeCatalogQuery:
    def __init__(self, parent: "FakeSupabaseForClassification"):
        self._parent = parent

    def select(self, _cols: str):
        return self

    def eq(self, *_a, **_k):
        return self

    def range(self, start: int, end: int):
        self._start = start
        self._end = end
        return self

    def execute(self):
        return self._parent._execute_range(self._start, self._end)


class FakeSupabaseForClassification:
    """
    Simulates PostgREST: each request returns at most ``per_request_cap`` rows
    even when .range() asks for more.
    """

    def __init__(self, total: int, per_request_cap: int = 1000):
        self.total = total
        self.per_request_cap = per_request_cap
        self.range_calls: list[tuple[int, int]] = []

    def table(self, name: str):
        assert name == "catalog_items"
        return _FakeCatalogQuery(self)

    def _make_row(self, index: int) -> dict:
        return {
            "title": f"Title {index}",
            "edition_title": None,
            "media_type": "film",
            "category": None,
            "format": "Blu-ray",
            "notes": None,
            "source_type": "catalog",
            "film_id": "00000000-0000-0000-0000-000000000001",
            "tmdb_match_status": None,
        }

    def _execute_range(self, start: int, end: int) -> _ExecResult:
        self.range_calls.append((start, end))
        requested = end - start + 1
        remaining = self.total - start
        if remaining <= 0:
            return _ExecResult([])
        # Server returns at most per_request_cap rows per HTTP response
        n = min(requested, remaining, self.per_request_cap)
        rows = [self._make_row(start + i) for i in range(n)]
        return _ExecResult(rows)


def test_fetch_paginates_beyond_single_1000_row_page():
    total = 2500
    fake = FakeSupabaseForClassification(total=total, per_request_cap=1000)
    # Match production default page size
    old_ps = cm._CLASSIFICATION_PAGE_SIZE
    try:
        cm._CLASSIFICATION_PAGE_SIZE = 1000
        rows = cm._fetch_active_rows_for_classification(fake)
    finally:
        cm._CLASSIFICATION_PAGE_SIZE = old_ps

    assert len(rows) == total
    assert len(fake.range_calls) == 3
    cc, _linked, _ft = cm._classification_counts(rows)
    assert cc["film"] + cc["tv"] + cc["unknown"] == total


def test_assert_classification_raises_when_fetch_incomplete():
    try:
        cm._assert_classification_counts_match_active_catalog(
            total_active=5000,
            class_counts={"film": 1000, "tv": 0, "unknown": 0},
            rows_fetched=1000,
        )
    except RuntimeError as exc:
        assert "5000" in str(exc) and "1000" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_assert_classification_raises_when_buckets_dont_sum():
    try:
        cm._assert_classification_counts_match_active_catalog(
            total_active=100,
            class_counts={"film": 50, "tv": 30, "unknown": 10},
            rows_fetched=100,
        )
    except RuntimeError as exc:
        assert "bucket" in str(exc).lower() or "film+tv+unknown" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
