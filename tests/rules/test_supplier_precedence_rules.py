"""
Tests for supplier precedence and best-offer selection.

Verifies Tape Film is always preferred, and that availability/stock/price
ranking works correctly.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.supplier_precedence_rules import (
    pick_best_row,
    pick_representative,
    supplier_rank,
)


def _row(supplier, avail="supplier_stock", stock=10, price=29.99, title="Test Film", **kw):
    return {
        "supplier": supplier,
        "availability_status": avail,
        "supplier_stock_status": stock,
        "calculated_sale_price": price,
        "supplier_priority": kw.get("supplier_priority", supplier_rank(supplier)),
        "title": title,
        **kw,
    }


class TestSupplierRank:
    def test_tape_film_highest(self):
        assert supplier_rank("Tape Film") == 0

    def test_moovies_middle(self):
        assert supplier_rank("Moovies") == 1
        assert supplier_rank("moovies") == 1

    def test_lasgo_lowest(self):
        assert supplier_rank("Lasgo") == 2

    def test_unknown_supplier(self):
        assert supplier_rank("Random Supplier") == 9


class TestPickBestRow:
    def test_tape_film_preferred_over_moovies(self):
        rows = [
            _row("Moovies", stock=100, price=25.99),
            _row("Tape Film", stock=5, price=35.99),
        ]
        best = pick_best_row(rows)
        assert best["supplier"] == "Tape Film"

    def test_in_stock_preferred_over_out(self):
        rows = [
            _row("Moovies", avail="supplier_out", stock=0),
            _row("Lasgo", avail="supplier_stock", stock=10),
        ]
        best = pick_best_row(rows)
        assert best["supplier"] == "Lasgo"

    def test_higher_stock_preferred(self):
        rows = [
            _row("Moovies", stock=5, price=29.99),
            _row("Moovies", stock=50, price=29.99),
        ]
        best = pick_best_row(rows)
        assert best["supplier_stock_status"] == 50

    def test_lower_price_preferred(self):
        rows = [
            _row("Moovies", stock=10, price=39.99),
            _row("Moovies", stock=10, price=24.99),
        ]
        best = pick_best_row(rows)
        assert best["calculated_sale_price"] == 24.99

    def test_supplier_preference_filter(self):
        rows = [
            _row("Moovies", price=25.99),
            _row("Lasgo", price=19.99),
        ]
        best = pick_best_row(rows, supplier_preference="lasgo")
        assert best["supplier"] == "Lasgo"

    def test_empty_list_returns_none(self):
        assert pick_best_row([]) is None

    def test_unmatched_supplier_preference_returns_none(self):
        rows = [_row("Moovies")]
        assert pick_best_row(rows, supplier_preference="lasgo") is None


class TestPickRepresentative:
    def test_prefers_metadata_rich_row(self):
        rows = [
            _row("Lasgo", tmdb_id=None, tmdb_title=None, genres=None, top_cast=None),
            _row("Moovies", tmdb_id=550, tmdb_title="Fight Club", genres="Drama", top_cast="Brad Pitt"),
        ]
        rep = pick_representative(rows)
        assert rep["tmdb_id"] == 550

    def test_supplier_tiebreaker(self):
        rows = [
            _row("Lasgo", tmdb_id=550, tmdb_title="FC", genres="Drama", top_cast="BP"),
            _row("Moovies", tmdb_id=550, tmdb_title="FC", genres="Drama", top_cast="BP"),
        ]
        rep = pick_representative(rows)
        assert rep["supplier"] == "Moovies"
