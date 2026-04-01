from __future__ import annotations

import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.services.shopify_store_sync_service import derive_catalog_availability_from_listing


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
