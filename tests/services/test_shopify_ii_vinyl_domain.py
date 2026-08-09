"""Vinyl/soundtrack domain exclusion for film Inventory Intelligence."""

from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.config.inventory_dual_write import InventoryDualWriteFlags
from app.services.shopify_ii_product_domain import (
    SOUNDTRACKS_COLLECTION_HANDLE,
    is_film_inventory_release,
    is_vinyl_soundtrack_listing,
)
from app.services.shopify_release_dual_write_service import (
    dual_write_shopify_listings_to_releases,
    shopify_ii_dual_write_exclusion_reason,
)
from app.services.stock_availability_service import StockAvailabilityService


ON_SHOPIFY = InventoryDualWriteFlags(
    enabled=True,
    shopify=True,
    supplier=False,
    purchase_orders=False,
    auto_accept_min_confidence=0.95,
    fresh_max_hours=36,
    aging_max_hours=72,
    create_supplier_only_releases=True,
    supplier_batch_size=500,
    supplier_in_chunk_size=150,
)


@pytest.mark.parametrize(
    "title",
    [
        "Blade Runner 2049 4K Ultra HD",
        "Alien Blu-ray",
        "The Gift 4K Ultra HD",
        "Some Title DVD",
    ],
)
def test_film_formats_not_excluded_by_title_alone(title):
    assert (
        shopify_ii_dual_write_exclusion_reason(
            {"product_title": title, "product_type": "", "media_format": "4K UHD"}
        )
        is None
    )


def test_vinyl_identified_by_soundtracks_collection():
    row = {
        "product_title": "La La Land (Original Motion Picture Soundtrack)",
        "collection_handles": SOUNDTRACKS_COLLECTION_HANDLE,
        "media_format": "Vinyl",
    }
    assert is_vinyl_soundtrack_listing(row) is True
    assert shopify_ii_dual_write_exclusion_reason(row) == "vinyl_soundtrack"


def test_vinyl_identified_by_media_format_alone():
    row = {"product_title": "Hans Zimmer: The Classics Vinyl", "media_format": "Vinyl"}
    assert shopify_ii_dual_write_exclusion_reason(row) == "vinyl_soundtrack"


def test_vinyl_identified_by_soundtracks_product_id_set():
    pid = "gid://shopify/Product/999"
    row = {"product_title": "Sinners (Original Soundtrack)", "shopify_product_id": pid}
    assert shopify_ii_dual_write_exclusion_reason(row) is None
    assert (
        shopify_ii_dual_write_exclusion_reason(row, soundtrack_product_ids={pid})
        == "vinyl_soundtrack"
    )


def test_broad_vinyl_substring_in_unrelated_title_not_enough():
    # Without structured fields, do not exclude on title alone.
    assert (
        shopify_ii_dual_write_exclusion_reason(
            {"product_title": "Vinyl Horror Collection 4K Ultra HD", "product_type": ""}
        )
        is None
    )


def test_dual_write_skips_vinyl_keeps_film():
    from tests.services.test_inventory_dual_write_behaviour import FakeSupabase

    sb = FakeSupabase()
    rows = [
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/vinyl1",
            "shopify_product_id": "gid://shopify/Product/v1",
            "product_title": "Baby Driver (Original Soundtrack)",
            "collection_handles": "soundtracks,popular-in-stock-now",
            "media_format": "Vinyl",
            "barcode": "0889854536916",
            "tracks_inventory": True,
            "shopify_inventory_item_id": "gid://shopify/InventoryItem/v",
            "inventory_quantity": 2,
        },
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/film1",
            "shopify_product_id": "gid://shopify/Product/f1",
            "product_title": "Hush Limited Edition 4K Ultra HD",
            "media_format": "4K UHD",
            "collection_handles": "popular-in-stock-now",
            "barcode": "5027035030333",
            "tracks_inventory": True,
            "shopify_inventory_item_id": "gid://shopify/InventoryItem/f",
            "inventory_quantity": 7,
        },
    ]
    stats = dual_write_shopify_listings_to_releases(
        sb,
        rows,
        shop="a61446-1c.myshopify.com",
        location_id="gid://shopify/Location/78213775584",
        inventory_levels_by_variant={
            "gid://shopify/ProductVariant/film1": {
                "available": 7,
                "committed": 0,
                "on_hand": 7,
                "incoming": 0,
            }
        },
        flags=ON_SHOPIFY,
    )
    assert stats["skipped_non_release_by_reason"]["vinyl_soundtrack"] == 1
    assert stats["channels_upserted"] == 1
    assert stats["tape_levels_upserted"] == 1
    titles = {r.get("title") for r in sb.store.get("release_variants", [])}
    assert "Baby Driver (Original Soundtrack)" not in titles
    assert "Hush Limited Edition 4K Ultra HD" in titles


def test_film_inventory_release_domain_gate():
    assert is_film_inventory_release({"format": "4K UHD"}) is True
    assert is_film_inventory_release({"format": "Vinyl"}) is False
    assert is_film_inventory_release({"product_domain": "music_vinyl"}) is False
    assert is_film_inventory_release({"product_domain": "film"}) is True


def test_search_inventory_excludes_music_domain_releases():
    class _FakeSB:
        def table(self, name):
            return _FakeTable(name)

    class _FakeTable:
        def __init__(self, name):
            self.name = name
            self._op = "select"
            self._filters = []

        def select(self, *_a, **_k):
            return self

        def ilike(self, *_a, **_k):
            return self

        def eq(self, *_a, **_k):
            return self

        def in_(self, col, vals):
            self._filters.append((col, list(vals)))
            return self

        def limit(self, *_a, **_k):
            return self

        def execute(self):
            if self.name == "release_variants":
                return type(
                    "R",
                    (),
                    {
                        "data": [
                            {
                                "id": "film-1",
                                "title": "Blade Runner 4K Ultra HD",
                                "format": "4K UHD",
                                "primary_barcode": "1",
                                "publication_status": "published",
                                "active": True,
                            },
                            {
                                "id": "vinyl-1",
                                "title": "Blade Runner Soundtrack Vinyl",
                                "format": "Vinyl",
                                "primary_barcode": "2",
                                "publication_status": "published",
                                "active": True,
                                "product_domain": "music_vinyl",
                            },
                        ]
                    },
                )()
            if self.name == "release_shopify_listings":
                return type("R", (), {"data": []})()
            return type("R", (), {"data": []})()

    out = StockAvailabilityService(_FakeSB()).search_inventory("Blade Runner", limit=20)
    ids = {c["release_variant_id"] for c in out["candidates"]}
    assert "film-1" in ids
    assert "vinyl-1" not in ids
