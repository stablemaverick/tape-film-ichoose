"""
Tests for catalog update whitelists.

Verifies that stock-sync and catalog-sync modes apply the correct field sets,
and that TMDB/film-link fields are never included in update payloads.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.catalog_update_rules import (
    ALL_PROTECTED_FIELDS,
    CATALOG_SYNC_WHITELIST,
    STOCK_SYNC_WHITELIST,
    filter_update_payload,
    get_update_whitelist,
    validate_payload_safety,
)


class TestWhitelistSelection:
    def test_stock_sync_returns_commercial_only(self):
        wl = get_update_whitelist(existing_only=True)
        assert "supplier_stock_status" in wl
        assert "cost_price" in wl
        assert "title" not in wl
        assert "format" not in wl
        assert "media_release_date" not in wl

    def test_catalog_sync_includes_identity(self):
        wl = get_update_whitelist(existing_only=False)
        assert "supplier_stock_status" in wl
        assert "title" in wl
        assert "format" in wl
        assert "director" in wl
        assert "studio" in wl
        assert "media_release_date" in wl


class TestFilterPayload:
    def test_strips_non_whitelisted_fields(self):
        payload = {
            "title": "Test",
            "cost_price": 10.0,
            "tmdb_id": 12345,
            "film_id": "abc",
        }
        result = filter_update_payload(payload, CATALOG_SYNC_WHITELIST)
        assert "title" in result
        assert "cost_price" in result
        assert "tmdb_id" not in result
        assert "film_id" not in result

    def test_stock_sync_strips_identity(self):
        payload = {
            "title": "Test",
            "cost_price": 10.0,
            "supplier_stock_status": 5,
        }
        result = filter_update_payload(payload, STOCK_SYNC_WHITELIST)
        assert "cost_price" in result
        assert "supplier_stock_status" in result
        assert "title" not in result

    def test_stock_sync_strips_media_release_date(self):
        payload = {
            "cost_price": 10.0,
            "media_release_date": "2025-06-01",
        }
        result = filter_update_payload(payload, STOCK_SYNC_WHITELIST)
        assert "cost_price" in result
        assert "media_release_date" not in result


class TestPayloadSafety:
    def test_clean_payload_returns_empty(self):
        payload = {"title": "Test", "cost_price": 10.0}
        violations = validate_payload_safety(payload)
        assert violations == []

    def test_tmdb_field_flagged(self):
        payload = {"title": "Test", "tmdb_id": 12345}
        violations = validate_payload_safety(payload)
        assert "tmdb_id" in violations

    def test_film_link_field_flagged(self):
        payload = {"film_id": "abc", "film_link_status": "linked"}
        violations = validate_payload_safety(payload)
        assert "film_id" in violations
        assert "film_link_status" in violations

    def test_no_overlap_between_whitelists_and_protected(self):
        assert len(CATALOG_SYNC_WHITELIST & ALL_PROTECTED_FIELDS) == 0
        assert len(STOCK_SYNC_WHITELIST & ALL_PROTECTED_FIELDS) == 0
