"""
Tests for TMDB enrichment rules.

Verifies match-once lock behavior and daily vs recovery mode filtering.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.tmdb_rules import (
    build_matched_update,
    build_no_clean_title_update,
    build_not_found_update,
    is_row_locked,
    should_enrich_row,
)


class TestMatchOnceLock:
    def test_unlocked_row(self):
        row = {"tmdb_last_refreshed_at": None, "active": True}
        assert not is_row_locked(row)

    def test_locked_matched_row(self):
        row = {"tmdb_last_refreshed_at": "2025-01-01T00:00:00Z", "tmdb_match_status": "matched"}
        assert is_row_locked(row)

    def test_locked_not_found_row(self):
        row = {"tmdb_last_refreshed_at": "2025-01-01T00:00:00Z", "tmdb_match_status": "not_found"}
        assert is_row_locked(row)


class TestShouldEnrichRow:
    def test_new_unlinked_row_qualifies_daily(self):
        row = {"active": True, "tmdb_last_refreshed_at": None, "film_id": None}
        assert should_enrich_row(row, daily=True)

    def test_linked_row_skipped_in_daily(self):
        row = {"active": True, "tmdb_last_refreshed_at": None, "film_id": "some-uuid"}
        assert not should_enrich_row(row, daily=True)

    def test_linked_row_qualifies_in_recovery(self):
        row = {"active": True, "tmdb_last_refreshed_at": None, "film_id": "some-uuid"}
        assert should_enrich_row(row, daily=False)

    def test_already_enriched_never_qualifies(self):
        row = {"active": True, "tmdb_last_refreshed_at": "2025-01-01T00:00:00Z", "film_id": None}
        assert not should_enrich_row(row, daily=True)
        assert not should_enrich_row(row, daily=False)

    def test_inactive_row_skipped(self):
        row = {"active": False, "tmdb_last_refreshed_at": None, "film_id": None}
        assert not should_enrich_row(row, daily=True)


class TestBuildUpdates:
    def test_not_found_stamps_timestamp(self):
        update = build_not_found_update("2025-01-01T00:00:00Z")
        assert update["tmdb_match_status"] == "not_found"
        assert update["tmdb_last_refreshed_at"] == "2025-01-01T00:00:00Z"

    def test_no_clean_title_stamps_status(self):
        update = build_no_clean_title_update("2025-01-01T00:00:00Z")
        assert update["tmdb_match_status"] == "no_clean_title"

    def test_matched_update_has_all_fields(self):
        tmdb_match = {"id": 550, "title": "Fight Club"}
        details = {
            "release_date": "1999-10-15",
            "genres": [{"name": "Drama"}, {"name": "Thriller"}],
            "poster_path": "/poster.jpg",
            "backdrop_path": "/backdrop.jpg",
            "vote_average": 8.4,
            "vote_count": 25000,
            "popularity": 50.5,
            "production_countries": [{"name": "United States of America"}],
        }
        credits = {
            "cast": [{"name": "Brad Pitt"}, {"name": "Edward Norton"}],
            "crew": [{"name": "David Fincher", "job": "Director"}],
        }
        update = build_matched_update(
            tmdb_match, details, credits, "movie", None, "2025-01-01T00:00:00Z"
        )
        assert update["tmdb_match_status"] == "matched"
        assert update["tmdb_id"] == 550
        assert update["tmdb_title"] == "Fight Club"
        assert update["director"] == "David Fincher"
        assert update["film_released"] == "1999-10-15"
        assert "Drama" in update["genres"]
        assert "Brad Pitt" in update["top_cast"]
        assert update["country_of_origin"] == "United States of America"

    def test_existing_director_preserved_for_movie(self):
        tmdb_match = {"id": 550, "title": "Fight Club"}
        details = {
            "release_date": "1999-10-15",
            "genres": [],
            "production_countries": [],
        }
        credits = {"cast": [], "crew": []}
        update = build_matched_update(
            tmdb_match, details, credits, "movie", "Pre-existing Director", "2025-01-01T00:00:00Z"
        )
        assert update["director"] == "Pre-existing Director"
