"""
Tests for content_classification_rules (film / tv / unknown).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.content_classification_rules import (  # noqa: E402
    classify_content_type,
    strong_tv_title,
    supplier_indicates_tv,
)


class TestStrongTvTitle:
    def test_season_number(self):
        assert strong_tv_title("breaking bad season 4 blu-ray")

    def test_s00e00(self):
        assert strong_tv_title("show s01e03 collection")

    def test_complete_series_tv(self):
        assert strong_tv_title("the sopranos complete series")

    def test_complete_series_movie_collection_rejected(self):
        assert not strong_tv_title("harry potter complete 8-film collection")

    def test_lone_series_word_not_enough(self):
        assert not strong_tv_title("dark star series of films")


class TestSupplierTv:
    def test_tv_series_phrase(self):
        assert supplier_indicates_tv("bbc drama tv series box")

    def test_season_range_in_meta(self):
        assert supplier_indicates_tv("gift set seasons 1-3")


class TestClassifyObviousTv:
    def test_explicit_media_type_tv(self):
        row = {"title": "Anything", "media_type": "tv"}
        assert classify_content_type(row) == "tv"

    def test_season_in_title_overrides_default_film_media_type(self):
        row = {
            "title": "The Wire Season 2",
            "media_type": "film",
            "format": "Blu-ray",
        }
        assert classify_content_type(row) == "tv"

    def test_matched_tmdb_tv_search_type(self):
        # Title triggers detect_tmdb_search_type "tv" (contains "series ") but not strong_tv_title
        row = {
            "title": "Something Series Collection",
            "media_type": "film",
            "tmdb_match_status": "matched",
        }
        assert classify_content_type(row) == "tv"


class TestClassifyAmbiguousAndFilm:
    def test_default_pipeline_film_media_type(self):
        row = {"title": "The Matrix", "media_type": "film"}
        assert classify_content_type(row) == "film"

    def test_ambiguous_title_no_format_unknown(self):
        row = {"title": "Echo", "media_type": None, "format": None}
        assert classify_content_type(row) == "unknown"

    def test_physical_format_implies_film_when_not_tv(self):
        row = {"title": "Some Title", "media_type": None, "format": "Blu-ray"}
        assert classify_content_type(row) == "film"

    def test_matched_movie_path_implies_film(self):
        row = {
            "title": "Inception",
            "media_type": None,
            "format": None,
            "tmdb_match_status": "matched",
        }
        assert classify_content_type(row) == "film"

    def test_harmonized_title_used_for_detection(self):
        row = {
            "title": "Retail SKU Echo",
            "harmonized_title": "Show Name Season 2",
            "media_type": "film",
        }
        assert classify_content_type(row) == "tv"
