"""
Tests for cross-supplier harmonization rules.

Verifies field ownership: Lasgo title wins, Moovies format/studio/director wins,
Lasgo release date wins.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.rules.harmonization_rules import (
    compute_harmonized_update,
    determine_leader_supplier,
    pick_best_director,
    pick_best_format,
    pick_best_release_date,
    pick_best_studio,
    pick_best_title,
)


def _offer(supplier, title=None, fmt=None, studio=None, director=None, release=None):
    return {
        "supplier": supplier,
        "title": title,
        "format": fmt,
        "studio": studio,
        "director": director,
        "media_release_date": release,
        "harmonized_title": title,
        "harmonized_format": fmt,
        "harmonized_studio": studio,
        "harmonized_director": director,
    }


class TestPickBestTitle:
    def test_lasgo_title_wins_over_moovies(self):
        group = [
            _offer("Lasgo", title="The Matrix"),
            _offer("Moovies", title="Matrix, The"),
        ]
        assert pick_best_title(group) == "The Matrix"

    def test_moovies_fallback_when_no_lasgo(self):
        group = [
            _offer("Moovies", title="Inception"),
        ]
        assert pick_best_title(group) == "Inception"

    def test_lasgo_none_falls_to_moovies(self):
        group = [
            _offer("Lasgo", title=None),
            _offer("Moovies", title="Blade Runner"),
        ]
        assert pick_best_title(group) == "Blade Runner"


class TestPickBestFormat:
    def test_moovies_format_wins_over_lasgo(self):
        group = [
            _offer("Lasgo", fmt="Blu-ray"),
            _offer("Moovies", fmt="4K Ultra HD + Blu-ray"),
        ]
        assert pick_best_format(group) == "4K Ultra HD + Blu-ray"

    def test_lasgo_fallback_when_no_moovies(self):
        group = [
            _offer("Lasgo", fmt="Blu-ray"),
        ]
        assert pick_best_format(group) == "Blu-ray"


class TestPickBestStudio:
    def test_moovies_studio_wins(self):
        group = [
            _offer("Lasgo", studio="WB"),
            _offer("Moovies", studio="Warner Bros."),
        ]
        assert pick_best_studio(group) == "Warner Bros."


class TestPickBestDirector:
    def test_moovies_director_wins(self):
        group = [
            _offer("Lasgo", director=None),
            _offer("Moovies", director="Christopher Nolan"),
        ]
        assert pick_best_director(group) == "Christopher Nolan"


class TestPickBestReleaseDate:
    def test_lasgo_release_date_wins(self):
        group = [
            _offer("Lasgo", release="2025-06-15"),
            _offer("Moovies", release="2025-06-01"),
        ]
        assert pick_best_release_date(group) == "2025-06-15"

    def test_moovies_fallback_when_no_lasgo(self):
        group = [
            _offer("Moovies", release="2025-03-01"),
        ]
        assert pick_best_release_date(group) == "2025-03-01"


class TestDetermineLeaderSupplier:
    def test_moovies_is_leader_when_present(self):
        group = [
            _offer("Lasgo"),
            _offer("Moovies"),
        ]
        assert determine_leader_supplier(group) == "moovies"

    def test_lasgo_is_leader_when_no_moovies(self):
        group = [
            _offer("Lasgo"),
            _offer("Tape Film"),
        ]
        assert determine_leader_supplier(group) == "lasgo"


class TestComputeHarmonizedUpdate:
    def test_returns_none_when_no_changes(self):
        row = _offer("Moovies", title="Inception", fmt="4K", studio="WB", director="Nolan")
        result = compute_harmonized_update(
            row, "Inception", "4K", "WB", "Nolan", None, "moovies", "2025-01-01T00:00:00Z"
        )
        assert result is None

    def test_returns_diff_when_title_changed(self):
        row = _offer("Moovies", title="Matrix The")
        result = compute_harmonized_update(
            row, "The Matrix", None, None, None, None, "lasgo", "2025-01-01T00:00:00Z"
        )
        assert result is not None
        assert result["harmonized_title"] == "The Matrix"
        assert result["harmonized_from_supplier"] == "lasgo"

    def test_includes_release_date_when_changed(self):
        row = {
            "harmonized_title": "Test",
            "harmonized_format": None,
            "harmonized_studio": None,
            "harmonized_director": None,
            "media_release_date": "2025-01-01",
        }
        result = compute_harmonized_update(
            row, "Test", None, None, None, "2025-06-15", "lasgo", "2025-01-01T00:00:00Z"
        )
        assert result is not None
        assert result["media_release_date"] == "2025-06-15"
