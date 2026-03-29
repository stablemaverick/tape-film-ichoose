"""
Collection / bundle harness: multi-film titles must not get a naive single-film TMDB link.

Asserts ``classify_collection_listing``, ``label_film_collection_harness``, and
``search_tmdb_catalog_isolated`` (blocked path). After TV-specific helper changes,
film bundle rows must still block; TV home-video exceptions must not apply to these SKUs.
"""

from __future__ import annotations

from typing import List
from unittest.mock import patch

import pytest

from app.helpers.tmdb_match_helpers import _is_tv_single_series_home_video_collection
from app.services.tmdb_isolated_match_service import (
    classify_collection_listing,
    label_film_collection_harness,
    search_tmdb_catalog_isolated,
)

# Original 20 — multi-film / bundle risk (must stay blocked).
COLLECTION_HARNESS_TITLES: List[str] = [
    "James Bond Collection (Blu-ray)",
    "Mission: Impossible 6-Movie Collection",
    "A / B / C Triple Feature",
    "4 Film Favorites: Horror Collection",
    "Transformers 5-Movie Collection",
    "Middle-earth Extended Edition Collection",
    "Harry Potter Complete 8-Film Collection",
    "Marvel Phase One Box Set",
    "Friday the 13th 8-Movie Collection",
    "Star Wars Skywalker Saga 9-Movie Collection",
    "Indiana Jones 4-Movie Collection",
    "The Conjuring Universe 3-Film Collection",
    "Jurassic World 5-Movie Collection",
    "Fast & Furious 8-Movie Collection",
    "X-Men Collection",
    "Toy Story 4 Movie Collection",
    "Pirates of the Caribbean 5-Film Collection",
    "Alien / Predator / Prometheus Bundle",
    "Godzilla Showa-Era Collection",
    "Studio Ghibli Film Collection Limited Edition",
]

# 22 additional Moovies-style film bundle / anthology / franchise SKUs (must stay blocked).
# Wording uses explicit collection / box-set / multi-film / slash cues so the *current*
# classifier marks them as ``collection_bundle`` (plain “Trilogy” alone is not blocked).
MOOVIES_FILM_BUNDLE_TITLES: List[str] = [
    "The Lord of the Rings 3-Film Collection (Blu-ray)",
    "The Hobbit Trilogy Box Set",
    "Christopher Nolan 4K Ultra HD Collection",
    "Quentin Tarantino XX 10-Film Collection",
    "Batman: The Dark Knight Trilogy Collection",
    "Spider-Man Legacy Collection (3 Films)",
    "Back to the Future Trilogy Box Set",
    "Die Hard 5-Movie Collection",
    "Rocky Heavyweight Collection",
    "John Wick Chapters 1-3 Box Set",
    "The Cornetto Trilogy: Shaun of the Dead / Hot Fuzz / The World's End",
    "Before Sunrise Trilogy Box Set",
    "Mad Max Collection",
    "Planet of the Apes Trilogy Collection",
    "The Mummy Trilogy 3-Film Collection (1999-2008)",
    "Ocean's Trilogy Collection",
    "Pitch Perfect 3-Movie Collection",
    "Fifty Shades 3-Film Collection",
    "The Hangover Trilogy Collection",
    "The Purge 4-Movie Collection",
    "Insidious 4-Film Collection",
    "A Quiet Place Double Feature",
]

ALL_FILM_BUNDLE_TITLES: List[str] = COLLECTION_HARNESS_TITLES + MOOVIES_FILM_BUNDLE_TITLES

# Single-feature titles (must remain single_film_safe).
SINGLE_FILM_CONTROL_TITLES: List[str] = [
    "Inception",
    "The Matrix",
    "Dune: Part Two",
    "Oppenheimer",
    "Poor Things",
]


@pytest.mark.parametrize(
    "title",
    COLLECTION_HARNESS_TITLES,
    ids=[f"legacy_{i:02d}" for i in range(1, len(COLLECTION_HARNESS_TITLES) + 1)],
)
def test_legacy_collection_harness_blocks_classifier(title: str) -> None:
    info = classify_collection_listing(title)
    assert info["block_single_film_match"] is True, (
        f"Expected block for {title!r}; got {info!r}"
    )
    assert info["kind"] == "collection_bundle", (
        f"Expected collection_bundle for {title!r}; got {info!r}"
    )
    assert info["reasons"], f"Expected non-empty reasons for {title!r}; got {info!r}"
    assert label_film_collection_harness(info) == "blocked_collection_candidate"


@pytest.mark.parametrize(
    "title",
    MOOVIES_FILM_BUNDLE_TITLES,
    ids=[f"moovies_{i:02d}" for i in range(1, len(MOOVIES_FILM_BUNDLE_TITLES) + 1)],
)
def test_moovies_film_bundles_block_classifier(title: str) -> None:
    info = classify_collection_listing(title)
    assert info["block_single_film_match"] is True, (
        f"Expected block for {title!r}; got {info!r}"
    )
    assert info["kind"] == "collection_bundle", (
        f"Expected collection_bundle for {title!r}; got {info!r}"
    )
    assert info["reasons"], f"Expected non-empty reasons for {title!r}; got {info!r}"
    assert label_film_collection_harness(info) == "blocked_collection_candidate"


@pytest.mark.parametrize("title", ALL_FILM_BUNDLE_TITLES, ids=[f"b{i:02d}" for i in range(1, len(ALL_FILM_BUNDLE_TITLES) + 1)])
@patch("app.services.tmdb_isolated_match_service.requests.get")
def test_film_bundles_no_tmdb_http_when_blocked(mock_get, title: str) -> None:
    """Blocked rows must not call TMDB (safe rejection before HTTP)."""
    out = search_tmdb_catalog_isolated(
        title,
        "dummy-key",
        "https://api.themoviedb.org/3",
        source_year=None,
        edition_title="",
    )
    assert out["status"] == "blocked", (
        f"Expected blocked for {title!r}; got {out!r}"
    )
    mock_get.assert_not_called()


@pytest.mark.parametrize("title", ALL_FILM_BUNDLE_TITLES, ids=[f"tv_ex_{i:02d}" for i in range(1, len(ALL_FILM_BUNDLE_TITLES) + 1)])
def test_tv_collection_exception_does_not_exempt_film_bundles(title: str) -> None:
    """TV single-show home-video exceptions must not clear obvious film bundles."""
    assert not _is_tv_single_series_home_video_collection(title), (
        f"TV exemption must be false for film bundle: {title!r}"
    )


@pytest.mark.parametrize(
    "title",
    SINGLE_FILM_CONTROL_TITLES,
    ids=SINGLE_FILM_CONTROL_TITLES,
)
def test_single_film_titles_remain_single_candidate(title: str) -> None:
    info = classify_collection_listing(title)
    assert info["kind"] == "single_candidate"
    assert info["block_single_film_match"] is False
    assert "no_collection_signal" in info["reasons"]
    assert label_film_collection_harness(info) == "single_film_safe"


def test_empty_title_is_bundle_unresolved() -> None:
    info = classify_collection_listing("")
    assert info["kind"] == "ambiguous"
    assert info["block_single_film_match"] is True
    assert label_film_collection_harness(info) == "bundle_unresolved"
