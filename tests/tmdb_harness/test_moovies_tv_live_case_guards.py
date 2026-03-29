"""
Guards for the 20 Moovies TV strings exercised by ``run_tv_live_cases`` (live API harness).

Uses only ``app.helpers`` + ``app.rules`` so this file stays valid without the isolated matcher module.
"""

from __future__ import annotations

import pytest

from app.helpers.tmdb_match_helpers import detect_tmdb_search_type, is_collection_or_bundle
from app.rules.content_classification_rules import strong_tv_title

# Same 20 strings as ``run_tv_live_cases.CASE_TITLES`` (inlined so this file commits standalone).
MOOVIES_TV_LIVE_CASE_TITLES: tuple[str, ...] = (
    "Game of Thrones: The Complete Seventh Season",
    "The Crown: Season 4",
    "Line of Duty: Series 6",
    "Doctor Who: The Complete David Tennant Collection",
    "Stranger Things: Season 3",
    "Better Call Saul: Season 6",
    "South Park: Season 25",
    "Planet Earth II: Complete Series",
    "Band of Brothers",
    "Chernobyl",
    "Twin Peaks: A Limited Event Series",
    "The Last of Us: Season 1",
    "House of the Dragon: Season One",
    "Yellowstone: Season 5 Part 1",
    "The Walking Dead: The Complete Eleventh Season",
    "Attack on Titan: Final Season Part 2",
    "Neon Genesis Evangelion: Complete Series",
    "Battlestar Galactica (2004): The Complete Series",
    "The Office: The Complete Series",
    "Pride and Prejudice (1995)",
)

PRIDE_PREJUDICE_1995 = "Pride and Prejudice (1995)"

# Moovies list is “TV aisle” SKUs; a few bare miniseries titles still match via ``/search/movie``
# under current routing (proven acceptable on live TMDb harness).
MOOVIES_TV_LIST_MOVIE_ROUTE_OK = frozenset(
    {
        PRIDE_PREJUDICE_1995,
        "Band of Brothers",
        "Chernobyl",
    }
)


def _should_route_tmdb_tv(title: str, edition_title: str = "") -> bool:
    """Mirrors ``should_route_tmdb_tv`` in ``tmdb_isolated_match_service`` (production path uses helpers + rules)."""
    if detect_tmdb_search_type(title) == "tv":
        return True
    blob = f"{title or ''} {edition_title or ''}".strip().lower()
    if not blob:
        return False
    return strong_tv_title(blob)


@pytest.mark.parametrize(
    "title",
    MOOVIES_TV_LIVE_CASE_TITLES,
    ids=[f"moovies_tv_{i + 1:02d}" for i in range(len(MOOVIES_TV_LIVE_CASE_TITLES))],
)
def test_moovies_tv_live_titles_not_collection_blocked(title: str) -> None:
    """First gate in ``search_tmdb_movie_safe`` must not treat these as film bundles."""
    assert not is_collection_or_bundle(title), (
        f"Unexpected collection/bundle block for TV-style title: {title!r}"
    )


@pytest.mark.parametrize(
    "title",
    [t for t in MOOVIES_TV_LIVE_CASE_TITLES if t not in MOOVIES_TV_LIST_MOVIE_ROUTE_OK],
    ids=[
        f"route_{i + 1:02d}"
        for i in range(len(MOOVIES_TV_LIVE_CASE_TITLES) - len(MOOVIES_TV_LIST_MOVIE_ROUTE_OK))
    ],
)
def test_moovies_tv_live_titles_route_tv(title: str) -> None:
    assert _should_route_tmdb_tv(title, ""), f"Expected TV route for {title!r}"


@pytest.mark.parametrize("title", sorted(MOOVIES_TV_LIST_MOVIE_ROUTE_OK))
def test_moovies_tv_list_movie_route_exceptions_stay_movie(title: str) -> None:
    assert not _should_route_tmdb_tv(title, "")
