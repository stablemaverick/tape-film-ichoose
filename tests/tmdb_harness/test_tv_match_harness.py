"""
TV matching harness: 20 curated supplier-style titles → TMDB TV search path.

Uses ``search_tmdb_catalog_isolated`` with mocked HTTP (no live TMDB / no API key required).
On failure, assertions include the full ``TmdbIsolatedMatchResult`` for debugging.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

import pytest

from app.services.tmdb_isolated_match_service import search_tmdb_catalog_isolated


def _tv_hit(
    tmdb_id: int,
    name: str,
    *,
    first_air_date: str = "2013-01-01",
) -> Dict[str, Any]:
    return {
        "id": tmdb_id,
        "name": name,
        "original_name": name,
        "first_air_date": first_air_date,
        "popularity": 10.0,
    }


# (case_id, title, edition, source_year, mock_tv_results, expected_tmdb_id)
# source_year=None avoids strict year scoring in pick_best_tmdb_match (season vs premiere year).
TV_HARNESS_CASES: List[Tuple[str, str, str, Optional[int], List[Dict[str, Any]], int]] = [
    ("tv01", "Breaking Bad Season 1", "", None, [_tv_hit(1396, "Breaking Bad", first_air_date="2008-01-20")], 1396),
    ("tv02", "The Office Complete Series", "", None, [_tv_hit(2316, "The Office", first_air_date="2005-03-24")], 2316),
    ("tv03", "Game of Thrones Season 7", "", None, [_tv_hit(1399, "Game of Thrones", first_air_date="2011-04-17")], 1399),
    ("tv04", "Sherlock Series 1", "", None, [_tv_hit(19885, "Sherlock", first_air_date="2010-07-25")], 19885),
    ("tv05", "Chernobyl Limited Series", "", None, [_tv_hit(87108, "Chernobyl", first_air_date="2019-05-06")], 87108),
    ("tv06", "True Detective Season 2", "", None, [_tv_hit(46648, "True Detective", first_air_date="2014-01-12")], 46648),
    # Nature doc — use "Complete Series" so TV routing wins over duplicate film entry.
    ("tv07", "Planet Earth II Complete Series", "", None, [_tv_hit(55866, "Planet Earth II", first_air_date="2016-11-06")], 55866),
    ("tv08", "The Crown Season 4", "", None, [_tv_hit(65494, "The Crown", first_air_date="2016-11-04")], 65494),
    ("tv09", "Line of Duty Series 6", "", None, [_tv_hit(4614, "Line of Duty", first_air_date="2012-06-26")], 4614),
    ("tv10", "Fleabag Season 1", "", None, [_tv_hit(67026, "Fleabag", first_air_date="2016-07-21")], 67026),
    ("tv11", "Star Trek: Discovery Season 3", "", None, [_tv_hit(67198, "Star Trek: Discovery", first_air_date="2017-09-24")], 67198),
    ("tv12", "The Wire Complete Series", "", None, [_tv_hit(1438, "The Wire", first_air_date="2002-06-02")], 1438),
    ("tv13", "The Mandalorian Season 2", "", None, [_tv_hit(82856, "The Mandalorian", first_air_date="2019-11-12")], 82856),
    ("tv14", "Stranger Things Season 4", "", None, [_tv_hit(66732, "Stranger Things", first_air_date="2016-07-15")], 66732),
    ("tv15", "Better Call Saul Season 5", "", None, [_tv_hit(60059, "Better Call Saul", first_air_date="2015-02-08")], 60059),
    ("tv16", "South Park Season 20", "", None, [_tv_hit(2190, "South Park", first_air_date="1997-08-13")], 2190),
    ("tv17", "Doctor Who Season 12", "", None, [_tv_hit(57243, "Doctor Who", first_air_date="2005-03-26")], 57243),
    ("tv18", "Succession Season 3", "", None, [_tv_hit(76331, "Succession", first_air_date="2018-06-03")], 76331),
    ("tv19", "The Last of Us Season 1", "", None, [_tv_hit(100088, "The Last of Us", first_air_date="2023-01-15")], 100088),
    ("tv20", "Wednesday Season 1", "", None, [_tv_hit(119051, "Wednesday", first_air_date="2022-11-23")], 119051),
]


@pytest.mark.parametrize(
    "case_id,title,edition,source_year,mock_results,expected_id",
    TV_HARNESS_CASES,
    ids=[c[0] for c in TV_HARNESS_CASES],
)
@patch("app.services.tmdb_isolated_match_service.requests.get")
def test_tv_harness_curated_match(
    mock_get: Mock,
    case_id: str,
    title: str,
    edition: str,
    source_year: Optional[int],
    mock_results: List[Dict[str, Any]],
    expected_id: int,
) -> None:
    mresp = Mock()
    mresp.raise_for_status = Mock()
    mresp.json.return_value = {"results": mock_results, "page": 1, "total_results": len(mock_results)}
    mock_get.return_value = mresp

    out = search_tmdb_catalog_isolated(
        title,
        "dummy-key",
        "https://api.themoviedb.org/3",
        source_year=source_year,
        edition_title=edition,
    )

    assert out.get("status") == "matched", (
        f"[{case_id}] expected matched; got {out!r}; "
        f"failure_detail={out.get('failure_detail')!r}; reasons={out.get('reasons')}"
    )
    assert out.get("search_type") == "tv", f"[{case_id}] expected TV route: {out!r}"
    assert out.get("tmdb_id") == expected_id, f"[{case_id}] tmdb_id mismatch: {out!r}"
    mock_get.assert_called()
    urls = [call.args[0] for call in mock_get.call_args_list]
    assert any("/search/tv" in str(u) for u in urls), f"[{case_id}] expected /search/tv in {urls!r}"


@patch("app.services.tmdb_isolated_match_service.requests.get")
def test_tv_harness_rejects_movie_endpoint_for_season_title(mock_get: Mock) -> None:
    """Sanity: season-style title must not succeed via movie search."""
    mresp = Mock()
    mresp.raise_for_status = Mock()
    mresp.json.return_value = {"results": [], "page": 1, "total_results": 0}
    mock_get.return_value = mresp

    out = search_tmdb_catalog_isolated(
        "The Americans Season 1",
        "k",
        "https://api.themoviedb.org/3",
        source_year=2013,
        edition_title="",
    )
    assert out["status"] in ("matched", "not_found")
    urls = [call.args[0] for call in mock_get.call_args_list]
    assert not any("/search/movie" in str(u) for u in urls), (
        "TV-routed title should not call movie search; got " + repr(urls)
    )
