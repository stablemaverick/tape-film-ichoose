"""
Regression: TMDB safe-match false positive (Mission Impossible 3 vs Mitch-Match).

Does not require TMDB API. Documents current ``is_safe_tmdb_match`` behavior and
validates the isolated guard in ``tmdb_false_positive_guardrails``.
"""

from __future__ import annotations

import pytest

from app.helpers.tmdb_match_helpers import is_safe_tmdb_match, pick_best_tmdb_match
from tests.tmdb_harness.tmdb_false_positive_guardrails import (
    is_safe_tmdb_match_with_noise_guard,
    reject_tmdb_candidate_missing_and_extra_tokens,
)

# Exact bad match reported (supplier title vs TMDB-style result title)
MISSION_III_STEELBOOK = (
    "Mission Impossible 3 Limited Edition Steelbook 4K Ultra HD + Blu-Ray"
)
MITCH_MATCH_BAD = "Mitch-Match #31 Mission Impossible"


def test_repro_legacy_is_safe_accepts_mitch_match_false_positive() -> None:
    """Documents why the bad candidate was accepted: n-1 overlap + missing sequel token."""
    assert is_safe_tmdb_match(MISSION_III_STEELBOOK, MITCH_MATCH_BAD) is True


def test_noise_guard_rejects_mitch_match() -> None:
    assert reject_tmdb_candidate_missing_and_extra_tokens(
        MISSION_III_STEELBOOK, MITCH_MATCH_BAD
    ) is True
    assert is_safe_tmdb_match_with_noise_guard(MISSION_III_STEELBOOK, MITCH_MATCH_BAD) is False


def test_noise_guard_allows_mi3_when_tmdb_omits_sequel_number() -> None:
    """Sequel token absent on candidate, no extra junk — keep accepting."""
    assert reject_tmdb_candidate_missing_and_extra_tokens(
        MISSION_III_STEELBOOK, "Mission Impossible"
    ) is False
    assert is_safe_tmdb_match(MISSION_III_STEELBOOK, "Mission Impossible") is True
    assert is_safe_tmdb_match_with_noise_guard(MISSION_III_STEELBOOK, "Mission Impossible") is True


def test_noise_guard_allows_extra_tokens_when_all_source_tokens_present() -> None:
    """Candidate is superset (subtitle / edition words) — extra only, no missing."""
    assert reject_tmdb_candidate_missing_and_extra_tokens(
        MISSION_III_STEELBOOK,
        "Mission Impossible 3 Theatrical",
    ) is False


def test_pick_best_prefers_junk_when_year_missing_tie(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    With source_year=None, both safe candidates score 100; first list position wins.
    That reproduces accepting Mitch-Match when it appears before the real film.
    """
    from app.helpers import tmdb_match_helpers as mm

    results = [
        {
            "id": 999001,
            "title": MITCH_MATCH_BAD,
            "release_date": "2020-01-01",
        },
        {
            "id": 999002,
            "title": "Mission: Impossible III",
            "release_date": "2006-05-05",
        },
    ]

    best_legacy = pick_best_tmdb_match(MISSION_III_STEELBOOK, None, results, "movie")
    assert best_legacy is not None
    assert best_legacy.get("title") == MITCH_MATCH_BAD

    orig_safe = mm.is_safe_tmdb_match

    def guarded_safe(src: str, cand: str) -> bool:
        return orig_safe(src, cand) and not reject_tmdb_candidate_missing_and_extra_tokens(
            src, cand
        )

    monkeypatch.setattr(mm, "is_safe_tmdb_match", guarded_safe)
    best_guarded = pick_best_tmdb_match(MISSION_III_STEELBOOK, None, results, "movie")
    assert best_guarded is not None
    assert "Impossible III" in (best_guarded.get("title") or "")
