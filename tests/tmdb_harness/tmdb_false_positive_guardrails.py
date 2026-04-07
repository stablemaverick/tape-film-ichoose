"""
Isolated TMDB false-positive guardrails (harness / regression; not wired to production).

Root cause (Mission Impossible 3 steelbook vs "Mitch-Match #31 Mission Impossible"):
``is_safe_tmdb_match`` allows overlap >= len(source_tokens) - 1. For source tokens
``{mission, impossible, 3}``, overlap ``{mission, impossible}`` satisfies that rule
because the tolerated missing token is the sequel numeral ``3``. Unrelated tokens on
the candidate (``mitch``, ``match``, ``31``) are ignored — there is no penalty for
*extra* junk when overlap still hits the threshold.

Failure bucket: **unsafe candidate acceptance** via **weak penalty for unrelated
title prefixes** combined with **sequel numeral tolerance** (not a normalization bug).

Proposed minimal production fix (when ready): after the existing overlap checks pass,
reject when both sets are non-empty::

    missing = source_tokens - candidate_tokens
    extra = candidate_tokens - source_tokens
    if missing and extra:
        return False

This is conservative: it rejects only when the candidate simultaneously drops a
source token *and* introduces tokens not present on the source (podcast/clip show
noise + franchise substring). It does **not** loosen global thresholds.

Similar risks: any multi-token supplier title where TMDB returns a longer unrelated
title that embeds the franchise substring (e.g. "Episode N … Fast & Furious …")
may trip the same pattern if overlap clears n-1 without extra/missing discipline.

When ``source_year`` is absent, ``pick_best_tmdb_match`` tie-breaks equal scores by
result order — junk can win if it sorts first.
"""

from __future__ import annotations

from app.helpers.tmdb_match_helpers import is_safe_tmdb_match, title_tokens


def reject_tmdb_candidate_missing_and_extra_tokens(
    source_title: str,
    candidate_title: str,
) -> bool:
    """
    Return True if this pair should be rejected due to junk + dropped source tokens.

    When both ``missing`` and ``extra`` are non-empty, the candidate is not a pure
    subset extension nor a pure sequel drop — it mixes unrelated tokens with missing
    franchise tokens (e.g. sequel digit).
    """
    source_tokens = set(title_tokens(source_title))
    candidate_tokens = set(title_tokens(candidate_title))
    if not source_tokens or not candidate_tokens:
        return False
    missing = source_tokens - candidate_tokens
    extra = candidate_tokens - source_tokens
    return bool(missing and extra)


def is_safe_tmdb_match_with_noise_guard(source_title: str, candidate_title: str) -> bool:
    """
    Intended future behavior: existing ``is_safe_tmdb_match`` plus missing/extra guard.

    Use in regression tests until production adopts the same check inside
    ``is_safe_tmdb_match``.
    """
    if not is_safe_tmdb_match(source_title, candidate_title):
        return False
    if reject_tmdb_candidate_missing_and_extra_tokens(source_title, candidate_title):
        return False
    return True
