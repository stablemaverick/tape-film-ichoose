"""
Isolated TMDB matching helpers for harnesses and offline quality work.

Does **not** wire into enrichment pipeline orchestration. Prefer safe rejection over
wrong links. Reuses ``tmdb_match_helpers`` and ``content_classification_rules``.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional, TypedDict

FilmCollectionHarnessLabel = Literal[
    "blocked_collection_candidate",
    "bundle_unresolved",
    "single_film_safe",
]

import requests

from app.helpers.tmdb_match_helpers import (
    build_search_query_variants,
    build_tv_search_query_variants,
    detect_tmdb_search_type,
    extract_year,
    is_collection_or_bundle,
    is_safe_tmdb_match,
    normalize_match_title,
    pick_best_tmdb_match,
)
from app.rules.content_classification_rules import strong_tv_title

MatchStatus = Literal["matched", "not_found", "blocked"]


class TmdbIsolatedMatchResult(TypedDict, total=False):
    status: MatchStatus
    tmdb_id: Optional[int]
    search_type: Literal["movie", "tv"]
    """Endpoint used for TMDB search."""
    reasons: List[str]
    """Human-readable trace (blocked / not_found / match path)."""
    candidate_title: Optional[str]
    """TMDB result title or name when matched."""
    failure_detail: str
    """Single-line summary for pytest assertion messages."""


def should_route_tmdb_tv(title: str, edition_title: str = "") -> bool:
    """
    TV routing: legacy ``detect_tmdb_search_type`` plus strong TV title heuristics
    (season / complete series / limited series / S01E01, etc.).
    """
    if detect_tmdb_search_type(title) == "tv":
        return True
    blob = f"{title or ''} {edition_title or ''}".strip().lower()
    if not blob:
        return False
    return strong_tv_title(blob)


_COLLECTION_EXTRA = re.compile(
    r"(?:\b(?:double|triple|quad)\s+feature\b)"
    r"|(?:\b\d+\s*[-–]\s*(?:film|movie)s?\b)"
    r"|(?:\bfilms?\s+from\b)"
    r"|(?:\b(?:five|four|three|two|six|seven|eight|nine|ten)[-\s]+movie\b)"
    r"|(?:\b\d+\s*[-–]\s*pack\b)"
    r"|(?:\bphase\s+(?:one|two|three|four)\b.*\bcollection\b)"
    r"|(?:\bskywalker\b.*\bcollection\b)"
    r"|(?:\buniverse\b.*\bfilm\b)"
    r"|(?:\bshowa\b.*\bcollection\b)",
    re.IGNORECASE,
)


def classify_collection_listing(title: str) -> Dict[str, Any]:
    """
    Conservative classification: multi-title / bundle / set rows should not receive
    a single-film TMDB link from this matcher.

    Returns:
      - kind: ``collection_bundle`` | ``ambiguous`` | ``single_candidate``
      - reasons: list of short codes
      - block_single_film_match: bool
    """
    t = str(title or "").strip()
    reasons: List[str] = []
    if not t:
        return {
            "kind": "ambiguous",
            "reasons": ["empty_title"],
            "block_single_film_match": True,
        }

    if is_collection_or_bundle(t):
        reasons.append("is_collection_or_bundle")

    if _COLLECTION_EXTRA.search(t):
        reasons.append("collection_extra_pattern")

    # Slash-separated multiple titles (often double/triple features)
    if t.count("/") >= 2:
        reasons.append("multi_slash_titles")

    tl = t.lower()
    if "box set" in tl or "boxset" in tl:
        reasons.append("box_set")

    block = len(reasons) > 0
    kind: Literal["collection_bundle", "ambiguous", "single_candidate"]
    if block:
        kind = "collection_bundle"
    else:
        kind = "single_candidate"

    return {
        "kind": kind,
        "reasons": reasons or ["no_collection_signal"],
        "block_single_film_match": block,
    }


def label_film_collection_harness(classifier: Dict[str, Any]) -> FilmCollectionHarnessLabel:
    """
    Map ``classify_collection_listing`` output to harness vocabulary for film QA:

    - ``blocked_collection_candidate`` — treat as multi-title / bundle risk (no naive single-film link).
    - ``bundle_unresolved`` — empty or unusable title row.
    - ``single_film_safe`` — eligible for ordinary single-title TMDB matching heuristics.
    """
    kind = classifier.get("kind")
    if kind == "ambiguous":
        return "bundle_unresolved"
    if classifier.get("block_single_film_match"):
        return "blocked_collection_candidate"
    return "single_film_safe"


def search_tmdb_catalog_isolated(
    title: str,
    tmdb_api_key: str,
    tmdb_api_url: str,
    *,
    source_year: Optional[int] = None,
    edition_title: str = "",
) -> TmdbIsolatedMatchResult:
    """
    Single entry point for harness: routes TV vs movie, blocks obvious collections.

    Safe rejection: collections return ``status=blocked`` with reasons; no TMDB call.
    """
    reasons: List[str] = []
    if is_collection_or_bundle(title):
        reasons.append("blocked:is_collection_or_bundle")
        return {
            "status": "blocked",
            "reasons": reasons,
            "failure_detail": "Title matches collection/bundle heuristics; skip single-entity TMDB link.",
        }

    coll = classify_collection_listing(title)
    if coll["block_single_film_match"]:
        reasons.extend([f"blocked:{r}" for r in coll["reasons"]])
        return {
            "status": "blocked",
            "reasons": reasons,
            "failure_detail": f"Collection classifier: kind={coll['kind']}, reasons={coll['reasons']!r}",
        }

    tv = should_route_tmdb_tv(title, edition_title)
    search_type: Literal["movie", "tv"] = "tv" if tv else "movie"
    endpoint = "tv" if tv else "movie"
    reasons.append(f"route:{search_type}")

    query_variants = (
        build_tv_search_query_variants(title)
        if search_type == "tv"
        else build_search_query_variants(title)
    )

    for query in query_variants:
        params: Dict[str, Any] = {
            "api_key": tmdb_api_key,
            "query": query,
            "include_adult": False,
        }
        response = requests.get(
            f"{tmdb_api_url.rstrip('/')}/search/{endpoint}",
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
        if not results:
            reasons.append(f"no_results:query={query!r}")
            continue

        best = pick_best_tmdb_match(title, source_year, results, search_type)
        if best:
            cand = (
                best.get("title")
                or best.get("name")
                or best.get("original_title")
                or best.get("original_name")
                or ""
            )
            return {
                "status": "matched",
                "tmdb_id": int(best["id"]),
                "search_type": search_type,
                "reasons": reasons + ["pick_best_tmdb_match:accepted"],
                "candidate_title": str(cand) if cand else None,
                "failure_detail": "",
            }
        reasons.append(f"no_safe_candidate:query={query!r}")

    detail = (
        f"No acceptable TMDB {search_type} match after variants; " + "; ".join(reasons[-5:])
    )
    return {
        "status": "not_found",
        "reasons": reasons,
        "search_type": search_type,
        "failure_detail": detail,
    }


def explain_match_rejection(
    source_title: str,
    candidate_title: str,
    *,
    search_type: Literal["movie", "tv"],
) -> str:
    """Verbose reason when ``is_safe_tmdb_match`` would reject (for test diagnostics)."""
    sn = normalize_match_title(source_title)
    cn = normalize_match_title(candidate_title)
    if not sn or not cn:
        return f"empty normalized title (source={sn!r}, candidate={cn!r})"
    if is_safe_tmdb_match(source_title, candidate_title):
        return "would_accept"
    return (
        f"is_safe_tmdb_match=false: normalized source={sn!r} candidate={cn!r} "
        f"(search_type={search_type})"
    )
