"""
Harness-only fuzzy retrieval: reuses ``normalize_match_title``, ``title_tokens``,
``build_search_query_variants`` from ``app.helpers.tmdb_match_helpers``.

Does not import production agent modules (they are stubs). Scoring is for
experiments only — not TMDB thresholds.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from app.helpers.tmdb_match_helpers import (
    build_search_query_variants,
    normalize_match_title,
    title_tokens,
)


def _catalog_blob(row: Dict[str, Any]) -> str:
    t = str(row.get("title") or "")
    e = str(row.get("edition_title") or "")
    return f"{t} {e}".strip()


def _loose_tokens(text: str) -> List[str]:
    """Tokens from normalized text; split underscores so ``sac_2045`` overlaps ``sac`` + ``2045``."""
    n = normalize_match_title(text)
    out: List[str] = []
    for w in n.split():
        if "_" in w:
            out.extend(w.replace("_", " ").split())
        else:
            out.append(w)
    return [t for t in out if t]


def _score_query_against_blob(query_norm: str, catalog_norm: str) -> float:
    if not query_norm or not catalog_norm:
        return 0.0
    sm = SequenceMatcher(None, query_norm, catalog_norm).ratio()
    qt = set(_loose_tokens(query_norm))
    tt = set(_loose_tokens(catalog_norm))
    if not qt:
        return 0.0
    inter = qt & tt
    union = qt | tt
    jacc = len(inter) / len(union) if union else 0.0
    recall = len(inter) / len(qt)
    # Substring boost (agent-style ilike behaviour approximation)
    sub = 0.15 if query_norm in catalog_norm else 0.0
    if catalog_norm in query_norm and len(catalog_norm) >= 6:
        sub = max(sub, 0.1)
    return 0.45 * recall + 0.25 * jacc + 0.30 * sm + sub


def score_row(query: str, row: Dict[str, Any]) -> float:
    blob = _catalog_blob(row)
    cn = normalize_match_title(blob)
    variants = build_search_query_variants(query)
    if not variants:
        variants = [normalize_match_title(query)]
    best = 0.0
    for v in variants:
        qn = normalize_match_title(v)
        if not qn:
            continue
        best = max(best, _score_query_against_blob(qn, cn))
    return best


def rank_catalog_for_query(query: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for row in rows:
        s = score_row(query, row)
        scored.append(
            (
                s,
                {
                    "catalog_id": row["id"],
                    "title": row["title"],
                    "edition_title": row.get("edition_title") or "",
                    "score": round(s, 6),
                    "tmdb_id": row.get("tmdb_id"),
                    "tmdb_match_status": row.get("tmdb_match_status"),
                },
            )
        )
    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored]


def find_rank(expected_id: str, ranked: List[Dict[str, Any]]) -> Optional[int]:
    for i, item in enumerate(ranked, start=1):
        if item["catalog_id"] == expected_id:
            return i
    return None


def classify_failure_bucket(
    query: str,
    expected_row: Dict[str, Any],
    rank: Optional[int],
    top_k: int,
    ranked: List[Dict[str, Any]],
) -> str:
    if rank is not None and rank <= top_k:
        return ""

    title_l = (expected_row.get("title") or "").lower()
    q_l = query.lower()

    if rank is not None and rank > top_k:
        # Refine: very close scores → ranking; else seasonal/collection heuristics
        exp_score = next(
            (x["score"] for x in ranked if x["catalog_id"] == expected_row.get("id")),
            0.0,
        )
        top_score = ranked[0]["score"] if ranked else 0.0
        if exp_score > 0 and top_score - exp_score < 0.06:
            return "ranking issue (item found but too low)"

    if re.search(r"\b(season|series|complete|part)\b", q_l) or re.search(
        r"\b(season|series|complete)\b", title_l
    ):
        return "season / series wording normalization"

    if re.search(r"\b(collection|boxset|box set|films)\b", title_l) or re.search(
        r"\b(collection|boxset|box set)\b", q_l
    ):
        return "collection / boxset / bundle wording mismatch"

    if any(
        x in title_l
        for x in (
            "ghost",
            "ajin",
            "nozaki",
            "shirobako",
            "hanasaku",
            "otaku",
            "yokai",
        )
    ):
        return "anime / transliteration / stylization mismatch"

    if re.search(r"[^\w\s']", query):
        return "punctuation / formatting normalization"

    if rank is not None and rank > top_k:
        return "ranking issue (item found but too low)"

    return "token overlap too weak"


def fixability_hint(bucket: str) -> str:
    m = {
        "punctuation / formatting normalization": "query normalization",
        "season / series wording normalization": "query normalization",
        "anime / transliteration / stylization mismatch": "title alias expansion",
        "collection / boxset / bundle wording mismatch": "query normalization + alias expansion",
        "token overlap too weak": "fuzzy scoring",
        "ranking issue (item found but too low)": "ranking",
    }
    return m.get(bucket, "should be left alone for now")


def failure_notes(
    query: str,
    expected_row: Dict[str, Any],
    bucket: str,
) -> str:
    return (
        f"bucket={bucket}; query={query!r}; expected_title={expected_row.get('title')!r}; "
        f"fixture_notes={expected_row.get('fixture_notes', '')!r}"
    )
