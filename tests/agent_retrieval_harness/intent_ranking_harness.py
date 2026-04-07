"""
Intent-aware scoring on top of lexical baseline (isolated harness).

Combines ``media_ranking_v2.score_baseline_with_breakdown`` with metadata boosts
from fixture director / studio / year / availability / awards fields.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from tests.agent_retrieval_harness.intent_detection_harness import (
    IntentCategory,
    detect_intent_category,
)
from tests.agent_retrieval_harness.media_ranking_v2 import score_baseline_with_breakdown


def _extract_year_from_query(q: str) -> List[int]:
    out: List[int] = []
    for m in re.finditer(r"\b(19|20)\d{2}\b", q):
        try:
            out.append(int(m.group(0)))
        except ValueError:
            pass
    return out


def _decade_hint(q: str) -> str | None:
    ql = q.lower()
    if "80s" in ql or "1980" in ql:
        return "80s"
    if "90s" in ql or "1990" in ql:
        return "90s"
    if "2000s" in ql or "2000" in ql and "s" in ql:
        return "2000s"
    if "2010s" in ql:
        return "2010s"
    return None


def _metadata_boost(
    query: str,
    row: Dict[str, Any],
    intent: IntentCategory,
) -> Tuple[float, Dict[str, float]]:
    """Isolated boosts; values are small relative to ~1.0 lexical base."""
    parts: Dict[str, float] = {}
    q = query.lower()
    director = str(row.get("director") or "").lower()
    studio = str(row.get("studio") or "").lower()
    title = str(row.get("title") or "").lower()
    genre = str(row.get("genre_tags") or "").lower()
    awards = str(row.get("awards_note") or "").lower()
    year = row.get("film_year")
    av = str(row.get("availability_status") or "").lower()
    rb = str(row.get("ranking_bucket") or "").lower()

    total = 0.0

    if intent == "director" and director:
        for token in re.findall(r"[a-z]+", director):
            if len(token) > 3 and token in q:
                parts["director_name_match"] = 0.22
                total += 0.22
                break

    if intent == "actor":
        la = str(row.get("lead_actor") or "").lower()
        if la:
            for tok in la.split():
                if len(tok) > 2 and tok in q:
                    parts["lead_actor_match"] = 0.24
                    total += 0.24
                    break

    if intent == "year_decade":
        years = _extract_year_from_query(q)
        if isinstance(year, int) and years and year == years[0]:
            parts["year_exact"] = 0.25
            total += 0.25
        dh = _decade_hint(q)
        if dh == "80s" and isinstance(year, int) and 1980 <= year <= 1989:
            parts["decade_80s"] = 0.2
            total += 0.2
        if "horror" in q and "horror" in genre:
            parts["genre_horror"] = 0.12
            total += 0.12

    if intent == "distributor_label":
        for lab, key in (
            ("criterion", "criterion"),
            ("arrow video", "arrow"),
            ("arrow", "arrow"),
            ("indicator", "indicator"),
        ):
            if lab in q and lab in studio + " " + title:
                parts[f"label_{key}"] = 0.28
                total += 0.28
                break

    if intent == "awards_discovery":
        if "oscar" in q and "oscar" in awards:
            parts["awards_oscar"] = 0.24
            total += 0.24
        if "best picture" in q and "best picture" in awards:
            parts["awards_best_picture"] = 0.15
            total += 0.15

    if intent == "commerce":
        if "in stock" in q or "available" in q:
            if av == "store_stock" or "store" in rb:
                parts["commerce_in_stock"] = 0.2
                total += 0.2
        if "preorder" in q or "pre-order" in q:
            if "preorder" in av or "preorder" in rb:
                parts["commerce_preorder"] = 0.2
                total += 0.2
        if "4k" in q or "ultra hd" in q or "uhd" in q:
            if "4k" in title or "uhd" in title or "ultra" in title:
                parts["commerce_format_4k"] = 0.15
                total += 0.15
        if "best edition" in q:
            if "steelbook" in title or "4k" in title:
                parts["commerce_best_edition"] = 0.12
                total += 0.12

    return total, parts


def score_intent_aware(
    query: str,
    row: Dict[str, Any],
    intent_override: str | None = None,
) -> Tuple[float, Dict[str, Any]]:
    intent: IntentCategory = (
        intent_override if intent_override else detect_intent_category(query)
    )
    base_score, base_wrap = score_baseline_with_breakdown(query, row)
    meta_total, meta_parts = _metadata_boost(query, row, intent)
    bd = base_wrap.get("score_breakdown") or {}
    base_lex = float(bd.get("base_lexical", base_score))
    total = base_lex + meta_total
    breakdown = {
        **base_wrap["score_breakdown"],
        "intent_category": intent,
        "metadata_boost_total": round(meta_total, 6),
        **{f"meta_{k}": round(v, 6) for k, v in meta_parts.items()},
        "intent_total_score": round(total, 6),
    }
    return total, {"score_breakdown": breakdown, "total_score": round(total, 6)}


def rank_intent_aware(
    query: str,
    rows: List[Dict[str, Any]],
    *,
    intent_override: str | None = None,
) -> List[Dict[str, Any]]:
    detected = intent_override or detect_intent_category(query)
    out: List[Dict[str, Any]] = []
    for row in rows:
        total, wrap = score_intent_aware(query, row, intent_override=detected)
        out.append(
            {
                "catalog_id": row["id"],
                "title": row["title"],
                "score": round(total, 6),
                "score_breakdown": wrap["score_breakdown"],
                "director": row.get("director"),
                "studio": row.get("studio"),
                "film_year": row.get("film_year"),
            }
        )
    out.sort(key=lambda x: x["score"], reverse=True)
    return out
