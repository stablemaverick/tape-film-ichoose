"""
Intent harness failure taxonomy (isolated): intent vs retrieval vs metadata vs ranking.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from tests.agent_retrieval_harness.intent_query_cases import IntentQueryCase


IntentFailureKind = str  # noqa: UP007


def _row_by_id(rows: Sequence[Dict[str, Any]], catalog_id: str) -> Optional[Dict[str, Any]]:
    for r in rows:
        if r.get("id") == catalog_id:
            return dict(r)
    return None


def _metadata_gap_for_intent(expected_intent: str, row: Dict[str, Any]) -> bool:
    """True if fixture row is missing fields that intent-aware ranking would need."""
    if expected_intent == "director":
        return not str(row.get("director") or "").strip()
    if expected_intent == "actor":
        return not str(row.get("lead_actor") or "").strip()
    if expected_intent == "year_decade":
        return row.get("film_year") is None
    if expected_intent == "distributor_label":
        st = (str(row.get("studio") or "") + " " + str(row.get("title") or "")).lower()
        return not any(x in st for x in ("criterion", "arrow", "indicator"))
    if expected_intent == "awards_discovery":
        return not str(row.get("awards_note") or "").strip()
    if expected_intent == "commerce":
        return not str(row.get("availability_status") or "").strip()
    return False


def best_rank_for_acceptable(
    ranked: Sequence[Dict[str, Any]],
    acceptable_ids: Sequence[str],
) -> Tuple[Optional[int], Optional[str]]:
    """Lowest 1-based rank among acceptable catalog ids; id that achieved it."""
    best: Optional[int] = None
    best_id: Optional[str] = None
    id_to_rank = {r["catalog_id"]: i + 1 for i, r in enumerate(ranked)}
    for cid in acceptable_ids:
        rnk = id_to_rank.get(cid)
        if rnk is None:
            continue
        if best is None or rnk < best:
            best = rnk
            best_id = cid
    return best, best_id


def classify_intent_failure(
    case: IntentQueryCase,
    *,
    detected_intent: str,
    ranked: List[Dict[str, Any]],
    rows: List[Dict[str, Any]],
    pass_within_rank: int,
    overall_pass: bool,
) -> str:
    """
    When ``overall_pass`` is False, return a single diagnostic kind.

    Kinds: intent_detection | missing_metadata | weak_filtering | ranking_issue | leave_alone_v1

    ``leave_alone_v1`` is reserved for optional harness cases that mark ambiguous queries; the default
    runner does not emit it unless extended.
    """
    if overall_pass:
        return ""

    expected_intent = case["expected_intent"]
    acceptable = list(case["expected_acceptable_ids"])
    best_r, best_id = best_rank_for_acceptable(ranked, acceptable)

    if detected_intent != expected_intent:
        return "intent_detection"

    if best_id is None:
        return "missing_metadata"

    row = _row_by_id(rows, best_id)
    if row is None:
        return "missing_metadata"

    if _metadata_gap_for_intent(expected_intent, row):
        return "missing_metadata"

    winner = ranked[0] if ranked else None
    win_score = float(winner["score"]) if winner else 0.0
    exp_item = next((x for x in ranked if x["catalog_id"] == best_id), None)
    exp_score = float(exp_item["score"]) if exp_item else 0.0
    gap = win_score - exp_score

    if best_r is not None and best_r > pass_within_rank:
        if gap < 0.06:
            return "weak_filtering"
        return "ranking_issue"

    return "ranking_issue"
