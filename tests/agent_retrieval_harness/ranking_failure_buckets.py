"""
Failure bucket taxonomy for ranking workstream (isolated harness only).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


def classify_ranking_failure(
    query: str,
    expected_row: Dict[str, Any],
    rank: Optional[int],
    winner: Optional[Dict[str, Any]],
) -> str:
    """When top-1 fails under v2, assign a diagnostic bucket."""
    if rank == 1:
        return ""
    title_l = (expected_row.get("title") or "").lower()
    q = query.lower()
    win_title = (winner or {}).get("title", "").lower()

    if re.search(r"\bghost\b.*\bshell\b", q) and re.search(r"\bghost\b.*\bshell\b", win_title):
        if ("sac" in q or "2045" in q) and "sac" not in win_title and "2045" not in win_title:
            return "franchise disambiguation"
        if "sac" not in q and "1995" in win_title:
            return "franchise disambiguation"

    if re.search(r"\b(season|series|part|complete)\b", q) or re.search(
        r"\b(season|series|part)\b", title_l
    ):
        return "season / series / part interpretation"

    if re.search(r"[^\w\s']", query) or "_" in title_l:
        return "punctuation / stylization normalization"

    if re.search(r"\b(collection|boxset|box set|anthology)\b", q) or re.search(
        r"\b(collection|boxset)\b", title_l
    ):
        return "collection / boxset intent detection"

    if re.search(
        r"\b(de la|director|by)\b", q
    ) or " by " in title_l:
        return "creator / anthology intent detection"

    return "ranking still too generic"
