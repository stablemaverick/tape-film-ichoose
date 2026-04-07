"""
Harness-only intent category detection (regex / keyword rules).

Does not call production ``parseTapeAgentQueryDeterministic`` — approximates v1 intent buckets for evaluation.
"""

from __future__ import annotations

import re
from typing import Literal

IntentCategory = Literal[
    "director",
    "actor",
    "year_decade",
    "distributor_label",
    "awards_discovery",
    "commerce",
    "title_search",
]


def detect_intent_category(query: str) -> IntentCategory:
    q = (query or "").strip().lower()
    if not q:
        return "title_search"

    if re.search(r"\b(films?\s+by|movies\s+by|directed\s+by)\b", q):
        return "director"

    if re.search(
        r"\b(starring|movies\s+with|films\s+with|featuring|actor|actress)\b", q
    ):
        return "actor"

    if re.search(
        r"\b(19[89]\d|20[0-2]\d)\b"
        r"|\b(19|20)?80s\b|\b(19|20)?90s\b"
        r"|\b2000s\b|\b2010s\b|\bdecade\b",
        q,
    ):
        return "year_decade"

    if re.search(
        r"\b(criterion|arrow\s+video|indicator|shout\s+factory|88\s+films|eureka|label|studio)\b",
        q,
    ):
        return "distributor_label"

    if re.search(
        r"\b(oscar|academy\s+award|bafta|cannes|award\s+winning|best\s+picture)\b",
        q,
    ):
        return "awards_discovery"

    if re.search(
        r"\b(in\s+stock|available|pre[- ]?order|best\s+edition|4k|uhd|ultra\s+hd)\b",
        q,
    ):
        return "commerce"

    return "title_search"
