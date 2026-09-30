"""
Australian film classification → Shopify product metafields.

``custom.classification_description`` is a single-line text metafield restricted to the exact choice strings
below; ``custom.classification`` is a file reference to the matching rating image (production store
MediaImage IDs).
"""

from __future__ import annotations

import re
from typing import Iterable, Optional

AU_CLASSIFICATION_CHOICES: dict[str, str] = {
    "G": "G: Very mild impact",
    "PG": "PG: Mild impact",
    "M": "M: Moderate impact. Not recommended for people under 15",
    "MA 15+": (
        "MA 15+: Strong impact People under 15 must be accompanied by a parent or adult guardian to hire or buy "
        "these films or games or to see these films in a cinema. These films cannot be demonstrated in a public place"
    ),
    "R 18+": (
        "R 18+: People under 18 are not permitted to buy or hire these films or games or to see these films in a "
        "cinema. These films cannot be demonstrated in a public place"
    ),
}

AU_CLASSIFICATION_IMAGES: dict[str, str] = {
    "G": "gid://shopify/MediaImage/34840671158496",
    "PG": "gid://shopify/MediaImage/34840671191264",
    "M": "gid://shopify/MediaImage/34840671256800",
    "MA 15+": "gid://shopify/MediaImage/34840671224032",
    "R 18+": "gid://shopify/MediaImage/34840671289568",
}

RESTRICTIVENESS: tuple[str, ...] = ("G", "PG", "M", "MA 15+", "R 18+", "X 18+", "RC")

_ALIASES = {
    "G": "G", "PG": "PG", "M": "M", "M15+": "M", "MA": "MA 15+", "MA15+": "MA 15+", "R": "R 18+", "R18+": "R 18+",
    "X": "X 18+", "X18+": "X 18+", "RC": "RC",
}


def normalise_au_rating(raw: Optional[str]) -> Optional[str]:
    """'MA15+' / 'ma 15+' → 'MA 15+'; unrated / exempt / CTC values → None."""
    if not raw:
        return None
    key = re.sub(r"\s+", "", raw.upper())
    return _ALIASES.get(key)


def most_restrictive(ratings: Iterable[str]) -> Optional[str]:
    ranked = [r for r in ratings if r in RESTRICTIVENESS]
    return max(ranked, key=RESTRICTIVENESS.index) if ranked else None
