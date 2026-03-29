"""
Moovies film bundle / box-set harness titles.

``STRONG_BUNDLE_TITLES``: explicit collection / box-set / multi-film / slash cues — expected to
block naive single-film TMDB linking under current isolated classifier.

``PLAIN_TRILOGY_PROBES``: marginal SKUs with only “Trilogy” (no collection / box set / N-film wording).
These are **expected** to classify as ``single_film_safe`` today — documented false-negative gap.
"""

from __future__ import annotations

from typing import List, Literal, Tuple

Tag = Literal["strong_bundle", "plain_trilogy_probe"]

# 17 + 3 = 20 titles total for the harness.

# 17 titles with explicit bundle cues (should block).
STRONG_BUNDLE_TITLES: Tuple[str, ...] = (
    "The Lord of the Rings 3-Film Collection (Blu-ray)",
    "The Hobbit Trilogy Box Set",
    "Christopher Nolan 4K Ultra HD Collection",
    "Quentin Tarantino XX 10-Film Collection",
    "Batman: The Dark Knight Trilogy Collection",
    "Spider-Man Legacy Collection (3 Films)",
    "Back to the Future Trilogy Box Set",
    "Die Hard 5-Movie Collection",
    "Rocky Heavyweight Collection",
    "John Wick Chapters 1-3 Box Set",
    "The Cornetto Trilogy: Shaun of the Dead / Hot Fuzz / The World's End",
    "Before Sunrise Trilogy Box Set",
    "Mad Max Collection",
    "Planet of the Apes Trilogy Collection",
    "The Mummy Trilogy 3-Film Collection (1999-2008)",
    "Ocean's Trilogy Collection",
    "Pitch Perfect 3-Movie Collection",
)

# 3 marginal probes: plain “Trilogy” without extra cues (heuristic gap).
PLAIN_TRILOGY_PROBES: Tuple[str, ...] = (
    "The Lord of the Rings Trilogy",
    "Before Sunrise Trilogy",
    "Mad Max Trilogy",
)


def all_harness_rows() -> List[Tuple[str, Tag]]:
    rows: List[Tuple[str, Tag]] = [(t, "strong_bundle") for t in STRONG_BUNDLE_TITLES]
    rows.extend((t, "plain_trilogy_probe") for t in PLAIN_TRILOGY_PROBES)
    return rows
