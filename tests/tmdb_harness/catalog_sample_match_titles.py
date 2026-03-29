"""
Fixed 20-title catalog sample for isolated TMDB classification / matching harness.

Split:
- TV / episodic candidates (live TMDB in harness).
- Collection / bundle / ambiguous film-set candidates (classifier + blocked-path checks; no live TMDB unless diagnostics).
"""

from __future__ import annotations

from typing import List, Literal, Tuple

Group = Literal["tv_episodic", "collection_bundle"]

# 14 rows — episodic / TV-style SKUs
TV_EPISODIC_CANDIDATES: Tuple[str, ...] = (
    "Worzel Gummidge: The Combined Harvest Complete Collection",
    "Prison Break Complete Season 3",
    "Father Brown: Series 10",
    "Ghost In The Shell:Sac_2045 Part 2 (Standard Bd)",
    "Cosmos  A Spacetime Odyssey Season One",
    "The Wombles: The Complete Series",
    "Monthly Girls Nozaki-Kun",
    "Fleabag: Series 1",
    "Otaku Elf Blu-Ray",
    "A Sign Of Affection - The Complete Season",
    "Ajin Season 2 Blu-Ray",
    "Deep Water Mini Series (2016) Blu-Ray",
    "Hanasaku Iroha Collection",
    "Shirobako Limited Collectors Edition Blu-Ray",
)

# 6 rows — bundles / collections / ambiguous sets
COLLECTION_BUNDLE_CANDIDATES: Tuple[str, ...] = (
    "Sophia Loren Gold Boxset",
    "Takashi Ishii: The Agel Guts (Collection Limited Edition)",
    "Love. Death & Apocalypse: Three Films By Alex De La Iglesia",
    "Yokai Monsters Collection",
    "Alive: Live From Caracalla & The Private Life Of A Star",
    "Man Between",
)


def all_rows() -> List[Tuple[str, Group]]:
    out: List[Tuple[str, Group]] = [(t, "tv_episodic") for t in TV_EPISODIC_CANDIDATES]
    out.extend((t, "collection_bundle") for t in COLLECTION_BUNDLE_CANDIDATES)
    return out
