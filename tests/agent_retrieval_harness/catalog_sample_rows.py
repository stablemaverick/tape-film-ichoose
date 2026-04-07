"""
Fixed in-memory catalog rows for agent retrieval harness (not live DB).

Mirrors realistic Moovies-style SKUs: missing TMDB, not_found, TV/anime/collection wording.
"""

from __future__ import annotations

from typing import Any, Dict, List, TypedDict


class CatalogFixtureRow(TypedDict, total=False):
    id: str
    title: str
    edition_title: str
    tmdb_id: None | int
    tmdb_match_status: str
    fixture_notes: str


FIXTURE_ROWS: List[CatalogFixtureRow] = [
    {
        "id": "ghost_1995_film",
        "title": "Ghost In The Shell (1995) Blu-Ray",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "matched",
        "fixture_notes": "distractor: feature film vs SAC_2045 TV",
    },
    {
        "id": "ghost_sac_2045",
        "title": "Ghost In The Shell:Sac_2045 Part 2 (Standard Bd)",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "anime TV; punctuation/colon/underscore in title",
    },
    {
        "id": "ajin_s2",
        "title": "Ajin Season 2 Blu-Ray",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "anime season SKU",
    },
    {
        "id": "father_brown_s10",
        "title": "Father Brown: Series 10",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "matched",
        "fixture_notes": "TV series",
    },
    {
        "id": "prison_break_s3",
        "title": "Prison Break Complete Season 3",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "TV season wording",
    },
    {
        "id": "wombles_complete",
        "title": "The Wombles: The Complete Series",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "matched",
        "fixture_notes": "complete series",
    },
    {
        "id": "fleabag_s1",
        "title": "Fleabag: Series 1",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "matched",
        "fixture_notes": "TV",
    },
    {
        "id": "otaku_elf",
        "title": "Otaku Elf Blu-Ray",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "anime short title",
    },
    {
        "id": "shirobako_le",
        "title": "Shirobako Limited Collectors Edition Blu-Ray",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "anime + edition noise",
    },
    {
        "id": "alex_de_la_iglesia_3films",
        "title": "Love. Death & Apocalypse: Three Films By Alex De La Iglesia",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "multi-film / director name",
    },
    {
        "id": "sophia_loren_box",
        "title": "Sophia Loren Gold Boxset",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "boxset / actor collection",
    },
    {
        "id": "yokai_monsters",
        "title": "Yokai Monsters Collection",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "collection bundle",
    },
    {
        "id": "takashi_ishii",
        "title": "Takashi Ishii: The Agel Guts (Collection Limited Edition)",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "director + collection LE",
    },
    {
        "id": "hanasaku_collection",
        "title": "Hanasaku Iroha Collection",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "anime collection",
    },
    {
        "id": "nozaki_kun",
        "title": "Monthly Girls Nozaki-Kun",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "anime; hyphen/kun",
    },
    {
        "id": "worzel_combined",
        "title": "Worzel Gummidge: The Combined Harvest Complete Collection",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "TV complete collection",
    },
    {
        "id": "deep_water_mini",
        "title": "Deep Water Mini Series (2016) Blu-Ray",
        "edition_title": "",
        "tmdb_id": None,
        "tmdb_match_status": "not_found",
        "fixture_notes": "mini series + year",
    },
]


def rows_by_id() -> Dict[str, CatalogFixtureRow]:
    return {r["id"]: r for r in FIXTURE_ROWS}
