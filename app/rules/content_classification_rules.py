"""
Deterministic content classification for health reporting and TMDB routing.

Returns one of: film | tv | unknown

TV is assigned only when signals are strong. Everything else falls through to
explicit film signals or unknown.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Literal, Optional

from app.helpers.tmdb_match_helpers import detect_tmdb_search_type

ContentType = Literal["film", "tv", "unknown"]


def _s(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _norm_media_type(value: Any) -> str:
    v = _s(value).lower()
    if v in ("tv", "television", "series"):
        return "tv"
    if v in ("film", "movie", "movies"):
        return "film"
    return v


# Strong TV cues in supplier-facing text (category, notes, format line, etc.)
_SUPPLIER_TV_PHRASES = (
    "tv series",
    "television series",
    "television show",
    "bbc television",
    "season collection",
    "series box set",
    "seasons 1-",
    "seasons 1–",
)

_SUPPLIER_TV_REGEX = re.compile(
    r"(?:\bseasons?\s+\d+\s*[-–]\s*\d+)|(?:\bseason\s+\d+\b)",
    re.IGNORECASE,
)

_MOVIE_COLLECTION_NEAR_SERIES = re.compile(r"(movie|film)\s+collection", re.IGNORECASE)


def _complete_series_tv_title(title_lower: str) -> bool:
    """‘Complete series’ is usually TV unless it is clearly a film collection set."""
    if not re.search(r"\bcomplete\s+series\b", title_lower):
        return False
    if _MOVIE_COLLECTION_NEAR_SERIES.search(title_lower):
        return False
    return True


# Strong TV title patterns (conservative: avoid lone “series” / “season” words)
_STRONG_TV_TITLE_PATTERNS = [
    re.compile(r"\bseason\s+\d+\b", re.IGNORECASE),
    re.compile(
        r"\bseason\s+(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bs\d{1,2}\s*e\d{1,2}\b", re.IGNORECASE),  # S01E02
    re.compile(r"\bminiseries\b|\bmini-series\b", re.IGNORECASE),
    re.compile(r"\blimited\s+series\b", re.IGNORECASE),
    re.compile(r"\ball\s+episodes\b", re.IGNORECASE),
    re.compile(r"\bseries\s+\d+\b", re.IGNORECASE),  # “series 1”, “series 2”
    re.compile(r"\bthe\s+complete\s+\w+\s+season\b", re.IGNORECASE),
]

_PHYSICAL_FILM_FORMAT = re.compile(
    r"blu[\s-]?ray|4k|uhd|ultra\s*hd|dvd",
    re.IGNORECASE,
)


def _supplier_blob(row: Dict[str, Any]) -> str:
    """Concatenate supplier-visible metadata. harmonized_* only if present on row (e.g. staging)."""
    parts = [
        _s(row.get("category")),
        _s(row.get("raw_category")),
        _s(row.get("notes")),
        _s(row.get("format")),
        _s(row.get("source_type")),
    ]
    if "harmonized_title" in row:
        parts.append(_s(row.get("harmonized_title")))
    return " ".join(p for p in parts if p).lower()


def _title_for_classification(row: Dict[str, Any]) -> str:
    """Prefer harmonized_title only when the row dict actually includes that key (not on catalog_items)."""
    if "harmonized_title" in row:
        h = _s(row.get("harmonized_title"))
        if h:
            return h
    return _s(row.get("title"))


def supplier_indicates_tv(meta_lower: str) -> bool:
    """True when supplier metadata strongly suggests episodic / TV product."""
    if not meta_lower:
        return False
    for phrase in _SUPPLIER_TV_PHRASES:
        if phrase in meta_lower:
            return True
    return bool(_SUPPLIER_TV_REGEX.search(meta_lower))


def strong_tv_title(title_and_edition_lower: str) -> bool:
    """Strong episodic / TV cues in title + edition combined."""
    if not title_and_edition_lower:
        return False
    if _complete_series_tv_title(title_and_edition_lower):
        return True
    return any(p.search(title_and_edition_lower) for p in _STRONG_TV_TITLE_PATTERNS)


def physical_movie_format_hint(format_value: Any) -> bool:
    """Blu-ray / 4K / DVD style physical movie SKU (weak film signal)."""
    return bool(_PHYSICAL_FILM_FORMAT.search(_s(format_value)))


def classify_content_type(row: Dict[str, Any]) -> ContentType:
    """
    Classify a catalog or staging-shaped row.

    For ``catalog_items`` rows, only ``title`` is used for title heuristics (no harmonized_*).
    If ``harmonized_title`` is present on the dict (e.g. staging_supplier_offers), it is preferred.

    Priority:
      1. Explicit media_type / supplier metadata
      2. Title + edition heuristics (strong TV only)
      3. TMDB routing context (matched rows: search type used at enrichment)
      4. Explicit media_type film from pipeline
      5. Physical format hint -> film
      6. unknown
    """
    mt = _norm_media_type(row.get("media_type"))
    title = _title_for_classification(row)
    edition = _s(row.get("edition_title"))
    title_blob = f"{title} {edition}".lower()
    meta_lower = _supplier_blob(row)

    if mt == "tv":
        return "tv"

    if supplier_indicates_tv(meta_lower):
        return "tv"

    if strong_tv_title(title_blob):
        return "tv"

    tstatus = _s(row.get("tmdb_match_status")).lower()
    if tstatus == "matched":
        st = detect_tmdb_search_type(title)
        if st == "tv":
            return "tv"
        if st == "movie":
            return "film"

    if mt == "film":
        return "film"

    if physical_movie_format_hint(row.get("format")):
        return "film"

    return "unknown"
