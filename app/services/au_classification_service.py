"""
Resolve a film's Australian classification: TMDB AU certifications first, then the Australian Classification
Board database (classification.gov.au, via domain-restricted web search — the site blocks direct fetches).
When sources disagree the most restrictive rating is used and the result is flagged.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from app.clients.openai_client import OpenAIClient, output_json, usage_tokens
from app.rules.au_classification import (
    AU_CLASSIFICATION_CHOICES,
    AU_CLASSIFICATION_IMAGES,
    most_restrictive,
    normalise_au_rating,
)
from app.rules.distributor_sources import domain_matches

CLASSIFICATION_DOMAIN = "classification.gov.au"
_TMDB_RELEASE_TYPES = {1: "premiere", 2: "theatrical_limited", 3: "theatrical", 4: "digital", 5: "physical", 6: "tv"}

_BOARD_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "entries": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "title": {"type": "string"},
                    "year": {"type": "string"},
                    "category": {"type": "string"},
                    "classification": {"type": "string"},
                    "date_of_classification": {"type": "string"},
                    "url": {"type": "string"},
                },
                "required": ["title", "year", "category", "classification", "date_of_classification", "url"],
            },
        }
    },
    "required": ["entries"],
}


def _loose(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower().replace("'", "").replace("’", "")).strip()


def tmdb_au_certifications(tmdb: Any, tmdb_id: int) -> List[Dict[str, str]]:
    data = tmdb.get_release_dates(int(tmdb_id)) or {}
    out: List[Dict[str, str]] = []
    for country in data.get("results") or []:
        if country.get("iso_3166_1") != "AU":
            continue
        for rd in country.get("release_dates") or []:
            rating = normalise_au_rating(rd.get("certification"))
            if rating:
                out.append({
                    "rating": rating,
                    "release_type": _TMDB_RELEASE_TYPES.get(rd.get("type"), str(rd.get("type"))),
                    "date": (rd.get("release_date") or "")[:10],
                })
    return out


def board_search(
    client: OpenAIClient, model: str, *, film_title: str, year: Optional[str]
) -> tuple[List[Dict[str, str]], tuple[int, int]]:
    resp = client.responses(
        model=model,
        input=(
            f"Find the Australian Classification Board (National Classification Database) entries for the film "
            f"\"{film_title}\"{f' ({year})' if year else ''}. List each entry you find on classification.gov.au for "
            "this film: title, year, category (e.g. Film - Sale/Hire, Film - Public Exhibition, Film (Advertisement)), "
            "classification (G, PG, M, MA 15+, R 18+, X 18+, RC), date of classification and the entry URL. "
            "Only include entries you actually found; return an empty list if none."
        ),
        tools=[{"type": "web_search", "filters": {"allowed_domains": [CLASSIFICATION_DOMAIN]}}],
        tool_choice="required",
        json_schema=_BOARD_SCHEMA,
        schema_name="classifications",
        reasoning_effort="low",
    )
    wanted = _loose(film_title)
    entries: List[Dict[str, str]] = []
    for entry in output_json(resp).get("entries") or []:
        category = (entry.get("category") or "").lower()
        if not domain_matches(entry.get("url") or "", (CLASSIFICATION_DOMAIN,)):
            continue
        if "film" not in category or any(w in category for w in ("advert", "trailer")):
            continue
        if wanted and wanted not in _loose(entry.get("title") or ""):
            continue
        rating = normalise_au_rating(entry.get("classification"))
        if rating:
            entries.append({**entry, "rating": rating})
    return entries, usage_tokens(resp)


def resolve_au_classification(
    *,
    tmdb: Any,
    client: Optional[OpenAIClient],
    model: str,
    film_title: str,
    year: Optional[str],
    tmdb_id: Optional[int],
) -> Dict[str, Any]:
    """Returns ``{"rating", "choice", "image_gid", "source", "evidence", "review_reasons", tokens...}``."""
    result: Dict[str, Any] = {
        "rating": None, "choice": None, "image_gid": None, "source": None, "evidence": [],
        "review_reasons": [], "tokens_in": 0, "tokens_out": 0, "search_calls": 0,
    }
    candidates: List[str] = []

    if tmdb and tmdb_id:
        certs = tmdb_au_certifications(tmdb, tmdb_id)
        if certs:
            result["source"] = "tmdb"
            result["evidence"] = [{**c, "tmdb_id": tmdb_id} for c in certs]
            candidates = [c["rating"] for c in certs]

    if not candidates and client:
        entries, (ti, to) = board_search(client, model, film_title=film_title, year=year)
        result["tokens_in"] += ti
        result["tokens_out"] += to
        result["search_calls"] += 1
        if entries:
            result["source"] = "classification_board_search"
            result["evidence"] = entries
            candidates = [e["rating"] for e in entries]
            result["review_reasons"].append("classification_from_search")

    if not candidates:
        result["review_reasons"].append("classification_missing")
        return result
    if len(set(candidates)) > 1:
        result["review_reasons"].append("classification_conflict")

    rating = most_restrictive(candidates)
    result["rating"] = rating
    if rating not in AU_CLASSIFICATION_CHOICES:
        result["review_reasons"].append("classification_unmappable")
        return result
    result["choice"] = AU_CLASSIFICATION_CHOICES[rating]
    result["image_gid"] = AU_CLASSIFICATION_IMAGES[rating]
    return result
