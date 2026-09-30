"""
LLM extraction of synopsis / edition contents / format / special features from official page text
(description drafting, step 2). The model only sees the fetched official text; lists are copied,
not reworded, and are checked afterwards by ``description_verification_service``.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from app.clients.openai_client import OpenAIClient, output_json, usage_tokens
from app.services.description_sources_service import SourcePage

EDITION_MATCH_VALUES = [
    "barcode_confirmed",
    "same_edition_likely",
    "other_territory_same_edition",
    "different_edition",
    "no_edition_page",
]

_EXTRACT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "edition_source_id": {"type": "string"},
        "edition_match": {"type": "string", "enum": EDITION_MATCH_VALUES},
        "edition_match_reason": {"type": "string"},
        "synopsis_available": {"type": "boolean"},
        "synopsis_source_ids": {"type": "array", "items": {"type": "string"}},
        "synopsis": {"type": "string"},
        "edition_contents": {"type": "array", "items": {"type": "string"}},
        "technical_format": {"type": "array", "items": {"type": "string"}},
        "special_features": {"type": "array", "items": {"type": "string"}},
        "notes": {"type": "string"},
    },
    "required": [
        "edition_source_id", "edition_match", "edition_match_reason", "synopsis_available",
        "synopsis_source_ids", "synopsis", "edition_contents", "technical_format", "special_features", "notes",
    ],
}

EXTRACT_INSTRUCTIONS = """You prepare UK shop product descriptions for films on physical media.
You are given catalogue data for one product and text from official distributor/studio pages (S1, S2 ...).

Hard rules:
- Use ONLY the supplied source text. Do not use outside knowledge. Never invent or infer facts.
- Ignore anything on the page about other products (recommendations, "frequently bought together", menus).
- Ignore prices, pre-order notices, delivery/returns text and calls to action.

edition_match — does an edition page describe THIS product (same film, same format, same edition type)?
- barcode_confirmed: the page shows the product's barcode.
- same_edition_likely: same film, format and edition type (e.g. 4K UHD limited edition), no barcode shown.
- other_territory_same_edition: same edition but clearly for another territory (e.g. US UPC / US release).
- different_edition: page is for a different format or edition (e.g. Blu-ray page for a 4K product,
  standard vs limited when the catalogue says limited).
- no_edition_page: no source is an edition/product page (film pages only).
edition_source_id: the S-id of the edition page, or "" if none.

synopsis: 2 short paragraphs separated by a blank line, 90-140 words, British English spelling,
spoiler-free. Base it on the distributor's own synopsis text, lightly edited (trim marketing, calls to
action, prices and dates). Write film titles in normal title case, not capitals. If the source synopsis
is short, write a shorter synopsis (one paragraph is fine) — never pad with themes, context or
historical detail that the source does not state.
Every name, place, number and claim must appear in the sources. If no source contains a synopsis
or plot description, set synopsis_available=false and synopsis="".

Lists — copy lines exactly as written in the source (you may drop bullet characters only). Do not
reword, merge, split, translate or add lines. Only fill lists from the edition page, and only when
edition_match is barcode_confirmed, same_edition_likely or other_territory_same_edition; otherwise
leave all three lists empty.
- special_features: the COMPLETE list under the page's special features / extras / contents heading,
  every line in page order, including restoration, presentation, audio, subtitle, booklet and
  packaging lines if the page lists them there. Where the page groups features by disc, keep the
  disc lines' items in order (omit the disc heading lines themselves).
- edition_contents: packaging and physical contents lines (limited edition numbering, steelbook,
  booklet, posters, art cards, slipcase), copied from the page. These may repeat special_features lines.
- technical_format: technical specification lines only — discs/format, presentation/resolution, HDR,
  audio, subtitles, region, aspect ratio, running time, certificate — copied from the page. These may
  repeat special_features lines. For label/value spec tables write "Label: value" using the page's
  own words. Never include barcode/EAN, SKU/catalogue number, price, release date, release year,
  director, cast, brand or country lines.

notes: brief plain-English notes on anything uncertain (edition doubts, conflicting details)."""


def extract_from_sources(
    client: OpenAIClient,
    model: str,
    *,
    catalog: Dict[str, Any],
    pages: List[SourcePage],
) -> tuple[Dict[str, Any], tuple[int, int]]:
    sources = [
        {"id": f"S{i}", "url": p.url, "kind": p.page_kind, "barcode_on_page": p.barcode_on_page, "text": p.text}
        for i, p in enumerate(pages, 1)
    ]
    payload = {"catalogue_product": catalog, "sources": sources}
    resp = client.responses(
        model=model,
        instructions=EXTRACT_INSTRUCTIONS,
        input=json.dumps(payload, ensure_ascii=False),
        json_schema=_EXTRACT_SCHEMA,
        schema_name="product_description",
        reasoning_effort="low",
    )
    return output_json(resp), usage_tokens(resp)


_SUMMARY_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {"synopsis": {"type": "string"}, "facts_used": {"type": "array", "items": {"type": "string"}}},
    "required": ["synopsis", "facts_used"],
}

SUMMARY_INSTRUCTIONS = """Write a short, original, spoiler-free product summary for a film on physical media,
for a UK shop. British English. 60-110 words, 1-2 paragraphs.

Open with the story set-up (who, where, what they want), naming the lead characters with their actors
in brackets, then a closing sentence naming the director and year. Do not list runtime or genres.

Use ONLY the facts supplied (title, year, director, cast and character names, genres, countries,
plot outline). Use names, places and countries exactly as written in the facts — do not convert
them into adjectives or synonyms (e.g. do not turn "France" into "French" or "Brittany" into "Breton").
Do not add any other facts, awards, trivia or opinions presented as fact. The plot outline is
reference material written by someone else: express its facts in your own words and do not reuse its
phrasing (no run of 5+ consecutive words from it). List the facts you used in facts_used."""


def write_own_summary(
    client: OpenAIClient, model: str, *, facts: Dict[str, Any], avoid_phrases: Optional[List[str]] = None
) -> tuple[Dict[str, Any], tuple[int, int]]:
    payload: Dict[str, Any] = {"facts": facts}
    if avoid_phrases:
        payload["phrases_you_must_not_use"] = avoid_phrases
    resp = client.responses(
        model=model,
        instructions=SUMMARY_INSTRUCTIONS,
        input=json.dumps(payload, ensure_ascii=False),
        json_schema=_SUMMARY_SCHEMA,
        schema_name="own_summary",
        reasoning_effort="low",
    )
    return output_json(resp), usage_tokens(resp)


def _tmdb_exact_match(tmdb: Any, film_title: str, year: Optional[str]) -> Optional[int]:
    def key(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", value.lower().replace("'", ""))

    wanted = key(film_title)
    if not wanted:
        return None
    matches = [
        r for r in tmdb.search(film_title) or []
        if wanted in (key(r.get("title") or ""), key(r.get("original_title") or ""))
        and (not year or (r.get("release_date") or "").startswith(year))
    ]
    if len(matches) > 1:
        # TMDB keeps undated stub records (announced remakes etc.) alongside the released film.
        matches = [r for r in matches if r.get("release_date")]
    return int(matches[0]["id"]) if len(matches) == 1 else None


def tmdb_facts(tmdb: Any, tmdb_id: Optional[int], catalog: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Factual inputs for an own summary from TMDB details + credits (never copied verbatim).

    Without a catalogue ``tmdb_id`` the film is looked up by title, accepting only an exact title
    match (and year when known).
    """
    if not tmdb:
        return None
    if not tmdb_id:
        tmdb_id = _tmdb_exact_match(tmdb, catalog.get("film_title") or "", catalog.get("year"))
        if not tmdb_id:
            return None
    pair = tmdb.get_details_and_credits(int(tmdb_id))
    if not pair:
        return None
    details, credits = pair
    directors = [c["name"] for c in credits.get("crew") or [] if c.get("job") == "Director"]
    cast = [
        {"actor": c.get("name"), "character": c.get("character")}
        for c in (credits.get("cast") or [])[:6]
    ]
    return {
        "title": details.get("title") or catalog.get("film_title"),
        "year": (details.get("release_date") or "")[:4] or None,
        "directors": directors,
        "cast": cast,
        "genres": [g.get("name") for g in details.get("genres") or []],
        "countries": [c.get("name") for c in details.get("production_countries") or []],
        "runtime_minutes": details.get("runtime"),
        "plot_outline_reference": details.get("overview"),
    }
