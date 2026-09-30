"""
Draft product descriptions for catalogue barcodes from official distributor sources.

Pipeline per barcode: resolve distributor → find official pages → extract (LLM, source text only)
→ verify → fall back (official film page, then own factual summary from TMDB facts) → build HTML.

This module never writes to Shopify or Supabase; see docs/product-description-drafting-plan.md.
"""

from __future__ import annotations

import html as htmllib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.clients.openai_client import OpenAIClient
from app.helpers.text_helpers import clean_text
from app.rules.distributor_sources import domain_matches, resolve_distributor
from app.services.au_classification_service import resolve_au_classification
from app.services.description_extraction_service import (
    _tmdb_exact_match,
    extract_from_sources,
    tmdb_facts,
    write_own_summary,
)
from app.services.description_sources_service import (
    SourcePage,
    film_title_from_catalog,
    find_official_pages,
)
from app.services.description_verification_service import (
    check_lines,
    copied_runs,
    disallowed_urls,
    synopsis_shape_issues,
    unsupported_terms,
)

DEFAULT_MODEL = "gpt-5-mini"
DEFAULT_WORKERS = 4
# gpt-5-mini list prices (USD per 1M tokens) and web search tool calls (USD per call).
PRICE_IN_PER_M = 0.25
PRICE_OUT_PER_M = 2.00
PRICE_PER_SEARCH = 0.01

CATALOG_SELECT = (
    "id,barcode,title,studio,format,media_release_date,film_released,tmdb_id,director,top_cast,genres,"
    "country_of_origin,active"
)

_USABLE_EDITION_MATCHES = {"barcode_confirmed", "same_edition_likely", "other_territory_same_edition"}


@dataclass
class DraftingContext:
    client: OpenAIClient
    model: str
    tmdb: Any = None
    classify: bool = True


def build_description_html(record: Dict[str, Any]) -> str:
    def section(title: str, items: List[str]) -> str:
        if not items:
            return ""
        return f"<h3>{title}</h3><ul>" + "".join(f"<li>{htmllib.escape(x)}</li>" for x in items) + "</ul>"

    paras = "".join(
        f"<p>{htmllib.escape(p.strip())}</p>" for p in (record.get("synopsis") or "").splitlines() if p.strip()
    )
    body = paras + section("Edition Contents", record.get("edition_contents") or [])
    body += section("Format", record.get("technical_format") or [])
    if record.get("special_features"):
        body += section("Special Features", record["special_features"])
    else:
        body += "<h3>Special Features</h3><p>Special features to be confirmed by the distributor.</p>"
    return body


def _split_piped(lines: List[str]) -> List[str]:
    out: List[str] = []
    for line in lines:
        out += [part.strip() for part in line.split(" | ") if part.strip()]
    return out


def _catalog_context(row: Dict[str, Any], film_title: str, year: Optional[str]) -> Dict[str, Any]:
    return {
        "barcode": row.get("barcode"),
        "catalogue_title": row.get("title"),
        "film_title": film_title,
        "film_year": year,
        "format": row.get("format"),
        "label": row.get("studio"),
        "media_release_date": row.get("media_release_date"),
    }


def draft_description(row: Dict[str, Any], ctx: DraftingContext) -> Dict[str, Any]:
    barcode = str(row["barcode"])
    title = clean_text(row.get("title")) or barcode
    film_title, year_in_title = film_title_from_catalog(title)
    year = year_in_title or (str(row.get("film_released") or "")[:4] or None)
    dist = resolve_distributor(row.get("studio"))

    reasons: List[str] = []
    notes: List[str] = []
    tokens_in = tokens_out = search_calls = 0
    pages: List[SourcePage] = []
    attempts: List[str] = []

    record: Dict[str, Any] = {
        "barcode": barcode,
        "catalog_item_id": str(row.get("id")) if row.get("id") else None,
        "product_title": title,
        "film_title": film_title,
        "distributor": dist.name if dist else clean_text(row.get("studio")),
        "distributor_key": dist.key if dist else None,
        "source_urls": [],
        "source_type": "none",
        "edition_match": "no_edition_page",
        "edition_match_confirmed": False,
        "synopsis_source": "none",
        "synopsis": "",
        "edition_contents": [],
        "technical_format": [],
        "special_features": [],
        "features_status": "tbc",
    }

    if not dist:
        reasons.append("distributor_unmapped")
        notes.append(f"Label {row.get('studio')!r} is not in the distributor registry; no official source searched.")
    else:
        search = find_official_pages(
            ctx.client, ctx.model, dist,
            title=title, film_title=film_title, format_label=clean_text(row.get("format")) or "",
            barcode=barcode, year=year,
        )
        pages = search.pages
        attempts = search.attempts
        tokens_in += search.tokens_in
        tokens_out += search.tokens_out
        search_calls += search.search_calls
        if any(p.text_origin == "web_search_quote" for p in pages):
            reasons.append("page_read_via_web_search")

    extraction: Optional[Dict[str, Any]] = None
    if pages:
        extraction, (ti, to) = extract_from_sources(
            ctx.client, ctx.model, catalog=_catalog_context(row, film_title, year), pages=pages
        )
        tokens_in += ti
        tokens_out += to

    edition_page: Optional[SourcePage] = None
    synopsis_pages: List[SourcePage] = []
    if extraction:
        match = extraction.get("edition_match") or "no_edition_page"
        sid = extraction.get("edition_source_id") or ""
        if match in _USABLE_EDITION_MATCHES and sid.startswith("S") and sid[1:].isdigit() and 0 < int(sid[1:]) <= len(pages):
            edition_page = pages[int(sid[1:]) - 1]
        if edition_page:
            dropped: List[str] = []
            for key in ("special_features", "edition_contents", "technical_format"):
                check = check_lines(extraction.get(key) or [], edition_page.text)
                record[key] = _split_piped(check.kept)
                dropped += check.dropped
            if dropped:
                reasons.append("dropped_unverified_lines")
                record["dropped_lines"] = dropped
        if edition_page and edition_page.barcode_on_page:
            match = "barcode_confirmed"
        record["edition_match"] = match
        record["edition_match_confirmed"] = match == "barcode_confirmed"
        if extraction.get("edition_match_reason"):
            notes.append(f"Edition: {extraction['edition_match_reason']}")
        if extraction.get("notes"):
            notes.append(extraction["notes"])

        if extraction.get("synopsis_available") and (extraction.get("synopsis") or "").strip():
            synopsis_pages = [
                pages[int(s[1:]) - 1]
                for s in extraction.get("synopsis_source_ids") or []
                if s.startswith("S") and s[1:].isdigit() and 0 < int(s[1:]) <= len(pages)
            ] or list(pages)
            synopsis = extraction["synopsis"].strip()
            reference = "\n".join(p.text for p in synopsis_pages) + f"\n{title}\n{row.get('studio') or ''}"
            missing = unsupported_terms(synopsis, reference)
            if missing:
                reasons.append("synopsis_unsupported_terms")
                record["synopsis_unsupported_terms"] = missing
            reasons += synopsis_shape_issues(synopsis, min_words=35, max_words=170)
            record["synopsis"] = synopsis
            record["synopsis_source"] = "distributor"

    tmdb_id = row.get("tmdb_id")
    if not tmdb_id and ctx.tmdb:
        tmdb_id = _tmdb_exact_match(ctx.tmdb, film_title, year)

    if ctx.classify:
        cls = resolve_au_classification(
            tmdb=ctx.tmdb, client=ctx.client, model=ctx.model, film_title=film_title, year=year, tmdb_id=tmdb_id
        )
        record["au_classification"] = {k: cls[k] for k in ("rating", "choice", "image_gid", "source", "evidence")}
        reasons += cls["review_reasons"]
        tokens_in += cls["tokens_in"]
        tokens_out += cls["tokens_out"]
        search_calls += cls["search_calls"]

    if record["synopsis_source"] == "none":
        facts = (
            tmdb_facts(ctx.tmdb, tmdb_id, {"film_title": film_title, "year": year}) if ctx.tmdb else None
        )
        if facts:
            outline = facts.get("plot_outline_reference") or ""
            own, (ti, to) = write_own_summary(ctx.client, ctx.model, facts=facts)
            tokens_in += ti
            tokens_out += to
            synopsis = (own.get("synopsis") or "").strip()
            copies = copied_runs(synopsis, outline)
            if copies:
                own, (ti, to) = write_own_summary(ctx.client, ctx.model, facts=facts, avoid_phrases=copies)
                tokens_in += ti
                tokens_out += to
                synopsis = (own.get("synopsis") or "").strip()
                copies = copied_runs(synopsis, outline)
            reference = json.dumps(facts, ensure_ascii=False) + f"\n{title}"
            missing = unsupported_terms(synopsis, reference)
            if missing:
                reasons.append("synopsis_unsupported_terms")
                record["synopsis_unsupported_terms"] = missing
            if copies:
                reasons.append("own_summary_copied_phrases")
                record["own_summary_copied_phrases"] = copies
            reasons += synopsis_shape_issues(synopsis, min_words=50, max_words=130)
            record["synopsis"] = synopsis
            record["synopsis_source"] = "own_summary"
            reasons.append("own_summary")
            notes.append("No official synopsis found; summary written from TMDB facts (director, cast, genres, plot outline).")
        else:
            reasons.append("no_synopsis")
            notes.append("No official synopsis and no TMDB facts available.")

    edition_used = bool(edition_page) and (
        any(record[k] for k in ("special_features", "edition_contents", "technical_format"))
        or (record["synopsis_source"] == "distributor" and edition_page in synopsis_pages)
    )
    used_pages = ([edition_page] if edition_used else []) + [
        p for p in synopsis_pages if p is not edition_page and record["synopsis_source"] == "distributor"
    ]
    record["source_urls"] = [p.url for p in used_pages]
    if edition_used and dist and domain_matches(edition_page.url, dist.domains) and record["edition_match"] in (
        "barcode_confirmed", "same_edition_likely"
    ):
        record["source_type"] = "official_distributor"
    elif used_pages:
        record["source_type"] = "official_fallback"
    if record["edition_match"] == "other_territory_same_edition":
        reasons.append("other_territory_edition")

    record["features_status"] = "announced" if record["special_features"] else "tbc"
    if record["special_features"] and not record["edition_match_confirmed"]:
        reasons.append("edition_not_confirmed")

    bad = disallowed_urls(record["source_urls"], dist)
    if bad:
        reasons.append("disallowed_url")
        record["source_urls"] = [u for u in record["source_urls"] if u not in bad]

    record["notes"] = " ".join(n.strip() for n in notes if n and n.strip())
    record["needs_review"] = bool(reasons)
    record["review_reasons"] = sorted(set(reasons))
    record["text_origins"] = sorted({p.text_origin for p in used_pages})
    record["attempts"] = attempts
    record["tokens_in"] = tokens_in
    record["tokens_out"] = tokens_out
    record["search_calls"] = search_calls
    record["est_cost_usd"] = round(
        tokens_in / 1e6 * PRICE_IN_PER_M + tokens_out / 1e6 * PRICE_OUT_PER_M + search_calls * PRICE_PER_SEARCH, 4
    )
    record["description_html"] = build_description_html(record)
    return record


def fetch_catalog_rows(supabase: Any, barcodes: List[str]) -> Dict[str, Dict[str, Any]]:
    rows = (
        supabase.table("catalog_items").select(CATALOG_SELECT).in_("barcode", barcodes).eq("active", True).execute().data
        or []
    )
    by_barcode: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        by_barcode.setdefault(str(row["barcode"]), row)
    return by_barcode


def run_product_description_drafting(
    *,
    barcodes: List[str],
    supabase: Any,
    model: Optional[str] = None,
    workers: Optional[int] = None,
    tmdb: Any = None,
    classify: bool = True,
) -> List[Dict[str, Any]]:
    model = model or os.getenv("DESCRIPTION_MODEL") or DEFAULT_MODEL
    workers = workers or int(os.getenv("DESCRIPTION_MAX_WORKERS") or DEFAULT_WORKERS)
    ctx = DraftingContext(client=OpenAIClient(), model=model, tmdb=tmdb, classify=classify)
    rows = fetch_catalog_rows(supabase, barcodes)

    def _one(barcode: str) -> Dict[str, Any]:
        row = rows.get(barcode)
        if not row:
            return {"barcode": barcode, "error": "no active catalog row", "needs_review": True}
        try:
            return draft_description(row, ctx)
        except Exception as exc:
            return {"barcode": barcode, "product_title": row.get("title"), "error": str(exc), "needs_review": True}

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        return list(pool.map(_one, barcodes))
