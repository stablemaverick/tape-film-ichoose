"""
Find and fetch official distributor pages for a barcode (description drafting, step 1).

Order: Shopify store barcode lookup → domain-restricted web search → fetch page text. Pages that
block automated fetches (e.g. Cloudflare) are read through the web-search model instead and marked
``text_origin = "web_search_quote"`` so they are always flagged for review.
"""

from __future__ import annotations

import html as htmllib
import json
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any, List, Optional
from urllib.parse import urlparse

import httpx

from app.clients.openai_client import OpenAIClient, output_json, usage_tokens
from app.rules.distributor_sources import (
    FETCH_MIRRORS,
    INSECURE_TLS_DOMAINS,
    DistributorSource,
    domain_matches,
    is_allowed_url,
    url_domain,
)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}
MAX_PAGE_TEXT_CHARS = 60_000
MIN_QUOTE_CHARS = 200

_SKIP_TAGS = {"script", "style", "noscript", "svg", "template", "iframe", "head", "nav", "footer",
              "header", "form", "select", "button"}
_BLOCK_TAGS = {"p", "div", "li", "ul", "ol", "br", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "td",
               "th", "section", "article", "dd", "dt", "table", "blockquote"}


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: List[str] = []
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag in _SKIP_TAGS:
            self.skip_depth += 1
        elif tag in _BLOCK_TAGS or tag == "br":
            self.parts.append("\n")
        if tag == "li" and not self.skip_depth:
            self.parts.append("- ")

    def handle_startendtag(self, tag: str, attrs: Any) -> None:
        if tag == "br":
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in _SKIP_TAGS:
            self.skip_depth = max(0, self.skip_depth - 1)
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.skip_depth:
            self.parts.append(data)


_FORMAT_WORDS_RE = re.compile(
    r"\b(limited|collector'?s|collectors|edition|steelbook|4k|ultra\s*hd|uhd|blu-?ray|dvd|special|"
    r"deluxe|anniversary|box\s*set|standard)\b|\+",
    re.IGNORECASE,
)


def _loose(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower().replace("'", "")).strip()


def film_title_from_catalog(title: str) -> tuple[str, Optional[str]]:
    """'Pressure (2026) Limited Edition Steelbook 4K Ultra HD' -> ('Pressure', '2026')."""
    year_match = re.search(r"\((\d{4})\)", title)
    stripped = re.sub(r"\([^)]*\)", " ", title)
    stripped = _FORMAT_WORDS_RE.sub(" ", stripped)
    stripped = re.sub(r"\s+", " ", stripped).strip(" -:,")
    return stripped or title, (year_match.group(1) if year_match else None)


_EMBEDDED_STRING_RE = re.compile(r'"((?:[^"\\]|\\.){200,})"')


def embedded_text_strings(html: str) -> List[str]:
    """Long prose strings embedded in script/attribute JSON (client-rendered product copy)."""
    out: List[str] = []
    for match in _EMBEDDED_STRING_RE.finditer(htmllib.unescape(html)):
        raw = match.group(1)
        try:
            value = json.loads(f'"{raw}"')
        except ValueError:
            continue
        if "<" in value and ">" in value:
            value = htmllib.unescape(re.sub(r"<[^>]+>", "\n", value))
        if value.count(" ") < 30 or re.search(r"[{};]\s*(var|function|return|const)\b", value):
            continue
        out.append(value)
    return out


def html_to_text(html: str, max_chars: int = MAX_PAGE_TEXT_CHARS, focus: Optional[str] = None) -> str:
    """Visible page text plus embedded prose strings (only those mentioning ``focus`` when given)."""
    parser = _TextExtractor()
    parser.feed(html)
    visible = "".join(parser.parts)
    focus_key = _loose(focus) if focus else None
    embedded = "\n".join(
        s for s in embedded_text_strings(html) if not focus_key or focus_key in _loose(s)
    )
    lines: List[str] = []
    seen: set[str] = set()
    for raw in (visible + "\n" + embedded).splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if not line or line in ("-",):
            continue
        key = line.lower()
        if len(line) < 120 and key in seen:
            continue
        seen.add(key)
        lines.append(line)
    return "\n".join(lines)[:max_chars]


@dataclass
class SourcePage:
    url: str
    text: str
    page_kind: str  # "edition" | "film" | "unknown"
    text_origin: str  # "fetched" | "shopify_json" | "official_mirror" | "web_search_quote"
    barcode_on_page: bool = False


@dataclass
class SourceSearchResult:
    pages: List[SourcePage] = field(default_factory=list)
    attempts: List[str] = field(default_factory=list)
    tokens_in: int = 0
    tokens_out: int = 0
    search_calls: int = 0


def fetch_url(url: str, *, timeout: float = 30.0) -> httpx.Response:
    verify = not domain_matches(url, INSECURE_TLS_DOMAINS)
    with httpx.Client(headers=BROWSER_HEADERS, follow_redirects=True, timeout=timeout, verify=verify) as client:
        return client.get(url)


def shopify_barcode_lookup(dist: DistributorSource, barcode: str) -> Optional[SourcePage]:
    """Search a Shopify store for the barcode and confirm it against product variant barcodes."""
    if not dist.shopify_base:
        return None
    resp = fetch_url(f"{dist.shopify_base}/search?q={barcode}&type=product")
    if resp.status_code != 200:
        return None
    handles: List[str] = []
    for handle in re.findall(r"/products/([a-z0-9][a-z0-9\-]*)", resp.text):
        if handle not in handles:
            handles.append(handle)
    for handle in handles[:6]:
        pj = fetch_url(f"{dist.shopify_base}/products/{handle}.json")
        if pj.status_code != 200:
            continue
        product = (pj.json() or {}).get("product") or {}
        barcodes = {str(v.get("barcode") or "").strip() for v in product.get("variants") or []}
        if barcode not in barcodes:
            continue
        variant_lines = [
            f"Variant: {v.get('title')} | barcode {v.get('barcode')} | SKU {v.get('sku')}"
            for v in product.get("variants") or []
        ]
        text = "\n".join(
            [f"Product title: {product.get('title')}", *variant_lines, html_to_text(product.get("body_html") or "")]
        )
        return SourcePage(
            url=f"{dist.shopify_base}/products/{handle}",
            text=text,
            page_kind="edition",
            text_origin="shopify_json",
            barcode_on_page=True,
        )
    return None


_SEARCH_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "edition_page_urls": {"type": "array", "items": {"type": "string"}},
        "film_page_urls": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["edition_page_urls", "film_page_urls"],
}


def web_search_candidates(
    client: OpenAIClient,
    model: str,
    dist: DistributorSource,
    *,
    title: str,
    format_label: str,
    barcode: str,
    year: Optional[str],
) -> tuple[List[str], List[str], tuple[int, int]]:
    prompt = (
        f"Find official pages on the distributor's own website for this home-entertainment release.\n"
        f"Title: {title}\nFormat: {format_label}\nBarcode (EAN): {barcode}\n"
        f"Film year: {year or 'unknown'}\nDistributor: {dist.name}\n\n"
        "edition_page_urls: product/release pages for this exact edition (same film, same format and "
        "edition type, e.g. 4K UHD limited edition vs standard). Prefer UK pages.\n"
        "film_page_urls: the distributor's general page for the film (synopsis/cast), if one exists.\n"
        "Only return URLs you actually found on the permitted domains. Return empty lists if none."
    )
    resp = client.responses(
        model=model,
        input=prompt,
        tools=[{"type": "web_search", "filters": {"allowed_domains": list(dist.all_domains)}}],
        tool_choice="required",
        json_schema=_SEARCH_SCHEMA,
        schema_name="official_pages",
        reasoning_effort="low",
    )
    data = output_json(resp)
    edition = [u for u in data.get("edition_page_urls") or [] if is_allowed_url(u, dist)]
    film = [u for u in data.get("film_page_urls") or [] if is_allowed_url(u, dist) and u not in edition]
    return edition[:3], film[:2], usage_tokens(resp)


_QUOTE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "page_found": {"type": "boolean"},
        "verbatim_text": {"type": "string"},
    },
    "required": ["page_found", "verbatim_text"],
}


def web_search_quote(
    client: OpenAIClient, model: str, dist: DistributorSource, url: str
) -> tuple[Optional[str], tuple[int, int]]:
    """Read a page that blocks direct fetches via the web-search tool; returns verbatim page text."""
    resp = client.responses(
        model=model,
        input=(
            f"Open this page: {url}\n"
            "Copy, word for word, the page's synopsis/description, any edition or packaging details, "
            "technical specifications (discs, region, audio, subtitles, aspect ratio) and the full special "
            "features list. Do not summarise, reword or add anything. If you cannot open the page, set "
            "page_found to false."
        ),
        tools=[{"type": "web_search", "filters": {"allowed_domains": list(dist.all_domains)}}],
        tool_choice="required",
        json_schema=_QUOTE_SCHEMA,
        schema_name="page_quote",
        reasoning_effort="low",
    )
    data = output_json(resp)
    text = (data.get("verbatim_text") or "").strip()
    usable = data.get("page_found") and len(text) >= MIN_QUOTE_CHARS
    return (text if usable else None), usage_tokens(resp)


def mirror_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    host = url_domain(url)
    for domain, base in FETCH_MIRRORS.items():
        if host == domain or host.endswith("." + domain):
            path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
            return base.rstrip("/") + path
    return None


def web_search_film_pages(
    client: OpenAIClient, model: str, dist: DistributorSource, *, film_title: str, year: Optional[str]
) -> tuple[List[str], tuple[int, int]]:
    resp = client.responses(
        model=model,
        input=(
            f"Find the distributor's own official page about the film \"{film_title}\""
            f"{f' ({year})' if year else ''} — a film/title page, official film site page or press page "
            f"that includes a synopsis. Distributor: {dist.name}.\n"
            "Return those URLs in film_page_urls and leave edition_page_urls empty. Only return URLs you "
            "actually found on the permitted domains; return empty lists if none."
        ),
        tools=[{"type": "web_search", "filters": {"allowed_domains": list(dist.all_domains)}}],
        tool_choice="required",
        json_schema=_SEARCH_SCHEMA,
        schema_name="official_pages",
        reasoning_effort="low",
    )
    data = output_json(resp)
    urls = [u for u in (data.get("film_page_urls") or []) + (data.get("edition_page_urls") or []) if is_allowed_url(u, dist)]
    return list(dict.fromkeys(urls))[:2], usage_tokens(resp)


def _load_page(
    client: OpenAIClient, model: str, dist: DistributorSource, url: str, kind: str, barcode: str,
    focus: str, result: SourceSearchResult,
) -> Optional[SourcePage]:
    try:
        resp = fetch_url(url)
        status = resp.status_code
    except httpx.HTTPError as exc:
        status, resp = None, None
        result.attempts.append(f"fetch_error {url}: {exc}")
    if resp is not None and status == 200:
        text = html_to_text(resp.text, focus=focus)
        result.attempts.append(f"fetched {url} ({len(text)} chars)")
        return SourcePage(url=str(resp.url), text=text, page_kind=kind, text_origin="fetched",
                          barcode_on_page=barcode in resp.text)
    if status == 404:
        result.attempts.append(f"not_found {url}")
        return None
    mirror = mirror_url(url)
    if mirror:
        try:
            mresp = fetch_url(mirror)
        except httpx.HTTPError as exc:
            mresp = None
            result.attempts.append(f"mirror_error {mirror}: {exc}")
        if mresp is not None and mresp.status_code == 200:
            text = html_to_text(mresp.text, focus=focus)
            result.attempts.append(f"fetch_blocked {url} status={status}; fetched official mirror {mirror}")
            return SourcePage(url=url, text=text, page_kind=kind, text_origin="official_mirror",
                              barcode_on_page=barcode in mresp.text)
    result.attempts.append(f"fetch_blocked {url} status={status}; reading via web search")
    text, (ti, to) = web_search_quote(client, model, dist, url)
    result.tokens_in += ti
    result.tokens_out += to
    result.search_calls += 1
    if not text:
        result.attempts.append(f"web_search_quote_failed {url}")
        return None
    return SourcePage(url=url, text=text[:MAX_PAGE_TEXT_CHARS], page_kind=kind, text_origin="web_search_quote",
                      barcode_on_page=barcode in text)


def slugify_title(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", title.lower().replace("'", "").replace("’", "")).strip("-")


def film_page_candidates(dist: DistributorSource, *, film_title: str, year: Optional[str]) -> List[str]:
    slug = slugify_title(film_title)
    years = [year, str(int(year) - 1)] if year and year.isdigit() else []
    urls: List[str] = []
    for pattern in dist.film_page_patterns:
        if "{year}" in pattern:
            urls += [pattern.format(slug=slug, year=y) for y in years]
        else:
            urls.append(pattern.format(slug=slug))
    return list(dict.fromkeys(urls))


def guess_film_page(
    dist: DistributorSource, *, film_title: str, year: Optional[str], barcode: str, result: SourceSearchResult
) -> Optional[SourcePage]:
    """Try the distributor's known film-page URL patterns; accept only pages that mention the title."""
    wanted = _loose(film_title)
    for url in film_page_candidates(dist, film_title=film_title, year=year):
        try:
            resp = fetch_url(url)
        except httpx.HTTPError:
            continue
        if resp.status_code != 200:
            continue
        text = html_to_text(resp.text, focus=film_title)
        if wanted and wanted in _loose(text):
            result.attempts.append(f"film_page_pattern_match {resp.url}")
            return SourcePage(url=str(resp.url), text=text, page_kind="film", text_origin="fetched",
                              barcode_on_page=barcode in resp.text)
    result.attempts.append("film_page_pattern_no_match")
    return None


def find_official_pages(
    client: OpenAIClient,
    model: str,
    dist: DistributorSource,
    *,
    title: str,
    film_title: str,
    format_label: str,
    barcode: str,
    year: Optional[str],
) -> SourceSearchResult:
    result = SourceSearchResult()

    try:
        page = shopify_barcode_lookup(dist, barcode)
    except httpx.HTTPError as exc:
        page = None
        result.attempts.append(f"shopify_lookup_error: {exc}")
    if page:
        result.attempts.append(f"shopify_barcode_match {page.url}")
        result.pages.append(page)
        return result
    if dist.shopify_base:
        result.attempts.append("shopify_barcode_no_match")

    edition_urls, film_urls, (ti, to) = web_search_candidates(
        client, model, dist, title=title, format_label=format_label, barcode=barcode, year=year
    )
    result.tokens_in += ti
    result.tokens_out += to
    result.search_calls += 1
    result.attempts.append(f"web_search edition={edition_urls} film={film_urls}")

    for url in edition_urls:
        page = _load_page(client, model, dist, url, "edition", barcode, film_title, result)
        if page:
            result.pages.append(page)
            break
    for url in film_urls:
        page = _load_page(client, model, dist, url, "film", barcode, film_title, result)
        if page:
            result.pages.append(page)
            break

    if not result.pages:
        page = guess_film_page(dist, film_title=film_title, year=year, barcode=barcode, result=result)
        if page:
            result.pages.append(page)
            return result
        more_urls, (ti, to) = web_search_film_pages(client, model, dist, film_title=film_title, year=year)
        result.tokens_in += ti
        result.tokens_out += to
        result.search_calls += 1
        result.attempts.append(f"web_search_film film={more_urls}")
        for url in more_urls:
            page = _load_page(client, model, dist, url, "film", barcode, film_title, result)
            if page:
                result.pages.append(page)
                break
    return result
