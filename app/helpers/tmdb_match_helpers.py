import re
from typing import List

import requests


def build_search_query_variants(title):
    base = normalize_match_title(title)
    if not base:
        return []

    variants = [base]
    # Fallback variant removes common function words that suppliers often drop/add.
    no_articles = re.sub(r"\b(a|an|the|of)\b", " ", base, flags=re.IGNORECASE)
    no_articles = re.sub(r"\s+", " ", no_articles).strip()
    if no_articles and no_articles != base:
        variants.append(no_articles)

    return variants


# TV SKU suffixes (normalized lowercase titles) — strip longest / most specific first.
_TV_PACKAGING_STRIP_END = (
    re.compile(r"\s+\bfinal\s+season\s+part\s+\d+\s*$", re.IGNORECASE),
    re.compile(r"\s+\bseason\s+\d+\s+part\s+\d+\s*$", re.IGNORECASE),
    re.compile(
        r"\s+\bthe\s+complete\s+[a-z][a-z\s\'-]{0,48}\s+collection\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        r"\s+\bthe\s+complete\s+(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth)\s+season\s*$",
        re.IGNORECASE,
    ),
    re.compile(r"\s+\blimited\s+event\s+series\s*$", re.IGNORECASE),
    re.compile(r"\s+\blimited\s+series\s*$", re.IGNORECASE),
    re.compile(r"\s+\bcomplete\s+series\s*$", re.IGNORECASE),
    re.compile(r"\s+\bcomplete\s+season\s*$", re.IGNORECASE),
    re.compile(
        r"\s+\bseason\s+(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s*$",
        re.IGNORECASE,
    ),
    re.compile(r"\s+\bseries\s+\d+\s*$", re.IGNORECASE),
    re.compile(r"\s+\bseason\s+\d+\s*$", re.IGNORECASE),
    re.compile(r"\s+\bpart\s+\d+\s*$", re.IGNORECASE),
)


def _strip_one_tv_packaging_layer(normalized: str) -> str:
    """Remove one trailing TV packaging phrase from a normalized title."""
    t = re.sub(r"\s+", " ", (normalized or "").strip())
    if not t:
        return ""
    for pat in _TV_PACKAGING_STRIP_END:
        t2 = pat.sub("", t)
        if t2 != t:
            return re.sub(r"\s+", " ", t2).strip()
    return t


def _tv_strip_chain_from_normalized(normalized: str) -> List[str]:
    """Iteratively strip TV packaging; returns unique non-empty strings in order tried."""
    chain: List[str] = []
    seen: set[str] = set()
    cur = re.sub(r"\s+", " ", (normalized or "").strip())
    while cur and cur not in seen:
        seen.add(cur)
        chain.append(cur)
        nxt = _strip_one_tv_packaging_layer(cur)
        if not nxt or nxt == cur:
            break
        cur = nxt
    return chain


def build_tv_search_query_variants(title: str) -> List[str]:
    """
    TV-only search query variants for TMDB ``/search/tv``.

    Starts from ``build_search_query_variants``, then adds de-packaged variants
    (season/series/part/complete-series tails) and optional ``<title> <year>`` when
    the supplier title carries a parenthetical edition year (e.g. Battlestar 2004).
    Does not change film / movie search paths.
    """
    base_variants = build_search_query_variants(title)
    if not base_variants:
        return []

    year_hint = extract_year(title)
    out: List[str] = []
    seen: set[str] = set()

    def add(q: str) -> None:
        q = re.sub(r"\s+", " ", (q or "").strip()).lower()
        if len(q) < 2:
            return
        if q not in seen:
            seen.add(q)
            out.append(q)

    for v in base_variants:
        for link in _tv_strip_chain_from_normalized(v):
            add(link)
            if year_hint and str(year_hint) not in link.split():
                add(f"{link} {year_hint}")

    return out


def _is_tv_single_series_home_video_collection(title: str) -> bool:
    """
    True when ``collection`` refers to a single episodic show box set, not a multi-film bundle.

    Used to avoid blocking obvious one-show TV SKUs on the substring ``collection`` alone.
    """
    t = str(title or "").lower()
    if "collection" not in t:
        return False

    # Hard multi-film / franchise bundle cues — keep strict blocking.
    if "movie collection" in t or "film collection" in t:
        return False
    if re.search(r"\b\d+\s*[-–]\s*(?:film|movie)s?\b", t):
        return False
    if re.search(r"\b(?:five|four|three|two|six|seven|eight|nine|ten)\s*[-\s]+movie\b", t):
        return False
    if " phase " in t or "films" in t or "film favorites" in t:
        return False
    if t.count("/") >= 2:
        return False

    if re.search(r"\bcomplete\s+series\b", t) and "movie" not in t and "film" not in t:
        return True

    # "The Complete … Collection" actor / era sets (e.g. David Tennant Doctor Who run).
    m = re.search(
        r"\bthe\s+complete\s+([a-z][a-z\s\'-]{0,40})\s+collection\b",
        t,
        re.IGNORECASE,
    )
    if m:
        if re.search(r"\b(movie|films?|phase)\b", t):
            return False
        span = m.group(1).lower()
        # Multi-film franchise sets often use "The Complete <Franchise> Collection".
        _FRANCHISE_COMPLETE_COLLECTION = (
            "james bond",
            "indiana jones",
            "mission impossible",
            "fast and furious",
            "fast & furious",
            "jurassic",
            "transformers",
            "marvel",
            "middle earth",
            "middle-earth",
            "pirates of the caribbean",
            "harry potter",
            "star wars",
            "x-men",
            "james bond collection",
        )
        if any(fr in span for fr in _FRANCHISE_COMPLETE_COLLECTION):
            return False
        return True

    return False


def normalize_match_title(value):
    if not value:
        return ""

    text = str(value).lower().strip()

    text = re.sub(r"\b4k\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bultra\s*hd\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bultrahd\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\buhd\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdual[\s-]?format\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bblu[\s-]?ray(s)?\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bblu\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bray\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdvd(s)?\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdisc(s)?\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\blimited edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bspecial edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bcollector'?s edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bultimate edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\banniversary edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bremaster(?:ed)?\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\brestor(?:ed|ation)\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsteelbook\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bbox set\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdeluxe edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdeluxe\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bslipcase\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bslipcover\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[-:]\s*(4k|uhd|ultra\s*hd|blu[\s-]?ray|dvd).*$", "", text, flags=re.IGNORECASE)

    text = re.sub(r"\b3d\s*\+\s*2d\b", "", text, flags=re.IGNORECASE)
    # Keep season/series wording in the cleaned title as a TV signal.
    text = re.sub(r"\bthe collection\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bcomplete legacy collection\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bmovie collection\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\baka\b", "", text, flags=re.IGNORECASE)

    # normalise separators / wording
    text = re.sub(r"\s*&\s*", " and ", text, flags=re.IGNORECASE)
    text = re.sub(r"\band\b", " and ", text, flags=re.IGNORECASE)

    # subtitle separator normalization
    text = re.sub(r"\s-\s", " ", text)
    text = re.sub(r"\s:\s", " ", text)

    # common supplier-title normalizations
    text = re.sub(r"\bthe fantastic four\b", "fantastic four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bfantastic 4\b", "fantastic four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bfantastic four first steps\b", "fantastic four first steps", text, flags=re.IGNORECASE)
    text = re.sub(r"\(aka.*?\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\baka\s+.*$", "", text, flags=re.IGNORECASE)

    text = re.sub(r"\bwaynes\b", "wayne s", text, flags=re.IGNORECASE)
    text = re.sub(r"\bbrewsters\b", "brewster s", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsatans\b", "satan s", text, flags=re.IGNORECASE)

    text = re.sub(r"\bii\b", "2", text, flags=re.IGNORECASE)
    text = re.sub(r"\biii\b", "3", text, flags=re.IGNORECASE)

    # common supplier-title normalizations
    text = re.sub(r"\bvolume\s+1\b", "vol 1", text, flags=re.IGNORECASE)
    text = re.sub(r"\bvolume\s+2\b", "vol 2", text, flags=re.IGNORECASE)
    text = re.sub(r"\bvolume\s+3\b", "vol 3", text, flags=re.IGNORECASE)

    text = re.sub(r"\bfantastic 4\b", "fantastic four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bthe fantastic four\b", "fantastic four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bfantastic 4\b", "fantastic four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bfour\b", "four", text, flags=re.IGNORECASE)
    text = re.sub(r"\bmonty pythons\b", "monty python s", text, flags=re.IGNORECASE)
    text = re.sub(r"\bferris buellers\b", "ferris bueller s", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdevils own\b", "devil s own", text, flags=re.IGNORECASE)
    text = re.sub(r"\b310 to yuma\b", "3 10 to yuma", text, flags=re.IGNORECASE)
    text = re.sub(r"\bet\b", "e t", text, flags=re.IGNORECASE)

    # sequel-number forms used by suppliers
    text = re.sub(r"\bjurassic world 2\b", "jurassic world fallen kingdom", text, flags=re.IGNORECASE)
    text = re.sub(r"\bice age 2\b", "ice age the meltdown", text, flags=re.IGNORECASE)
    text = re.sub(r"\bthe smurfs 3\b", "smurfs the lost village", text, flags=re.IGNORECASE)

    # punctuation / brackets
    text = re.sub(r"\(.*?\)|\[.*?\]", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s-\s", " ", text)
    text = re.sub(r"\s:\s", " ", text)

    return text


def title_tokens(value):
    return [t for t in normalize_match_title(value).split() if t]


def extract_year(value):
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    match = re.search(r"\b(18|19|20)\d{2}\b", text)
    if not match:
        return None

    try:
        return int(match.group(0))
    except Exception:
        return None


def is_safe_tmdb_match(source_title, candidate_title):
    source_norm = normalize_match_title(source_title)
    candidate_norm = normalize_match_title(candidate_title)

    if not source_norm or not candidate_norm:
        return False

    # safest path
    if source_norm == candidate_norm:
        return True

    # allow subtitle extensions like "blade runner final cut" vs "blade runner"
    if candidate_norm.startswith(source_norm + " "):
        return True

    if source_norm.startswith(candidate_norm + " "):
        return True

    source_tokens = set(title_tokens(source_title))
    candidate_tokens = set(title_tokens(candidate_title))

    if not source_tokens or not candidate_tokens:
        return False

    overlap = source_tokens & candidate_tokens

    # be strict on short titles
    if len(source_tokens) <= 2:
        return False

    # allow exact token match ignoring order (handles & / and cleanly)
    if source_tokens == candidate_tokens:
        return True

    # allow 1-token tolerance (handles small differences)
    return len(overlap) >= len(source_tokens) - 1


def detect_tmdb_search_type(title):
    t = str(title or "").lower()

    # Single-show TV home video (share logic with collection false-positive reduction).
    if _is_tv_single_series_home_video_collection(title):
        return "tv"

    if re.search(r"\b(?:limited\s+event\s+series|event\s+series)\b", t):
        return "tv"
    if "complete season" in t or "complete series" in t:
        return "tv"
    if re.search(
        r"\b(?:first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth)\s+season\b",
        t,
    ):
        return "tv"

    if (
        "season " in t
        or "seasons " in t
        or "series " in t
        or "episodes " in t
        or "complete series" in t
    ):
        return "tv"

    return "movie"


def is_collection_or_bundle(title):
    t = str(title or "").lower()

    if _is_tv_single_series_home_video_collection(title):
        return False

    return (
        "/" in t
        or "collection" in t
        or "movie collection" in t
        or "complete legacy collection" in t
        or "4 films" in t
        or "3 films" in t
        or "2 movie collection" in t
    )


def pick_best_tmdb_match(source_title, source_year, results, search_type):
    safe_results = []

    for result in results:
        candidate_title = (
            result.get("title")
            or result.get("name")
            or result.get("original_title")
            or result.get("original_name")
            or ""
        )

        if not is_safe_tmdb_match(source_title, candidate_title):
            continue

        candidate_date = result.get("release_date") if search_type == "movie" else result.get("first_air_date")
        candidate_year = extract_year(candidate_date)

        score = 100

        if source_year and candidate_year:
            year_diff = abs(source_year - candidate_year)

            if year_diff == 0:
                score += 40
            elif year_diff == 1:
                score += 20
            elif year_diff <= 3:
                score += 5
            else:
                score -= 50

        safe_results.append((score, result))

    if not safe_results:
        return None

    safe_results.sort(key=lambda x: x[0], reverse=True)
    best_score, best_result = safe_results[0]

    # reject weak ambiguous matches
    if best_score < 80:
        return None

    return best_result


def search_tmdb_movie_safe(title, tmdb_api_key, tmdb_api_url, source_year=None):
    """
    Search TMDB movie or TV with safe collection rejection and shared TV improvements.

    ``title`` should be the **supplier-facing catalog string** (e.g. ``clean_text`` output),
    not a pre-normalized slug. Routing (``detect_tmdb_search_type``), collection checks,
    and TV query variants all run on this string so they stay aligned with callers that
    derive ``search_type`` from the same raw title.
    """
    if is_collection_or_bundle(title):
        return None

    search_type = detect_tmdb_search_type(title)
    endpoint = "tv" if search_type == "tv" else "movie"
    query_variants = (
        build_tv_search_query_variants(title)
        if search_type == "tv"
        else build_search_query_variants(title)
    )
    for query in query_variants:
        params = {
            "api_key": tmdb_api_key,
            "query": query,
            "include_adult": False,
        }
        response = requests.get(f"{tmdb_api_url}/search/{endpoint}", params=params, timeout=30)
        response.raise_for_status()
        results = response.json().get("results", [])
        if not results:
            continue

        best = pick_best_tmdb_match(title, source_year, results, search_type)
        if best:
            return best

    return None
