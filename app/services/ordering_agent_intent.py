"""
Ordering Agent V1 — Film release search intent + candidate ranking.

Deterministic only. LLM may enrich intent upstream; this module never invents
stock or price.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Optional


BARCODE_RE = re.compile(r"\b(\d{12,14})\b")
YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")

FORMAT_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b4k\b|\buhd\b|\bultra\s*hd\b", re.I), "4K UHD"),
    (re.compile(r"\bblu[\s-]?ray\b|\bbluray\b", re.I), "Blu-ray"),
    (re.compile(r"\bdvd\b", re.I), "DVD"),
]

LABEL_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bsecond\s+sight\b", re.I), "Second Sight"),
    (re.compile(r"\bcriterion\b", re.I), "Criterion"),
    (re.compile(r"\barrow\b", re.I), "Arrow"),
    (re.compile(r"\bradiance\b", re.I), "Radiance"),
    (re.compile(r"\beureka\b", re.I), "Eureka"),
    (re.compile(r"\b88\s*films\b", re.I), "88 Films"),
    (re.compile(r"\bvinegar\s+syndrome\b", re.I), "Vinegar Syndrome"),
    (re.compile(r"\bseverin\b", re.I), "Severin"),
    (re.compile(r"\bimprint\b", re.I), "Imprint"),
]

NOISE_RE = re.compile(
    r"\b("
    r"do you have|have you got|can you (?:get|order|find)|can i (?:get|order|buy)|"
    r"i'?m (?:looking for|after)|looking for|what versions? of|which versions? of|"
    r"is there|please|thanks|thank you|"
    r"order|buy|available|stock|get me|for me"
    r")\b",
    re.I,
)

ADVERSARIAL_SUPPLIER_RE = re.compile(
    r"\b("
    r"which supplier|what supplier|supplier(?:s)?|lasgo|moovies|wholesaler|"
    r"supplier (?:cost|sku|price|quantity)|raw inventory|ignore your rules|"
    r"ignore your instructions|pretend i'?m an admin|show me the raw|"
    r"cost price|unit cost"
    r")\b",
    re.I,
)

PRICING_ATTACK_RE = re.compile(
    r"\b("
    r"give me \d+%\s*off|10%\s*off|\d+%\s*off|"
    r"supplier cost plus(?:\s+\d+%)?|plus \d+%|"
    r"recalculate(?: using)?(?: today'?s)?(?: exchange rate)?|"
    r"exchange rate|ignore the listed price|charge a\$?\s*\d+|"
    r"what will you sell|sell .{0,40} for|listed price"
    r")\b",
    re.I,
)


@dataclass
class ReleaseSearchIntent:
    title: Optional[str] = None
    year: Optional[int] = None
    format: Optional[str] = None
    edition: Optional[str] = None
    label: Optional[str] = None
    steelbook: Optional[bool] = None
    limited_edition: Optional[bool] = None
    collectors_edition: Optional[bool] = None
    box_set: Optional[bool] = None
    barcode: Optional[str] = None
    query_text: str = ""
    intent: str = "find_release"  # find_release|check_availability|check_price|clarify_release|unknown

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def extract_release_intent(message: str) -> ReleaseSearchIntent:
    raw = (message or "").strip()
    intent = ReleaseSearchIntent(query_text=raw)
    if not raw:
        intent.intent = "unknown"
        return intent

    qn = _norm(raw)

    adversarial = bool(ADVERSARIAL_SUPPLIER_RE.search(qn))
    if adversarial:
        intent.intent = "check_availability"
    elif re.search(r"\b(how much|price|cost|what(?:'| i)?s the price|10%\s*off|exchange rate)\b", qn):
        intent.intent = "check_price"
    elif re.search(r"\b(do you have|available|in stock|can you get|can i get|order)\b", qn):
        intent.intent = "check_availability"
    else:
        intent.intent = "find_release"

    m = BARCODE_RE.search(raw)
    if m:
        intent.barcode = m.group(1)

    ym = YEAR_RE.search(raw)
    if ym:
        intent.year = int(ym.group(1))

    for pat, fmt in FORMAT_PATTERNS:
        if pat.search(raw):
            intent.format = fmt
            break

    for pat, label in LABEL_PATTERNS:
        if pat.search(raw):
            intent.label = label
            break

    if re.search(r"\bsteelbook\b", qn):
        intent.steelbook = True
        intent.edition = intent.edition or "Steelbook"
    if re.search(r"\blimited\s+edition\b", qn):
        intent.limited_edition = True
        intent.edition = intent.edition or "Limited Edition"
    if re.search(r"\bcollector'?s?\s+edition\b", qn):
        intent.collectors_edition = True
        intent.edition = intent.edition or "Collector's Edition"
    if re.search(r"\bbox\s*set\b|\bboxset\b", qn):
        intent.box_set = True
        intent.edition = intent.edition or "Box Set"

    title = raw
    title = ADVERSARIAL_SUPPLIER_RE.sub(" ", title)
    title = PRICING_ATTACK_RE.sub(" ", title)
    title = re.sub(
        r"\b(show me|give me|tell me|how many|how much(?: are you paying)?|"
        r"what(?:'?s| is) your|coming from|from|is this)\b",
        " ",
        title,
        flags=re.I,
    )
    for pat, _ in FORMAT_PATTERNS + LABEL_PATTERNS:
        title = pat.sub(" ", title)
    title = YEAR_RE.sub(" ", title)
    title = BARCODE_RE.sub(" ", title)
    title = re.sub(
        r"\b(steelbook|limited\s+edition|collector'?s?\s+edition|box\s*set|blu[\s-]?ray|bluray|4k|uhd|dvd)\b",
        " ",
        title,
        flags=re.I,
    )
    title = NOISE_RE.sub(" ", title)
    title = re.sub(r"\s+", " ", title).strip(" -")
    title = re.sub(r"^(has|have)\s+", "", title, flags=re.I)
    title = re.sub(r"\bedition of\b", " ", title, flags=re.I)
    title = re.sub(r"\bof\b", " ", title, flags=re.I)
    title = re.sub(r"[?!.,£$%]+", " ", title)
    title = re.sub(r"\s+", " ", title).strip(" -")
    title = re.sub(r"^(the|a|an)\s+", "", title, flags=re.I).strip()
    title = re.sub(r"\s+\bon\b$", "", title, flags=re.I).strip()
    intent.title = title or None
    return intent


def _format_match(candidate_format: Optional[str], wanted: Optional[str]) -> int:
    if not wanted:
        return 0
    cf = _norm(candidate_format or "")
    w = _norm(wanted)
    if not cf:
        return 0
    if w in {"4k uhd", "4k"} and ("4k" in cf or "uhd" in cf):
        return 2
    if w in {"blu-ray", "blu ray"} and ("blu" in cf):
        return 2
    if w == "dvd" and "dvd" in cf:
        return 2
    return -2


def _title_score(cand_title: str, wanted: Optional[str]) -> int:
    if not wanted:
        return 0
    ct = _norm(cand_title)
    wt = _norm(wanted)
    if not wt:
        return 0
    if ct == wt:
        return 8
    if ct.startswith(wt) or wt in ct:
        return 5
    # token overlap
    wt_toks = set(wt.split())
    ct_toks = set(ct.split())
    if not wt_toks:
        return 0
    overlap = len(wt_toks & ct_toks) / len(wt_toks)
    if overlap >= 0.8:
        return 4
    if overlap >= 0.5:
        return 2
    return -1


def rank_release_candidates(
    candidates: list[dict[str, Any]],
    intent: ReleaseSearchIntent,
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Deterministic ranking. Higher score first. Never uses supplier cost."""
    scored: list[tuple[int, dict[str, Any]]] = []
    for c in candidates:
        title = str(c.get("title") or "")
        fmt = str(c.get("format") or "")
        score = 0
        score += _title_score(title, intent.title)
        # Prefer explicit format field; also score format tokens in title.
        fmt_score = _format_match(fmt, intent.format) + _format_match(title, intent.format)
        score += fmt_score

        tn = _norm(title)
        # Prefer titles whose format token matches requested format more tightly
        if intent.format and fmt_score > 0:
            score += 1
        if intent.year is not None:
            if str(intent.year) in title:
                score += 3
            else:
                # soft penalty only when year was explicit
                score -= 1

        if intent.label:
            if _norm(intent.label) in tn:
                score += 4
            else:
                score -= 2

        if intent.steelbook is True:
            if "steelbook" in tn:
                score += 4
            else:
                score -= 3

        if intent.limited_edition is True:
            if "limited" in tn:
                score += 3
            else:
                score -= 2

        if intent.collectors_edition is True:
            if "collector" in tn:
                score += 3
            else:
                score -= 2

        if intent.box_set is True:
            if "box" in tn:
                score += 3
            else:
                score -= 1

        if intent.barcode and c.get("barcode") == intent.barcode:
            score += 20

        scored.append((score, c))

    scored.sort(key=lambda x: (-x[0], _norm(str(x[1].get("title") or ""))))
    out = []
    for score, c in scored[: max(1, min(limit, 10))]:
        row = dict(c)
        row["rank_score"] = score
        out.append(row)
    return out


def dominant_candidate(ranked: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Return single dominant candidate or None if ambiguous."""
    if not ranked:
        return None
    if len(ranked) == 1:
        return ranked[0]
    top = ranked[0]
    second = ranked[1]
    top_s = int(top.get("rank_score") or 0)
    sec_s = int(second.get("rank_score") or 0)
    # Exact barcode always dominates
    if top_s >= 20:
        return top
    # Clear win if meaningfully ahead and positive match
    if top_s >= 4 and top_s - sec_s >= 2:
        return top
    if top_s >= 6 and top_s > sec_s:
        return top
    return None


def customer_choice_label(candidate: dict[str, Any]) -> str:
    parts = [str(candidate.get("title") or "Untitled").strip()]
    fmt = (candidate.get("format") or "").strip()
    if fmt and fmt.lower() not in parts[0].lower():
        parts.append(fmt)
    return " — ".join(p for p in parts if p)
