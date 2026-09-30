"""
Deterministic checks on drafted descriptions (description drafting, step 3). No LLM calls.

- Every list line must appear in the official source text (normalised containment or fuzzy ≥ 0.9);
  lines that do not are dropped and reported.
- Source URLs must be on the distributor allowlist and not blocklisted.
- Names and numbers in the synopsis must appear in the source text or supplied facts.
- Own summaries must not reuse long runs of words from the reference plot outline.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Iterable, List, Optional

from app.rules.distributor_sources import DistributorSource, is_allowed_url

LINE_MATCH_THRESHOLD = 0.9
COPY_RUN_WORDS = 6

def normalise(text: str) -> str:
    text = text.lower().replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    text = text.replace("–", "-").replace("—", "-").replace("&amp;", "&")
    text = re.sub(r"^[\s\-•*·]+", "", text)
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


@dataclass
class LineCheck:
    kept: List[str] = field(default_factory=list)
    dropped: List[str] = field(default_factory=list)


def check_lines(lines: Iterable[str], source_text: str) -> LineCheck:
    source_norm = normalise(source_text)
    source_lines = [normalise(l) for l in source_text.splitlines() if l.strip()]
    result = LineCheck()
    for line in lines:
        norm = normalise(line)
        if not norm:
            continue
        if norm in source_norm:
            result.kept.append(line.strip())
            continue
        best = 0.0
        for src in source_lines:
            matcher = SequenceMatcher(None, norm, src)
            if matcher.real_quick_ratio() < LINE_MATCH_THRESHOLD or matcher.quick_ratio() < LINE_MATCH_THRESHOLD:
                continue
            best = max(best, matcher.ratio())
            if best >= LINE_MATCH_THRESHOLD:
                break
        (result.kept if best >= LINE_MATCH_THRESHOLD else result.dropped).append(line.strip())
    return result


def _name_tokens(text: str) -> List[str]:
    tokens: List[str] = []
    for sentence in re.split(r"(?<=[.!?:;])\s+|\n+", text):
        words = re.findall(r"[A-Za-zÀ-ÿ][\w'’À-ÿ\-]*", sentence)
        for i, word in enumerate(words):
            if i == 0 or not word[0].isupper() or len(word) < 3:
                continue
            tokens.append(word)
    return tokens


def unsupported_terms(synopsis: str, reference_text: str) -> List[str]:
    """Capitalised names (not sentence-initial) and numbers in the synopsis missing from the reference."""
    ref = normalise(reference_text)
    ref_words = set(ref.split())
    missing: List[str] = []
    for token in _name_tokens(synopsis):
        key = normalise(re.sub(r"['’]s$", "", token))
        if key and key not in ref_words and key not in ref and key not in missing:
            missing.append(key)
    for number in re.findall(r"\d[\d,.]*", synopsis):
        key = normalise(number)
        if key and key not in ref and key not in missing:
            missing.append(key)
    return missing


def copied_runs(text: str, reference: str, run_words: int = COPY_RUN_WORDS) -> List[str]:
    words = normalise(text).split()
    ref = f" {normalise(reference)} "
    hits: List[str] = []
    for i in range(len(words) - run_words + 1):
        chunk = " ".join(words[i : i + run_words])
        if f" {chunk} " in ref and chunk not in hits:
            hits.append(chunk)
    return hits


def synopsis_shape_issues(synopsis: str, *, min_words: int, max_words: int) -> List[str]:
    issues: List[str] = []
    words = len(synopsis.split())
    paragraphs = [p for p in synopsis.splitlines() if p.strip()]
    if words < min_words or words > max_words:
        issues.append(f"synopsis length {words} words (expected {min_words}-{max_words})")
    if len(paragraphs) > 3:
        issues.append(f"synopsis has {len(paragraphs)} paragraphs")
    return issues


def disallowed_urls(urls: Iterable[str], dist: Optional[DistributorSource]) -> List[str]:
    return [u for u in urls if not is_allowed_url(u, dist)]
