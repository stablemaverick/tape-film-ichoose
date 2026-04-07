"""
Isolated media retrieval ranking / disambiguation (Tape Agent v1 workstream).

Not wired to production. Builds on ``retrieval_engine`` baseline lexical scoring and adds
explainable boosts/penalties for evaluation only.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, MutableMapping, Optional, Set, Tuple

from app.helpers.tmdb_match_helpers import build_search_query_variants, normalize_match_title

from tests.agent_retrieval_harness.retrieval_engine import _catalog_blob, _loose_tokens

# --- corpus stats for rare-token boosts (computed once per run) ---


def _df_by_token(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    df: Dict[str, int] = {}
    for row in rows:
        seen: Set[str] = set()
        blob = _catalog_blob(row)
        for tok in _loose_tokens(normalize_match_title(blob)):
            if len(tok) < 3:
                continue
            if tok not in seen:
                seen.add(tok)
                df[tok] = df.get(tok, 0) + 1
    return df


def _is_rare_token(tok: str, df: Dict[str, int], max_df: int = 2) -> bool:
    return len(tok) >= 4 and df.get(tok, 999) <= max_df


# --- baseline decomposition (matches retrieval_engine weights) ---


def _base_lexical_components(query_norm: str, catalog_norm: str) -> Dict[str, float]:
    if not query_norm or not catalog_norm:
        return {
            "token_recall": 0.0,
            "token_jaccard": 0.0,
            "sequence_ratio": 0.0,
            "substring_boost": 0.0,
            "base_lexical": 0.0,
        }
    sm = SequenceMatcher(None, query_norm, catalog_norm).ratio()
    qt = set(_loose_tokens(query_norm))
    tt = set(_loose_tokens(catalog_norm))
    if not qt:
        return {
            "token_recall": 0.0,
            "token_jaccard": 0.0,
            "sequence_ratio": sm,
            "substring_boost": 0.0,
            "base_lexical": 0.0,
        }
    inter = qt & tt
    union = qt | tt
    jacc = len(inter) / len(union) if union else 0.0
    recall = len(inter) / len(qt)
    sub = 0.15 if query_norm in catalog_norm else 0.0
    if catalog_norm in query_norm and len(catalog_norm) >= 6:
        sub = max(sub, 0.1)
    base = 0.45 * recall + 0.25 * jacc + 0.30 * sm + sub
    return {
        "token_recall": round(recall, 6),
        "token_jaccard": round(jacc, 6),
        "sequence_ratio": round(sm, 6),
        "substring_boost": round(sub, 6),
        "base_lexical": round(base, 6),
    }


def score_baseline_with_breakdown(query: str, row: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
    blob = _catalog_blob(row)
    cn = normalize_match_title(blob)
    variants = build_search_query_variants(query)
    if not variants:
        variants = [normalize_match_title(query)]
    best_total = -1.0
    best_detail: Dict[str, Any] = {}
    best_qn = ""
    for v in variants:
        qn = normalize_match_title(v)
        if not qn:
            continue
        comp = _base_lexical_components(qn, cn)
        t = comp["base_lexical"]
        if t > best_total:
            best_total = t
            best_qn = qn
            best_detail = {
                "query_variant_used": v,
                "query_normalized": qn,
                "catalog_normalized": cn,
                **comp,
            }
    if best_total < 0:
        best_total = 0.0
        best_detail = {"base_lexical": 0.0, "query_normalized": "", "catalog_normalized": cn}
    return best_total, {"score_breakdown": best_detail, "total_score": round(best_total, 6)}


# --- v2: isolated boosts / penalties ---


def _query_subtype_cues(qn: str) -> Dict[str, bool]:
    t = qn.lower()
    return {
        "has_sac": bool(re.search(r"\bsac\b|sac_2045|sac2045", t)),
        "has_2045": "2045" in t,
        "has_part": bool(re.search(r"\bpart\s*2\b|\bpart\s+2\b|\bii\b", t)),
        "has_season": bool(re.search(r"\bseason\b", t)),
        "has_series": bool(re.search(r"\bseries\b", t)),
        "has_complete_series": "complete" in t and "series" in t,
        "has_collection_intent": bool(
            re.search(r"\b(collection|boxset|box set|anthology|films by)\b", t)
        ),
        "has_year_1995": "1995" in t,
    }


def _row_cues(blob: str, row: Dict[str, Any]) -> Dict[str, bool]:
    n = normalize_match_title(blob)
    tl = n.lower()
    raw = f"{row.get('title','')} {row.get('edition_title','')}".lower()
    return {
        "has_sac": "sac" in tl or "sac_2045" in raw.replace(" ", "_"),
        "has_2045": "2045" in tl,
        "has_part2": bool(re.search(r"\bpart\s*2\b|\bpart\s+2\b", raw)),
        "has_season": "season" in tl,
        "has_series": "series" in tl,
        "has_complete_series": "complete" in tl and "series" in tl,
        "has_collection": bool(re.search(r"\b(collection|boxset|box set)\b", raw)),
        "has_paren_1995": bool(re.search(r"\(1995\)|\b1995\b", row.get("title") or "")),
        "is_anthology_director": " by " in raw or "films by" in raw,
    }


def _creator_tokens_overlap(query_norm: str, catalog_norm: str) -> float:
    """Boost when multi-token creator name overlaps (e.g. alex / iglesia / de la)."""
    q = set(_loose_tokens(query_norm))
    c = set(_loose_tokens(catalog_norm))
    if len(q) < 2:
        return 0.0
    inter = q & c
    if len(inter) < 2:
        return 0.0
    # penalize generic tokens
    noise = {"the", "and", "of", "de", "la", "el", "by", "films"}
    meaningful = {x for x in inter if x not in noise and len(x) > 2}
    if len(meaningful) >= 2:
        return 0.12
    return 0.05 * len(meaningful)


def _anime_stylization_boost(qn: str, catalog_norm: str) -> float:
    """Small boost when stylized tokens align (underscore codes, hyphenated kun)."""
    boost = 0.0
    if "nozaki" in qn and "nozaki" in catalog_norm:
        boost += 0.04
    if "2045" in qn and "2045" in catalog_norm:
        boost += 0.05
    if "sac" in qn.lower() and ("sac" in catalog_norm.lower() or "sac_2045" in catalog_norm.lower()):
        boost += 0.06
    return min(boost, 0.15)


def score_v2_with_breakdown(
    query: str,
    row: Dict[str, Any],
    *,
    df: Dict[str, int],
) -> Tuple[float, Dict[str, Any]]:
    base_score, base_wrap = score_baseline_with_breakdown(query, row)
    bd: Dict[str, Any] = dict(base_wrap["score_breakdown"])
    blob = _catalog_blob(row)
    qn = bd.get("query_normalized") or normalize_match_title(query)
    cn = bd.get("catalog_normalized") or normalize_match_title(blob)

    qc = _query_subtype_cues(qn)
    rc = _row_cues(blob, row)

    rare_boost = 0.0
    qt = set(t for t in _loose_tokens(qn) if _is_rare_token(t, df))
    ct = set(_loose_tokens(cn))
    for t in qt & ct:
        if _is_rare_token(t, df):
            rare_boost += 0.055

    season_boost = 0.0
    if qc["has_season"] and rc["has_season"]:
        season_boost += 0.08
    if qc["has_series"] and rc["has_series"]:
        season_boost += 0.05
    if qc["has_complete_series"] and rc["has_complete_series"]:
        season_boost += 0.1
    if qc["has_part"] and rc["has_part2"]:
        season_boost += 0.12

    collection_boost = 0.0
    if qc["has_collection_intent"] and rc["has_collection"]:
        collection_boost += 0.09

    creator_boost = _creator_tokens_overlap(qn, cn)

    anime_boost = _anime_stylization_boost(qn, cn)

    franchise_penalty = 0.0
    gits_franchise_row = "ghost" in cn and "shell" in cn
    if gits_franchise_row and (qc["has_sac"] or qc["has_2045"]):
        if not (rc["has_sac"] or rc["has_2045"]):
            franchise_penalty -= 0.28

    year_film_penalty = 0.0
    if gits_franchise_row and rc["has_paren_1995"] and not qc["has_year_1995"]:
        # Prefer not to surface year-marked feature film when user omits year (disambiguation)
        year_film_penalty -= 0.22

    components: MutableMapping[str, Any] = {
        "base_lexical": bd.get("base_lexical", base_score),
        "rare_token_boost": round(rare_boost, 6),
        "season_series_part_boost": round(season_boost, 6),
        "collection_boxset_boost": round(collection_boost, 6),
        "creator_anthology_boost": round(creator_boost, 6),
        "anime_stylization_boost": round(anime_boost, 6),
        "franchise_subtype_penalty": round(franchise_penalty, 6),
        "year_marked_film_penalty": round(year_film_penalty, 6),
    }

    total = (
        float(components["base_lexical"])
        + float(components["rare_token_boost"])
        + float(components["season_series_part_boost"])
        + float(components["collection_boxset_boost"])
        + float(components["creator_anthology_boost"])
        + float(components["anime_stylization_boost"])
        + float(components["franchise_subtype_penalty"])
        + float(components["year_marked_film_penalty"])
    )
    total = max(total, 0.0)
    components["total_score"] = round(total, 6)

    out = {
        "score_breakdown": {**bd, **components},
        "total_score": round(total, 6),
        "cues": {"query": qc, "row": rc},
    }
    return total, out


def rank_with_scorer(
    query: str,
    rows: List[Dict[str, Any]],
    *,
    mode: str,
    df: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    df = df or _df_by_token(rows)
    out: List[Dict[str, Any]] = []
    for row in rows:
        if mode == "baseline":
            total, wrap = score_baseline_with_breakdown(query, row)
            breakdown = wrap["score_breakdown"]
        elif mode == "v2":
            total, wrap = score_v2_with_breakdown(query, row, df=df)
            breakdown = wrap["score_breakdown"]
        else:
            raise ValueError(mode)
        item = {
            "catalog_id": row["id"],
            "title": row["title"],
            "edition_title": row.get("edition_title") or "",
            "score": round(total, 6),
            "score_breakdown": breakdown,
            "tmdb_id": row.get("tmdb_id"),
            "tmdb_match_status": row.get("tmdb_match_status"),
        }
        if mode == "v2" and "cues" in wrap:
            item["disambiguation_cues"] = wrap["cues"]
        out.append(item)
    out.sort(key=lambda x: x["score"], reverse=True)
    return out
