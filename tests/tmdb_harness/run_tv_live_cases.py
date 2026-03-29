#!/usr/bin/env python3
"""
Live TMDB diagnostic harness: run the isolated catalog match flow against real API.

- Recreates ``tests/tmdb_harness/output/`` on each run (no silent overwrites).
- Writes full search payloads (no truncation of ``results``) and a summary JSON.
- Does not import or modify production enrichment services.

Usage:
  export TMDB_API_KEY=...  # required
  export TMDB_API_URL=https://api.themoviedb.org/3  # optional
  python tests/tmdb_harness/run_tv_live_cases.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.helpers.tmdb_match_helpers import (  # noqa: E402
    build_search_query_variants,
    build_tv_search_query_variants,
    detect_tmdb_search_type,
    extract_year,
    is_collection_or_bundle,
    is_safe_tmdb_match,
    pick_best_tmdb_match,
)
from app.services.tmdb_isolated_match_service import (  # noqa: E402
    classify_collection_listing,
    should_route_tmdb_tv,
)

OUTPUT_ROOT = Path(__file__).resolve().parent / "output"
PAYLOADS_DIR = OUTPUT_ROOT / "payloads"
SUMMARY_PATH = OUTPUT_ROOT / "tv_live_cases_summary.json"

CASE_TITLES: List[str] = [
    "Game of Thrones: The Complete Seventh Season",
    "The Crown: Season 4",
    "Line of Duty: Series 6",
    "Doctor Who: The Complete David Tennant Collection",
    "Stranger Things: Season 3",
    "Better Call Saul: Season 6",
    "South Park: Season 25",
    "Planet Earth II: Complete Series",
    "Band of Brothers",
    "Chernobyl",
    "Twin Peaks: A Limited Event Series",
    "The Last of Us: Season 1",
    "House of the Dragon: Season One",
    "Yellowstone: Season 5 Part 1",
    "The Walking Dead: The Complete Eleventh Season",
    "Attack on Titan: Final Season Part 2",
    "Neon Genesis Evangelion: Complete Series",
    "Battlestar Galactica (2004): The Complete Series",
    "The Office: The Complete Series",
    "Pride and Prejudice (1995)",
]

FailureBucket = Literal[
    "TV detection",
    "normalization",
    "season parsing",
    "wrong TMDb search type",
    "bad candidate scoring",
    "unsafe acceptance threshold",
]


def _case_id(index: int) -> str:
    return f"tv_live_{index:02d}"


def _redact_api_key_from_url(url: str) -> str:
    """Remove api_key query param from logged URLs (payload files must stay shareable)."""
    p = urlparse(url)
    pairs = [
        (k, "<redacted>" if k == "api_key" else v)
        for k, v in parse_qsl(p.query, keep_blank_values=True)
    ]
    new_q = urlencode(pairs)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))


def _title_suggests_tv_sku(title: str) -> bool:
    """
    Broader-than-production cue list for diagnostic buckets only.

    Catches cases like “Limited Event Series” where ``should_route_tmdb_tv`` is
    still false but a human would treat the SKU as TV.
    """
    if should_route_tmdb_tv(title, ""):
        return True
    t = (title or "").lower()
    if re.search(r"\blimited\s+event\s+series\b", t):
        return True
    if re.search(r"\bcomplete\s+(series|collection)\b", t):
        return True
    if re.search(r"\b(season|series)\s+\d", t):
        return True
    if re.search(
        r"\b(season|series)\s+(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\b",
        t,
    ):
        return True
    return False


def _candidate_label(r: Dict[str, Any], search_type: str) -> str:
    return str(
        r.get("title")
        or r.get("name")
        or r.get("original_title")
        or r.get("original_name")
        or ""
    )


def _date_field(r: Dict[str, Any], search_type: str) -> Optional[str]:
    if search_type == "movie":
        return r.get("release_date")
    return r.get("first_air_date")


def _analyze_pick_failure(
    source_title: str,
    source_year: Optional[int],
    results: List[Dict[str, Any]],
    search_type: Literal["movie", "tv"],
) -> Tuple[str, str]:
    """
    Return (code, detail) where code is 'unsafe_all' | 'score_reject' | 'no_results'.
    Mirrors ``pick_best_tmdb_match`` scoring so the bucket matches reality.
    """
    if not results:
        return "no_results", "empty results list"

    safe_results: List[Tuple[int, Dict[str, Any]]] = []
    for result in results:
        cand = _candidate_label(result, search_type)
        if not is_safe_tmdb_match(source_title, cand):
            continue
        cand_date = _date_field(result, search_type)
        cy = extract_year(cand_date)
        score = 100
        if source_year and cy:
            yd = abs(source_year - cy)
            if yd == 0:
                score += 40
            elif yd == 1:
                score += 20
            elif yd <= 3:
                score += 5
            else:
                score -= 50
        safe_results.append((score, result))

    if not safe_results:
        return "unsafe_all", "no result passed is_safe_tmdb_match"

    safe_results.sort(key=lambda x: x[0], reverse=True)
    best_score, _ = safe_results[0]
    if best_score < 80:
        return "score_reject", f"best safe candidate score was {best_score} (< 80)"
    return "score_reject", "pick_best returned None despite score >= 80 (unexpected)"


def _infer_failure_bucket(
    status: str,
    title: str,
    search_type: str,
    reasons: List[str],
    query_trace: List[Dict[str, Any]],
) -> Optional[FailureBucket]:
    if status == "matched":
        return None
    if status == "blocked":
        return "normalization"

    tl = (title or "").lower()
    # Heuristic: obvious season/part wording but matcher never uses structured season IDs — flag for review.
    if status == "not_found" and re.search(
        r"\bseason\b|\bseries\b|\bpart\s+\d+\b|\bfinal\s+season\b", tl
    ):
        # Prefer more specific buckets when we can explain the actual failure.
        pass

    had_results = any(
        isinstance(t.get("results_count"), int) and t["results_count"] > 0 for t in query_trace
    )
    if not had_results:
        suggests_tv = _title_suggests_tv_sku(title)
        if search_type == "movie" and suggests_tv:
            return "TV detection"
        if search_type == "movie" and detect_tmdb_search_type(title) == "tv":
            return "TV detection"
        if search_type == "tv" and not suggests_tv:
            return "wrong TMDb search type"
        return "normalization"

    # Had API results but no acceptance
    last_with_results = next(
        (t for t in reversed(query_trace) if t.get("results_count", 0) > 0),
        None,
    )
    if not last_with_results:
        return "normalization"

    code = last_with_results.get("pick_failure_code", "")
    if code == "unsafe_all":
        return "unsafe acceptance threshold"
    if code == "score_reject":
        return "bad candidate scoring"

    return "normalization"


def _explain_outcome(
    status: str,
    failure_bucket: Optional[str],
    reasons: List[str],
    failure_detail: str,
) -> str:
    if status == "matched":
        return "Isolated flow accepted a candidate via pick_best_tmdb_match."
    if status == "blocked":
        return "Blocked before TMDB: collection/bundle heuristics."
    parts = [failure_detail or "not_found"]
    if failure_bucket:
        parts.append(f"bucket={failure_bucket}")
    return "; ".join(parts)


def run_isolated_flow_with_trace(
    title: str,
    tmdb_api_key: str,
    tmdb_api_url: str,
    *,
    edition_title: str = "",
) -> Dict[str, Any]:
    """Mirror ``search_tmdb_catalog_isolated`` with per-request payload capture."""
    reasons: List[str] = []
    detect_st = detect_tmdb_search_type(title)
    route_tv = should_route_tmdb_tv(title, edition_title)
    search_type: Literal["movie", "tv"] = "tv" if route_tv else "movie"
    endpoint = search_type
    query_variants = (
        build_tv_search_query_variants(title)
        if search_type == "tv"
        else build_search_query_variants(title)
    )

    tv_calls: List[Dict[str, Any]] = []
    movie_calls: List[Dict[str, Any]] = []

    out: Dict[str, Any] = {
        "raw_input_title": title,
        "normalized_query_variants_attempted": list(query_variants),
        "detected_search_type_from_title": detect_st,
        "route_tv_heuristic": route_tv,
        "chosen_search_type": search_type,
        "endpoints_called": [],
        "reasons": reasons,
        "status": "not_found",
        "tmdb_id": None,
        "candidate_title": None,
        "failure_detail": "",
        "query_trace": [],
    }

    if is_collection_or_bundle(title):
        reasons.append("blocked:is_collection_or_bundle")
        out.update(
            {
                "status": "blocked",
                "failure_detail": "Title matches collection/bundle heuristics; skip single-entity TMDB link.",
            }
        )
        return out

    coll = classify_collection_listing(title)
    if coll["block_single_film_match"]:
        reasons.extend([f"blocked:{r}" for r in coll["reasons"]])
        out.update(
            {
                "status": "blocked",
                "failure_detail": f"Collection classifier: kind={coll['kind']}, reasons={coll['reasons']!r}",
            }
        )
        return out

    reasons.append(f"route:{search_type}")
    base_url = tmdb_api_url.rstrip("/")
    search_url = f"{base_url}/search/{endpoint}"
    out["endpoints_called"] = [f"/search/{endpoint}"]

    source_year = extract_year(title)

    for query in query_variants:
        params = {
            "api_key": tmdb_api_key,
            "query": query,
            "include_adult": False,
        }
        response = requests.get(search_url, params=params, timeout=30)
        response.raise_for_status()
        body_text = response.text
        body_json = response.json()

        call_record = {
            "query": query,
            "request_url": _redact_api_key_from_url(str(response.url)),
            "status_code": response.status_code,
            "response_body_text": body_text,
            "response_body_parsed": body_json,
        }
        if search_type == "tv":
            tv_calls.append(call_record)
        else:
            movie_calls.append(call_record)

        results = body_json.get("results") or []
        trace_row: Dict[str, Any] = {
            "query": query,
            "results_count": len(results),
            "pick_failure_code": None,
            "pick_failure_detail": None,
        }

        if not results:
            reasons.append(f"no_results:query={query!r}")
            out["query_trace"].append(trace_row)
            continue

        best = pick_best_tmdb_match(title, source_year, results, search_type)
        if best:
            cand = _candidate_label(best, search_type)
            reasons.append("pick_best_tmdb_match:accepted")
            out.update(
                {
                    "status": "matched",
                    "tmdb_id": int(best["id"]),
                    "search_type": search_type,
                    "candidate_title": cand or None,
                    "failure_detail": "",
                }
            )
            out["query_trace"].append(trace_row)
            out["tv_calls"] = tv_calls
            out["movie_calls"] = movie_calls
            return out

        code, detail = _analyze_pick_failure(title, source_year, results, search_type)
        trace_row["pick_failure_code"] = code
        trace_row["pick_failure_detail"] = detail
        reasons.append(f"no_safe_candidate:query={query!r}")
        out["query_trace"].append(trace_row)

    detail = (
        f"No acceptable TMDB {search_type} match after variants; " + "; ".join(reasons[-5:])
    )
    out.update(
        {
            "status": "not_found",
            "search_type": search_type,
            "failure_detail": detail,
        }
    )
    out["tv_calls"] = tv_calls
    out["movie_calls"] = movie_calls
    return out


def _top5_summary(results: List[Dict[str, Any]], search_type: str) -> List[Dict[str, Any]]:
    out = []
    for r in results[:5]:
        out.append(
            {
                "id": r.get("id"),
                "name": _candidate_label(r, search_type),
                "first_air_date": r.get("first_air_date"),
                "release_date": r.get("release_date"),
            }
        )
    return out


def prepare_output_dir() -> None:
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    PAYLOADS_DIR.mkdir(parents=True, exist_ok=True)


def main() -> int:
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not api_key:
        print("ERROR: TMDB_API_KEY is required in the environment.", file=sys.stderr)
        return 1
    api_url = os.environ.get("TMDB_API_URL", "https://api.themoviedb.org/3").strip()

    prepare_output_dir()

    summary_cases: List[Dict[str, Any]] = []
    failure_buckets: Dict[str, int] = {b: 0 for b in [
        "TV detection",
        "normalization",
        "season parsing",
        "wrong TMDb search type",
        "bad candidate scoring",
        "unsafe acceptance threshold",
    ]}

    matched_n = 0
    unmatched_n = 0

    for i, title in enumerate(CASE_TITLES, start=1):
        cid = _case_id(i)
        trace = run_isolated_flow_with_trace(title, api_key, api_url)
        st = trace["status"]
        search_type = trace.get("search_type") or trace["chosen_search_type"]

        tv_calls = trace.get("tv_calls") or []
        movie_calls = trace.get("movie_calls") or []

        if tv_calls:
            with open(PAYLOADS_DIR / f"{cid}_search_tv.json", "w", encoding="utf-8") as f:
                json.dump({"calls": tv_calls}, f, indent=2, ensure_ascii=False)
        if movie_calls:
            with open(PAYLOADS_DIR / f"{cid}_search_movie.json", "w", encoding="utf-8") as f:
                json.dump({"calls": movie_calls}, f, indent=2, ensure_ascii=False)

        first_hits: List[Dict[str, Any]] = []
        calls = tv_calls if search_type == "tv" else movie_calls
        for c in calls:
            parsed = c.get("response_body_parsed") or {}
            res = parsed.get("results") or []
            if res:
                first_hits = res
                break
        top5 = _top5_summary(first_hits, search_type)

        fb = _infer_failure_bucket(
            st, title, search_type, trace["reasons"], trace["query_trace"]
        )
        if st != "matched" and fb:
            failure_buckets[fb] = failure_buckets.get(fb, 0) + 1

        if st == "matched":
            matched_n += 1
        else:
            unmatched_n += 1

        explanation = _explain_outcome(st, fb, trace["reasons"], trace.get("failure_detail", ""))

        case_summary = {
            "case_id": cid,
            "raw_input_title": title,
            "normalized_query_variants_attempted": trace["normalized_query_variants_attempted"],
            "detected_search_type": trace["detected_search_type_from_title"],
            "route_tv_heuristic": trace["route_tv_heuristic"],
            "chosen_search_type": search_type,
            "endpoints_called": trace["endpoints_called"],
            "top_5_candidates": top5,
            "accepted_tmdb_id": trace.get("tmdb_id"),
            "accepted_name": trace.get("candidate_title"),
            "failure_bucket": fb,
            "status": st,
            "explanation": explanation,
            "reasons_trace": trace["reasons"],
        }
        summary_cases.append(case_summary)

        final_payload = {
            "case_id": cid,
            "raw_input_title": title,
            "status": st,
            "search_type": search_type,
            "tmdb_id": trace.get("tmdb_id"),
            "candidate_title": trace.get("candidate_title"),
            "failure_detail": trace.get("failure_detail", ""),
            "failure_bucket": fb,
            "reasons": trace["reasons"],
            "normalized_query_variants_attempted": trace["normalized_query_variants_attempted"],
            "query_trace": trace["query_trace"],
            "detected_search_type_from_title": trace["detected_search_type_from_title"],
            "route_tv_heuristic": trace["route_tv_heuristic"],
        }
        with open(PAYLOADS_DIR / f"{cid}_final_decision.json", "w", encoding="utf-8") as f:
            json.dump(final_payload, f, indent=2, ensure_ascii=False)

    doc = {
        "schema_version": 1,
        "tmdb_api_url": api_url,
        "cases": summary_cases,
    }
    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)

    n = len(CASE_TITLES)
    print()
    print("=== TMDB live TV harness — terminal report ===")
    print(f"Total cases:     {n}")
    print(f"Matched:         {matched_n}")
    print(f"Unmatched:       {unmatched_n}")
    print()
    print("Failure counts by bucket (blocked + not_found only):")
    for b, c in failure_buckets.items():
        if c:
            print(f"  {b}: {c}")
    print()
    print("--- Summary table (all cases) ---")
    w_id, w_st, w_t = 12, 10, 42
    print(f"{'case_id':<{w_id}} {'status':<{w_st}} {'tmdb_id':>8}  title")
    print("-" * (w_id + w_st + 8 + 3 + w_t))
    for c in summary_cases:
        tid = c.get("accepted_tmdb_id")
        tid_s = str(tid) if tid is not None else "-"
        title_short = (c["raw_input_title"] or "")[: w_t - 2]
        if len(c["raw_input_title"] or "") > w_t - 2:
            title_short += ".."
        print(
            f"{c['case_id']:<{w_id}} {c['status']:<{w_st}} {tid_s:>8}  {title_short}"
        )
    print()
    print("--- Failing cases only ---")
    fails = [c for c in summary_cases if c["status"] != "matched"]
    if not fails:
        print("(none)")
    else:
        for c in fails:
            print(
                f"  {c['case_id']}  bucket={c.get('failure_bucket')!r}  "
                f"status={c['status']!r}  type={c.get('chosen_search_type')!r}"
            )
            print(f"    title: {c['raw_input_title']!r}")
            print(f"    why:   {c.get('explanation', '')}")
    print()
    print(f"Summary written: {SUMMARY_PATH}")
    print(f"Payloads dir:    {PAYLOADS_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
