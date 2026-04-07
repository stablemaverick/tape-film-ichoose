#!/usr/bin/env python3
"""
Isolated catalog sample TMDB pass — classification + matching only.

- TV / episodic rows: live TMDB via ``run_isolated_flow_with_trace`` (same behaviour as
  ``run_tv_live_cases``); full raw payloads per query; failure buckets aligned with
  ``run_tv_live_cases._infer_failure_bucket`` plus a narrow ``season parsing`` override.
- Collection / bundle rows: classifier + ``search_tmdb_catalog_isolated`` with mocked HTTP
  when the path would block (proves no naive TMDB call). No live TMDB unless you add a
  diagnostic branch later.

Does not touch enrichment jobs, orchestration, or production services.

Usage:
  export TMDB_API_KEY=...  # required for TV rows
  python tests/tmdb_harness/run_catalog_sample_match_pass.py
"""

from __future__ import annotations

import csv
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import Mock, patch

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.helpers.tmdb_match_helpers import is_collection_or_bundle  # noqa: E402
from app.services.tmdb_isolated_match_service import (  # noqa: E402
    classify_collection_listing,
    label_film_collection_harness,
    search_tmdb_catalog_isolated,
    should_route_tmdb_tv,
)
from tests.tmdb_harness.catalog_sample_match_titles import (  # noqa: E402
    COLLECTION_BUNDLE_CANDIDATES,
    TV_EPISODIC_CANDIDATES,
)
from tests.tmdb_harness.run_tv_live_cases import (  # noqa: E402
    _explain_outcome,
    _infer_failure_bucket,
    _title_suggests_tv_sku,
    _top5_summary,
    run_isolated_flow_with_trace,
)

OUTPUT_ROOT = Path(__file__).resolve().parent / "catalog_sample_match_output"
SUMMARY_JSON = OUTPUT_ROOT / "catalog_sample_match_summary.json"
SUMMARY_CSV = OUTPUT_ROOT / "catalog_sample_match_summary.csv"
SUMMARY_MD = OUTPUT_ROOT / "CATALOG_SAMPLE_MATCH_TABLE.md"

FAILURE_BUCKETS_ORDERED = (
    "TV detection",
    "normalization",
    "season parsing",
    "wrong TMDb search type",
    "bad candidate scoring",
    "unsafe acceptance threshold",
)


def _empty_tmdb_response() -> Mock:
    m = Mock()
    m.raise_for_status = Mock()
    m.json.return_value = {"results": [], "page": 1, "total_results": 0}
    return m


def _infer_tv_failure_bucket_extended(
    status: str,
    title: str,
    search_type: str,
    reasons: List[str],
    query_trace: List[Dict[str, Any]],
) -> Optional[str]:
    """Maps outcomes to the six diagnostic buckets; refines ``normalization`` → ``season parsing`` when apt."""
    base = _infer_failure_bucket(status, title, search_type, reasons, query_trace)
    if status == "matched":
        return None
    if status == "blocked":
        return "normalization"
    tl = (title or "").lower()
    had_results = any(
        isinstance(t.get("results_count"), int) and t["results_count"] > 0 for t in query_trace
    )
    if base not in ("normalization", None):
        return base
    if (
        not had_results
        and status == "not_found"
        and re.search(
            r"\b(season|series)\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\b"
            r"|\bpart\s+\d+\b|\bmini\s+series\b",
            tl,
        )
    ):
        return "season parsing"
    return base


def _tv_future_routing_note(
    status: str,
    failure_bucket: Optional[str],
    route_tv: bool,
    title: str,
) -> str:
    """Short recommendation for product follow-up (harness-only)."""
    if status == "matched":
        return "ok_matched_no_routing_change_needed"
    suggests = _title_suggests_tv_sku(title)
    if status == "blocked":
        return "blocked_pre_tmdb_not_tv_routing"
    if not route_tv and suggests:
        return "candidate_improve_tv_routing_heuristic"
    if failure_bucket in ("TV detection", "wrong TMDb search type"):
        return "candidate_improve_tv_routing_or_search_type"
    if failure_bucket == "season parsing":
        return "candidate_season_part_query_or_structured_parse"
    if failure_bucket in ("bad candidate scoring", "unsafe acceptance threshold"):
        return "candidate_scoring_or_safety_threshold"
    if failure_bucket == "normalization":
        return "candidate_query_normalization"
    return "review"


def _bundle_recommendation(
    blocked: bool,
    label: str,
) -> tuple[str, str]:
    """(stay_blocked_vs_review, one_line_reason)"""
    if blocked or label == "blocked_collection_candidate":
        return "stay_blocked_as_bundle_or_collection", "Classifier blocks naive single-entity match."
    if label == "bundle_unresolved":
        return "stay_blocked_review_title", "Empty/ambiguous bundle classification."
    return "review_single_film_risk", "Classifier allows single-title path; treat as routing/scoring review."


def _run_tv_case(
    index: int,
    title: str,
    api_key: str,
    api_url: str,
) -> Dict[str, Any]:
    case_id = f"cs_tv_{index:02d}"
    trace = run_isolated_flow_with_trace(title, api_key, api_url)
    st = trace["status"]
    search_type = trace.get("search_type") or trace["chosen_search_type"]
    route_tv = bool(trace.get("route_tv_heuristic"))

    tv_calls = trace.get("tv_calls") or []
    movie_calls = trace.get("movie_calls") or []
    calls = tv_calls if search_type == "tv" else movie_calls

    first_hits: List[Dict[str, Any]] = []
    for c in calls:
        parsed = c.get("response_body_parsed") or {}
        res = parsed.get("results") or []
        if res:
            first_hits = res
            break
    top5 = _top5_summary(first_hits, search_type)

    fb = _infer_tv_failure_bucket_extended(st, title, search_type, trace["reasons"], trace["query_trace"])
    explanation = _explain_outcome(st, fb, trace["reasons"], trace.get("failure_detail", ""))
    routing_note = _tv_future_routing_note(st, fb, route_tv, title)

    doc: Dict[str, Any] = {
        "case_id": case_id,
        "group": "tv_episodic",
        "raw_input_title": title,
        "chosen_search_type": search_type,
        "detected_search_type_from_title": trace.get("detected_search_type_from_title"),
        "route_tv_heuristic": route_tv,
        "should_route_tmdb_tv": should_route_tmdb_tv(title, ""),
        "title_suggests_tv_sku_diagnostic": _title_suggests_tv_sku(title),
        "normalized_query_variants_attempted": trace.get("normalized_query_variants_attempted"),
        "endpoints_called": trace.get("endpoints_called"),
        "match_status": st,
        "accepted_tmdb_id": trace.get("tmdb_id"),
        "accepted_candidate_title": trace.get("candidate_title"),
        "failure_bucket": fb,
        "failure_detail": trace.get("failure_detail", ""),
        "explanation": explanation,
        "future_tv_routing_note": routing_note,
        "reasons_trace": trace["reasons"],
        "query_trace": trace["query_trace"],
        "top_5_candidates_first_nonempty_response": top5,
        "raw_tmdb_search_payloads": {
            "tv_calls": tv_calls,
            "movie_calls": movie_calls,
        },
    }
    out_path = OUTPUT_ROOT / f"{case_id}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)

    return {
        "case_id": case_id,
        "group": "tv_episodic",
        "raw_input_title": title,
        "chosen_search_type": search_type,
        "match_status": st,
        "accepted_tmdb_id": trace.get("tmdb_id"),
        "failure_bucket": fb,
        "future_tv_routing_note": routing_note,
        "top_5_candidates": top5,
        "explanation": explanation,
        "path": str(out_path),
    }


def _run_bundle_case(index: int, title: str) -> Dict[str, Any]:
    case_id = f"cs_bundle_{index:02d}"
    icob = is_collection_or_bundle(title)
    clf = classify_collection_listing(title)
    label = label_film_collection_harness(clf)
    blocked_classifier = bool(clf.get("block_single_film_match"))

    naive_blocked = blocked_classifier or icob
    rec, rec_reason = _bundle_recommendation(naive_blocked, label)

    with patch("app.services.tmdb_isolated_match_service.requests.get") as mock_get:
        mock_get.return_value = _empty_tmdb_response()
        out = search_tmdb_catalog_isolated(
            title,
            "dummy-key",
            "https://api.themoviedb.org/3",
            source_year=None,
            edition_title="",
        )
        isolated_status = out["status"]
        isolated_detail = out.get("failure_detail", "")
        if out["status"] == "blocked":
            assert not mock_get.called, "blocked path must not call TMDB"
            tmdb_http_used = False
        else:
            tmdb_http_used = bool(mock_get.called)

    doc: Dict[str, Any] = {
        "case_id": case_id,
        "group": "collection_bundle",
        "raw_input_title": title,
        "is_collection_or_bundle": icob,
        "classify_collection_listing": clf,
        "harness_label": label,
        "blocked_from_naive_single_title_matching": naive_blocked,
        "isolated_matcher_status": isolated_status,
        "isolated_matcher_detail": isolated_detail,
        "tmdb_live_called_in_harness": False,
        "mocked_tmdb_http_for_isolated_probe": tmdb_http_used,
        "recommendation": rec,
        "recommendation_reason": rec_reason,
    }
    out_path = OUTPUT_ROOT / f"{case_id}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)

    return {
        "case_id": case_id,
        "group": "collection_bundle",
        "raw_input_title": title,
        "harness_label": label,
        "blocked_from_naive_single_title_matching": naive_blocked,
        "isolated_matcher_status": isolated_status,
        "recommendation": rec,
        "path": str(out_path),
    }


def _write_csv(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(SUMMARY_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _write_md(
    tv_rows: List[Dict[str, Any]],
    bundle_rows: List[Dict[str, Any]],
    meta: Dict[str, Any],
) -> None:
    lines: List[str] = []
    lines.append("# Catalog sample TMDB match pass (isolated harness)\n")
    lines.append(f"TMDB API base: `{meta.get('tmdb_api_url', '')}`\n")
    lines.append("## TV / episodic candidates\n")
    lines.append(
        "| case_id | match | type | tmdb_id | failure_bucket | future_tv_routing_note | title |"
    )
    lines.append("|---------|-------|------|---------|----------------|-------------------------|-------|")
    for r in tv_rows:
        title = (r.get("raw_input_title") or "").replace("|", "\\|")
        lines.append(
            f"| {r.get('case_id','')} | {r.get('match_status','')} | {r.get('chosen_search_type','')} | "
            f"{r.get('accepted_tmdb_id','')} | {r.get('failure_bucket','')} | "
            f"{str(r.get('future_tv_routing_note','')).replace('|', ' ')} | {title[:80]} |"
        )
    lines.append("\n## Collection / bundle candidates\n")
    lines.append("| case_id | blocked (naive) | harness_label | recommendation | title |")
    lines.append("|---------|-----------------|---------------|------------------|-------|")
    for r in bundle_rows:
        title = (r.get("raw_input_title") or "").replace("|", "\\|")
        lines.append(
            f"| {r.get('case_id','')} | {r.get('blocked_from_naive_single_title_matching','')} | "
            f"{r.get('harness_label','')} | {r.get('recommendation','')} | {title[:80]} |"
        )
    lines.append("\n## Roll-ups (harness interpretation)\n")
    lines.append(f"- **TV — looks good (matched):** {meta.get('tv_matched_titles', [])}")
    lines.append(
        f"- **TV — blocked before TMDB (collection/bundle heuristic on TV-ish SKU):** "
        f"{meta.get('tv_blocked_pre_tmdb_collection_heuristic_titles', [])}"
    )
    lines.append(f"- **TV — candidates for routing / search-type / query work:** {meta.get('tv_improvement_titles', [])}")
    lines.append(f"- **Bundle — stay blocked / collection-safe:** {meta.get('bundle_stay_blocked_titles', [])}")
    lines.append(f"- **Bundle — review (single-title path open):** {meta.get('bundle_review_titles', [])}")
    lines.append("")
    SUMMARY_MD.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not api_key:
        print("ERROR: TMDB_API_KEY is required for TV rows.", file=sys.stderr)
        return 1
    api_url = os.environ.get("TMDB_API_URL", "https://api.themoviedb.org/3").strip()

    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    tv_summaries: List[Dict[str, Any]] = []
    for i, title in enumerate(TV_EPISODIC_CANDIDATES, start=1):
        tv_summaries.append(_run_tv_case(i, title, api_key, api_url))

    bundle_summaries: List[Dict[str, Any]] = []
    for i, title in enumerate(COLLECTION_BUNDLE_CANDIDATES, start=1):
        bundle_summaries.append(_run_bundle_case(i, title))

    tv_matched = [r["raw_input_title"] for r in tv_summaries if r.get("match_status") == "matched"]
    tv_blocked_collection = [
        r["raw_input_title"] for r in tv_summaries if r.get("match_status") == "blocked"
    ]
    tv_improve = [
        r["raw_input_title"]
        for r in tv_summaries
        if r.get("match_status") != "matched"
        and str(r.get("future_tv_routing_note", "")).startswith("candidate_")
    ]
    bundle_stay = [
        r["raw_input_title"]
        for r in bundle_summaries
        if r.get("recommendation", "").startswith("stay_blocked")
    ]
    bundle_review = [
        r["raw_input_title"]
        for r in bundle_summaries
        if not str(r.get("recommendation", "")).startswith("stay_blocked")
    ]

    flat_table: List[Dict[str, Any]] = []
    for r in tv_summaries:
        flat_table.append(
            {
                "case_id": r["case_id"],
                "group": "tv_episodic",
                "title": r["raw_input_title"],
                "match_status": r.get("match_status"),
                "chosen_search_type": r.get("chosen_search_type"),
                "accepted_tmdb_id": r.get("accepted_tmdb_id"),
                "failure_bucket": r.get("failure_bucket"),
                "future_tv_routing_note": r.get("future_tv_routing_note"),
            }
        )
    for r in bundle_summaries:
        flat_table.append(
            {
                "case_id": r["case_id"],
                "group": "collection_bundle",
                "title": r["raw_input_title"],
                "match_status": r.get("isolated_matcher_status"),
                "chosen_search_type": "",
                "accepted_tmdb_id": "",
                "failure_bucket": "",
                "future_tv_routing_note": r.get("recommendation"),
            }
        )

    meta = {
        "tmdb_api_url": api_url,
        "failure_bucket_labels": list(FAILURE_BUCKETS_ORDERED),
        "tv_matched_titles": tv_matched,
        "tv_blocked_pre_tmdb_collection_heuristic_titles": tv_blocked_collection,
        "tv_improvement_titles": tv_improve,
        "bundle_stay_blocked_titles": bundle_stay,
        "bundle_review_titles": bundle_review,
    }

    doc: Dict[str, Any] = {
        "harness": "catalog_sample_match_pass",
        "schema_version": 1,
        **meta,
        "counts": {
            "tv_episodic": len(tv_summaries),
            "collection_bundle": len(bundle_summaries),
            "tv_matched": len(tv_matched),
            "tv_unmatched": len(tv_summaries) - len(tv_matched),
        },
        "tv_cases": tv_summaries,
        "bundle_cases": bundle_summaries,
        "flat_table": flat_table,
    }
    with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)

    _write_csv(flat_table)
    _write_md(tv_summaries, bundle_summaries, meta)

    print(f"Wrote per-title JSON under: {OUTPUT_ROOT}")
    print(f"Summary: {SUMMARY_JSON}")
    print(f"CSV: {SUMMARY_CSV}")
    print(f"Markdown: {SUMMARY_MD}")
    print()
    print("TV matched:", len(tv_matched), "/", len(tv_summaries))
    print("Bundle stay-blocked:", len(bundle_stay), "/", len(bundle_summaries))
    print("Bundle review:", bundle_review)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
