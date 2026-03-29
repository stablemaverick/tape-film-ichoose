#!/usr/bin/env python3
"""
Isolated Moovies TV matching workstream — live TMDB only, no enrichment/pipeline.

- Reads titles from ``moovies_tv_workstream_titles.MOOVIES_TV_WORKSTREAM_TITLES``.
- Uses the same isolated trace flow as ``run_tv_live_cases`` (no production enrichment).
- Writes raw search payloads + JSON summary + a compact text report under ``workstream_output/``.

Usage:
  export TMDB_API_KEY=...
  python tests/tmdb_harness/run_moovies_tv_workstream.py
"""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.tmdb_harness.moovies_tv_workstream_titles import (  # noqa: E402
    MOOVIES_TV_WORKSTREAM_TITLES,
)
from tests.tmdb_harness.run_tv_live_cases import (  # noqa: E402
    _explain_outcome,
    _infer_failure_bucket,
    _top5_summary,
    run_isolated_flow_with_trace,
)

OUTPUT_ROOT = Path(__file__).resolve().parent / "workstream_output"
PAYLOADS_DIR = OUTPUT_ROOT / "payloads"
SUMMARY_JSON = OUTPUT_ROOT / "moovies_tv_workstream_summary.json"
COMPACT_REPORT = OUTPUT_ROOT / "MOOVIES_TV_WORKSTREAM_REPORT.txt"


def _case_id(index: int) -> str:
    return f"ws_{index:02d}"


def _prepare_output_dir() -> None:
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    PAYLOADS_DIR.mkdir(parents=True, exist_ok=True)


def _write_compact_report(
    cases: List[Dict[str, Any]],
    api_url: str,
    matched_n: int,
    unmatched_n: int,
) -> None:
    lines: List[str] = []
    lines.append("Moovies TV workstream — compact report (isolated, live TMDB)")
    lines.append(f"API base: {api_url}")
    lines.append(f"Cases: {len(cases)}  |  matched: {matched_n}  |  unmatched: {unmatched_n}")
    lines.append("")
    lines.append(
        f"{'id':<8} {'route':<7} {'match':<8} {'tmdb_id':>8}  {'failure_bucket':<28}  title"
    )
    lines.append("-" * 120)
    for c in cases:
        st = c.get("status", "")
        route = c.get("chosen_search_type", "-")
        match = "yes" if st == "matched" else "no"
        tid = c.get("accepted_tmdb_id")
        tid_s = str(tid) if tid is not None else "-"
        fb = c.get("failure_bucket") or "-"
        title = (c.get("raw_input_title") or "")[:55]
        lines.append(f"{c.get('case_id',''):<8} {route:<7} {match:<8} {tid_s:>8}  {fb:<28}  {title}")
    lines.append("")
    lines.append("Failures detail:")
    for c in cases:
        if c.get("status") == "matched":
            continue
        lines.append(f"  {c.get('case_id')}: {c.get('raw_input_title')!r}")
        lines.append(f"    bucket: {c.get('failure_bucket')!r}  status: {c.get('status')!r}")
        lines.append(f"    note: {c.get('explanation', '')[:200]}")
    lines.append("")
    lines.append(f"Full JSON: {SUMMARY_JSON}")
    lines.append(f"Raw payloads: {PAYLOADS_DIR}/")
    COMPACT_REPORT.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    api_key = os.environ.get("TMDB_API_KEY", "").strip()
    if not api_key:
        print("ERROR: TMDB_API_KEY is required.", file=sys.stderr)
        return 1
    api_url = os.environ.get("TMDB_API_URL", "https://api.themoviedb.org/3").strip()

    _prepare_output_dir()
    titles = list(MOOVIES_TV_WORKSTREAM_TITLES)
    if len(titles) != 20:
        print(f"WARNING: expected 20 titles, got {len(titles)}", file=sys.stderr)

    summary_cases: List[Dict[str, Any]] = []
    matched_n = 0
    unmatched_n = 0

    for i, title in enumerate(titles, start=1):
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

        fb = _infer_failure_bucket(st, title, search_type, trace["reasons"], trace["query_trace"])
        explanation = _explain_outcome(st, fb, trace["reasons"], trace.get("failure_detail", ""))

        if st == "matched":
            matched_n += 1
        else:
            unmatched_n += 1

        case_summary: Dict[str, Any] = {
            "case_id": cid,
            "raw_input_title": title,
            "route_chosen": search_type,
            "chosen_search_type": search_type,
            "match_status": st,
            "accepted_tmdb_id": trace.get("tmdb_id"),
            "accepted_name": trace.get("candidate_title"),
            "failure_bucket": fb,
            "top_5_candidates": top5,
            "normalized_query_variants_attempted": trace["normalized_query_variants_attempted"],
            "detected_search_type": trace["detected_search_type_from_title"],
            "route_tv_heuristic": trace["route_tv_heuristic"],
            "endpoints_called": trace["endpoints_called"],
            "explanation": explanation,
            "reasons_trace": trace["reasons"],
            "status": st,
        }
        summary_cases.append(case_summary)

        final_payload = {
            "case_id": cid,
            "raw_input_title": title,
            "status": st,
            "search_type": search_type,
            "route_chosen": search_type,
            "tmdb_id": trace.get("tmdb_id"),
            "candidate_title": trace.get("candidate_title"),
            "failure_detail": trace.get("failure_detail", ""),
            "failure_bucket": fb,
            "reasons": trace["reasons"],
            "query_trace": trace["query_trace"],
        }
        with open(PAYLOADS_DIR / f"{cid}_final_decision.json", "w", encoding="utf-8") as f:
            json.dump(final_payload, f, indent=2, ensure_ascii=False)

    doc = {
        "workstream": "moovies_tv_isolated",
        "schema_version": 1,
        "tmdb_api_url": api_url,
        "case_count": len(summary_cases),
        "matched": matched_n,
        "unmatched": unmatched_n,
        "cases": summary_cases,
    }
    with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)

    _write_compact_report(summary_cases, api_url, matched_n, unmatched_n)

    print(COMPACT_REPORT.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
