#!/usr/bin/env python3
"""
Agent-style fuzzy retrieval harness (isolated; no production agent or pipeline wiring).

Reuses ``app.helpers.tmdb_match_helpers`` normalization / query variants only.

Usage:
  python tests/agent_retrieval_harness/run_agent_retrieval_harness.py
  python tests/agent_retrieval_harness/run_agent_retrieval_harness.py --top-k 1   # strict: ranking stress
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.agent_retrieval_harness.catalog_sample_rows import FIXTURE_ROWS, rows_by_id  # noqa: E402
from tests.agent_retrieval_harness.query_cases import QUERY_CASES  # noqa: E402
from tests.agent_retrieval_harness.retrieval_engine import (  # noqa: E402
    classify_failure_bucket,
    failure_notes,
    find_rank,
    fixability_hint,
    rank_catalog_for_query,
)

OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def _run_one(query: str, expected_id: str, top_k: int) -> Dict[str, Any]:
    by_id = rows_by_id()
    expected_row = by_id[expected_id]
    ranked = rank_catalog_for_query(query, list(FIXTURE_ROWS))
    rank = find_rank(expected_id, ranked)
    passed = rank is not None and rank <= top_k
    bucket = ""
    if not passed:
        bucket = classify_failure_bucket(
            query, expected_row, rank, top_k, ranked
        )
    fix = fixability_hint(bucket) if bucket else ""

    return {
        "query": query,
        "expected_catalog_id": expected_id,
        "expected_title": expected_row["title"],
        "top_k": top_k,
        "expected_rank": rank,
        "pass": passed,
        "failure_bucket": bucket or None,
        "likely_fixability": fix or None,
        "notes": failure_notes(query, expected_row, bucket) if bucket else "ok",
        "ranked_results": ranked[:25],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Agent fuzzy retrieval harness (isolated).")
    ap.add_argument(
        "--top-k",
        type=int,
        default=15,
        metavar="K",
        help="Pass if expected row is within top K results (default 15). Use 1 to stress ranking.",
    )
    args = ap.parse_args()
    top_k = max(1, args.top_k)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cases_out: List[Dict[str, Any]] = []
    passed_n = 0
    failed_n = 0
    bucket_counts: Counter[str] = Counter()

    for i, (query, expected_id) in enumerate(QUERY_CASES, start=1):
        one = _run_one(query, expected_id, top_k)
        one["case_index"] = i
        cases_out.append(one)
        per_path = OUTPUT_DIR / f"query_{i:02d}.json"
        with open(per_path, "w", encoding="utf-8") as f:
            json.dump(one, f, indent=2, ensure_ascii=False)

        if one["pass"]:
            passed_n += 1
        else:
            failed_n += 1
            if one.get("failure_bucket"):
                bucket_counts[one["failure_bucket"]] += 1

    near_misses = [
        {
            "query": c["query"],
            "expected_catalog_id": c["expected_catalog_id"],
            "expected_rank": c["expected_rank"],
            "winner_catalog_id": c["ranked_results"][0]["catalog_id"] if c["ranked_results"] else None,
        }
        for c in cases_out
        if c["pass"] and (c.get("expected_rank") or 0) > 1
    ]

    top_5_improvements = [
        {
            "rank": 1,
            "item": "Disambiguate franchise vs spin-off (e.g. GITS 1995 film vs SAC_2045) using year tokens, TV cues, or catalog media hints.",
            "evidence": "Near-miss: query `ghost in the shell` ranks SAC_2045 second behind the 1995 film when both exist.",
        },
        {
            "rank": 2,
            "item": "Query normalization: underscore/space for stylized codes (`sac_2045`, `sac 2045`).",
            "evidence": "Harness uses loose underscore splitting; production agent should align.",
        },
        {
            "rank": 3,
            "item": "Season/series tail stripping for user queries (mirror TV packaging helpers).",
            "evidence": "Reduces mismatch between short queries and long SKU titles.",
        },
        {
            "rank": 4,
            "item": "Alias expansion for director-only queries (`alex de la iglesia` → anthology title).",
            "evidence": "Director substring matches long anthology title; fragile without aliases.",
        },
        {
            "rank": 5,
            "item": "Ranking: boost rare distinctive tokens (`wombles`, `nozaki`, `yokai`) when overlap is sparse.",
            "evidence": "Prevents generic rows from winning when edition noise is high.",
        },
    ]

    summary = {
        "harness": "agent_fuzzy_retrieval",
        "schema_version": 1,
        "top_k_pass_threshold": top_k,
        "near_misses": near_misses,
        "top_5_retrieval_improvements": top_5_improvements,
        "fixture_row_count": len(FIXTURE_ROWS),
        "query_case_count": len(QUERY_CASES),
        "passed": passed_n,
        "failed": failed_n,
        "failure_bucket_counts": dict(bucket_counts),
        "cases": cases_out,
    }

    summary_path = OUTPUT_DIR / "agent_retrieval_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    csv_path = OUTPUT_DIR / "agent_retrieval_summary.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "query",
                "expected_catalog_id",
                "expected_rank",
                "pass",
                "failure_bucket",
                "likely_fixability",
            ]
        )
        for c in cases_out:
            w.writerow(
                [
                    c["query"],
                    c["expected_catalog_id"],
                    c["expected_rank"],
                    c["pass"],
                    c.get("failure_bucket") or "",
                    c.get("likely_fixability") or "",
                ]
            )

    # Markdown report
    failed_cases = [c for c in cases_out if not c["pass"]]
    passed_cases = [c for c in cases_out if c["pass"]]

    lines: List[str] = []
    lines.append("# Agent fuzzy retrieval harness report\n")
    lines.append(f"- **Top-K threshold:** {top_k}")
    lines.append(f"- **Fixture rows:** {len(FIXTURE_ROWS)}")
    lines.append(f"- **Queries:** {len(QUERY_CASES)}")
    lines.append(f"- **Passed:** {passed_n}")
    lines.append(f"- **Failed:** {failed_n}\n")

    lines.append("## Passed queries\n")
    for c in passed_cases:
        lines.append(f"- `{c['query']!s}` → `{c['expected_catalog_id']}` (rank {c['expected_rank']})")

    lines.append("\n## Failed queries\n")
    for c in failed_cases:
        lines.append(
            f"- `{c['query']!s}` → expected `{c['expected_catalog_id']}` "
            f"(rank {c['expected_rank']}) — **{c.get('failure_bucket')}** — "
            f"fixability: *{c.get('likely_fixability')}*"
        )

    lines.append("\n## Failure bucket counts\n")
    for b, n in bucket_counts.most_common():
        lines.append(f"- {b}: {n}")

    lines.append("\n## Likely improvement buckets (from failures)\n")
    for b, n in bucket_counts.most_common():
        lines.append(f"- **{b}** ({n}): see fixability hints on failed rows.")

    lines.append("\n## Near misses (passed but not rank 1)\n")
    if near_misses:
        for nm in near_misses:
            lines.append(
                f"- `{nm['query']!s}` → expected `{nm['expected_catalog_id']}` at rank **{nm['expected_rank']}** "
                f"(top result: `{nm['winner_catalog_id']}`)"
            )
    else:
        lines.append("(none)")

    lines.append("\n## Top 5 highest-value retrieval improvements (heuristic)\n")
    for im in top_5_improvements:
        lines.append(f"{im['rank']}. {im['item']} — *{im['evidence']}*")

    lines.append("\n## Low-risk improvements to consider later\n")
    lines.append(
        "- **Query normalization:** strip/replace punctuation; expand `part 2` ↔ `ii`; "
        "underscore/space equivalence for stylized show codes (e.g. SAC 2045).\n"
        "- **Alias expansion:** director-only queries → known anthology titles; "
        "transliteration variants for anime.\n"
        "- **Ranking:** boost when rare tokens (e.g. `wombles`, `nozaki`) match strongly "
        "even if edition noise dilutes raw token overlap.\n"
    )

    lines.append("\n## Recommendation (next move)\n")
    # Prefer failure buckets; else near-misses → ranking
    fix_counts: Counter[str] = Counter()
    for c in failed_cases:
        fx = c.get("likely_fixability") or ""
        if fx:
            fix_counts[fx] += 1
    if not fix_counts and near_misses:
        rec = (
            "Prioritize **ranking / disambiguation** (see near misses): expected rows are retrieved "
            "but not always at rank 1 when similar titles compete."
        )
    elif not fix_counts:
        rec = "Baseline is strong on this fixture set; optional **alias expansion** for edge cases."
    else:
        top_fix = fix_counts.most_common(1)[0][0]
        if top_fix == "query normalization":
            rec = "Prioritize **query normalization** (season/part/punctuation/stylization)."
        elif top_fix == "title alias expansion":
            rec = "Prioritize **title alias expansion** (anime + director/anthology names)."
        elif top_fix == "fuzzy scoring":
            rec = "Prioritize **fuzzy scoring** (token overlap / loose tokenization)."
        elif top_fix == "ranking":
            rec = "Prioritize **ranking** (re-order when expected item is present but low)."
        else:
            rec = f"Prioritize **{top_fix}** based on failure mix."

    lines.append(rec + "\n")

    lines.append("\n## Artifacts\n")
    lines.append(f"- Per-query JSON: `{OUTPUT_DIR}/query_*.json`")
    lines.append(f"- Summary JSON: `{summary_path}`")
    lines.append(f"- CSV: `{csv_path}`\n")

    report_path = OUTPUT_DIR / "AGENT_RETRIEVAL_HARNESS_REPORT.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")

    print(report_path.read_text(encoding="utf-8"))
    print(f"\nWrote: {summary_path}")
    return 0 if failed_n == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
