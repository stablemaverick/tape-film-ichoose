#!/usr/bin/env python3
"""
Intent-aware retrieval / ranking harness (isolated; no production wiring).

Evaluates ``detect_intent_category`` + ``rank_intent_aware`` against
``intent_query_cases`` and ``INTENT_FIXTURE_ROWS``.

Usage:
  python3 tests/agent_retrieval_harness/run_intent_retrieval_harness.py
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

from tests.agent_retrieval_harness.intent_catalog_fixtures import (  # noqa: E402
    rows_as_dict_list,
)
from tests.agent_retrieval_harness.intent_detection_harness import detect_intent_category  # noqa: E402
from tests.agent_retrieval_harness.intent_failure_classification import (  # noqa: E402
    best_rank_for_acceptable,
    classify_intent_failure,
)
from tests.agent_retrieval_harness.intent_query_cases import INTENT_QUERY_CASES  # noqa: E402
from tests.agent_retrieval_harness.intent_ranking_harness import rank_intent_aware  # noqa: E402

OUTPUT_DIR = Path(__file__).resolve().parent / "intent_harness_output"


def _trim_ranked(ranked: List[Dict[str, Any]], limit: int = 25) -> List[Dict[str, Any]]:
    return [dict(x) for x in ranked[:limit]]


def _run_case(case: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    query = case["query"]
    expected_intent = case["expected_intent"]
    acceptable = list(case["expected_acceptable_ids"])
    pass_within = int(case.get("pass_within_rank") or 3)

    detected = detect_intent_category(query)
    ranked = rank_intent_aware(query, rows)
    best_r, best_id = best_rank_for_acceptable(ranked, acceptable)

    intent_pass = detected == expected_intent
    retrieval_pass = best_r is not None and best_r <= pass_within
    overall_pass = intent_pass and retrieval_pass

    failure_kind = ""
    if not overall_pass:
        failure_kind = classify_intent_failure(
            case,
            detected_intent=detected,
            ranked=ranked,
            rows=rows,
            pass_within_rank=pass_within,
            overall_pass=overall_pass,
        )

    return {
        "case_id": case["case_id"],
        "intent_group": case.get("intent_group", ""),
        "query": query,
        "notes": case.get("notes", ""),
        "expected_intent": expected_intent,
        "detected_intent_category": detected,
        "intent_pass": intent_pass,
        "expected_acceptable_catalog_ids": acceptable,
        "best_acceptable_rank": best_r,
        "best_acceptable_id": best_id,
        "pass_within_rank": pass_within,
        "retrieval_pass": retrieval_pass,
        "pass": overall_pass,
        "failure_kind": failure_kind or None,
        "ranked_results": _trim_ranked(ranked),
    }


def _build_top_5_improvements(
    cases_out: List[Dict[str, Any]],
    failure_kinds: Counter[str],
) -> List[Dict[str, Any]]:
    """Heuristic top-5 product improvements from failure mix + case ids."""
    failed = [c for c in cases_out if not c["pass"]]
    items: List[Dict[str, Any]] = []

    def add(rank: int, item: str, evidence: str) -> None:
        items.append({"rank": rank, "item": item, "evidence": evidence})

    r = 1
    if failure_kinds.get("intent_detection", 0):
        add(
            r,
            "Tighten isolated intent detector ordering and multi-signal rules (commerce vs year vs awards) before shipping intent-aware retrieval.",
            f"Failures tagged intent_detection: {failure_kinds['intent_detection']} case(s).",
        )
        r += 1
    if failure_kinds.get("missing_metadata", 0):
        add(
            r,
            "Backfill director, lead_actor, label, awards_note, and availability on catalog rows used for intent boosts.",
            f"Failures tagged missing_metadata: {failure_kinds['missing_metadata']} case(s).",
        )
        r += 1
    if failure_kinds.get("weak_filtering", 0):
        add(
            r,
            "Add intent-specific hard filters (year window, label token, preorder flag) before lexical ranking.",
            f"Failures tagged weak_filtering: {failure_kinds['weak_filtering']} case(s).",
        )
        r += 1
    if failure_kinds.get("ranking_issue", 0):
        add(
            r,
            "Calibrate metadata boosts vs lexical base (esp. commerce in-stock vs format, decade vs generic action).",
            f"Failures tagged ranking_issue: {failure_kinds['ranking_issue']} case(s).",
        )
        r += 1

    # Fill from concrete failed cases
    for c in failed:
        if r > 5:
            break
        if c.get("failure_kind") == "intent_detection":
            add(
                r,
                f"Query `{c['query'][:48]}…` — align detector with expected `{c['expected_intent']}`.",
                f"Detected `{c['detected_intent_category']}` ({c['case_id']}).",
            )
            r += 1

    defaults = [
        (
            "Expose intent label in agent traces for debugging retrieval vs ranking splits.",
            "Isolated harness already records detected vs expected intent.",
        ),
        (
            "Regression-test commerce queries (stock, preorder, premium) against availability + format fields.",
            "Commerce cases are high-risk for v1 customer trust.",
        ),
    ]
    for item, ev in defaults:
        if r > 5:
            break
        add(r, item, ev)
        r += 1

    pad = [
        (
            "Add negative constraints for title-only noise when intent is non-title (e.g. penalize wrong decade).",
            "Decade and year intent harness cases depend on film_year alignment.",
        ),
        (
            "Consider distributor synonyms (e.g. Arrow vs Arrow Video) in label matching.",
            "Label fixtures use studio + title tokens.",
        ),
    ]
    for item, ev in pad:
        if len(items) >= 5:
            break
        add(len(items) + 1, item, ev)

    more = [
        (
            "Document multi-signal queries where v1 should not force a single intent (leave as title_search + facets).",
            "Avoid overfitting the isolated detector to ambiguous shopper phrasing.",
        ),
    ]
    for item, ev in more:
        if len(items) >= 5:
            break
        add(len(items) + 1, item, ev)

    out = items[:5]
    for i, x in enumerate(out, start=1):
        x["rank"] = i
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Intent-aware retrieval harness (isolated).")
    ap.add_argument(
        "--out-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help="Output directory for JSON, CSV, markdown",
    )
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = rows_as_dict_list()
    cases_out: List[Dict[str, Any]] = []
    failure_kinds: Counter[str] = Counter()
    passed_n = 0
    failed_n = 0

    for i, case in enumerate(INTENT_QUERY_CASES, start=1):
        one = _run_case(case, rows)
        one["case_index"] = i
        cases_out.append(one)
        per_path = out_dir / f"intent_query_{case['case_id']}.json"
        with open(per_path, "w", encoding="utf-8") as f:
            json.dump(one, f, indent=2, ensure_ascii=False)

        if one["pass"]:
            passed_n += 1
        else:
            failed_n += 1
            fk = one.get("failure_kind")
            if fk:
                failure_kinds[fk] += 1

    top_5 = _build_top_5_improvements(cases_out, failure_kinds)

    summary = {
        "harness": "intent_aware_retrieval_ranking",
        "schema_version": 1,
        "fixture_row_count": len(rows),
        "query_case_count": len(INTENT_QUERY_CASES),
        "passed": passed_n,
        "failed": failed_n,
        "failure_kind_counts": dict(failure_kinds),
        "top_5_intent_search_improvements_v1": top_5,
        "cases": cases_out,
    }

    summary_path = out_dir / "intent_retrieval_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    csv_path = out_dir / "intent_retrieval_summary.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "case_id",
                "query",
                "expected_intent",
                "detected_intent",
                "intent_pass",
                "best_acceptable_rank",
                "retrieval_pass",
                "pass",
                "failure_kind",
            ]
        )
        for c in cases_out:
            w.writerow(
                [
                    c["case_id"],
                    c["query"],
                    c["expected_intent"],
                    c["detected_intent_category"],
                    c["intent_pass"],
                    c["best_acceptable_rank"] if c["best_acceptable_rank"] is not None else "",
                    c["retrieval_pass"],
                    c["pass"],
                    c.get("failure_kind") or "",
                ]
            )

    lines: List[str] = []
    lines.append("# Intent-aware retrieval harness report\n")
    lines.append(f"- **Fixture rows:** {len(rows)}")
    lines.append(f"- **Queries:** {len(INTENT_QUERY_CASES)}")
    lines.append(f"- **Passed:** {passed_n}")
    lines.append(f"- **Failed:** {failed_n}\n")

    lines.append("## Failure kind counts\n")
    if failure_kinds:
        for k, n in failure_kinds.most_common():
            lines.append(f"- {k}: {n}")
    else:
        lines.append("(none)")

    lines.append("\n## Passed cases\n")
    for c in [x for x in cases_out if x["pass"]]:
        lines.append(
            f"- `{c['case_id']}` `{c['query']!s}` → intent `{c['detected_intent_category']}`, "
            f"best acceptable rank **{c['best_acceptable_rank']}**"
        )

    lines.append("\n## Failed cases\n")
    for c in [x for x in cases_out if not x["pass"]]:
        lines.append(
            f"- `{c['case_id']}` `{c['query']!s}` — expected intent `{c['expected_intent']}`, "
            f"detected `{c['detected_intent_category']}`, best rank {c['best_acceptable_rank']} — "
            f"**{c.get('failure_kind') or 'unknown'}**"
        )

    lines.append("\n## Top 5 highest-value intent-aware search improvements (v1)\n")
    for im in top_5:
        lines.append(f"{im['rank']}. {im['item']} — *{im['evidence']}*")

    lines.append("\n## Artifacts\n")
    lines.append(f"- Per-query JSON: `{out_dir}/intent_query_*.json`")
    lines.append(f"- Summary JSON: `{summary_path}`")
    lines.append(f"- CSV: `{csv_path}`\n")

    report_path = out_dir / "INTENT_RETRIEVAL_HARNESS_REPORT.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")

    print(report_path.read_text(encoding="utf-8"))
    print(f"\nWrote: {summary_path}")
    return 0 if failed_n == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
