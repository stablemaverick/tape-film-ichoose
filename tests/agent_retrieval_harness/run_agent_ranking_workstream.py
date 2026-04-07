#!/usr/bin/env python3
"""
Tape Agent v1 — isolated media retrieval ranking / disambiguation workstream.

Compares **baseline** lexical scoring (``retrieval_engine``) vs **v2** isolated ranking
(``media_ranking_v2``) with per-component score breakdowns. Does not modify production.

Usage:
  python3 tests/agent_retrieval_harness/run_agent_ranking_workstream.py
  python3 tests/agent_retrieval_harness/run_agent_ranking_workstream.py --stress
  python3 tests/agent_retrieval_harness/run_agent_ranking_workstream.py --top-k 15
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.agent_retrieval_harness.catalog_sample_rows import FIXTURE_ROWS, rows_by_id  # noqa: E402
from tests.agent_retrieval_harness.media_ranking_v2 import (  # noqa: E402
    _df_by_token,
    rank_with_scorer,
)
from tests.agent_retrieval_harness.query_cases import (  # noqa: E402
    OPTIONAL_STRESS_CASES,
    TOP1_EVAL_CASES,
)
from tests.agent_retrieval_harness.ranking_failure_buckets import (  # noqa: E402
    classify_ranking_failure,
)
from tests.agent_retrieval_harness.retrieval_engine import find_rank  # noqa: E402

OUTPUT_DIR = Path(__file__).resolve().parent / "ranking_workstream_output"


def _trim_ranked(ranked: List[Dict[str, Any]], limit: int = 25) -> List[Dict[str, Any]]:
    out = []
    for item in ranked[:limit]:
        x = dict(item)
        # Keep score_breakdown; drop huge nested if any
        out.append(x)
    return out


def _run_case(
    query: str,
    expected_id: str,
    top_k: int,
    df: Dict[str, int],
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    by_id = rows_by_id()
    expected_row = by_id[expected_id]

    baseline_ranked = rank_with_scorer(query, rows, mode="baseline", df=df)
    v2_ranked = rank_with_scorer(query, rows, mode="v2", df=df)

    rb = find_rank(expected_id, baseline_ranked)
    r2 = find_rank(expected_id, v2_ranked)

    pass_topk_baseline = rb is not None and rb <= top_k
    pass_topk_v2 = r2 is not None and r2 <= top_k
    pass_top1_baseline = rb == 1
    pass_top1_v2 = r2 == 1

    winner_b = baseline_ranked[0] if baseline_ranked else None
    winner_v = v2_ranked[0] if v2_ranked else None

    fail_bucket = ""
    if not pass_top1_v2:
        fail_bucket = classify_ranking_failure(query, expected_row, r2, winner_v)

    notes: List[str] = []
    if not pass_top1_v2:
        notes.append(f"v2_top1_fail: {fail_bucket or 'unknown'}")
        if pass_top1_baseline and not pass_top1_v2:
            notes.append("regression vs baseline top-1 (unexpected — review v2 weights)")
        if rb != r2:
            notes.append(f"rank_delta baseline→v2: {rb} → {r2}")
    else:
        if not pass_top1_baseline and pass_top1_v2:
            notes.append("v2_fixed_top1_vs_baseline")

    return {
        "query": query,
        "expected_catalog_id": expected_id,
        "expected_title": expected_row["title"],
        "top_k_threshold": top_k,
        "baseline": {
            "expected_rank": rb,
            "pass_top_1": pass_top1_baseline,
            "pass_top_k": pass_topk_baseline,
            "winner_catalog_id": winner_b["catalog_id"] if winner_b else None,
            "winner_score": winner_b["score"] if winner_b else None,
            "ranked_results": _trim_ranked(baseline_ranked),
        },
        "v2_isolated": {
            "expected_rank": r2,
            "pass_top_1": pass_top1_v2,
            "pass_top_k": pass_topk_v2,
            "winner_catalog_id": winner_v["catalog_id"] if winner_v else None,
            "winner_score": winner_v["score"] if winner_v else None,
            "ranked_results": _trim_ranked(v2_ranked),
        },
        "comparison": {
            "expected_rank_before": rb,
            "expected_rank_after": r2,
            "top1_pass_before": pass_top1_baseline,
            "top1_pass_after": pass_top1_v2,
            "topk_pass_before": pass_topk_baseline,
            "topk_pass_after": pass_topk_v2,
        },
        "failure_bucket_v2_top1": fail_bucket or None,
        "notes": "; ".join(notes) if notes else "ok",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Agent ranking / disambiguation workstream (isolated).")
    ap.add_argument("--top-k", type=int, default=15, metavar="K", help="Top-K recall threshold (default 15).")
    ap.add_argument(
        "--stress",
        action="store_true",
        help="Append OPTIONAL_STRESS_CASES (franchise ambiguity) to show baseline vs v2 delta.",
    )
    args = ap.parse_args()
    top_k = max(1, args.top_k)

    rows = list(FIXTURE_ROWS)
    df = _df_by_token(rows)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    eval_cases = list(TOP1_EVAL_CASES)
    if args.stress:
        eval_cases = eval_cases + list(OPTIONAL_STRESS_CASES)

    cases: List[Dict[str, Any]] = []
    for i, (query, expected_id) in enumerate(eval_cases, start=1):
        c = _run_case(query, expected_id, top_k, df, rows)
        c["case_index"] = i
        cases.append(c)
        with open(OUTPUT_DIR / f"rank_query_{i:02d}.json", "w", encoding="utf-8") as f:
            json.dump(c, f, indent=2, ensure_ascii=False)

    before_top1 = sum(1 for c in cases if c["comparison"]["top1_pass_before"])
    after_top1 = sum(1 for c in cases if c["comparison"]["top1_pass_after"])
    before_topk = sum(1 for c in cases if c["comparison"]["topk_pass_before"])
    after_topk = sum(1 for c in cases if c["comparison"]["topk_pass_after"])

    compare_rows = [
        {
            "query": c["query"],
            "expected_id": c["expected_catalog_id"],
            "rank_baseline": c["comparison"]["expected_rank_before"],
            "rank_v2": c["comparison"]["expected_rank_after"],
            "top1_before": c["comparison"]["top1_pass_before"],
            "top1_after": c["comparison"]["top1_pass_after"],
        }
        for c in cases
    ]

    fail_buckets = Counter(
        c["failure_bucket_v2_top1"]
        for c in cases
        if c.get("failure_bucket_v2_top1")
    )

    remaining_hard = [c["query"] for c in cases if not c["comparison"]["top1_pass_after"]]

    summary = {
        "workstream": "tape_agent_v1_ranking_disambiguation",
        "schema_version": 1,
        "fixture_row_count": len(rows),
        "query_count": len(eval_cases),
        "included_optional_stress": bool(args.stress),
        "top_k_threshold": top_k,
        "rates": {
            "top1_pass_rate_baseline": round(before_top1 / len(cases), 4) if cases else 0.0,
            "top1_pass_rate_v2_isolated": round(after_top1 / len(cases), 4) if cases else 0.0,
            "topk_pass_rate_baseline": round(before_topk / len(cases), 4) if cases else 0.0,
            "topk_pass_rate_v2_isolated": round(after_topk / len(cases), 4) if cases else 0.0,
            "top1_pass_count_baseline": before_top1,
            "top1_pass_count_v2": after_top1,
        },
        "before_after_comparison": compare_rows,
        "failure_bucket_counts_v2_top1": dict(fail_buckets),
        "remaining_hard_failures_top1_v2": remaining_hard,
        "cases": cases,
    }

    summary_path = OUTPUT_DIR / "ranking_workstream_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    csv_path = OUTPUT_DIR / "ranking_workstream_summary.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "query",
                "expected_id",
                "rank_baseline",
                "rank_v2",
                "top1_baseline",
                "top1_v2",
                "failure_bucket_v2",
            ]
        )
        for c in cases:
            w.writerow(
                [
                    c["query"],
                    c["expected_catalog_id"],
                    c["comparison"]["expected_rank_before"],
                    c["comparison"]["expected_rank_after"],
                    c["comparison"]["top1_pass_before"],
                    c["comparison"]["top1_pass_after"],
                    c.get("failure_bucket_v2_top1") or "",
                ]
            )

    # Markdown report
    lines: List[str] = []
    lines.append("# Tape Agent v1 — ranking / disambiguation workstream (isolated)\n")
    lines.append("## Headline rates\n")
    lines.append(
        f"| Metric | Baseline (lexical) | V2 isolated |\n"
        f"|--------|--------------------|-------------|\n"
        f"| Top-1 pass rate | **{before_top1}/{len(cases)}** ({summary['rates']['top1_pass_rate_baseline']:.0%}) | "
        f"**{after_top1}/{len(cases)}** ({summary['rates']['top1_pass_rate_v2_isolated']:.0%}) |\n"
        f"| Top-{top_k} pass rate | **{before_topk}/{len(cases)}** | **{after_topk}/{len(cases)}** |\n"
    )
    if args.stress:
        lines.append(
            "\n**Note:** `--stress` appended franchise-ambiguity queries; headline rates include those rows.\n"
        )
        stress_rows = [c for c in cases if c["query"] == "ghost in the shell"]
        if stress_rows:
            s = stress_rows[0]
            lines.append("\n### Franchise stress case (`ghost in the shell` → SAC_2045)\n")
            lines.append(
                f"- Baseline winner: `{s['baseline']['winner_catalog_id']}` at rank 1; "
                f"expected row at rank **{s['comparison']['expected_rank_before']}**.\n"
                f"- V2 winner: `{s['v2_isolated']['winner_catalog_id']}`; "
                f"expected row at rank **{s['comparison']['expected_rank_after']}**.\n"
            )

    lines.append("\n## Passed top-1 (v2)\n")
    for c in cases:
        if c["comparison"]["top1_pass_after"]:
            lines.append(f"- `{c['query']}` → `{c['expected_catalog_id']}` (baseline rank {c['comparison']['expected_rank_before']})")

    lines.append("\n## Failed top-1 (v2)\n")
    v2_fails = [c for c in cases if not c["comparison"]["top1_pass_after"]]
    if not v2_fails:
        lines.append("(none)")
    else:
        for c in v2_fails:
            lines.append(
                f"- `{c['query']}` — bucket: **{c.get('failure_bucket_v2_top1') or 'n/a'}** — "
                f"baseline rank {c['comparison']['expected_rank_before']}, v2 rank {c['comparison']['expected_rank_after']}"
            )

    lines.append("\n## Remaining hard failures (v2 top-1)\n")
    lines.append(", ".join(f"`{x}`" for x in remaining_hard) if remaining_hard else "(none)")

    lines.append("\n## Failure buckets (v2 top-1 misses)\n")
    for b, n in fail_buckets.most_common():
        lines.append(f"- {b}: {n}")

    lines.append("\n## Low-risk ranking improvements for production later\n")
    lines.append(
        "- **Franchise gates:** apply subtype penalties only within the same franchise head "
        "(e.g. GITS) — matches isolated v2.\n"
        "- **Year-marked SKUs:** down-rank parenthetical year when the query omits a year "
        "(feature-film disambiguation).\n"
        "- **Rare-token / IDF boosts:** up-weight distinctive tokens that are rare in the catalog.\n"
        "- **Season/part alignment:** reward `part N` / `season N` agreement between query and title.\n"
    )

    lines.append("\n## Defer until after v1 launch\n")
    lines.append(
        "- Learned cross-encoder reranker over catalog embeddings.\n"
        "- Full alias graph (anime English/Japanese title pairs) maintained in CMS.\n"
        "- User-specific click priors / session context.\n"
    )

    lines.append("\n## Recommendation — minimal rules worth promoting for v1\n")
    lines.append(
        "1. **GITS-style franchise disambiguation:** when the query includes `sac` / `2045`, "
        "suppress competing `Ghost in the Shell` SKUs that lack those subtype tokens; "
        "when the query omits a year, lightly down-rank `(YYYY)` feature-film rows.\n"
        "2. **Part/season agreement:** small boost when `part 2` / `season 3` tokens align.\n"
        "3. **Distinctive token weighting:** modest IDF-style boost for rare overlapping tokens "
        "(e.g. `wombles`, `yokai`, `nozaki`).\n"
        "**Wait:** heavy anime alias tables, ML rerankers, and global score retuning — post-v1.\n"
    )

    lines.append("\n## Artifacts\n")
    lines.append(f"- Per-query: `{OUTPUT_DIR}/rank_query_NN.json` (baseline + v2 + breakdowns)")
    lines.append(f"- Summary: `{summary_path}`")
    lines.append(f"- CSV: `{csv_path}`")
    lines.append(f"- Comparison list embedded in `ranking_workstream_summary.json` → `before_after_comparison`\n")

    report_path = OUTPUT_DIR / "RANKING_WORKSTREAM_REPORT.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")

    with open(OUTPUT_DIR / "before_after_ranking.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "schema_version": 1,
                "rows": compare_rows,
                "rates": summary["rates"],
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    print(report_path.read_text(encoding="utf-8"))
    print(f"\nWrote {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
