#!/usr/bin/env python3
"""
Isolated Moovies film bundle harness — classifier + isolated matcher only (no enrichment).

- Proves strong bundle titles are blocked from naive single-film TMDB linking.
- Flags plain “Trilogy” probes as known heuristic gaps (false negatives for blocking).

Uses mocked TMDB HTTP when the flow would otherwise call the API (probes).

Usage:
  python tests/tmdb_harness/run_moovies_film_bundle_harness.py
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import Mock, patch

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.helpers.tmdb_match_helpers import is_collection_or_bundle  # noqa: E402
from app.services.tmdb_isolated_match_service import (  # noqa: E402
    classify_collection_listing,
    label_film_collection_harness,
    search_tmdb_catalog_isolated,
)

from tests.tmdb_harness.moovies_film_bundle_harness_titles import Tag, all_harness_rows  # noqa: E402

OUTPUT_ROOT = Path(__file__).resolve().parent / "film_bundle_harness_output"
SUMMARY_JSON = OUTPUT_ROOT / "moovies_film_bundle_harness_summary.json"
REPORT_TXT = OUTPUT_ROOT / "MOOVIES_FILM_BUNDLE_HARNESS_REPORT.txt"


def _empty_tmdb_response() -> Mock:
    m = Mock()
    m.raise_for_status = Mock()
    m.json.return_value = {"results": [], "page": 1, "total_results": 0}
    return m


def _run_one(title: str, tag: Tag) -> Dict[str, Any]:
    icob = is_collection_or_bundle(title)
    clf = classify_collection_listing(title)
    label = label_film_collection_harness(clf)
    blocked_by_classifier = bool(clf.get("block_single_film_match"))

    # Strong bundles: must not hit TMDB.
    if tag == "strong_bundle":
        with patch("app.services.tmdb_isolated_match_service.requests.get") as mock_get:
            out = search_tmdb_catalog_isolated(
                title,
                "dummy-key",
                "https://api.themoviedb.org/3",
                source_year=None,
                edition_title="",
            )
            mock_get.assert_not_called()
        unexpected = out["status"] != "blocked" or not blocked_by_classifier or label != "blocked_collection_candidate"
    else:
        # Probes: flow may call TMDB — return empty, expect not blocked at isolated matcher gate.
        with patch("app.services.tmdb_isolated_match_service.requests.get") as mock_get:
            mock_get.return_value = _empty_tmdb_response()
            out = search_tmdb_catalog_isolated(
                title,
                "dummy-key",
                "https://api.themoviedb.org/3",
                source_year=None,
                edition_title="",
            )
            assert mock_get.called, "probe should reach TMDB search with current heuristics"
        unexpected = out["status"] == "blocked"

    gap_kind: str | None = None
    if tag == "strong_bundle" and (label == "single_film_safe" or out["status"] != "blocked"):
        gap_kind = "unexpected_non_block_strong_bundle"
    if tag == "plain_trilogy_probe" and label == "single_film_safe":
        gap_kind = "known_plain_trilogy_gap"

    return {
        "title": title,
        "tag": tag,
        "is_collection_or_bundle": icob,
        "classifier": clf,
        "harness_label": label,
        "isolated_status": out["status"],
        "isolated_failure_detail": out.get("failure_detail", ""),
        "blocked_from_naive_single_film_link": out["status"] == "blocked",
        "unexpected_vs_expectation": unexpected,
        "gap_note": gap_kind,
    }


def main() -> int:
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    rows_out: List[Dict[str, Any]] = []
    strong_ok = 0
    strong_fail = 0
    probes_plain = 0

    for i, (title, tag) in enumerate(all_harness_rows(), start=1):
        row = _run_one(title, tag)
        row["case_id"] = f"fb_{i:02d}"
        rows_out.append(row)
        if tag == "strong_bundle":
            if row["blocked_from_naive_single_film_link"] and not row["unexpected_vs_expectation"]:
                strong_ok += 1
            else:
                strong_fail += 1
        else:
            probes_plain += 1

    summary = {
        "harness": "moovies_film_bundle_isolated",
        "schema_version": 1,
        "counts": {
            "total": len(rows_out),
            "strong_bundle_expected": len([r for r in rows_out if r["tag"] == "strong_bundle"]),
            "strong_bundle_blocked_ok": strong_ok,
            "strong_bundle_mismatch": strong_fail,
            "plain_trilogy_probes": probes_plain,
        },
        "cases": rows_out,
    }
    with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    lines: List[str] = []
    lines.append("Moovies film bundle harness (isolated classifier + search_tmdb_catalog_isolated)")
    lines.append("")
    lines.append(
        f"Strong bundle titles (expect block): {strong_ok}/"
        f"{summary['counts']['strong_bundle_expected']} passed  |  mismatches: {strong_fail}"
    )
    lines.append(f"Plain trilogy probes (document gap): {probes_plain}")
    lines.append("")
    lines.append(f"{'id':<8} {'tag':<20} {'blocked':<8} {'label':<28}  title")
    lines.append("-" * 110)
    for r in rows_out:
        blk = "yes" if r["blocked_from_naive_single_film_link"] else "no"
        lines.append(
            f"{r['case_id']:<8} {r['tag']:<20} {blk:<8} {r['harness_label']:<28}  {r['title'][:55]}"
        )
    lines.append("")
    lines.append("Strong-bundle mismatches (should be empty):")
    for r in rows_out:
        if r["tag"] != "strong_bundle":
            continue
        if r["blocked_from_naive_single_film_link"] and not r["unexpected_vs_expectation"]:
            continue
        lines.append(f"  {r['case_id']}: {r['title']!r}")
        lines.append(f"    classifier={r['classifier']!r} isolated={r['isolated_status']!r}")
    if strong_fail == 0:
        lines.append("  (none)")
    lines.append("")
    lines.append("Plain “Trilogy” probes — known gap (no collection / box / N-film in title):")
    for r in rows_out:
        if r["tag"] != "plain_trilogy_probe":
            continue
        lines.append(f"  {r['case_id']}: {r['title']!r}")
        lines.append(
            f"    blocked={r['blocked_from_naive_single_film_link']}  label={r['harness_label']!r}  "
            f"is_collection_or_bundle={r['is_collection_or_bundle']}"
        )
    lines.append("")
    lines.append(f"JSON: {SUMMARY_JSON}")
    REPORT_TXT.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    return 0 if strong_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
