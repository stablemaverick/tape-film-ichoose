#!/usr/bin/env python3
"""
Complete cross-supplier identity report for supplier dual-write (temp project only).

Uses explicit PostgREST pagination (range) — never assumes a single page covers all rows.

Usage:
  venv/bin/python scripts/inventory_intelligence_test/run_cross_supplier_identity_report.py \\
    --env-file .env.inventory-test
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

PROD_REF = "zdvjokkslhpoftimvdis"
TEMP_REF_DEFAULT = "vwbuwgfzksrzfmbqhqtn"
PAGE_SIZE = 1000


def _repo() -> Path:
    return Path(__file__).resolve().parents[2]


def _project_ref(url: str) -> str:
    host = (url or "").replace("https://", "").replace("http://", "").split("/")[0]
    return host.split(".")[0]


def fetch_all(
    sb: Any,
    table: str,
    columns: str,
    *,
    page_size: int = PAGE_SIZE,
    eq: Optional[Dict[str, Any]] = None,
    order: str = "id",
) -> List[Dict[str, Any]]:
    """Page through an entire table using PostgREST Range headers via .range()."""
    out: List[Dict[str, Any]] = []
    start = 0
    while True:
        end = start + page_size - 1
        q = sb.table(table).select(columns).order(order).range(start, end)
        if eq:
            for col, val in eq.items():
                q = q.eq(col, val)
        resp = q.execute()
        rows = resp.data or []
        out.extend(rows)
        if len(rows) < page_size:
            break
        start += page_size
    return out


def _norm(s: Any) -> str:
    return (str(s).strip().casefold() if s is not None else "")


def _format_family(fmt: Any) -> str:
    t = _norm(fmt)
    if not t:
        return ""
    if "4k" in t or "uhd" in t or "ultra hd" in t:
        return "4k"
    if "blu" in t:
        return "bluray"
    if "dvd" in t:
        return "dvd"
    return t


def _compatible_releases(a: Dict[str, Any], b: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Return (compatible, conflict_reasons). Empty attrs do not conflict."""
    conflicts: List[str] = []
    fa, fb = _format_family(a.get("format")), _format_family(b.get("format"))
    if fa and fb and fa != fb:
        conflicts.append(f"format:{fa}!={fb}")
    ea, eb = _norm(a.get("edition_title") or a.get("edition")), _norm(
        b.get("edition_title") or b.get("edition")
    )
    if ea and eb and ea != eb:
        conflicts.append(f"edition:{ea}!={eb}")
    la, lb = _norm(a.get("studio") or a.get("label")), _norm(b.get("studio") or b.get("label"))
    if la and lb and la != lb:
        conflicts.append(f"studio:{la}!={lb}")
    ra, rb = _norm(a.get("region")), _norm(b.get("region"))
    if ra and rb and ra != rb:
        conflicts.append(f"region:{ra}!={rb}")
    # Release date: only flag if both present and differ by > 30 days roughly via string compare of dates
    da, db = str(a.get("release_date") or ""), str(b.get("release_date") or "")
    if da and db and da[:10] != db[:10]:
        conflicts.append(f"release_date:{da[:10]}!={db[:10]}")
    return (len(conflicts) == 0, conflicts)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env.inventory-test")
    parser.add_argument("--allow-temp-ref", default=TEMP_REF_DEFAULT)
    parser.add_argument(
        "--allow-production-ref",
        default="",
        help="Explicit production project ref required to run against production (safety gate).",
    )
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    args = parser.parse_args()

    repo = _repo()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    from dotenv import load_dotenv
    from supabase import create_client

    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        env_path = repo / env_path
    load_dotenv(env_path, override=True)

    url = os.environ.get("SUPABASE_URL") or ""
    key = os.environ.get("SUPABASE_SERVICE_KEY") or ""
    ref = _project_ref(url)
    if ref == PROD_REF:
        if args.allow_production_ref != PROD_REF:
            print(
                "STOP: production project refused without "
                f"--allow-production-ref {PROD_REF}",
                file=sys.stderr,
            )
            return 2
    elif ref != args.allow_temp_ref:
        print(f"STOP: unexpected ref {ref}", file=sys.stderr)
        return 2
    sb = create_client(url, key)
    page = args.page_size

    offers = fetch_all(
        sb,
        "supplier_offers",
        "id,supplier_id,supplier_sku,raw_barcode,release_variant_id,active,"
        "raw_payload,availability_status",
        page_size=page,
    )
    resolutions = fetch_all(
        sb,
        "supplier_sku_resolutions",
        "id,supplier_id,supplier_sku,raw_barcode,resolved_release_variant_id,"
        "match_method,match_confidence,review_status,active,notes",
        page_size=page,
    )
    releases = fetch_all(
        sb,
        "release_variants",
        "id,primary_barcode,title,format,publication_status,active,catalog_item_id,"
        "film_id,edition_id",
        page_size=page,
    )

    # Coverage exactness vs count headers
    coverage = {}
    for name, rows, table in (
        ("supplier_offers", offers, "supplier_offers"),
        ("supplier_sku_resolutions", resolutions, "supplier_sku_resolutions"),
        ("release_variants", releases, "release_variants"),
    ):
        counted = sb.table(table).select("*", count="exact").limit(0).execute().count
        coverage[name] = {
            "fetched": len(rows),
            "count_exact": counted,
            "complete": len(rows) == counted,
        }

    offers_by_sup = Counter(o["supplier_id"] for o in offers if o.get("active", True))
    res_by_status = Counter(r.get("review_status") for r in resolutions if r.get("active", True))
    res_by_method = Counter(r.get("match_method") for r in resolutions if r.get("active", True))

    release_by_id = {r["id"]: r for r in releases}
    res_by_key = {
        (r["supplier_id"], r["supplier_sku"]): r
        for r in resolutions
        if r.get("active", True)
    }
    # barcode -> offers
    by_barcode: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for o in offers:
        if not o.get("active", True):
            continue
        bc = (o.get("raw_barcode") or "").strip()
        if bc:
            by_barcode[bc].append(o)

    shared_barcodes = {
        bc: rows
        for bc, rows in by_barcode.items()
        if len({r["supplier_id"] for r in rows}) >= 2
    }

    same = 0
    diff = 0
    unresolved = 0
    attr_conflict_excluded = 0
    eligible = 0
    suspected_duplicates: List[Dict[str, Any]] = []
    suspected_false_merges: List[Dict[str, Any]] = []
    conflict_excluded_samples: List[Dict[str, Any]] = []

    for bc, rows in shared_barcodes.items():
        # Prefer one offer per supplier (first)
        by_sid: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            by_sid.setdefault(r["supplier_id"], r)
        if "moovies" not in by_sid or "lasgo" not in by_sid:
            # shared across other suppliers — still count if >=2 resolved
            sids = list(by_sid.keys())
            if len(sids) < 2:
                continue
            a, b = by_sid[sids[0]], by_sid[sids[1]]
        else:
            a, b = by_sid["moovies"], by_sid["lasgo"]

        ra, rb = a.get("release_variant_id"), b.get("release_variant_id")
        if not ra or not rb:
            unresolved += 1
            continue

        rel_a = release_by_id.get(ra) or {}
        rel_b = release_by_id.get(rb) or {}
        # Also compare offer payloads for format hints
        pa = a.get("raw_payload") or {}
        pb = b.get("raw_payload") or {}
        synth_a = {
            "format": rel_a.get("format") or pa.get("format"),
            "title": rel_a.get("title") or pa.get("title"),
            "edition_title": pa.get("edition_title") or pa.get("edition"),
            "studio": pa.get("studio") or pa.get("label"),
            "region": pa.get("region"),
            "release_date": pa.get("release_date") or pa.get("media_release_date"),
        }
        synth_b = {
            "format": rel_b.get("format") or pb.get("format"),
            "title": rel_b.get("title") or pb.get("title"),
            "edition_title": pb.get("edition_title") or pb.get("edition"),
            "studio": pb.get("studio") or pb.get("label"),
            "region": pb.get("region"),
            "release_date": pb.get("release_date") or pb.get("media_release_date"),
        }
        compatible, conflicts = _compatible_releases(synth_a, synth_b)

        if ra == rb:
            eligible += 1
            same += 1
            # False-merge check: compare *offer* attributes, not only the shared release row.
            offer_conflict_a = {
                "format": pa.get("format"),
                "title": pa.get("title"),
                "edition_title": pa.get("edition_title") or pa.get("edition"),
                "studio": pa.get("studio") or pa.get("label"),
                "region": pa.get("region"),
                "release_date": pa.get("release_date") or pa.get("media_release_date"),
            }
            offer_conflict_b = {
                "format": pb.get("format"),
                "title": pb.get("title"),
                "edition_title": pb.get("edition_title") or pb.get("edition"),
                "studio": pb.get("studio") or pb.get("label"),
                "region": pb.get("region"),
                "release_date": pb.get("release_date") or pb.get("media_release_date"),
            }
            _ok, conflicts = _compatible_releases(offer_conflict_a, offer_conflict_b)
            if conflicts:
                suspected_false_merges.append(
                    {
                        "barcode": bc,
                        "release_variant_id": ra,
                        "conflicts": conflicts,
                        "moovies": {
                            "sku": a.get("supplier_sku"),
                            **offer_conflict_a,
                        },
                        "lasgo": {
                            "sku": b.get("supplier_sku"),
                            **offer_conflict_b,
                        },
                    }
                )
        else:
            if not compatible:
                attr_conflict_excluded += 1
                if len(conflict_excluded_samples) < 25:
                    conflict_excluded_samples.append(
                        {
                            "barcode": bc,
                            "conflicts": conflicts,
                            "release_a": ra,
                            "release_b": rb,
                        }
                    )
                continue
            eligible += 1
            diff += 1
            def _res_for(sid: str, sku: str) -> Dict[str, Any]:
                return res_by_key.get((sid, sku)) or {}

            ra_res = _res_for(a["supplier_id"], a["supplier_sku"])
            rb_res = _res_for(b["supplier_id"], b["supplier_sku"])
            suspected_duplicates.append(
                {
                    "barcode": bc,
                    "release_a": {
                        "id": ra,
                        "title": synth_a.get("title"),
                        "format": synth_a.get("format"),
                        "edition": synth_a.get("edition_title"),
                        "studio": synth_a.get("studio"),
                        "region": synth_a.get("region"),
                        "release_date": synth_a.get("release_date"),
                        "supplier": a["supplier_id"],
                        "sku": a.get("supplier_sku"),
                        "match_method": ra_res.get("match_method"),
                        "confidence": ra_res.get("match_confidence"),
                        "review_status": ra_res.get("review_status"),
                    },
                    "release_b": {
                        "id": rb,
                        "title": synth_b.get("title"),
                        "format": synth_b.get("format"),
                        "edition": synth_b.get("edition_title"),
                        "studio": synth_b.get("studio"),
                        "region": synth_b.get("region"),
                        "release_date": synth_b.get("release_date"),
                        "supplier": b["supplier_id"],
                        "sku": b.get("supplier_sku"),
                        "match_method": rb_res.get("match_method"),
                        "confidence": rb_res.get("match_confidence"),
                        "review_status": rb_res.get("review_status"),
                    },
                }
            )

    rate = (same / eligible) if eligible else None

    # Resolver behaviour documentation (from code inspection)
    resolver_sequence = [
        "1. Active prior supplier_sku_resolutions (auto_accepted/manual) wins",
        "2. Exact barcode lookup against release_variants.primary_barcode + variant_identifiers",
        "3. Single barcode candidate → barcode_exact (auto_accept if confidence >= threshold)",
        "4. Multiple barcode candidates → barcode_ambiguous / needs_review",
        "5. No candidate → create supplier_only release (if flag on)",
        "6. Within-batch pending_barcode_release dedupes creates for the same barcode",
        "7. Cross-batch/cross-supplier convergence depends on barcode preload seeing earlier creates",
        "8. Format/edition compatibility is NOT currently checked before barcode_exact merge",
        "9. Offer identity remains (supplier_id, supplier_sku); release_variant_id is canonical",
    ]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project_ref": ref,
        "root_cause_1000_row_limit": {
            "cause": (
                "PostgREST/Supabase default max-rows is 1000. "
                "The prior report used .limit(100000) and/or a single .execute() page, "
                "which cannot exceed the server max-rows. "
                "Fallback 'recent rows' path also returned at most one page (~1000), "
                "and only Moovies rows (listed first in the synthetic feed) were counted."
            ),
            "fix": "fetch_all() with .range(start, end) pagination until short page",
        },
        "coverage": coverage,
        "offers_by_supplier": dict(offers_by_sup),
        "resolutions_by_status": dict(res_by_status),
        "resolutions_by_method": dict(res_by_method),
        "shared_barcodes_total": len(shared_barcodes),
        "shared_barcode_same_release": same,
        "shared_barcode_diff_release": diff,
        "shared_barcode_unresolved": unresolved,
        "shared_barcode_attr_conflict_excluded": attr_conflict_excluded,
        "shared_barcode_eligible_for_deterministic_comparison": eligible,
        "shared_barcode_same_release_rate": rate,
        "suspected_duplicate_releases_count": len(suspected_duplicates),
        "suspected_duplicate_releases_sample": suspected_duplicates[:40],
        "suspected_false_merges_count": len(suspected_false_merges),
        "suspected_false_merges_sample": suspected_false_merges[:40],
        "attr_conflict_excluded_sample": conflict_excluded_samples,
        "resolver_sequence_as_implemented": resolver_sequence,
        "needs_review_count": res_by_status.get("needs_review", 0),
    }

    # Recommendation gates (identity-only; perf already known green)
    complete = all(v["complete"] for v in coverage.values())
    material_dupes = len(suspected_duplicates) > max(10, int(0.01 * max(eligible, 1)))
    material_false = len(suspected_false_merges) > max(10, int(0.01 * max(eligible, 1)))
    converges = rate is not None and rate >= 0.95 and eligible > 0

    if complete and converges and not material_dupes and not material_false:
        recommendation = "READY TO RETEST"
    else:
        recommendation = "HOLD"

    report["production_readiness_gates"] = {
        "coverage_complete": complete,
        "convergence_rate_ge_95pct": bool(converges),
        "no_material_duplicate_pattern": not material_dupes,
        "no_material_false_merge_pattern": not material_false,
        "eligible_shared_barcodes": eligible,
    }
    report["recommendation"] = recommendation

    out = repo / "scripts/inventory_intelligence_test/cross_supplier_identity_report.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {out}")
    print(f"RECOMMENDATION: {recommendation}")
    return 0 if recommendation == "READY TO RETEST" else 1


if __name__ == "__main__":
    raise SystemExit(main())
