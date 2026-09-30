#!/usr/bin/env python3
"""
Draft product descriptions for catalogue barcodes from official distributor sources (dry run: JSON only).

Usage::

    ./venv/bin/python -m jobs.draft_product_descriptions --barcodes 5027035031033,5060974682973
    ./venv/bin/python -m jobs.draft_product_descriptions --barcodes-file tmp/publish_20260930_draft.txt \\
        --compare tmp/descriptions/new_drafts_20260930_descriptions.json

Writes nothing to Shopify or Supabase.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _barcodes(args: argparse.Namespace) -> List[str]:
    raw: List[str] = []
    if args.barcodes:
        raw += [b.strip() for b in args.barcodes.split(",") if b.strip()]
    if args.barcodes_file:
        raw += [l.strip() for l in Path(args.barcodes_file).read_text().splitlines() if l.strip()]
    return list(dict.fromkeys(raw))


def compare_to_reference(records: List[Dict[str, Any]], reference_path: Path) -> Dict[str, Any]:
    from app.services.description_verification_service import normalise

    ref = {r["barcode"]: r for r in json.loads(reference_path.read_text())}
    rows = []
    for rec in records:
        gold = ref.get(rec["barcode"])
        if not gold or rec.get("error"):
            rows.append({"barcode": rec["barcode"], "error": rec.get("error") or "not in reference"})
            continue
        gold_lines = {normalise(x) for x in gold.get("special_features") or []}
        auto_lines = [normalise(x) for x in rec.get("special_features") or []]
        overlap = sum(1 for x in auto_lines if x in gold_lines)
        rows.append({
            "barcode": rec["barcode"],
            "title": rec.get("film_title"),
            "source_type": [gold["source_type"], rec["source_type"]],
            "features_status": [gold["features_status"], rec["features_status"]],
            "synopsis_source": [gold["synopsis_source"], rec["synopsis_source"]],
            "features": [len(gold.get("special_features") or []), len(rec.get("special_features") or [])],
            "feature_lines_in_reference": overlap,
            "source_type_match": gold["source_type"] == rec["source_type"],
            "features_status_match": gold["features_status"] == rec["features_status"],
            "review_reasons": rec.get("review_reasons"),
        })
    ok = [r for r in rows if "error" not in r]
    return {
        "compared": len(ok),
        "source_type_match": sum(r["source_type_match"] for r in ok),
        "features_status_match": sum(r["features_status_match"] for r in ok),
        "both_match": sum(r["source_type_match"] and r["features_status_match"] for r in ok),
        "rows": rows,
    }


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Draft product descriptions (JSON only, no writes).")
    parser.add_argument("--barcodes", default=None)
    parser.add_argument("--barcodes-file", default=None)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--model", default=None)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--out", default=None, help="Output JSON path")
    parser.add_argument("--compare", default=None, help="Reference JSON to compare against")
    parser.add_argument("--no-tmdb", action="store_true", help="Skip TMDB facts for own summaries")
    args = parser.parse_args(argv)

    os.chdir(ROOT)
    from dotenv import load_dotenv

    load_dotenv(args.env_file, override=True)
    from app.clients.supabase_client import create_fresh_client
    from app.services.product_description_drafting_service import run_product_description_drafting

    barcodes = _barcodes(args)
    if not barcodes:
        print("Provide --barcodes and/or --barcodes-file", file=sys.stderr)
        return 1

    tmdb = None
    if not args.no_tmdb and os.getenv("TMDB_API_KEY"):
        from app.clients.tmdb_client import TmdbClient

        tmdb = TmdbClient()

    records = run_product_description_drafting(
        barcodes=barcodes,
        supabase=create_fresh_client(args.env_file),
        model=args.model,
        workers=args.workers,
        tmdb=tmdb,
    )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = Path(args.out or f"tmp/descriptions/auto_drafts_{stamp}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False))

    ok = [r for r in records if not r.get("error")]
    for r in records:
        if r.get("error"):
            print(f"ERROR {r['barcode']}: {r['error']}")
            continue
        print(
            f"{r['barcode']} | {r['source_type']:<20} | {r['features_status']:<9} | "
            f"feats={len(r['special_features']):<2} | syn={r['synopsis_source']:<11} | "
            f"${r['est_cost_usd']:.3f} | {r['film_title'][:32]:<32} | review={','.join(r['review_reasons'])}"
        )
    print(
        f"\nsummary: drafted={len(ok)} errors={len(records) - len(ok)} "
        f"features={dict(Counter(r['features_status'] for r in ok))} "
        f"source={dict(Counter(r['source_type'] for r in ok))} "
        f"needs_review={sum(1 for r in ok if r['needs_review'])} "
        f"cost_usd={sum(r['est_cost_usd'] for r in ok):.3f}"
    )
    print(f"output: {out}")

    if args.compare:
        report = compare_to_reference(records, Path(args.compare))
        cmp_path = out.with_name(out.stem + "_compare.json")
        cmp_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
        print(
            f"compare: {report['both_match']}/{report['compared']} match on source type + features status "
            f"(source_type {report['source_type_match']}, features_status {report['features_status_match']}) -> {cmp_path}"
        )
    return 0 if len(ok) == len(records) else 2


if __name__ == "__main__":
    raise SystemExit(main())
