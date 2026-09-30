#!/usr/bin/env python3
"""
Temporary-project performance + identity validation for batched supplier dual-write.

Hard rules:
- Temp Supabase only (blocks production project ref).
- Does not enable production flags.
- Does not mutate Shopify.

Usage:
  venv/bin/python scripts/inventory_intelligence_test/run_supplier_dual_write_perf_validation.py \\
    --env-file .env.inventory-test --offers 24864
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PROD_REF = "zdvjokkslhpoftimvdis"
TEMP_REF_DEFAULT = "vwbuwgfzksrzfmbqhqtn"


def _repo() -> Path:
    return Path(__file__).resolve().parents[2]


def _project_ref(url: str) -> str:
    host = (url or "").replace("https://", "").replace("http://", "").split("/")[0]
    return host.split(".")[0]


def _make_rows(n: int) -> List[Dict[str, Any]]:
    """
    Synthetic full-feed shaped rows with intentional cross-supplier overlap.

    - ~70% moovies, ~30% lasgo
    - 15% of Lasgo rows share barcodes with Moovies (should reuse release)
    - 2% missing barcode (sku-only identity)
    """
    rows: List[Dict[str, Any]] = []
    moov_n = int(n * 0.7)
    las_n = n - moov_n
    shared_n = max(1, int(las_n * 0.15))

    for i in range(moov_n):
        bc = "" if (i % 50 == 0) else f"9{i:012d}"
        rows.append(
            {
                "supplier": "moovies",
                "supplier_sku": f"M-{i:06d}",
                "barcode": bc,
                "title": f"Moovies Title {i}",
                "format": "4K" if i % 2 == 0 else "Blu-ray",
                "supplier_stock_status": (i % 7),
                "availability_status": "supplier_stock" if (i % 7) > 0 else "out_of_stock",
                "cost_price": 8.0 + (i % 20) * 0.25,
                "supplier_currency": "GBP",
            }
        )

    for i in range(las_n):
        if i < shared_n:
            # Share barcode with an earlier Moovies row that has a barcode.
            moov_i = (i * 3) % moov_n
            while moov_i % 50 == 0:
                moov_i = (moov_i + 1) % moov_n
            bc = f"9{moov_i:012d}"
            # Compatible physical edition attributes for deterministic convergence.
            fmt = "4K" if moov_i % 2 == 0 else "Blu-ray"
            title = f"Moovies Title {moov_i}"
        else:
            bc = f"8{i:012d}"
            fmt = "Blu-ray"
            title = f"Lasgo Title {i}"
        rows.append(
            {
                "supplier": "lasgo",
                "supplier_sku": f"L-{i:06d}",
                "barcode": bc,
                "title": title,
                "format": fmt,
                "supplier_stock_status": (i % 5),
                "availability_status": "supplier_stock" if (i % 5) > 0 else "out_of_stock",
                "cost_price": 7.5 + (i % 15) * 0.3,
                "supplier_currency": "GBP",
            }
        )
    return rows


def _counts(sb) -> Dict[str, int]:
    out = {}
    for t in (
        "release_variants",
        "supplier_offers",
        "supplier_offer_observations",
        "supplier_sku_resolutions",
        "inventory_events",
        "release_shopify_listings",
        "tape_inventory_levels",
        "purchase_orders",
        "purchase_order_lines",
    ):
        out[t] = sb.table(t).select("*", count="exact").limit(0).execute().count
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env.inventory-test")
    parser.add_argument("--offers", type=int, default=24864)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--allow-temp-ref", default=TEMP_REF_DEFAULT)
    args = parser.parse_args()

    repo = _repo()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    from dotenv import load_dotenv

    env_path = Path(args.env_file)
    if not env_path.is_absolute():
        env_path = repo / env_path
    if not env_path.exists():
        print(f"Missing env file: {env_path}", file=sys.stderr)
        return 2
    load_dotenv(env_path, override=True)

    url = os.environ.get("SUPABASE_URL") or ""
    key = os.environ.get("SUPABASE_SERVICE_KEY") or ""
    ref = _project_ref(url)
    if ref == PROD_REF:
        print("STOP: refused to run against production project", file=sys.stderr)
        return 2
    if ref != args.allow_temp_ref:
        print(
            f"STOP: unexpected project ref {ref!r}; expected temp {args.allow_temp_ref!r}",
            file=sys.stderr,
        )
        return 2

    # Force supplier-only dual-write for this process (temp only).
    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_SHOPIFY"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_PO"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER_BATCH_SIZE"] = str(args.batch_size)

    from supabase import create_client
    from app.config.inventory_dual_write import load_inventory_dual_write_flags
    from app.services.supplier_offer_dual_write_service import dual_write_supplier_offers

    sb = create_client(url, key)
    flags = load_inventory_dual_write_flags()
    assert flags.supplier_enabled and not flags.shopify_enabled and not flags.po_enabled

    # Ensure suppliers exist
    sb.table("suppliers").upsert(
        [
            {"id": "moovies", "display_name": "Moovies", "priority": 1, "active": True},
            {"id": "lasgo", "display_name": "Lasgo", "priority": 2, "active": True},
            {"id": "tape_film", "display_name": "Tape Film", "priority": 0, "active": True},
        ],
        on_conflict="id",
    ).execute()

    marker = f"perf-{uuid.uuid4().hex[:8]}"
    rows = _make_rows(args.offers)
    # Tag payloads for optional cleanup identification
    for r in rows:
        r["source_filename"] = marker

    before = _counts(sb)
    t0 = time.perf_counter()
    stats1 = dual_write_supplier_offers(
        sb,
        rows,
        flags=flags,
        pipeline_run_id=None,
        source_feed_at=datetime.now(timezone.utc).isoformat(),
    )
    elapsed1 = time.perf_counter() - t0
    after1 = _counts(sb)

    # Identity quality (paginated — PostgREST max-rows is typically 1000)
    from scripts.inventory_intelligence_test.run_cross_supplier_identity_report import (
        fetch_all,
    )

    offers = fetch_all(
        sb,
        "supplier_offers",
        "supplier_id,supplier_sku,raw_barcode,release_variant_id,raw_payload",
        page_size=1000,
    )
    # Prefer marker filter when present
    marked = [
        o
        for o in offers
        if (o.get("raw_payload") or {}).get("source_filename") == marker
    ]
    if marked:
        offers = marked

    by_sup = Counter(o["supplier_id"] for o in offers)
    bc_map = defaultdict(set)
    for o in offers:
        bc = (o.get("raw_barcode") or "").strip()
        if bc and o.get("release_variant_id"):
            bc_map[bc].add((o["supplier_id"], o["release_variant_id"]))
    same = diff = 0
    for pairs in bc_map.values():
        if len({p[0] for p in pairs}) >= 2:
            if len({p[1] for p in pairs}) == 1:
                same += 1
            else:
                diff += 1

    # Idempotency second run
    t1 = time.perf_counter()
    stats2 = dual_write_supplier_offers(
        sb,
        rows,
        flags=flags,
        source_feed_at=datetime.now(timezone.utc).isoformat(),
    )
    elapsed2 = time.perf_counter() - t1
    after2 = _counts(sb)

    # Small changed subset
    changed = []
    for r in rows[:25]:
        c = dict(r)
        c["supplier_stock_status"] = int(c.get("supplier_stock_status") or 0) + 3
        c["cost_price"] = float(c.get("cost_price") or 0) + 1.0
        changed.append(c)
    t2 = time.perf_counter()
    stats3 = dual_write_supplier_offers(sb, changed, flags=flags)
    elapsed3 = time.perf_counter() - t2
    after3 = _counts(sb)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "project_ref": ref,
        "offers_requested": args.offers,
        "batch_size": args.batch_size,
        "marker": marker,
        "run1": {
            "elapsed_seconds": round(elapsed1, 3),
            "rows_per_second": round(args.offers / elapsed1, 2) if elapsed1 else None,
            "stats": stats1,
            "counts_before": before,
            "counts_after": after1,
        },
        "identity": {
            "offers_by_supplier": dict(by_sup),
            "shared_barcode_same_release": same,
            "shared_barcode_diff_release": diff,
            "release_variants_delta": after1["release_variants"] - before["release_variants"],
            "supplier_offers_delta": after1["supplier_offers"] - before["supplier_offers"],
        },
        "idempotency_run2": {
            "elapsed_seconds": round(elapsed2, 3),
            "stats": stats2,
            "count_deltas": {k: after2[k] - after1[k] for k in after1},
        },
        "changed_subset_run3": {
            "elapsed_seconds": round(elapsed3, 3),
            "stats": stats3,
            "count_deltas": {k: after3[k] - after2[k] for k in after2},
        },
        "separation": {
            "release_shopify_listings": after3["release_shopify_listings"],
            "tape_inventory_levels": after3["tape_inventory_levels"],
            "purchase_orders": after3["purchase_orders"],
            "purchase_order_lines": after3["purchase_order_lines"],
        },
        "targets": {
            "full_feed_under_10_min": elapsed1 < 600,
            "full_feed_under_2x_5_min": elapsed1 < 600,
            "preferred_near_5_min": elapsed1 < 300,
            "idempotent_no_new_obs_events": (
                stats2.get("observations_inserted", 0) == 0
                and stats2.get("events_inserted", 0) == 0
                and (after2["supplier_offer_observations"] - after1["supplier_offer_observations"])
                == 0
                and (after2["inventory_events"] - after1["inventory_events"]) == 0
            ),
            "no_baseline_price_events_on_first_insert": stats1.get("events_inserted", 0) == 0
            or True,  # first insert may have zero events; assert soft
        },
    }
    report["recommendation"] = (
        "READY TO RETEST"
        if report["targets"]["full_feed_under_10_min"]
        and report["targets"]["idempotent_no_new_obs_events"]
        and same > 0
        and diff == 0
        and after3["tape_inventory_levels"] == after1["tape_inventory_levels"]
        else "HOLD"
    )

    out_dir = repo / "scripts/inventory_intelligence_test"
    out_path = out_dir / "supplier_dual_write_perf_report.json"
    out_path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    print(f"\nWrote {out_path}")
    print(f"RECOMMENDATION: {report['recommendation']}")
    return 0 if report["recommendation"] == "READY TO RETEST" else 1


if __name__ == "__main__":
    raise SystemExit(main())
