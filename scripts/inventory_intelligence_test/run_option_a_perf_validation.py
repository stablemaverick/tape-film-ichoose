#!/usr/bin/env python3
"""
Option A temp validation: bulk catalog stock updates + post-catalog projection.

Hard rules:
- Temp Supabase only (blocks production ref).
- Does not enable production flags.
- Does not mutate Shopify inventory.

Requires bulk_update_catalog_stock_fields applied on the temp project:
  psql "$DATABASE_URL" -f supabase/migrations/20260807120000_bulk_update_catalog_stock_fields.sql
  (or paste that migration into the temp project's SQL editor)

Usage:
  venv/bin/python scripts/inventory_intelligence_test/run_option_a_perf_validation.py \\
    --env-file .env.inventory-test
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROD_REF = "zdvjokkslhpoftimvdis"
TEMP_REF_DEFAULT = "vwbuwgfzksrzfmbqhqtn"


def _repo() -> Path:
    return Path(__file__).resolve().parents[2]


def _project_ref(url: str) -> str:
    host = (url or "").replace("https://", "").replace("http://", "").split("/")[0]
    return host.split(".")[0]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_rpc(sb: Any) -> Dict[str, Any]:
    try:
        resp = sb.rpc("bulk_update_catalog_stock_fields", {"payload": []}).execute()
        return {"ok": True, "data": resp.data}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _seed_catalog_rows(sb: Any, n: int, *, supplier: str = "moovies") -> List[str]:
    """Insert/upsert N catalog_items rows for bulk update timing. Returns ids."""
    ids: List[str] = []
    batch: List[Dict[str, Any]] = []
    for i in range(n):
        cid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"option-a-perf/{supplier}/{i}"))
        ids.append(cid)
        batch.append(
            {
                "id": cid,
                "title": f"OptionA Perf Title {i}",
                "barcode": f"88{i:011d}",
                "supplier": supplier,
                "active": True,
                "supplier_stock_status": 0,
                "availability_status": "supplier_out",
                "cost_price": 1.0,
                "calculated_sale_price": 2.0,
                "supplier_sku": f"OPT-A-{i:06d}",
            }
        )
        if len(batch) >= 200:
            sb.table("catalog_items").upsert(batch, on_conflict="id").execute()
            batch = []
    if batch:
        sb.table("catalog_items").upsert(batch, on_conflict="id").execute()
    return ids


def _build_update_rows(
    ids: List[str], *, qty_base: int = 5
) -> List[Tuple[str, Dict[str, Any]]]:
    rows: List[Tuple[str, Dict[str, Any]]] = []
    seen = _now()
    for i, cid in enumerate(ids):
        rows.append(
            (
                cid,
                {
                    "supplier_stock_status": qty_base + (i % 7),
                    "availability_status": "supplier_stock",
                    "cost_price": 10.0 + (i % 20) * 0.25,
                    "calculated_sale_price": 20.0 + (i % 20) * 0.5,
                    "supplier_sku": f"OPT-A-{i:06d}",
                    "supplier_last_seen_at": seen,
                },
            )
        )
    return rows


def _run_bulk(sb: Any, rows: List[Tuple[str, Dict[str, Any]]], batch_size: int) -> Dict[str, Any]:
    from app.services.catalog_upsert_service import RetryStats, apply_stock_sync_row_updates

    stats = RetryStats()
    t0 = time.perf_counter()
    n = apply_stock_sync_row_updates(
        sb, rows, stats=stats, batch_size=batch_size, update_mode="bulk"
    )
    elapsed = time.perf_counter() - t0
    return {
        "mode": "bulk",
        "rows": n,
        "elapsed_s": round(elapsed, 3),
        "ms_per_row": round((elapsed / max(n, 1)) * 1000, 2),
        "retries": stats.retries,
        "batch_size": batch_size,
    }


def _run_per_row_sample(sb: Any, rows: List[Tuple[str, Dict[str, Any]]], sample: int) -> Dict[str, Any]:
    from app.services.catalog_upsert_service import RetryStats, apply_stock_sync_row_updates

    sample_rows = rows[:sample]
    stats = RetryStats()
    t0 = time.perf_counter()
    n = apply_stock_sync_row_updates(
        sb, sample_rows, stats=stats, update_mode="per_row", progress_every=10_000
    )
    elapsed = time.perf_counter() - t0
    return {
        "mode": "per_row",
        "rows": n,
        "elapsed_s": round(elapsed, 3),
        "ms_per_row": round((elapsed / max(n, 1)) * 1000, 2),
        "retries": stats.retries,
        "extrapolated_4685_s": round((elapsed / max(n, 1)) * 4685, 1),
    }


def _count(sb: Any, table: str) -> int:
    return sb.table(table).select("id", count="exact").limit(1).execute().count or 0


def _projection_checks(sb: Any) -> Dict[str, Any]:
    from app.services.supplier_intelligence_projection_service import (
        project_supplier_intelligence_from_batches,
    )

    # Flags OFF → skipped
    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "0"
    skipped = project_supplier_intelligence_from_batches(sb, moovies_batch="none")

    # Flags ON with synthetic rows via dual_write path (post-catalog service load)
    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_SHOPIFY"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_PO"] = "0"

    from app.services.supplier_offer_dual_write_service import dual_write_supplier_offers
    from app.config.inventory_dual_write import load_inventory_dual_write_flags

    flags = load_inventory_dual_write_flags()
    # Small synthetic projection for failure-isolation + idempotency
    synth = []
    for i in range(200):
        synth.append(
            {
                "supplier": "moovies" if i % 2 == 0 else "lasgo",
                "supplier_sku": f"OPT-A-PROJ-{i:05d}",
                "barcode": f"77{i:011d}",
                "title": f"OptionA Projection {i}",
                "format": "Blu-ray",
                "supplier_stock_status": 1,
                "availability_status": "supplier_stock",
                "cost_price": 9.0,
                "supplier_currency": "GBP",
            }
        )
    t0 = time.perf_counter()
    first = dual_write_supplier_offers(sb, synth, flags=flags)
    t1 = time.perf_counter()
    second = dual_write_supplier_offers(sb, synth, flags=flags)
    t2 = time.perf_counter()

    # Restore OFF for safety in process
    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "0"

    return {
        "skipped_when_flags_off": skipped.get("status"),
        "first_write": {
            "elapsed_s": round(t1 - t0, 3),
            "upserted": first.get("upserted"),
            "observations_inserted": first.get("observations_inserted"),
            "events_inserted": first.get("events_inserted"),
            "errors": first.get("errors"),
            "db_requests": first.get("db_requests"),
        },
        "idempotent_replay": {
            "elapsed_s": round(t2 - t1, 3),
            "observations_inserted": second.get("observations_inserted"),
            "events_inserted": second.get("events_inserted"),
            "offers_inserted": second.get("offers_inserted"),
            "errors": second.get("errors"),
        },
        "separation": {
            "release_shopify_listings": _count(sb, "release_shopify_listings"),
            "tape_inventory_levels": _count(sb, "tape_inventory_levels"),
            "purchase_orders": _count(sb, "purchase_orders"),
            "purchase_order_lines": _count(sb, "purchase_order_lines"),
        },
    }


def _failure_isolation(sb: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    # Malformed id must not update unrelated rows
    before = sb.table("catalog_items").select("id,supplier_stock_status").limit(1).execute().data
    try:
        resp = sb.rpc(
            "bulk_update_catalog_stock_fields",
            {
                "payload": [
                    {
                        "id": "not-a-uuid",
                        "supplier_stock_status": 999,
                        "availability_status": "supplier_stock",
                        "cost_price": 1,
                        "calculated_sale_price": 2,
                        "supplier_sku": "X",
                        "supplier_last_seen_at": _now(),
                    }
                ]
            },
        ).execute()
        out["malformed_id"] = {"ok": True, "result": resp.data}
    except Exception as exc:
        out["malformed_id"] = {"ok": False, "error": str(exc)[:200]}
    after = sb.table("catalog_items").select("id,supplier_stock_status").limit(1).execute().data
    out["malformed_id_no_collateral"] = before == after

    # Projection failure isolation (does not raise to caller)
    from app.services.supplier_intelligence_projection_service import (
        project_supplier_intelligence_from_batches,
    )

    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "1"

    def boom(*_a, **_k):
        raise RuntimeError("forced projection failure")

    import app.services.supplier_intelligence_projection_service as proj_mod

    original = proj_mod.dual_write_supplier_offers
    proj_mod.dual_write_supplier_offers = boom  # type: ignore
    try:
        # Need staging rows path — empty batches → skipped/no_batch or failed on load
        failed = project_supplier_intelligence_from_batches(
            sb, moovies_batch="missing-batch-id"
        )
        # With empty staging, dual_write gets [] and succeeds; force via monkeypatch after load
        # Re-run with patched dual_write and non-empty synthetic load by calling dual_write path
        failed2 = {
            "status": "failed",
            "error": "forced projection failure",
        }
        try:
            # Call with patched function through project after seeding one staging-like path:
            # Use empty staging then manually invoke the exception path by wrapping
            from app.services.supplier_intelligence_projection_service import (
                dual_write_supplier_offers as _,
            )
        except Exception:
            pass
        # Directly exercise exception handler by calling project with patched dual_write
        # after injecting rows via Fake — simplest: call dual_write_supplier_offers boom via project
        # by temporarily replacing fetch to return rows
        original_fetch = proj_mod.fetch_staging_offers_for_batches

        def fake_fetch(*_a, **_k):
            return [{"supplier": "moovies", "barcode": "1", "supplier_sku": "x"}]

        proj_mod.fetch_staging_offers_for_batches = fake_fetch  # type: ignore
        failed = project_supplier_intelligence_from_batches(sb, moovies_batch="x")
        proj_mod.fetch_staging_offers_for_batches = original_fetch  # type: ignore
        out["projection_failure"] = failed
    finally:
        proj_mod.dual_write_supplier_offers = original  # type: ignore
        os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "0"
        os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "0"

    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env.inventory-test")
    parser.add_argument("--quiet-rows", type=int, default=400)
    parser.add_argument("--large-rows", type=int, default=4800)
    parser.add_argument("--per-row-sample", type=int, default=50)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--allow-temp-ref",
        default=TEMP_REF_DEFAULT,
        help="Expected temp project ref",
    )
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
        print("REFUSING: production project ref", file=sys.stderr)
        return 2
    if ref != args.allow_temp_ref:
        print(f"REFUSING: unexpected project ref {ref!r}", file=sys.stderr)
        return 2

    # Keep process-level dual-write OFF unless a check explicitly enables temporarily.
    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_SHOPIFY"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_PO"] = "0"
    os.environ["CATALOG_STOCK_UPDATE_MODE"] = "bulk"

    sb = create_client(url, key)
    report: Dict[str, Any] = {
        "generated_at": _now(),
        "project_ref": ref,
        "rpc": None,
        "quiet": None,
        "large": None,
        "per_row_sample": None,
        "projection": None,
        "failure_isolation": None,
        "acceptance": {},
        "recommendation": "HOLD",
    }

    rpc = _ensure_rpc(sb)
    report["rpc"] = rpc
    if not rpc.get("ok"):
        report["acceptance"]["rpc_present"] = False
        report["recommendation"] = "HOLD"
        out = repo / "scripts/inventory_intelligence_test/option_a_perf_report.json"
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        print(
            "\nHOLD: apply supabase/migrations/20260807120000_bulk_update_catalog_stock_fields.sql "
            "on the temp project, then re-run this script.",
            file=sys.stderr,
        )
        return 1

    print(f"Seeding quiet catalog rows n={args.quiet_rows}…")
    quiet_ids = _seed_catalog_rows(sb, args.quiet_rows)
    quiet_rows = _build_update_rows(quiet_ids)
    report["quiet"] = _run_bulk(sb, quiet_rows, args.batch_size)

    print(f"Seeding large catalog rows n={args.large_rows}…")
    large_ids = _seed_catalog_rows(sb, args.large_rows, supplier="lasgo")
    large_rows = _build_update_rows(large_ids, qty_base=3)
    report["large"] = _run_bulk(sb, large_rows, args.batch_size)

    print(f"Per-row sample n={args.per_row_sample}…")
    report["per_row_sample"] = _run_per_row_sample(sb, large_rows, args.per_row_sample)

    print("Projection + idempotency checks…")
    report["projection"] = _projection_checks(sb)

    print("Failure isolation…")
    report["failure_isolation"] = _failure_isolation(sb)

    large_ms = (report["large"] or {}).get("ms_per_row") or 999
    large_elapsed = (report["large"] or {}).get("elapsed_s") or 999
    per_row_ms = (report["per_row_sample"] or {}).get("ms_per_row") or 0
    idem = report["projection"]["idempotent_replay"]
    sep = report["projection"]["separation"]
    proj_fail = report["failure_isolation"].get("projection_failure") or {}

    accept = {
        "rpc_present": True,
        "bulk_not_linear_220ms": large_ms < 50,
        "large_under_120s": large_elapsed < 120,
        "bulk_faster_than_per_row": large_ms < max(per_row_ms * 0.25, 1),
        "idempotent_no_new_obs": (idem.get("observations_inserted") or 0) == 0,
        "idempotent_no_new_events": (idem.get("events_inserted") or 0) == 0,
        "no_shopify_tape_po": all(v == 0 for v in sep.values()),
        "projection_failure_isolated": proj_fail.get("status") == "failed",
        "malformed_id_safe": bool(
            report["failure_isolation"].get("malformed_id_no_collateral")
        ),
    }
    report["acceptance"] = accept
    report["recommendation"] = (
        "READY TO RETEST" if all(accept.values()) else "HOLD"
    )

    out = repo / "scripts/inventory_intelligence_test/option_a_perf_report.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nWrote {out}")
    print(f"RECOMMENDATION: {report['recommendation']}")
    return 0 if report["recommendation"] == "READY TO RETEST" else 1


if __name__ == "__main__":
    raise SystemExit(main())
