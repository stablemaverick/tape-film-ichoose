#!/usr/bin/env python3
"""
Controlled Phase 3b dual-write validation against a temporary Supabase project.

Uses synthetic/replayed inputs only. No production credentials. No Shopify mutations.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StageResult:
    stage: str
    ok: bool
    detail: str
    rows_written: Dict[str, int]
    verifier_passed: bool
    warnings: List[str]
    errors: List[str]


def _sql_execute(sb: Any, sql: str) -> None:
    # no RPC SQL endpoint in this project; use direct table operations elsewhere.
    # This helper remains for clarity and future use.
    raise NotImplementedError("Direct SQL execution is not available via PostgREST here.")


def _ensure_temp_url(url: str) -> None:
    if "zdvjokkslhpoftimvdis" in url:
        raise SystemExit("Refusing to run against production Supabase URL.")


def _create_tables_if_missing(sb: Any) -> None:
    """
    Ensure operational source tables needed by services exist in temp project.
    We use table-insert probes and rely on SQL editor-applied bootstrap/foundation.
    """
    # Shopfiy listings should exist for store sync path.
    # If missing, instruct operator to apply baseline migrations.
    required = [
        "shopify_listings",
        "catalog_items",
        "staging_moovies_raw",
        "staging_lasgo_raw",
        "staging_supplier_offers",
    ]
    missing: List[str] = []
    for t in required:
        try:
            sb.table(t).select("*", count="exact").limit(1).execute()
        except Exception:
            missing.append(t)
    if missing:
        raise SystemExit(
            "Temporary DB missing required operational tables for service replay: "
            + ", ".join(missing)
            + ". Apply baseline schema for these tables in temp project first."
        )


def _table_count(sb: Any, table: str) -> int:
    try:
        return int(sb.table(table).select("id", count="exact").limit(1).execute().count or 0)
    except Exception:
        return -1


def _snapshot_counts(sb: Any) -> Dict[str, int]:
    tables = [
        "catalog_items",
        "shopify_listings",
        "staging_supplier_offers",
        "release_variants",
        "release_shopify_listings",
        "tape_inventory_levels",
        "supplier_offers",
        "supplier_offer_observations",
        "inventory_events",
        "purchase_orders",
        "purchase_order_lines",
    ]
    return {t: _table_count(sb, t) for t in tables}


def _diff_counts(before: Dict[str, int], after: Dict[str, int]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    keys = set(before) | set(after)
    for k in sorted(keys):
        out[k] = int(after.get(k, 0) - before.get(k, 0))
    return out


def _verify(env_file: str, repo: Path) -> Tuple[bool, str]:
    cmd = [
        str(repo / "venv/bin/python"),
        str(repo / "scripts/inventory_intelligence_test/verify_invariants.py"),
        "--env-file",
        env_file,
        "--format",
        "json",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode == 0:
        try:
            payload = json.loads(p.stdout)
            return True, f"passed={payload.get('passed')} failed={payload.get('failed')}"
        except Exception:
            return True, "verifier passed"
    return False, (p.stderr or p.stdout)[-400:]


def _setup_env(repo: Path, env_file: str) -> Dict[str, str]:
    load_dotenv(repo / env_file, override=True)
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_KEY") or "").strip()
    if not url or not key:
        raise SystemExit("Missing SUPABASE_URL/SUPABASE_SERVICE_KEY in temp env file.")
    _ensure_temp_url(url)
    # Force flags ON only for this process.
    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_SHOPIFY"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "1"
    os.environ["INVENTORY_DUAL_WRITE_PO"] = "1"
    os.environ["INVENTORY_TEST_PROFILE"] = "temporary"
    return {"SUPABASE_URL": url, "SUPABASE_SERVICE_KEY": key}


def run() -> int:
    repo = _repo_root()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    env_file = ".env.inventory-test"
    creds = _setup_env(repo, env_file)
    from supabase import create_client

    sb = create_client(creds["SUPABASE_URL"], creds["SUPABASE_SERVICE_KEY"])
    _create_tables_if_missing(sb)

    # Imports after env setup.
    from app.services.normalize_offers_service import normalize_from_lasgo, normalize_from_moovies
    from app.services.purchase_order_dual_write_service import dual_write_purchase_orders
    from app.services.shopify_inventory_sync_service import run_shopify_inventory_sync
    import app.services.shopify_store_sync_service as shopify_store_sync_service
    import app.services.supplier_orders_report_service as supplier_orders_report_service
    from app.services.supplier_po_inbound import SupplierPoLine

    stage_results: List[StageResult] = []
    warnings: List[str] = []
    errors: List[str] = []

    # Baseline before flags ON runs
    baseline = _snapshot_counts(sb)

    # ------------------------------------------------------------------
    # Stage 1: Shopify store sync (real service with replayed synthetic client)
    # ------------------------------------------------------------------
    before = _snapshot_counts(sb)
    fake_calls: List[str] = []

    class FakeShopifyClient:
        def graphql(self, query: str, variables: Dict[str, Any] | None = None) -> Dict[str, Any]:
            q = (query or "").lower()
            if "mutation" in q or "inventorysetquantities" in q:
                fake_calls.append("MUTATION_CALL")
            fake_calls.append("graphql")
            return {}

    def _fake_fetch_shop_currency_code(_client: Any) -> str:
        return "AUD"

    def _fake_fetch_active_products(_client: Any) -> List[Dict[str, Any]]:
        # Includes positive, committed, and oversold/preorder-like rows.
        return [
            {
                "id": "gid://shopify/Product/9100000001",
                "title": "Synthetic Release Alpha",
                "vendor": "SyntheticVendor",
                "status": "ACTIVE",
                "publishedAt": "2026-01-01T00:00:00Z",
                "productType": "Movie",
                "directorMeta": {"value": "Synth Director"},
                "studioMeta": {"value": "Synth Studio"},
                "filmReleasedMeta": {"value": "2020-01-01"},
                "mediaReleaseMeta": {"value": "2026-01-01"},
                "variants": {
                    "nodes": [
                        {
                            "id": "gid://shopify/ProductVariant/9200000001",
                            "title": "4K",
                            "sku": "SYN-ALPHA-SKU",
                            "barcode": "9900000000001",
                            "price": "39.99",
                            "inventoryQuantity": 5,
                            "inventoryPolicy": "DENY",
                            "inventoryItem": {
                                "id": "gid://shopify/InventoryItem/9300000001",
                                "tracked": True,
                                "unitCost": {"amount": "10.00", "currencyCode": "AUD"},
                            },
                        }
                    ]
                },
            },
            {
                "id": "gid://shopify/Product/9100000002",
                "title": "Synthetic Release Beta",
                "vendor": "SyntheticVendor",
                "status": "ACTIVE",
                "publishedAt": "2026-01-01T00:00:00Z",
                "productType": "Movie",
                "directorMeta": {"value": "Synth Director"},
                "studioMeta": {"value": "Synth Studio"},
                "filmReleasedMeta": {"value": "2021-01-01"},
                "mediaReleaseMeta": {"value": "2026-01-01"},
                "variants": {
                    "nodes": [
                        {
                            "id": "gid://shopify/ProductVariant/9200000002",
                            "title": "Blu-ray",
                            "sku": "SYN-BETA-SKU",
                            "barcode": "9900000000002",
                            "price": "29.99",
                            "inventoryQuantity": 2,
                            "inventoryPolicy": "DENY",
                            "inventoryItem": {
                                "id": "gid://shopify/InventoryItem/9300000002",
                                "tracked": True,
                                "unitCost": {"amount": "9.00", "currencyCode": "AUD"},
                            },
                        }
                    ]
                },
            },
            {
                "id": "gid://shopify/Product/9100000003",
                "title": "Synthetic Release Gamma Preorder",
                "vendor": "SyntheticVendor",
                "status": "ACTIVE",
                "publishedAt": "2026-01-01T00:00:00Z",
                "productType": "Movie",
                "directorMeta": {"value": "Synth Director"},
                "studioMeta": {"value": "Synth Studio"},
                "filmReleasedMeta": {"value": "2022-01-01"},
                "mediaReleaseMeta": {"value": "2099-01-01"},
                "variants": {
                    "nodes": [
                        {
                            "id": "gid://shopify/ProductVariant/9200000003",
                            "title": "4K",
                            "sku": "SYN-GAMMA-SKU",
                            "barcode": "9900000000003",
                            "price": "44.99",
                            "inventoryQuantity": -2,
                            "inventoryPolicy": "CONTINUE",
                            "inventoryItem": {
                                "id": "gid://shopify/InventoryItem/9300000003",
                                "tracked": True,
                                "unitCost": {"amount": "12.00", "currencyCode": "AUD"},
                            },
                        }
                    ]
                },
            },
        ]

    # Monkeypatch real service internals for synthetic replay.
    shopify_store_sync_service.ShopifyClient = FakeShopifyClient
    shopify_store_sync_service.fetch_shop_currency_code = _fake_fetch_shop_currency_code
    shopify_store_sync_service.fetch_active_products = _fake_fetch_active_products

    # Ensure temp env values are used.
    result = shopify_store_sync_service.run_shopify_store_sync(
        env_file=str(repo / env_file), dry_run=False
    )
    after = _snapshot_counts(sb)
    rows = _diff_counts(before, after)
    ok_verify, detail_verify = _verify(env_file, repo)
    no_mutation_calls = "MUTATION_CALL" not in fake_calls
    stage_results.append(
        StageResult(
            stage="shopify_store_sync_replayed",
            ok=(result.get("status") == "ok" and no_mutation_calls and ok_verify),
            detail=f"result_status={result.get('status')} no_mutation_calls={no_mutation_calls}",
            rows_written=rows,
            verifier_passed=ok_verify,
            warnings=[],
            errors=[] if no_mutation_calls else ["mutation call detected in fake client"],
        )
    )

    # ------------------------------------------------------------------
    # Stage 2: Shopify inventory-level sync (read/compare path only)
    # ------------------------------------------------------------------
    before = _snapshot_counts(sb)
    inv_result = run_shopify_inventory_sync(
        env_file=str(repo / env_file),
        dry_run=True,
        apply=False,
    )
    after = _snapshot_counts(sb)
    rows = _diff_counts(before, after)
    ok_verify, detail_verify = _verify(env_file, repo)
    stage_results.append(
        StageResult(
            stage="shopify_inventory_sync_dry_run",
            ok=(inv_result.get("status") == "ok" and inv_result.get("apply_enabled") is False and ok_verify),
            detail=f"status={inv_result.get('status')} apply_enabled={inv_result.get('apply_enabled')}",
            rows_written=rows,
            verifier_passed=ok_verify,
            warnings=[],
            errors=[],
        )
    )

    # ------------------------------------------------------------------
    # Stage 3: Moovies normalization + dual-write
    # ------------------------------------------------------------------
    moov_batch = str(uuid.uuid4())
    sb.table("staging_moovies_raw").insert(
        {
            "id": str(uuid.uuid4()),
            "import_batch_id": moov_batch,
            "supplier": "moovies",
            "upsert_key": "seed:SYN-MOOV-NEW-SUPONLY",
            "source_filename": "synthetic_moov.tsv",
            "row_number": 1,
            "raw_title": "Synthetic Supplier Only New",
            "raw_barcode": "9900000000010",
            "raw_format": "Blu-ray",
            "raw_price": "7.50",
            "raw_qty": "6",
            "raw_release": "2026-08-01",
            "raw_sku": "SYN-MOOV-NEW-SUPONLY",
            "raw_studio": "Synth Studio",
            "raw_payload": {"seed": True},
            "imported_at": _now(),
        }
    ).execute()
    before = _snapshot_counts(sb)
    moov_count = normalize_from_moovies(sb, "staging_supplier_offers", moov_batch)
    after = _snapshot_counts(sb)
    rows = _diff_counts(before, after)
    ok_verify, detail_verify = _verify(env_file, repo)
    stage_results.append(
        StageResult(
            stage="normalize_moovies",
            ok=(moov_count >= 1 and ok_verify),
            detail=f"normalized_rows={moov_count}",
            rows_written=rows,
            verifier_passed=ok_verify,
            warnings=[],
            errors=[],
        )
    )

    # ------------------------------------------------------------------
    # Stage 4: Lasgo normalization + dual-write
    # ------------------------------------------------------------------
    las_batch = str(uuid.uuid4())
    sb.table("staging_lasgo_raw").insert(
        {
            "id": str(uuid.uuid4()),
            "import_batch_id": las_batch,
            "source_filename": "synthetic_lasgo.xlsx",
            "row_number": 1,
            "raw_title": "Synthetic Shared Barcode",
            "raw_ean": "9900000000010",
            "raw_format_l2": "4K",
            "raw_free_stock": "In stock",
            "raw_selling_price_sterling": "8.25",
            "raw_release_date": "2026-09-01",
            "raw_payload": {"seed": True},
            "imported_at": _now(),
        }
    ).execute()
    before = _snapshot_counts(sb)
    las_count = normalize_from_lasgo(sb, "staging_supplier_offers", las_batch)
    after = _snapshot_counts(sb)
    rows = _diff_counts(before, after)
    ok_verify, detail_verify = _verify(env_file, repo)
    stage_results.append(
        StageResult(
            stage="normalize_lasgo",
            ok=(las_count >= 1 and ok_verify),
            detail=f"normalized_rows={las_count}",
            rows_written=rows,
            verifier_passed=ok_verify,
            warnings=[],
            errors=[],
        )
    )

    # ------------------------------------------------------------------
    # Stage 5: Repeat identical Moovies input (dedupe behavior)
    # ------------------------------------------------------------------
    before = _snapshot_counts(sb)
    moov_count_repeat = normalize_from_moovies(sb, "staging_supplier_offers", moov_batch)
    after = _snapshot_counts(sb)
    rows = _diff_counts(before, after)
    ok_verify, detail_verify = _verify(env_file, repo)
    # Observations/events should not explode on unchanged data
    repeat_ok = rows.get("supplier_offer_observations", 0) <= 1 and rows.get("inventory_events", 0) <= 1
    stage_results.append(
        StageResult(
            stage="normalize_moovies_repeat_identical",
            ok=(moov_count_repeat >= 1 and ok_verify and repeat_ok),
            detail=f"normalized_rows={moov_count_repeat} obs_delta={rows.get('supplier_offer_observations')} evt_delta={rows.get('inventory_events')}",
            rows_written=rows,
            verifier_passed=ok_verify,
            warnings=[],
            errors=[] if repeat_ok else ["unexpected duplicate growth on repeat input"],
        )
    )

    # ------------------------------------------------------------------
    # Stage 6: Purchase-order ingestion dual-write (service path)
    # ------------------------------------------------------------------
    po_lines = [
        SupplierPoLine(
            order_id="SYN-PO-RUN-OPEN",
            sku="SYN-MOOV-DELTA",
            title="Synthetic Release Delta Dual Supplier",
            qty=5,
            unit_cost="12.50",
            line_total="62.50",
            status="pre-order",
            sku_key="syn-moov-delta",
            title_key="synthetic release delta dual supplier",
        ),
        SupplierPoLine(
            order_id="SYN-PO-RUN-PARTIAL",
            sku="SYN-MOOV-ALPHA",
            title="Synthetic Release Alpha",
            qty=4,
            unit_cost="12.00",
            line_total="48.00",
            status="picking",
            sku_key="syn-moov-alpha",
            title_key="synthetic release alpha",
        ),
    ]
    before = _snapshot_counts(sb)
    po_stats = dual_write_purchase_orders(
        sb,
        po_lines,
        supplier_label="moovies",
        source_filename="synthetic_po_replay.csv",
        location_id="gid://shopify/Location/9400000001",
    )
    after = _snapshot_counts(sb)
    rows = _diff_counts(before, after)
    ok_verify, detail_verify = _verify(env_file, repo)
    stage_results.append(
        StageResult(
            stage="purchase_order_dual_write",
            ok=(po_stats.get("errors", 0) == 0 and ok_verify),
            detail=f"stats={po_stats}",
            rows_written=rows,
            verifier_passed=ok_verify,
            warnings=[],
            errors=[],
        )
    )

    # ------------------------------------------------------------------
    # Stage 7: stale/failed feed guard
    # ------------------------------------------------------------------
    from app.services.supplier_offer_dual_write_service import dual_write_supplier_offers
    from app.config.inventory_dual_write import load_inventory_dual_write_flags

    flags = load_inventory_dual_write_flags()
    stale_rows = [
        {
            "supplier": "moovies",
            "supplier_sku": f"SYN-STALE-{i}",
            "barcode": f"99000000001{i:02d}",
            "availability_status": "supplier_out",
            "supplier_stock_status": 0,
            "cost_price": 7.0,
            "supplier_currency": "GBP",
            "title": f"Synthetic stale {i}",
            "format": "Blu-ray",
        }
        for i in range(60)
    ]
    before = _snapshot_counts(sb)
    stale_stats = dual_write_supplier_offers(
        sb,
        stale_rows,
        flags=flags,
        pipeline_failed_or_stale=True,
        source_feed_at=_now(),
        pipeline_completed_at=_now(),
    )
    after = _snapshot_counts(sb)
    rows = _diff_counts(before, after)
    ok_verify, detail_verify = _verify(env_file, repo)
    stale_ok = bool(stale_stats.get("blocked_mass_unavailable"))
    stage_results.append(
        StageResult(
            stage="supplier_stale_feed_guard",
            ok=(stale_ok and ok_verify),
            detail=f"stats={stale_stats}",
            rows_written=rows,
            verifier_passed=ok_verify,
            warnings=[],
            errors=[] if stale_ok else ["stale-feed mass unavailable was not blocked"],
        )
    )

    # Final flags OFF confirmation (process-local cleanup)
    os.environ["INVENTORY_DUAL_WRITE_ENABLED"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_SHOPIFY"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_SUPPLIER"] = "0"
    os.environ["INVENTORY_DUAL_WRITE_PO"] = "0"

    final = _snapshot_counts(sb)
    growth = _diff_counts(baseline, final)

    report = {
        "generated_at": _now(),
        "environment": {
            "supabase_url": creds["SUPABASE_URL"],
            "project_is_production": "zdvjokkslhpoftimvdis" in creds["SUPABASE_URL"],
            "validation_profile": os.getenv("INVENTORY_TEST_PROFILE", ""),
        },
        "flags": {
            "enabled_during_run": {
                "INVENTORY_DUAL_WRITE_ENABLED": "1",
                "INVENTORY_DUAL_WRITE_SHOPIFY": "1",
                "INVENTORY_DUAL_WRITE_SUPPLIER": "1",
                "INVENTORY_DUAL_WRITE_PO": "1",
            },
            "disabled_after_run": {
                "INVENTORY_DUAL_WRITE_ENABLED": os.getenv("INVENTORY_DUAL_WRITE_ENABLED"),
                "INVENTORY_DUAL_WRITE_SHOPIFY": os.getenv("INVENTORY_DUAL_WRITE_SHOPIFY"),
                "INVENTORY_DUAL_WRITE_SUPPLIER": os.getenv("INVENTORY_DUAL_WRITE_SUPPLIER"),
                "INVENTORY_DUAL_WRITE_PO": os.getenv("INVENTORY_DUAL_WRITE_PO"),
            },
        },
        "baseline_counts": baseline,
        "final_counts": final,
        "growth_from_baseline": growth,
        "stages": [asdict(s) for s in stage_results],
        "summary": {
            "all_stages_ok": all(s.ok for s in stage_results),
            "stages_failed": [s.stage for s in stage_results if not s.ok],
            "no_shopify_mutation_calls_detected": all(
                "MUTATION_CALL" not in (s.errors or []) for s in stage_results
            ),
        },
        "recommendation": (
            "Ready for production schema-only deployment with dual-write flags OFF."
            if all(s.ok for s in stage_results)
            else "Not ready: review failed stages before production schema-only deployment."
        ),
    }

    out = repo / "tmp/phase3b_controlled_validation_report.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {out}")
    print(json.dumps(report["summary"], indent=2))
    return 0 if report["summary"]["all_stages_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(run())

