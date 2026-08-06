#!/usr/bin/env python3
"""
Phase 3b dual-write verification + reconciliation report (read-only).

Usage:
  venv/bin/python scripts/observability/inventory_dual_write_verify.py --env-file .env.prod

Does not enable flags or mutate data. Reports whether foundation tables exist and
summarises row counts / basic invariants when present.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args()

    repo = _repo_root()
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))

    env_path = args.env_file
    if not os.path.isabs(env_path):
        env_path = str(repo / env_path)
    load_dotenv(env_path, override=True)

    from supabase import create_client
    from app.config.inventory_dual_write import load_inventory_dual_write_flags

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY", file=sys.stderr)
        return 2

    sb = create_client(url, key)
    flags = load_inventory_dual_write_flags()

    tables = [
        "suppliers",
        "release_variants",
        "release_shopify_listings",
        "variant_identifiers",
        "tape_inventory_levels",
        "supplier_offers",
        "supplier_offer_observations",
        "supplier_sku_resolutions",
        "purchase_orders",
        "purchase_order_lines",
        "inventory_events",
    ]

    report: dict = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "flags": {
            "INVENTORY_DUAL_WRITE_ENABLED": flags.enabled,
            "shopify": flags.shopify_enabled,
            "supplier": flags.supplier_enabled,
            "purchase_orders": flags.po_enabled,
        },
        "tables": {},
        "reconciliation": [],
        "assumptions": [
            "Foundation migration must be applied before dual-writes can persist rows.",
            "Flags default OFF — verify report with flags off still succeeds.",
            "Supplier dual-write must never create tape_inventory_levels rows.",
            "Shopify dual-write creates tape levels only for tracked/stocked channels.",
            "po_incoming_confirmed and shopify_incoming_reported remain separate facts.",
        ],
    }

    for t in tables:
        try:
            res = sb.table(t).select("*", count="exact").limit(1).execute()
            report["tables"][t] = {"exists": True, "approx_count": res.count}
        except Exception as exc:
            report["tables"][t] = {"exists": False, "error": str(exc)[:200]}

    # Reconciliation checks when tables exist
    if report["tables"].get("release_variants", {}).get("exists"):
        try:
            pubs = sb.table("release_variants").select("publication_status").limit(5000).execute()
            c = Counter((r.get("publication_status") or "?") for r in (pubs.data or []))
            report["publication_status_sample"] = dict(c)
            supplier_only = c.get("supplier_only", 0)
            report["reconciliation"].append(
                {
                    "check": "supplier_only_releases_present",
                    "ok": True,
                    "detail": f"sample_supplier_only={supplier_only} (releases can exist without Shopify)",
                }
            )
        except Exception as exc:
            report["reconciliation"].append(
                {"check": "publication_status", "ok": False, "detail": str(exc)[:200]}
            )

    if report["tables"].get("supplier_offers", {}).get("exists"):
        try:
            offers = (
                sb.table("supplier_offers")
                .select("supplier_id,release_variant_id,availability_status")
                .limit(2000)
                .execute()
            )
            rows = offers.data or []
            unresolved = sum(1 for r in rows if not r.get("release_variant_id"))
            report["reconciliation"].append(
                {
                    "check": "unresolved_supplier_offers_visible",
                    "ok": True,
                    "detail": f"sample_unresolved={unresolved}/{len(rows)}",
                }
            )
            # Ensure tape_film not polluting supplier_offers
            tape = sum(1 for r in rows if r.get("supplier_id") == "tape_film")
            report["reconciliation"].append(
                {
                    "check": "no_tape_film_supplier_offers_in_sample",
                    "ok": tape == 0,
                    "detail": f"tape_film_rows={tape}",
                }
            )
        except Exception as exc:
            report["reconciliation"].append(
                {"check": "supplier_offers_sample", "ok": False, "detail": str(exc)[:200]}
            )

    if report["tables"].get("tape_inventory_levels", {}).get("exists"):
        try:
            levels = (
                sb.table("tape_inventory_levels")
                .select(
                    "on_hand,committed,available,po_incoming_confirmed,shopify_incoming_reported"
                )
                .limit(2000)
                .execute()
            )
            rows = levels.data or []
            identity_mismatch = 0
            both_incoming = 0
            for r in rows:
                on_hand = int(r.get("on_hand") or 0)
                committed = int(r.get("committed") or 0)
                available = int(r.get("available") or 0)
                if available != on_hand - committed:
                    identity_mismatch += 1
                if int(r.get("po_incoming_confirmed") or 0) > 0 and int(
                    r.get("shopify_incoming_reported") or 0
                ) > 0:
                    both_incoming += 1
            report["reconciliation"].append(
                {
                    "check": "available_equals_on_hand_minus_committed",
                    "ok": identity_mismatch == 0,
                    "detail": f"mismatches={identity_mismatch}/{len(rows)}",
                }
            )
            report["reconciliation"].append(
                {
                    "check": "incoming_channels_not_auto_summed_storage",
                    "ok": True,
                    "detail": (
                        f"rows_with_both_incoming_facts={both_incoming} "
                        "(facts stored separately; derived sum must use reconciliation)"
                    ),
                }
            )
        except Exception as exc:
            report["reconciliation"].append(
                {"check": "tape_inventory_sample", "ok": False, "detail": str(exc)[:200]}
            )

    if args.format == "json":
        print(json.dumps(report, indent=2))
    else:
        print("=== Inventory dual-write verification ===")
        print(f"generated_at: {report['generated_at']}")
        print(f"flags: {report['flags']}")
        print("\nTables:")
        for t, info in report["tables"].items():
            if info.get("exists"):
                print(f"  OK  {t}: count≈{info.get('approx_count')}")
            else:
                print(f"  --  {t}: missing ({info.get('error')})")
        print("\nReconciliation:")
        for item in report["reconciliation"]:
            mark = "OK" if item.get("ok") else "FAIL"
            print(f"  [{mark}] {item['check']}: {item.get('detail')}")
        print("\nAssumptions:")
        for a in report["assumptions"]:
            print(f"  - {a}")

    missing = [t for t, i in report["tables"].items() if not i.get("exists")]
    return 0 if not missing else 1


if __name__ == "__main__":
    raise SystemExit(main())
