#!/usr/bin/env python3
"""
Reset inventory-intelligence fact tables on the TEMP Supabase project only.

Does not touch production. Does not delete suppliers seed rows.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROD_REF = "zdvjokkslhpoftimvdis"
TEMP_REF_DEFAULT = "vwbuwgfzksrzfmbqhqtn"


def _project_ref(url: str) -> str:
    host = (url or "").replace("https://", "").replace("http://", "").split("/")[0]
    return host.split(".")[0]


def _delete_all(sb, table: str, page: int = 500) -> int:
    deleted = 0
    while True:
        rows = sb.table(table).select("id").limit(page).execute().data or []
        if not rows:
            break
        ids = [r["id"] for r in rows if r.get("id")]
        if not ids:
            break
        # Delete in chunks via in_ filter
        for i in range(0, len(ids), 100):
            chunk = ids[i : i + 100]
            sb.table(table).delete().in_("id", chunk).execute()
            deleted += len(chunk)
    return deleted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", default=".env.inventory-test")
    parser.add_argument("--allow-temp-ref", default=TEMP_REF_DEFAULT)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()
    if not args.yes:
        print("Refusing without --yes", file=sys.stderr)
        return 2

    repo = Path(__file__).resolve().parents[2]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    from dotenv import load_dotenv
    from supabase import create_client

    env = Path(args.env_file)
    if not env.is_absolute():
        env = repo / env
    load_dotenv(env, override=True)
    url = os.environ["SUPABASE_URL"]
    ref = _project_ref(url)
    if ref == PROD_REF or ref != args.allow_temp_ref:
        print(f"STOP: refused project {ref}", file=sys.stderr)
        return 2

    sb = create_client(url, os.environ["SUPABASE_SERVICE_KEY"])
    order = [
        "inventory_events",
        "supplier_offer_observations",
        "supplier_sku_resolutions",
        "supplier_offers",
        "variant_identifiers",
        "tape_inventory_levels",
        "release_shopify_listings",
        "purchase_order_lines",
        "purchase_orders",
        "release_variants",
    ]
    for table in order:
        n = _delete_all(sb, table)
        print(f"deleted {table}={n}")
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
