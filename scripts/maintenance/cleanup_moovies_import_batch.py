#!/usr/bin/env python3
"""
Remove staging + catalog data tied to one Moovies import_batch_id.

Deletes (in order):
  1) catalog_items where supplier is moovies/Moovies and barcode appears in that batch's
     staging_supplier_offers rows (may remove rows that existed before the batch — see below).
  2) staging_supplier_offers where import_batch_id matches and supplier = moovies
  3) staging_moovies_raw where import_batch_id matches

Default batch: latest import_batch_id on staging_moovies_raw (by imported_at).

Safety:
  --dry-run   show counts only
  --yes       required to perform deletes

Catalog warning: without a batch id on catalog_items, we delete by (supplier, barcode) from
that batch's Moovies offers. That removes current Moovies catalog rows for those barcodes even
if they pre-dated the batch (same as rolling back a bad import).

Usage:
  venv/bin/python scripts/maintenance/cleanup_moovies_import_batch.py --dry-run
  venv/bin/python scripts/maintenance/cleanup_moovies_import_batch.py --batch-id <uuid> --yes
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List, Set

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client

from app.helpers.text_helpers import chunked


def _count_batch(sb, table: str, batch_id: str, extra_filters=None) -> int:
    q = sb.table(table).select("id", count="exact").eq("import_batch_id", batch_id).limit(1)
    if extra_filters:
        q = extra_filters(q)
    resp = q.execute()
    return int(resp.count or 0)


def resolve_latest_moovies_batch(sb) -> str:
    resp = (
        sb.table("staging_moovies_raw")
        .select("import_batch_id,imported_at")
        .order("imported_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if not rows or not rows[0].get("import_batch_id"):
        print("No rows in staging_moovies_raw; cannot resolve latest batch.", file=sys.stderr)
        raise SystemExit(1)
    return str(rows[0]["import_batch_id"])


def fetch_moovies_barcodes_for_batch(sb, batch_id: str, offers_table: str) -> List[str]:
    out: Set[str] = set()
    offset = 0
    page_size = 1000
    while True:
        resp = (
            sb.table(offers_table)
            .select("barcode")
            .eq("import_batch_id", batch_id)
            .eq("supplier", "moovies")
            .not_.is_("barcode", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = resp.data or []
        for r in page:
            bc = (r.get("barcode") or "").strip()
            if bc:
                out.add(bc)
        if len(page) < page_size:
            break
        offset += page_size
    return sorted(out)


def delete_catalog_for_barcodes(sb, barcodes: List[str], *, dry_run: bool) -> int:
    if not barcodes:
        return 0
    removed = 0
    suppliers = ("moovies", "Moovies")
    for sup in suppliers:
        for batch in chunked(barcodes, 150):
            if dry_run:
                q = (
                    sb.table("catalog_items")
                    .select("id", count="exact")
                    .eq("supplier", sup)
                    .in_("barcode", batch)
                    .limit(1)
                )
                removed += int(q.execute().count or 0)
                continue
            sb.table("catalog_items").delete().eq("supplier", sup).in_("barcode", batch).execute()
            # count not always returned; approximate by batch len for log
            removed += len(batch)
    return removed


def delete_offers_batch(sb, offers_table: str, batch_id: str, *, dry_run: bool) -> int:
    n = _count_batch(sb, offers_table, batch_id, lambda q: q.eq("supplier", "moovies"))
    if dry_run or n == 0:
        return n
    sb.table(offers_table).delete().eq("import_batch_id", batch_id).eq("supplier", "moovies").execute()
    return n


def delete_raw_batch(sb, batch_id: str, *, dry_run: bool) -> int:
    n = _count_batch(sb, "staging_moovies_raw", batch_id)
    if dry_run or n == 0:
        return n
    sb.table("staging_moovies_raw").delete().eq("import_batch_id", batch_id).execute()
    return n


def main() -> int:
    parser = argparse.ArgumentParser(description="Cleanup one Moovies import batch (raw, offers, catalog).")
    parser.add_argument("--env", default=".env", help="Path to .env (repo root)")
    parser.add_argument("--batch-id", default=None, help="import_batch_id (default: latest Moovies raw batch)")
    parser.add_argument("--offers-table", default="staging_supplier_offers")
    parser.add_argument(
        "--skip-catalog",
        action="store_true",
        help="Do not delete catalog_items (only staging offers + raw).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned counts; no deletes")
    parser.add_argument("--yes", action="store_true", help="Confirm destructive deletes")
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        print("Refusing to delete without --yes (or use --dry-run).", file=sys.stderr)
        return 2

    load_dotenv(os.path.join(ROOT, args.env))
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY", file=sys.stderr)
        return 1

    sb = create_client(url, key)
    batch_id = args.batch_id or resolve_latest_moovies_batch(sb)

    print(f"Target Moovies import_batch_id: {batch_id}")

    raw_n = _count_batch(sb, "staging_moovies_raw", batch_id)
    offers_n = _count_batch(
        sb, args.offers_table, batch_id, lambda q: q.eq("supplier", "moovies")
    )
    barcodes = fetch_moovies_barcodes_for_batch(sb, batch_id, args.offers_table)
    print(f"staging_moovies_raw rows: {raw_n}")
    print(f"staging_supplier_offers (moovies) rows: {offers_n}")
    print(f"Distinct barcodes on those offers: {len(barcodes)}")

    if not args.skip_catalog:
        cat_n = delete_catalog_for_barcodes(sb, barcodes, dry_run=True)
        print(f"catalog_items rows matching supplier moovies/Moovies + batch barcodes: {cat_n}")
    else:
        print("(--skip-catalog: not touching catalog_items)")

    if args.dry_run:
        print("Dry run complete; no changes made.")
        return 0

    if args.skip_catalog:
        print("Deleting staging_supplier_offers (moovies)…")
        delete_offers_batch(sb, args.offers_table, batch_id, dry_run=False)
        print("Deleting staging_moovies_raw…")
        delete_raw_batch(sb, batch_id, dry_run=False)
    else:
        print("Deleting catalog_items (moovies/Moovies, batch barcodes)…")
        delete_catalog_for_barcodes(sb, barcodes, dry_run=False)
        print("Deleting staging_supplier_offers (moovies)…")
        delete_offers_batch(sb, args.offers_table, batch_id, dry_run=False)
        print("Deleting staging_moovies_raw…")
        delete_raw_batch(sb, batch_id, dry_run=False)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
