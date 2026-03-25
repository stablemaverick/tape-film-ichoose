"""
Upsert staging_supplier_offers into catalog_items (preserving TMDB / film fields).

CLI shim: upsert_supplier_offers_to_catalog_items_preserve_tmdb.py
Pipeline: pipeline/05_upsert_to_catalog_items.py -> run_from_argv()
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
from dotenv import load_dotenv
from supabase import create_client

from app.helpers.text_helpers import clean_text
from app.rules.catalog_update_rules import filter_update_payload, get_update_whitelist
from app.services.catalog_offer_mapping import map_offer_to_catalog_row

# Stock-sync diff: compare offer vs catalog on these only. If all match, skip the write
# (including supplier_last_seen_at — no bump when nothing commercial changed).
STOCK_DIFF_COMPARE_KEYS = (
    "supplier_stock_status",
    "availability_status",
    "cost_price",
    "calculated_sale_price",
)

_TRANSIENT_EXC = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)


@dataclass
class RetryStats:
    """Transient-retry attempts across Step 4 API calls (each backoff cycle counts as 1)."""

    retries: int = 0


def execute_with_retry(
    query,
    max_retries: int = 6,
    label: str = "",
    stats: Optional[RetryStats] = None,
):
    delay = 1.0
    for attempt in range(max_retries):
        try:
            return query.execute()
        except _TRANSIENT_EXC as exc:
            if attempt == max_retries - 1:
                raise
            if stats is not None:
                stats.retries += 1
            print(f"WARN: transient error on {label} ({exc!r}); retry {attempt + 1}/{max_retries} in {delay:.1f}s")
            time.sleep(delay)
            delay = min(delay * 2, 60.0)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fmt_hhmmss(seconds: float) -> str:
    total = int(round(max(0.0, seconds)))
    h, r = divmod(total, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _norm_stock_compare(field: str, value: Any) -> Any:
    """Normalize DB vs staging values for equality on stock-diff keys."""
    if field in ("cost_price", "calculated_sale_price"):
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        if isinstance(value, Decimal):
            return round(float(value), 6)
        try:
            return round(float(value), 6)
        except (TypeError, ValueError):
            return str(value)
    if field == "supplier_stock_status":
        try:
            return int(value) if value is not None else 0
        except (TypeError, ValueError):
            return 0
    if field == "availability_status":
        return (clean_text(value) if value is not None else "") or ""
    return value


def stock_commercial_fields_differ(
    catalog_row: Dict[str, Any],
    update_payload: Dict[str, Any],
) -> bool:
    """True if any whitelisted commercial field (except last_seen) differs from catalog."""
    for key in STOCK_DIFF_COMPARE_KEYS:
        if key not in update_payload:
            continue
        cur = _norm_stock_compare(key, catalog_row.get(key))
        new = _norm_stock_compare(key, update_payload.get(key))
        if cur != new:
            return True
    return False


def _catalog_stock_snapshot_from_row(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "supplier_stock_status": r.get("supplier_stock_status"),
        "availability_status": r.get("availability_status"),
        "cost_price": r.get("cost_price"),
        "calculated_sale_price": r.get("calculated_sale_price"),
        "supplier_last_seen_at": r.get("supplier_last_seen_at"),
    }


def fetch_existing_catalog_item_ids(
    supabase,
    catalog_ids: List[str],
    *,
    chunk_size: int,
    stats: RetryStats,
) -> set[str]:
    """Return catalog_items.id values that still exist (batched IN queries)."""
    found: set[str] = set()
    unique = list(dict.fromkeys(catalog_ids))
    batches = list(chunked(unique, max(1, chunk_size)))
    for i, batch in enumerate(batches, 1):
        q = supabase.table("catalog_items").select("id").in_("id", batch)
        resp = execute_with_retry(
            q,
            label=f"catalog_items id existence batch {i}/{len(batches)} ({len(batch)} ids)",
            stats=stats,
        ).data or []
        for r in resp:
            found.add(str(r["id"]))
    return found


def apply_stock_sync_row_updates(
    supabase,
    rows: List[Tuple[str, Dict[str, Any]]],
    *,
    stats: RetryStats,
    progress_every: int = 500,
) -> int:
    """
    PATCH catalog_items by primary key only (no upsert).
    `rows` must already be filtered to ids that exist.
    """
    total = len(rows)
    for idx, (catalog_id, payload) in enumerate(rows, 1):
        q = supabase.table("catalog_items").update(payload).eq("id", catalog_id)
        execute_with_retry(
            q,
            label=f"stock catalog_items update {idx}/{total}",
            stats=stats,
        )
        if idx % progress_every == 0:
            print(f"  stock updated {idx}/{total}…")
        time.sleep(0.002)
    return total


def fetch_all_offers(
    supabase,
    table: str,
    page_size: int = 500,
    stats: Optional[RetryStats] = None,
) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    while True:
        q = (
            supabase.table(table)
            .select(
                "supplier,barcode,shopify_variant_id,cost_price,calculated_sale_price,availability_status,supplier_stock_status,active,media_type,source_priority,media_release_date,format,studio,director,title,harmonized_title,harmonized_format,harmonized_director,harmonized_studio,harmonized_from_supplier,harmonized_at,shopify_product_id"
            )
            .range(offset, offset + page_size - 1)
        )
        page = execute_with_retry(q, label=f"fetch_offers offset={offset}", stats=stats).data or []
        if not page:
            break
        all_rows.extend(page)
        if len(all_rows) % 2000 < page_size:
            print(f"  fetched {len(all_rows)} offers so far…")
        if len(page) < page_size:
            break
        offset += page_size
    print(f"  total offers loaded: {len(all_rows)}")
    return all_rows


def run_upsert(
    *,
    offers_table: str = "staging_supplier_offers",
    batch_size: int = 500,
    existing_only: bool = False,
    env_file: str = ".env",
) -> None:
    load_dotenv(env_file)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

    if not supabase_url or not supabase_key:
        print("Missing Supabase env", file=sys.stderr)
        raise SystemExit(1)

    supabase = create_client(supabase_url, supabase_key)
    retry_stats = RetryStats()

    whitelist = get_update_whitelist(existing_only=existing_only)
    mode_label = "stock-sync" if existing_only else "catalog-sync"
    print(f"Mode: {mode_label} (update whitelist: {len(whitelist)} fields)")

    t_step4_start = time.perf_counter()

    t0 = time.perf_counter()
    offers = fetch_all_offers(supabase, offers_table, stats=retry_stats)
    t_fetch = time.perf_counter() - t0
    print(
        f"[{mode_label} step4] read staging offers: {t_fetch:.2f}s ({len(offers)} rows)",
    )

    if not offers:
        print("No supplier offers found.")
        return

    barcode_select = (
        "id,barcode,supplier_stock_status,availability_status,cost_price,calculated_sale_price,supplier_last_seen_at"
        if existing_only
        else "id,barcode"
    )
    variant_select = (
        "id,shopify_variant_id,supplier_stock_status,availability_status,cost_price,calculated_sale_price,supplier_last_seen_at"
        if existing_only
        else "id,shopify_variant_id"
    )

    existing_ids: Dict[Tuple[str, str], str] = {}
    catalog_snapshots: Dict[str, Dict[str, Any]] = {}
    barcodes_by_supplier: Dict[str, List[str]] = defaultdict(list)
    variants_by_supplier: Dict[str, List[str]] = defaultdict(list)

    offers_for_keys: list[tuple[Dict[str, Any], Optional[Tuple[str, str]]]] = []
    for offer in offers:
        supplier = clean_text(offer.get("supplier")) or "Unknown"
        barcode = clean_text(offer.get("barcode"))
        if supplier == "Tape Film":
            variant = clean_text(offer.get("shopify_variant_id"))
            key = ("Tape Film", f"variant:{variant}") if variant else None
            if variant:
                variants_by_supplier[supplier].append(variant)
            if barcode:
                barcodes_by_supplier[supplier].append(barcode)
        else:
            key = (supplier, f"barcode:{barcode}") if barcode else None
            if barcode:
                barcodes_by_supplier[supplier].append(barcode)
        offers_for_keys.append((offer, key))

    t1 = time.perf_counter()
    print("Looking up existing catalog_items by barcode…")
    for supplier, barcodes in barcodes_by_supplier.items():
        unique_barcodes = list(set(barcodes))
        batches = list(chunked(unique_barcodes, 200))
        for i, batch in enumerate(batches, 1):
            q = (
                supabase.table("catalog_items")
                .select(barcode_select)
                .eq("supplier", supplier)
                .in_("barcode", batch)
            )
            resp = execute_with_retry(
                q,
                label=f"lookup barcodes {supplier} batch {i}/{len(batches)}",
                stats=retry_stats,
            ).data or []
            for r in resp:
                bc = clean_text(r.get("barcode"))
                cid = str(r["id"])
                if bc:
                    existing_ids[(supplier, f"barcode:{bc}")] = r["id"]
                if existing_only:
                    catalog_snapshots[cid] = _catalog_stock_snapshot_from_row(r)
            if i % 20 == 0 or i == len(batches):
                print(f"  {supplier}: looked up {i}/{len(batches)} barcode batches ({len(existing_ids)} matches)")

    for supplier, variants in variants_by_supplier.items():
        unique_variants = list(set(variants))
        batches = list(chunked(unique_variants, 200))
        for i, batch in enumerate(batches, 1):
            q = (
                supabase.table("catalog_items")
                .select(variant_select)
                .eq("supplier", supplier)
                .in_("shopify_variant_id", batch)
            )
            resp = execute_with_retry(
                q,
                label=f"lookup variants {supplier} batch {i}/{len(batches)}",
                stats=retry_stats,
            ).data or []
            for r in resp:
                v = clean_text(r.get("shopify_variant_id"))
                cid = str(r["id"])
                if v:
                    existing_ids[(supplier, f"variant:{v}")] = r["id"]
                if existing_only:
                    catalog_snapshots[cid] = _catalog_stock_snapshot_from_row(r)
            if i % 20 == 0 or i == len(batches):
                print(f"  {supplier}: looked up {i}/{len(batches)} variant batches ({len(existing_ids)} matches)")

    t_lookup = time.perf_counter() - t1
    print(f"Existing catalog_items found: {len(existing_ids)}")
    print(f"[{mode_label} step4] catalog id lookups: {t_lookup:.2f}s")

    inserts: List[Dict[str, Any]] = []
    updates: List[Tuple[str, Dict[str, Any]]] = []
    update_diagnostics: Dict[str, Tuple[str, str]] = {}

    t2 = time.perf_counter()
    for offer, key in offers_for_keys:
        supplier = clean_text(offer.get("supplier")) or "Unknown"
        if not key:
            continue

        catalog_id = existing_ids.get(key)

        update_payload = {
            "cost_price": offer.get("cost_price"),
            "calculated_sale_price": offer.get("calculated_sale_price"),
            "supplier_stock_status": offer.get("supplier_stock_status") or 0,
            "availability_status": clean_text(offer.get("availability_status")),
            "media_release_date": offer.get("media_release_date"),
            "supplier_last_seen_at": now_iso(),
            "title": clean_text(offer.get("harmonized_title")) or clean_text(offer.get("title")),
            "format": clean_text(offer.get("harmonized_format")) or clean_text(offer.get("format")),
            "director": clean_text(offer.get("harmonized_director")) or clean_text(offer.get("director")),
            "studio": clean_text(offer.get("harmonized_studio")) or clean_text(offer.get("studio")),
        }
        update_payload = filter_update_payload(update_payload, whitelist)

        if not catalog_id and supplier == "Tape Film":
            barcode = clean_text(offer.get("barcode"))
            if barcode:
                catalog_id = existing_ids.get(("Tape Film", f"barcode:{barcode}"))

        if catalog_id:
            cid_str = str(catalog_id)
            bc = clean_text(offer.get("barcode")) or ""
            var = clean_text(offer.get("shopify_variant_id")) or ""
            if bc:
                diag_detail = f"barcode={bc}"
            elif var:
                diag_detail = f"variant={var}"
            else:
                diag_detail = "(no barcode/variant)"
            update_diagnostics[cid_str] = (supplier, diag_detail)
            updates.append((cid_str, update_payload))
        else:
            if existing_only:
                continue
            inserts.append(map_offer_to_catalog_row(offer))

    # Last staging row per catalog id wins (matches previous sequential per-row behaviour).
    merged_updates: Dict[str, Dict[str, Any]] = {}
    merged_diag: Dict[str, Tuple[str, str]] = {}
    for catalog_id, payload in updates:
        merged_updates[catalog_id] = payload
        merged_diag[catalog_id] = update_diagnostics.get(catalog_id, ("?", "?"))
    updates = list(merged_updates.items())

    deduped_inserts: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in inserts:
        supplier = clean_text(row.get("supplier")) or "Unknown"
        barcode = clean_text(row.get("barcode")) or ""
        deduped_inserts[(supplier, barcode)] = row
    inserts = list(deduped_inserts.values())

    considered_updates = len(updates)
    unchanged_skipped = 0
    missing_snapshot_treated_changed = 0

    if existing_only:
        filtered: List[Tuple[str, Dict[str, Any]]] = []
        filtered_diag: Dict[str, Tuple[str, str]] = {}
        for catalog_id, payload in updates:
            snap = catalog_snapshots.get(catalog_id)
            diag = merged_diag.get(catalog_id, ("?", "?"))
            if snap is None:
                missing_snapshot_treated_changed += 1
                filtered.append((catalog_id, payload))
                filtered_diag[catalog_id] = diag
                continue
            if stock_commercial_fields_differ(snap, payload):
                filtered.append((catalog_id, payload))
                filtered_diag[catalog_id] = diag
            else:
                unchanged_skipped += 1
        updates = filtered
        merged_diag = filtered_diag
        if missing_snapshot_treated_changed:
            print(
                f"WARN: {missing_snapshot_treated_changed} update targets had no catalog snapshot; "
                "will verify id at write time; if row is missing, update will be skipped",
            )

    t_build_filter = time.perf_counter() - t2
    print(f"Prepared: {len(inserts)} inserts, {considered_updates} update targets (deduped by catalog id)")
    if existing_only:
        print(
            f"[{mode_label} step4] build payloads + diff filter: {t_build_filter:.3f}s "
            f"(considered={considered_updates}, unchanged skipped={unchanged_skipped}, to_write={len(updates)})",
        )
    else:
        print(f"[{mode_label} step4] build update payloads: {t_build_filter:.3f}s ({considered_updates} updates)")

    inserted = 0
    updated = 0

    t3 = time.perf_counter()
    if not existing_only:
        for batch in chunked(inserts, batch_size):
            if not batch:
                continue
            q = supabase.table("catalog_items").upsert(batch, on_conflict="supplier,barcode")
            execute_with_retry(q, label=f"upsert batch ({len(batch)} rows)", stats=retry_stats)
            inserted += len(batch)

    if existing_only:
        # Update-only by id. Never upsert: partial rows would INSERT and violate NOT NULL (e.g. title).
        skipped_missing_id = 0
        t_exist = time.perf_counter()
        want_ids = [cid for cid, _ in updates]
        existing_now = fetch_existing_catalog_item_ids(
            supabase,
            want_ids,
            chunk_size=batch_size,
            stats=retry_stats,
        )
        t_exist_done = time.perf_counter() - t_exist
        verified_updates: List[Tuple[str, Dict[str, Any]]] = []
        missing_samples: List[str] = []
        for catalog_id, payload in updates:
            if catalog_id in existing_now:
                verified_updates.append((catalog_id, payload))
            else:
                skipped_missing_id += 1
                if len(missing_samples) < 12:
                    missing_samples.append(catalog_id)
        if skipped_missing_id:
            print(
                f"WARN: {skipped_missing_id} stock update targets skipped "
                f"(catalog_items id not found at write time; {t_exist_done:.2f}s existence check)",
            )
            for mid in missing_samples:
                sup, detail = merged_diag.get(mid, ("?", "?"))
                print(f"  missing id={mid} supplier={sup} {detail}")
        else:
            print(
                f"[{mode_label} step4] catalog id existence check: {t_exist_done:.2f}s "
                f"({len(want_ids)} targets, all found)",
            )

        total_u = len(verified_updates)
        updated = apply_stock_sync_row_updates(
            supabase,
            verified_updates,
            stats=retry_stats,
            progress_every=500,
        )
        t_updates = time.perf_counter() - t3
        print(
            f"[{mode_label} step4] API per-row updates: {t_updates:.2f}s "
            f"({total_u} calls, {retry_stats.retries} retries)",
        )
        total_dur = time.perf_counter() - t_step4_start
        print("")
        print("Step 4 stock update summary")
        print(f"- considered: {considered_updates}")
        print(f"- changed: {updated}")
        print(f"- unchanged skipped: {unchanged_skipped}")
        print(f"- skipped missing catalog id: {skipped_missing_id}")
        print(f"- update calls: {updated}")
        print(f"- retries: {retry_stats.retries}")
        print(f"- duration: {fmt_hhmmss(total_dur)}")
        print("")
    else:
        total_updates = len(updates)
        for idx, (catalog_id, payload) in enumerate(updates, 1):
            q = supabase.table("catalog_items").update(payload).eq("id", catalog_id)
            execute_with_retry(q, label=f"update {idx}/{total_updates}", stats=retry_stats)
            updated += 1
            if idx % 500 == 0:
                print(f"  updated {idx}/{total_updates}…")
            time.sleep(0.002)
        t_updates = time.perf_counter() - t3
        print(
            f"[{mode_label} step4] API per-row updates: {t_updates:.2f}s "
            f"({total_updates} calls, {retry_stats.retries} retries)",
        )

    print(f"Operational sync complete. inserted={inserted} updated={updated} offers_total={len(offers)}")


def run_from_argv(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offers-table", default="staging_supplier_offers")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--existing-only",
        action="store_true",
        help="Only update existing catalog_items rows; never insert new rows.",
    )
    args = parser.parse_args(argv)
    try:
        run_upsert(
            offers_table=args.offers_table,
            batch_size=args.batch_size,
            existing_only=args.existing_only,
        )
    except SystemExit as e:
        code = e.code
        return int(code) if isinstance(code, int) else 1
    return 0


def main() -> None:
    raise SystemExit(run_from_argv())
