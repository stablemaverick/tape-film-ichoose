"""
Cross-supplier harmonization on staging_supplier_offers.

CLI shim: harmonize_supplier_offers.py
Pipeline: pipeline/04_harmonize_supplier_offers.py -> run_from_argv()
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Optional

import httpx
from dotenv import load_dotenv
from supabase import create_client

from app.helpers.text_helpers import clean_text, now_iso
from app.rules.harmonization_rules import (
    compute_harmonized_update,
    determine_leader_supplier,
    pick_best_director,
    pick_best_format,
    pick_best_release_date,
    pick_best_studio,
    pick_best_title,
)

RETRYABLE = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)


def execute_with_retry(build_execute: Callable, label: str = "", max_retries: int = 6):
    delay = 2.0
    for attempt in range(max_retries + 1):
        try:
            return build_execute().execute()
        except RETRYABLE as exc:
            if attempt == max_retries:
                raise
            wait = min(delay, 60)
            print(f"  RETRY {attempt+1}/{max_retries} ({type(exc).__name__}) {label} — waiting {wait:.0f}s")
            time.sleep(wait)
            delay *= 2
        except Exception as exc:
            err_str = str(exc)
            if "502" in err_str or "Bad gateway" in err_str or "504" in err_str:
                if attempt == max_retries:
                    raise
                wait = min(delay, 60)
                print(f"  RETRY {attempt+1}/{max_retries} (gateway error) {label} — waiting {wait:.0f}s")
                time.sleep(wait)
                delay *= 2
            else:
                raise


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_all_offers(supabase, table: str, page_size: int = 1000) -> List[Dict[str, Any]]:
    SELECT = (
        "id,supplier,barcode,title,format,director,studio,media_release_date,"
        "harmonized_title,harmonized_format,harmonized_director,harmonized_studio,"
        "harmonized_from_supplier,harmonized_at"
    )
    out: List[Dict[str, Any]] = []
    offset = 0
    while True:

        def _fetch():
            return (
                supabase.table(table)
                .select(SELECT)
                .not_.is_("barcode", "null")
                .range(offset, offset + page_size - 1)
            )

        resp = execute_with_retry(_fetch, label=f"fetch page offset={offset}")
        page = resp.data or []
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return out


def run_harmonize(
    *,
    table: str = "staging_supplier_offers",
    dry_run: bool = False,
    barcode: Optional[str] = None,
    env_file: str = ".env",
) -> None:
    load_dotenv(env_file)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in .env", file=sys.stderr)
        raise SystemExit(1)

    supabase = create_client(supabase_url, supabase_key)

    print("Loading all supplier offers with barcodes…")
    offers = fetch_all_offers(supabase, table)
    print(f"Loaded {len(offers)} offers.")

    by_barcode: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for offer in offers:
        bc = clean_text(offer.get("barcode"))
        if bc:
            by_barcode[bc].append(offer)

    if barcode:
        target = barcode.strip()
        by_barcode = {k: v for k, v in by_barcode.items() if k == target}

    multi_supplier = {
        bc: group
        for bc, group in by_barcode.items()
        if len({(clean_text(r.get("supplier")) or "").lower() for r in group}) > 1
    }

    print(
        f"Barcode groups: {len(by_barcode)} total, "
        f"{len(multi_supplier)} with multiple suppliers (harmonization candidates)."
    )

    ts = now_iso()
    updates_applied = 0
    rows_changed = 0
    skipped_no_change = 0

    for bc, group in multi_supplier.items():
        best_title = pick_best_title(group)
        best_format = pick_best_format(group)
        best_studio = pick_best_studio(group)
        best_director = pick_best_director(group)
        best_release_date = pick_best_release_date(group)
        leader = determine_leader_supplier(group)

        for row in group:
            update = compute_harmonized_update(
                row,
                best_title,
                best_format,
                best_studio,
                best_director,
                best_release_date,
                leader,
                ts,
            )
            if not update:
                skipped_no_change += 1
                continue

            row_id = row["id"]

            if dry_run:
                supplier = clean_text(row.get("supplier"))
                print(f"  DRY-RUN barcode={bc} supplier={supplier} changes={update}")
                rows_changed += 1
                continue

            def _update():
                return supabase.table(table).update(update).eq("id", row_id)

            execute_with_retry(_update, label=f"update row {row_id}")
            rows_changed += 1
            updates_applied += 1
            time.sleep(0.002)

    if dry_run and multi_supplier:
        title_diffs = 0
        format_diffs = 0
        release_diffs = 0
        samples: List[Dict[str, Any]] = []
        for bc, group in multi_supplier.items():
            by_sup = {(clean_text(r.get("supplier")) or "").lower(): r for r in group}
            lasgo = by_sup.get("lasgo")
            moovies = by_sup.get("moovies")
            tape = by_sup.get("tape film")

            best_t = pick_best_title(group)
            best_f = pick_best_format(group)
            best_rd = pick_best_release_date(group)

            for row in group:
                if best_t and clean_text(row.get("harmonized_title")) != best_t:
                    title_diffs += 1
                if best_f and clean_text(row.get("harmonized_format")) != best_f:
                    format_diffs += 1
                if best_rd and row.get("media_release_date") != best_rd:
                    release_diffs += 1

            if len(samples) < 15:
                suppliers_present = sorted(by_sup.keys())
                sample = {"barcode": bc, "suppliers": suppliers_present}
                if lasgo:
                    sample["lasgo_title"] = clean_text(lasgo.get("title"))
                if moovies:
                    sample["moovies_title"] = clean_text(moovies.get("title"))
                    sample["moovies_format"] = clean_text(moovies.get("format"))
                if tape:
                    sample["tape_film_title"] = clean_text(tape.get("title"))
                sample["best_title"] = best_t
                sample["best_format"] = best_f
                sample["best_release_date"] = best_rd
                samples.append(sample)

        print("\nDRY-RUN OVERLAP SUMMARY")
        print(f"  total_barcodes_with_offers: {len(by_barcode)}")
        print(f"  multi_supplier_barcode_groups: {len(multi_supplier)}")
        print(f"  title_updates_needed: {title_diffs}")
        print(f"  format_updates_needed: {format_diffs}")
        print(f"  release_date_updates_needed: {release_diffs}")
        if samples:
            print(f"\nSAMPLE HARMONIZATIONS (first {len(samples)}):")
            for s in samples:
                print(f"  barcode={s['barcode']}  suppliers={s['suppliers']}")
                if s.get("lasgo_title"):
                    print(f"    lasgo_title:  {s['lasgo_title']}")
                if s.get("moovies_title"):
                    print(f"    moovies_title: {s['moovies_title']}")
                if s.get("tape_film_title"):
                    print(f"    tape_film_title: {s['tape_film_title']}")
                if s.get("moovies_format"):
                    print(f"    moovies_format: {s['moovies_format']}")
                print(
                    f"    -> best_title={s['best_title']}  best_format={s['best_format']}  "
                    f"best_release={s['best_release_date']}"
                )

    mode = "DRY-RUN" if dry_run else "APPLIED"
    print(
        f"\nHarmonization {mode}. "
        f"multi_supplier_groups={len(multi_supplier)} "
        f"rows_changed={rows_changed} "
        f"skipped_no_change={skipped_no_change} "
        f"db_updates={updates_applied}"
    )


def run_from_argv(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Cross-supplier harmonization on staging_supplier_offers by barcode."
    )
    parser.add_argument("--table", default="staging_supplier_offers")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing.")
    parser.add_argument("--barcode", default=None, help="Only harmonize a specific barcode.")
    args = parser.parse_args(argv)
    try:
        run_harmonize(table=args.table, dry_run=args.dry_run, barcode=args.barcode)
    except SystemExit as e:
        code = e.code
        return int(code) if isinstance(code, int) else 1
    return 0


def main() -> None:
    raise SystemExit(run_from_argv())
