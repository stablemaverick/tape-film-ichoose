"""
Import Lasgo supplier file(s) into staging_lasgo_raw.

CLI shim: import_lasgo_raw.py
Pipeline: pipeline/02_import_lasgo_raw.py -> run_from_argv()
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

from app.helpers.text_helpers import chunked, lower_keys, pick


def fetch_existing_lasgo_barcodes(supabase, table: str, page_size: int = 1000) -> set[str]:
    out: set[str] = set()
    offset = 0
    pages = 0
    while True:
        resp = (
            supabase.table(table)
            .select("raw_ean")
            .not_.is_("raw_ean", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = resp.data or []
        if not page:
            break
        for r in page:
            bc = (r.get("raw_ean") or "").strip()
            if bc:
                out.add(bc)
        pages += 1
        if pages == 1 or pages % 10 == 0:
            print(
                f"[lasgo-import] fetched existing barcodes table={table!r} "
                f"page={pages} fetched_rows={len(page)} unique_barcodes={len(out)}"
            )
        if len(page) < page_size:
            break
        offset += page_size
    return out


def fetch_known_barcodes(supabase, supplier_names: list[str], page_size: int = 1000) -> set[str]:
    out: set[str] = set()
    for supplier in supplier_names:
        for table in ("staging_supplier_offers", "catalog_items"):
            offset = 0
            while True:
                resp = (
                    supabase.table(table)
                    .select("barcode")
                    .eq("supplier", supplier)
                    .not_.is_("barcode", "null")
                    .range(offset, offset + page_size - 1)
                    .execute()
                )
                page = resp.data or []
                if not page:
                    break
                for r in page:
                    bc = (r.get("barcode") or "").strip()
                    if bc:
                        out.add(bc)
                if len(page) < page_size:
                    break
                offset += page_size
    return out


def fetch_lasgo_identity_barcodes(supabase, page_size: int = 1000) -> set[str]:
    """
    Build the current Lasgo identity set from normalized/current tables.
    Avoid scanning full historical staging_lasgo_raw in stock_cost mode.
    """
    out: set[str] = set()
    suppliers = ["lasgo", "Lasgo"]
    sources = (
        ("catalog_items", "active catalog"),
        ("staging_supplier_offers", "normalized raw identity"),
    )

    for table, label in sources:
        for supplier in suppliers:
            offset = 0
            pages = 0
            while True:
                q = (
                    supabase.table(table)
                    .select("barcode")
                    .eq("supplier", supplier)
                    .not_.is_("barcode", "null")
                    .range(offset, offset + page_size - 1)
                )
                if table == "catalog_items":
                    q = q.eq("active", True)
                if table == "staging_supplier_offers":
                    q = q.eq("source_type", "catalog")

                resp = q.execute()
                page = resp.data or []
                if not page:
                    break
                for r in page:
                    bc = (r.get("barcode") or "").strip()
                    if bc:
                        out.add(bc)
                pages += 1
                if pages == 1 or pages % 10 == 0:
                    print(
                        f"[lasgo-import] identity scan table={table!r} source={label} supplier={supplier!r} "
                        f"page={pages} fetched_rows={len(page)} unique_barcodes={len(out)}"
                    )
                if len(page) < page_size:
                    break
                offset += page_size
    return out


def is_blu_ray_format(value: str) -> bool:
    v = (value or "").strip().lower()
    if not v:
        return False
    v = v.replace("_", " ").replace("/", " ").replace("-", " ")
    v = " ".join(v.split())
    return "blu ray" in v or "bluray" in v


def load_lasgo_file(filepath: str) -> pd.DataFrame:
    fp = filepath.lower()
    if fp.endswith(".csv"):
        return pd.read_csv(filepath, dtype=str).fillna("")
    if fp.endswith(".xlsx") or fp.endswith(".xls"):
        return pd.read_excel(filepath, dtype=str).fillna("")
    print(f"Unsupported file type: {filepath}", file=sys.stderr)
    raise SystemExit(1)


def iter_input_files(path_or_dir: str) -> Iterator[Path]:
    p = Path(path_or_dir)
    if p.is_dir():
        for child in sorted(p.iterdir()):
            if child.name.startswith("."):
                continue
            if child.suffix.lower() in {".xlsx", ".xls", ".csv"}:
                yield child
        return
    yield p


def import_lasgo_raw(
    path_or_dir: str,
    table: str = "staging_lasgo_raw",
    limit: Optional[int] = None,
    mode: str = "full",
    existing_only_in_raw: bool = False,
    *,
    env_file: str = ".env",
) -> str:
    load_dotenv(env_file)

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url:
        print("Missing SUPABASE_URL in .env", file=sys.stderr)
        raise SystemExit(1)
    if not supabase_key:
        print("Missing SUPABASE_SERVICE_KEY in .env", file=sys.stderr)
        raise SystemExit(1)

    supabase = create_client(supabase_url, supabase_key)

    batch_id = str(uuid.uuid4())
    inserted = 0
    skipped_non_bluray = 0
    skipped_unknown = 0
    existing_barcodes: set[str] = set()
    known_barcodes: set[str] = set()
    if mode == "stock_cost" and existing_only_in_raw:
        # Use current identity sources (catalog + normalized supplier offers)
        # instead of scanning the full historical raw staging table.
        identity_barcodes = fetch_lasgo_identity_barcodes(supabase)
        existing_barcodes = identity_barcodes
        known_barcodes = identity_barcodes

    for file_path in iter_input_files(path_or_dir):
        if not file_path.exists():
            print(f"File not found: {file_path}", file=sys.stderr)
            raise SystemExit(1)

        df = load_lasgo_file(str(file_path))

        rows: list[Dict[str, Any]] = []
        for idx, record in enumerate(df.to_dict(orient="records"), start=1):
            if limit is not None and inserted + len(rows) >= limit:
                break

            record_lc = lower_keys(record)

            raw_title = pick(record_lc, "TITLE", "Title", default="")
            raw_ean = pick(record_lc, "EAN/Barcode", "EAN", "Barcode", "UPC", default="")
            raw_format_l2 = pick(record_lc, "Format L2", "FORMAT", "Format", default="")
            if not is_blu_ray_format(raw_format_l2):
                skipped_non_bluray += 1
                continue
            raw_selling_price_sterling = pick(
                record_lc,
                "Selling Price Sterling",
                "Your Price ex VAT",
                "Your Price",
                "Price",
                default="",
            )
            raw_free_stock = pick(record_lc, "Free Stock", "AVAILABILITY", "Availability", default="")
            raw_release_date = pick(record_lc, "RELEASE", "Release Date", default="")
            raw_label = pick(record_lc, "Label", "STUDIO/BRAND", "Studio", default="")
            raw_artist = pick(record_lc, "Artist", "DIRECTOR/ARTIST", "Director", default="")

            base = {
                "import_batch_id": batch_id,
                "source_filename": file_path.name,
                "row_number": idx,
                "raw_payload": record,
            }

            if mode == "stock_cost":
                if existing_only_in_raw and (
                    (raw_ean and raw_ean not in known_barcodes and raw_ean not in existing_barcodes)
                    or (not raw_ean)
                ):
                    skipped_unknown += 1
                    continue
                rows.append(
                    {
                        **base,
                        "raw_ean": raw_ean,
                        "raw_selling_price_sterling": raw_selling_price_sterling,
                        "raw_free_stock": raw_free_stock,
                    }
                )
                continue

            rows.append(
                {
                    **base,
                    "raw_title": raw_title,
                    "raw_ean": raw_ean,
                    "raw_format_l2": raw_format_l2,
                    "raw_selling_price_sterling": raw_selling_price_sterling,
                    "raw_free_stock": raw_free_stock,
                    "raw_label": raw_label,
                    "raw_release_date": raw_release_date,
                    "raw_artist": raw_artist,
                }
            )

        for batch in chunked(rows, 1000):
            supabase.table(table).insert(batch).execute()

        inserted += len(rows)
        print(
            f"Imported {len(rows)} Lasgo raw rows from {file_path.name} "
            f"(running total: {inserted}, skipped non-blu-ray: {skipped_non_bluray})"
        )

        if limit is not None and inserted >= limit:
            break

    print(
        f"Lasgo raw import complete. Mode: {mode} Batch: {batch_id} "
        f"Total imported: {inserted} Skipped non-blu-ray: {skipped_non_bluray} "
        f"Skipped unknown: {skipped_unknown}"
    )
    return batch_id


def run_from_argv(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="A single file path or a directory (e.g. LasgoCat)")
    parser.add_argument("--limit", type=int, default=None, help="Max rows to import across all files")
    parser.add_argument(
        "--mode",
        choices=["full", "stock_cost"],
        default="full",
        help="full = refresh all raw fields; stock_cost = only refresh barcode/sku/price/qty",
    )
    parser.add_argument(
        "--existing-only-in-raw",
        action="store_true",
        help="In stock_cost mode, update only rows already present in raw (skip unknown barcodes).",
    )
    parser.add_argument("--table", default="staging_lasgo_raw")
    args = parser.parse_args(argv)
    try:
        import_lasgo_raw(
            args.path,
            table=args.table,
            limit=args.limit,
            mode=args.mode,
            existing_only_in_raw=args.existing_only_in_raw,
        )
    except SystemExit as e:
        code = e.code
        return int(code) if isinstance(code, int) else 1
    return 0


def main() -> None:
    raise SystemExit(run_from_argv())
