"""
Import Moovies supplier file into staging_moovies_raw.

CLI shim: import_moovies_raw.py
Pipeline: pipeline/01_import_moovies_raw.py -> run_from_argv()
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import uuid
from typing import Any, Dict, Optional

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

from app.helpers.text_helpers import chunked, lower_keys, normalize_key, pick


def load_moovies_file(filepath: str) -> pd.DataFrame:
    lp = filepath.lower()
    if lp.endswith(".txt"):
        return pd.read_csv(filepath, sep="|", dtype=str).fillna("")
    if lp.endswith(".csv"):
        return pd.read_csv(filepath, dtype=str).fillna("")
    if lp.endswith(".xls"):
        return pd.read_excel(filepath, dtype=str, engine="xlrd").fillna("")
    if lp.endswith(".xlsx"):
        return pd.read_excel(filepath, dtype=str, engine="openpyxl").fillna("")

    # Extension missing or unknown (common FTP name: "Feed-22-03-2026"). Sniff bytes —
    # pandas cannot pick an Excel engine without a known extension.
    with open(filepath, "rb") as f:
        head = f.read(8)
    # ZIP-based Office Open XML (.xlsx)
    if head[:2] == b"PK":
        return pd.read_excel(filepath, dtype=str, engine="openpyxl").fillna("")
    # Legacy OLE2 .xls
    if head[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return pd.read_excel(filepath, dtype=str, engine="xlrd").fillna("")
    # Typical Moovies inventory: pipe-delimited text
    try:
        df = pd.read_csv(filepath, sep="|", dtype=str, encoding_errors="replace").fillna("")
        if df.shape[1] >= 2:
            return df
    except Exception:
        pass
    try:
        df = pd.read_csv(filepath, dtype=str, encoding_errors="replace").fillna("")
        if df.shape[1] >= 2:
            return df
    except Exception:
        pass
    try:
        return pd.read_excel(filepath, dtype=str, engine="openpyxl").fillna("")
    except Exception as exc:
        raise ValueError(
            f"Could not parse Moovies file {filepath!r}: expected pipe-delimited text, "
            f".xlsx (zip), or .xls. Underlying error: {exc}"
        ) from exc


def sha256_file(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_existing_upsert_keys(supabase, table: str, page_size: int = 1000) -> set[str]:
    out: set[str] = set()
    offset = 0
    while True:
        resp = (
            supabase.table(table)
            .select("upsert_key")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = resp.data or []
        if not page:
            break
        for r in page:
            k = (r.get("upsert_key") or "").strip()
            if k:
                out.add(k)
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


def compute_upsert_key(supplier: str, raw_barcode: str, raw_sku: str, row_number: int) -> str:
    barcode = (raw_barcode or "").strip()
    sku = (raw_sku or "").strip()
    if barcode:
        return f"barcode:{barcode}"
    if sku:
        return f"sku:{sku}"
    return f"row:{row_number}"


def import_raw(
    filepath: str,
    table: str = "staging_moovies_raw",
    mode: str = "full",
    existing_only_in_raw: bool = False,
    limit: Optional[int] = None,
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

    df = load_moovies_file(filepath)
    batch_id = str(uuid.uuid4())
    file_hash = sha256_file(filepath)
    existing_keys: set[str] = set()
    known_barcodes: set[str] = set()
    if mode == "stock_cost" and existing_only_in_raw:
        existing_keys = fetch_existing_upsert_keys(supabase, table)
        known_barcodes = fetch_known_barcodes(supabase, ["moovies", "Moovies"])

    rows: list[Dict[str, Any]] = []
    skipped_unknown = 0

    for idx, record in enumerate(df.to_dict(orient="records"), start=1):
        if limit is not None and len(rows) >= limit:
            break
        record_lc = lower_keys(record)

        raw_title = pick(record_lc, "Description", "Title", "TITLE")
        raw_barcode = pick(record_lc, "Barcode", "EAN/Barcode", "EAN", "UPC", "SKU")
        raw_format = pick(record_lc, "Format", "FORMAT")
        raw_price = pick(record_lc, "Your Price", "Price", "PRICE")
        raw_qty = pick(record_lc, "Stock Available", "Qty", "QTY", default="")
        raw_release = pick(record_lc, "Release Date", "RELEASE", default="")
        raw_studio = pick(record_lc, "Label", "Studio", default="")
        raw_director = pick(record_lc, "Director", default="")
        raw_sku = pick(
            record_lc,
            "Product Code",
            "ProductCode",
            "Product",
            "Catalogue",
            "CATALOGUE",
            default="",
        )

        upsert_key = compute_upsert_key("moovies", raw_barcode, raw_sku, idx)

        base = {
            "supplier": "moovies",
            "upsert_key": upsert_key,
            "import_batch_id": batch_id,
            "source_filename": os.path.basename(filepath),
            "source_file_hash": file_hash,
            "row_number": idx,
            "raw_payload": record,
        }

        if mode == "stock_cost":
            if existing_only_in_raw and (
                (raw_barcode and raw_barcode not in known_barcodes)
                or (not raw_barcode and upsert_key not in existing_keys)
            ):
                skipped_unknown += 1
                continue
            rows.append(
                {
                    **base,
                    "raw_barcode": raw_barcode,
                    "raw_sku": raw_sku,
                    "raw_price": raw_price,
                    "raw_qty": raw_qty,
                    "raw_status": pick(record_lc, "Status", default=""),
                }
            )
            continue

        rows.append(
            {
                **base,
                "raw_title": raw_title,
                "raw_barcode": raw_barcode,
                "raw_format": raw_format,
                "raw_price": raw_price,
                "raw_qty": raw_qty,
                "raw_release": raw_release,
                "raw_studio": raw_studio,
                "raw_director": raw_director,
                "raw_sku": raw_sku,
                "raw_status": pick(record_lc, "Status", default=""),
                "raw_category": pick(record_lc, "Category", default=""),
                "raw_country_of_origin": pick(record_lc, "Country of Origin", default=""),
            }
        )

    for batch in chunked(rows, 1000):
        supabase.table(table).upsert(batch, on_conflict="supplier,upsert_key").execute()

    print(
        f"Imported {len(rows)} Moovies raw rows. "
        f"Mode: {mode} Batch: {batch_id} File hash: {file_hash} "
        f"Skipped unknown: {skipped_unknown}"
    )
    return batch_id


def run_from_argv(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath")
    parser.add_argument(
        "--mode",
        choices=["full", "stock_cost"],
        default="full",
        help="full = refresh all raw fields; stock_cost = only refresh barcode/sku/price/qty/status",
    )
    parser.add_argument(
        "--existing-only-in-raw",
        action="store_true",
        help="In stock_cost mode, update only rows already present in raw (skip unknown barcodes/skus).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Import at most N data rows from the file (after filters; useful for dry runs).",
    )
    args = parser.parse_args(argv)
    try:
        import_raw(
            args.filepath,
            mode=args.mode,
            existing_only_in_raw=args.existing_only_in_raw,
            limit=args.limit,
        )
    except SystemExit as e:
        code = e.code
        return int(code) if isinstance(code, int) else 1
    return 0


def main() -> None:
    raise SystemExit(run_from_argv())
