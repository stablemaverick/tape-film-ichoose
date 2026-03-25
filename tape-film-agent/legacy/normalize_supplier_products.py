import argparse
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from dotenv import load_dotenv
from supabase import create_client

from catalog_match_helpers import normalize_title


def die(msg: str) -> "None":
    print(msg, file=sys.stderr)
    raise SystemExit(1)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    text = str(value).replace(",", "").replace("+", "").strip()
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else 0


def parse_price_gbp(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    text = str(value).replace("£", "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(value: Any) -> Optional[str]:
    if not value:
        return None
    text = str(value).strip()
    for fmt in (
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def round_up_to_99(value: float) -> float:
    rounded = round(value, 2)
    whole = int(rounded)
    if rounded <= whole + 0.99:
        return round(whole + 0.99, 2)
    return round((whole + 1) + 0.99, 2)


def get_margin(cost_gbp: float) -> float:
    if cost_gbp <= 15:
        return 0.32
    if cost_gbp <= 30:
        return 0.28
    if cost_gbp <= 40:
        return 0.24
    return 0.20


def calculate_sale_price(cost_gbp: Optional[float]) -> Optional[float]:
    if cost_gbp is None:
        return None
    aud_base = cost_gbp * 2
    total_cost = aud_base * 1.12
    pre_gst_sale = total_cost * (1 + get_margin(cost_gbp))
    return round_up_to_99(pre_gst_sale * 1.10)


def chunked(items: Iterable[Any], size: int) -> Iterable[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_all_rows_for_batch(supabase, table: str, select_cols: str, batch_id: str, page_size: int = 1000) -> list[Dict[str, Any]]:
    all_rows: list[Dict[str, Any]] = []
    offset = 0
    while True:
        resp = (
            supabase.table(table)
            .select(select_cols)
            .eq("import_batch_id", batch_id)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = resp.data or []
        if not page:
            break
        all_rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return all_rows


def fetch_moovies_offer_by_barcode(
    supabase, offers_table: str, barcodes: list[str]
) -> Dict[str, Dict[str, Any]]:
    """
    For Lasgo harmonisation: fetch Moovies "better fields" for matching barcodes.
    """
    existing: Dict[str, Dict[str, Any]] = {}
    clean_barcodes = [b for b in (barcodes or []) if b]
    for batch in chunked(clean_barcodes, 200):
        resp = (
            supabase.table(offers_table)
            .select(
                "barcode,title,normalized_title,format,studio,media_release_date,source_type,source_priority"
            )
            .eq("supplier", "moovies")
            .in_("barcode", batch)
            .execute()
        )
        for row in resp.data or []:
            bc = row.get("barcode")
            if bc:
                existing[bc] = row
    return existing


def fetch_supplier_offer_by_barcode(
    supabase, offers_table: str, supplier: str, barcodes: list[str]
) -> Dict[str, Dict[str, Any]]:
    existing: Dict[str, Dict[str, Any]] = {}
    clean_barcodes = [b for b in (barcodes or []) if b]
    for batch in chunked(clean_barcodes, 200):
        resp = (
            supabase.table(offers_table)
            .select(
                "barcode,title,harmonized_title,format,harmonized_format,studio,harmonized_studio"
            )
            .eq("supplier", supplier)
            .in_("barcode", batch)
            .execute()
        )
        for row in resp.data or []:
            bc = row.get("barcode")
            if bc:
                existing[bc] = row
    return existing


def normalize_from_moovies(
    supabase,
    offers_table: str,
    batch_id: str,
) -> int:
    rows = fetch_all_rows_for_batch(
        supabase,
        "staging_moovies_raw",
        "id,raw_title,raw_barcode,raw_format,raw_category,raw_release,raw_studio,raw_country_of_origin,raw_sku,raw_price,raw_qty,source_filename,row_number",
        batch_id,
    )
    deduped_by_barcode: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        barcode = clean_text(r.get("raw_barcode"))
        if not barcode:
            continue

        title = clean_text(r.get("raw_title"))
        cost_gbp = parse_price_gbp(r.get("raw_price"))
        qty = parse_int(r.get("raw_qty"))
        deduped_by_barcode[barcode] = {
            "import_batch_id": batch_id,
            "supplier": "moovies",
            "source_filename": clean_text(r.get("source_filename")),
            "source_row_number": r.get("row_number"),
            "supplier_sku": clean_text(r.get("raw_sku")),
            "barcode": barcode,
            "title": title,
            "normalized_title": normalize_title(title or ""),
            "edition_title": None,
            "format": clean_text(r.get("raw_format")),
            "media_type": "film",
            "director": None,
            "studio": clean_text(r.get("raw_studio")),
            "media_release_date": parse_date(r.get("raw_release")),
            "supplier_stock_status": qty,
            "availability_status": "supplier_stock" if qty > 0 else "supplier_out",
            "supplier_currency": "GBP",
            "cost_price": cost_gbp,
            "calculated_sale_price": calculate_sale_price(cost_gbp),
            "source_priority": 1,
            "source_type": "catalog",
            "active": True,
            # harmonised fields: default to moovies' own values for now
            "harmonized_title": title,
            "harmonized_format": clean_text(r.get("raw_format")),
            "harmonized_director": None,
            "harmonized_studio": clean_text(r.get("raw_studio")),
            "harmonized_from_supplier": "moovies",
            "harmonized_at": now_iso(),
            "published_to_catalog": False,
            "published_catalog_item_id": None,
            "raw_source_id": r.get("id"),
            "raw_source_table": "staging_moovies_raw",
        }

    out = list(deduped_by_barcode.values())

    for batch in chunked(out, 1000):
        supabase.table(offers_table).upsert(batch, on_conflict="supplier,barcode").execute()

    print(f"Upserted {len(out)} staging_supplier_offers rows from Moovies batch {batch_id}")
    return len(out)


def normalize_from_lasgo(
    supabase,
    offers_table: str,
    batch_id: str,
) -> int:
    rows = fetch_all_rows_for_batch(
        supabase,
        "staging_lasgo_raw",
        "id,raw_title,raw_ean,raw_format_l2,raw_release_date,raw_label,raw_artist,raw_selling_price_sterling,raw_free_stock,source_filename,row_number",
        batch_id,
    )

    barcodes = [clean_text(r.get("raw_ean")) for r in rows]
    moovies = fetch_moovies_offer_by_barcode(supabase, offers_table, [b for b in barcodes if b])

    deduped_by_barcode: Dict[str, Dict[str, Any]] = {}
    moovies_title_updates: list[Dict[str, Any]] = []
    for r in rows:
        barcode = clean_text(r.get("raw_ean"))
        if not barcode:
            continue

        moovies_row = moovies.get(barcode)

        lasgo_title = clean_text(r.get("raw_title"))

        # Lasgo row: inherit Moovies richness where available, but keep Lasgo cost/qty.
        title = lasgo_title
        norm_title = normalize_title(title or "")

        moovies_format = (moovies_row or {}).get("format")
        moovies_studio = (moovies_row or {}).get("studio")
        moovies_release = (moovies_row or {}).get("media_release_date")

        lasgo_release = parse_date(r.get("raw_release_date"))

        if moovies_row and (lasgo_title or lasgo_release):
            # Update Moovies row title only; keep Moovies cost/stock and other canonical fields unchanged.
            update_row = {
                "import_batch_id": batch_id,
                "supplier": "moovies",
                "barcode": barcode,
                "harmonized_at": now_iso(),
            }
            if lasgo_title:
                update_row["harmonized_title"] = lasgo_title
            if lasgo_release:
                # Lasgo is the trusted day-to-day source for changing physical release dates.
                update_row["media_release_date"] = lasgo_release
            moovies_title_updates.append(update_row)

        cost_gbp = parse_price_gbp(r.get("raw_selling_price_sterling"))
        qty = parse_int(r.get("raw_free_stock"))

        deduped_by_barcode[barcode] = {
            "import_batch_id": batch_id,
            "supplier": "lasgo",
            "source_filename": clean_text(r.get("source_filename")),
            "source_row_number": r.get("row_number"),
            "supplier_sku": None,
            "barcode": barcode,
            "title": title,
            "normalized_title": norm_title,
            "edition_title": None,
            "format": clean_text(r.get("raw_format_l2")),
            "media_type": "film",
            "director": None,
            "studio": clean_text(r.get("raw_label")),
            "media_release_date": lasgo_release,
            "supplier_stock_status": qty,
            "availability_status": "supplier_stock" if qty > 0 else "supplier_out",
            "supplier_currency": "GBP",
            "cost_price": cost_gbp,
            "calculated_sale_price": calculate_sale_price(cost_gbp),
            "source_priority": 2,
            "source_type": "catalog",
            "active": True,
            # Harmonised fields: title from Lasgo, other fields inherited from Moovies when barcode matches.
            "harmonized_title": title,
            "harmonized_format": moovies_format or clean_text(r.get("raw_format_l2")),
            "harmonized_director": None,
            "harmonized_studio": moovies_studio or clean_text(r.get("raw_label")),
            "harmonized_from_supplier": "moovies" if moovies_row else "lasgo",
            "harmonized_at": now_iso(),
            "published_to_catalog": False,
            "published_catalog_item_id": None,
            "raw_source_id": r.get("id"),
            "raw_source_table": "staging_lasgo_raw",
        }

    out = list(deduped_by_barcode.values())

    for batch in chunked(out, 1000):
        supabase.table(offers_table).upsert(batch, on_conflict="supplier,barcode").execute()

    # Update only harmonized title fields on existing Moovies rows.
    for update_row in moovies_title_updates:
        (
            supabase.table(offers_table)
            .update(
                {
                    "harmonized_title": update_row["harmonized_title"],
                    "harmonized_at": update_row["harmonized_at"],
                }
            )
            .eq("supplier", "moovies")
            .eq("barcode", update_row["barcode"])
            .execute()
        )

    print(f"Upserted {len(out)} staging_supplier_offers rows from Lasgo batch {batch_id}")
    return len(out)


def parse_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    text = str(value).replace("£", "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_int_default_zero(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def is_future_release(date_str: Optional[str]) -> bool:
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(str(date_str))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt > datetime.now(timezone.utc)
    except Exception:
        return False


def normalize_from_shopify(
    supabase,
    offers_table: str,
    batch_id: str,
) -> int:
    rows = fetch_all_rows_for_batch(
        supabase,
        "staging_shopify_raw",
        "id,shopify_product_id,shopify_variant_id,raw_title,raw_variant_title,raw_barcode,raw_sku,raw_price,raw_inventory_qty,raw_inventory_policy,raw_vendor,raw_director,raw_studio,raw_film_released,raw_media_release_date,raw_unit_cost_amount,raw_unit_cost_currency,source_filename,row_number",
        batch_id,
    )

    barcodes = [clean_text(r.get("raw_barcode")) for r in rows]
    lasgo_by_barcode = fetch_supplier_offer_by_barcode(
        supabase, offers_table, "lasgo", [b for b in barcodes if b]
    )
    moovies_by_barcode = fetch_supplier_offer_by_barcode(
        supabase, offers_table, "moovies", [b for b in barcodes if b]
    )

    deduped_by_variant: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        variant_id = clean_text(r.get("shopify_variant_id"))
        if not variant_id:
            continue

        barcode = clean_text(r.get("raw_barcode"))
        product_title = clean_text(r.get("raw_title"))
        variant_title = clean_text(r.get("raw_variant_title"))

        title = product_title
        if variant_title and variant_title.lower() not in {"default title", "default"}:
            title = f"{product_title} — {variant_title}" if product_title else variant_title

        qty = parse_int_default_zero(r.get("raw_inventory_qty"))
        media_release_date = parse_date(r.get("raw_media_release_date"))
        if is_future_release(media_release_date):
            availability_status = "preorder"
        elif qty > 0:
            availability_status = "store_stock"
        else:
            availability_status = "store_out"

        lasgo_row = lasgo_by_barcode.get(barcode) if barcode else None
        moovies_row = moovies_by_barcode.get(barcode) if barcode else None

        harmonized_title = (
            (lasgo_row or {}).get("harmonized_title")
            or (lasgo_row or {}).get("title")
            or title
        )
        harmonized_format = (
            (moovies_row or {}).get("harmonized_format")
            or (moovies_row or {}).get("format")
            or (lasgo_row or {}).get("harmonized_format")
            or (lasgo_row or {}).get("format")
            or variant_title
        )
        harmonized_studio = (
            (moovies_row or {}).get("harmonized_studio")
            or (moovies_row or {}).get("studio")
            or (lasgo_row or {}).get("harmonized_studio")
            or (lasgo_row or {}).get("studio")
            or clean_text(r.get("raw_studio"))
            or clean_text(r.get("raw_vendor"))
        )
        harmonized_from_supplier = (
            "lasgo" if lasgo_row else ("moovies" if moovies_row else "Tape Film")
        )

        deduped_by_variant[variant_id] = {
            "import_batch_id": batch_id,
            "supplier": "Tape Film",
            "source_filename": clean_text(r.get("source_filename")),
            "source_row_number": r.get("row_number"),
            "supplier_sku": clean_text(r.get("raw_sku")),
            "barcode": barcode,
            "title": title,
            "normalized_title": normalize_title(title or ""),
            "edition_title": None,
            "format": variant_title if variant_title and variant_title.lower() not in {"default title", "default"} else None,
            "media_type": "film",
            "director": clean_text(r.get("raw_director")),
            "studio": clean_text(r.get("raw_studio")) or clean_text(r.get("raw_vendor")),
            "media_release_date": media_release_date,
            "supplier_stock_status": qty,
            "availability_status": availability_status,
            "supplier_currency": clean_text(r.get("raw_unit_cost_currency")) or "AUD",
            "cost_price": parse_float(r.get("raw_unit_cost_amount")),
            "calculated_sale_price": parse_float(r.get("raw_price")),
            "source_priority": 0,
            "source_type": "shopify",
            "active": True,
            "harmonized_title": harmonized_title,
            "harmonized_format": harmonized_format,
            "harmonized_director": clean_text(r.get("raw_director")),
            "harmonized_studio": harmonized_studio,
            "harmonized_from_supplier": harmonized_from_supplier,
            "harmonized_at": now_iso(),
            "published_to_catalog": False,
            "published_catalog_item_id": None,
            "raw_source_id": r.get("id"),
            "raw_source_table": "staging_shopify_raw",
            "shopify_product_id": clean_text(r.get("shopify_product_id")),
            "shopify_variant_id": variant_id,
        }

    out = list(deduped_by_variant.values())

    variant_ids = [r.get("shopify_variant_id") for r in out if r.get("shopify_variant_id")]
    existing_by_variant: Dict[str, str] = {}
    for batch in chunked(variant_ids, 200):
        resp = (
            supabase.table(offers_table)
            .select("id,shopify_variant_id")
            .eq("supplier", "Tape Film")
            .in_("shopify_variant_id", batch)
            .execute()
        )
        for row in resp.data or []:
            vid = row.get("shopify_variant_id")
            if vid:
                existing_by_variant[vid] = row.get("id")

    to_insert: list[Dict[str, Any]] = []
    to_update: list[Dict[str, Any]] = []
    for row in out:
        vid = row.get("shopify_variant_id")
        if vid and vid in existing_by_variant:
            to_update.append(row)
        else:
            to_insert.append(row)

    # Under current schema, Tape Film also has unique(supplier, barcode).
    # If a barcode already exists, treat it as an update target.
    barcodes = [r.get("barcode") for r in to_insert if r.get("barcode")]
    existing_barcodes: set[str] = set()
    for batch in chunked(barcodes, 200):
        resp = (
            supabase.table(offers_table)
            .select("barcode")
            .eq("supplier", "Tape Film")
            .in_("barcode", batch)
            .execute()
        )
        for row in resp.data or []:
            bc = row.get("barcode")
            if bc:
                existing_barcodes.add(bc)

    still_to_insert: list[Dict[str, Any]] = []
    for row in to_insert:
        bc = row.get("barcode")
        if bc and bc in existing_barcodes:
            to_update.append(row)
        else:
            still_to_insert.append(row)
    to_insert = still_to_insert

    # Guard against duplicate Tape Film barcodes within the same run when the
    # table enforces unique(supplier, barcode). Last row wins.
    deduped_insert_by_key: Dict[str, Dict[str, Any]] = {}
    for row in to_insert:
        bc = row.get("barcode")
        vid = row.get("shopify_variant_id")
        key = f"barcode:{bc}" if bc else f"variant:{vid}"
        deduped_insert_by_key[key] = row
    to_insert = list(deduped_insert_by_key.values())

    for row in to_update:
        q = (
            supabase.table(offers_table)
            .update(row)
            .eq("supplier", "Tape Film")
        )
        if row.get("shopify_variant_id") in existing_by_variant:
            q = q.eq("shopify_variant_id", row["shopify_variant_id"])
        else:
            # fallback path when matching existing Tape Film row by barcode
            q = q.eq("barcode", row.get("barcode"))
        q.execute()

    for batch in chunked(to_insert, 1000):
        supabase.table(offers_table).insert(batch).execute()

    print(
        f"Synced {len(out)} staging_supplier_offers rows from Shopify batch {batch_id} "
        f"(updated: {len(to_update)}, inserted: {len(to_insert)})"
    )
    return len(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offers-table", default="staging_supplier_offers")
    parser.add_argument("--moovies-batch", default=None)
    parser.add_argument("--lasgo-batch", default=None)
    parser.add_argument("--shopify-batch", default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No writes. Requires both --moovies-batch and --lasgo-batch; reports barcode overlaps and title updates.",
    )
    args = parser.parse_args()

    if not args.moovies_batch and not args.lasgo_batch and not args.shopify_batch:
        die("Provide --moovies-batch and/or --lasgo-batch and/or --shopify-batch")

    load_dotenv(".env")
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")
    if not url:
        die("Missing SUPABASE_URL in .env")
    if not key:
        die("Missing SUPABASE_SERVICE_KEY in .env")

    supabase = create_client(url, key)

    if args.dry_run:
        if not args.moovies_batch or not args.lasgo_batch:
            die("--dry-run currently requires both --moovies-batch and --lasgo-batch")

        moovies_resp = (
            supabase.table("staging_moovies_raw")
            .select("raw_barcode,raw_title,raw_format,raw_category")
            .eq("import_batch_id", args.moovies_batch)
            .execute()
        )
        moovies_rows = moovies_resp.data or []
        moovies_by_barcode: Dict[str, Dict[str, Any]] = {}
        for r in moovies_rows:
            bc = clean_text(r.get("raw_barcode"))
            if not bc:
                continue
            if bc not in moovies_by_barcode:
                moovies_by_barcode[bc] = r

        lasgo_resp = (
            supabase.table("staging_lasgo_raw")
            .select("raw_ean,raw_title")
            .eq("import_batch_id", args.lasgo_batch)
            .execute()
        )
        lasgo_rows = lasgo_resp.data or []

        overlap = 0
        would_update_title = 0
        would_inherit_fields = 0
        samples_by_barcode: Dict[str, Dict[str, Any]] = {}

        for r in lasgo_rows:
            bc = clean_text(r.get("raw_ean"))
            if not bc:
                continue
            m = moovies_by_barcode.get(bc)
            if not m:
                continue
            overlap += 1

            lasgo_title = clean_text(r.get("raw_title"))
            moovies_title = clean_text(m.get("raw_title"))

            if lasgo_title:
                would_inherit_fields += 1
                if moovies_title != lasgo_title:
                    would_update_title += 1
                    if bc not in samples_by_barcode and len(samples_by_barcode) < 15:
                        samples_by_barcode[bc] = {
                            "barcode": bc,
                            "moovies_title": moovies_title,
                            "lasgo_title": lasgo_title,
                            "moovies_format": clean_text(m.get("raw_format")),
                            "moovies_category": clean_text(m.get("raw_category")),
                        }

        print("DRY RUN RESULTS")
        print("  moovies_batch:", args.moovies_batch)
        print("  lasgo_batch:", args.lasgo_batch)
        print("  moovies_distinct_barcodes:", len(moovies_by_barcode))
        print("  lasgo_rows:", len(lasgo_rows))
        print("  barcode_overlaps:", overlap)
        print("  lasgo_rows_with_title_on_overlap:", would_inherit_fields)
        print("  moovies_title_updates_needed:", would_update_title)
        samples = list(samples_by_barcode.values())
        if samples:
            print("\nSAMPLE TITLE UPDATES (first 15):")
            for s in samples:
                print(f"- {s['barcode']}")
                print(f"  moovies: {s['moovies_title']}")
                print(f"  lasgo:   {s['lasgo_title']}")
                if s.get("moovies_format") or s.get("moovies_category"):
                    print(f"  moovies format/category: {s.get('moovies_format')} / {s.get('moovies_category')}")
        return

    if args.moovies_batch:
        normalize_from_moovies(supabase, args.offers_table, args.moovies_batch)
    if args.lasgo_batch:
        normalize_from_lasgo(supabase, args.offers_table, args.lasgo_batch)
    if args.shopify_batch:
        normalize_from_shopify(supabase, args.offers_table, args.shopify_batch)


if __name__ == "__main__":
    main()

