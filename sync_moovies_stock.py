import os
import sys
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL:
    raise ValueError("Missing SUPABASE_URL in .env")

if not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_SERVICE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None


def values_equal(a, b):
    if a is None and b is None:
        return True
    return str(a) == str(b)


def numbers_equal(a, b):
    try:
        a_num = float(a) if a is not None else 0.0
        b_num = float(b) if b is not None else 0.0
        return abs(a_num - b_num) < 0.0001
    except Exception:
        return False


def has_meaningful_change(existing_row, new_row):
    return any([
        not values_equal(existing_row.get("format"), new_row.get("format")),
        not numbers_equal(existing_row.get("cost_price"), new_row.get("cost_price")),
        not numbers_equal(existing_row.get("calculated_sale_price"), new_row.get("calculated_sale_price")),
        not numbers_equal(existing_row.get("supplier_stock_status"), new_row.get("supplier_stock_status")),
        not values_equal(existing_row.get("availability_status"), new_row.get("availability_status")),
        not values_equal(existing_row.get("active"), new_row.get("active")),
    ])


def normalize_barcode(value):
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if text == "":
        return None

    # Common Excel cleanup
    if text.endswith(".0"):
        text = text[:-2]

    try:
        if text.replace(".", "", 1).isdigit():
            if "." in text:
                text = str(int(float(text)))
    except Exception:
        pass

    return text

def write_unmatched_to_csv(unmatched_rows, filepath):
    if not unmatched_rows:
        return None

    output_path = filepath.rsplit(".", 1)[0] + "_UNMATCHED.csv"
    df = pd.DataFrame(unmatched_rows)
    df.to_csv(output_path, index=False)
    return output_path


def parse_float(value):
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text == "":
        return None

    text = text.replace("£", "").replace(",", "").strip()

    try:
        return float(text)
    except ValueError:
        return None


def parse_int(value):
    if pd.isna(value):
        return 0

    text = str(value).strip()
    if text == "":
        return 0

    text = text.replace(",", "").replace("+", "").strip()

    try:
        return int(float(text))
    except ValueError:
        return 0


def round_up_to_99(value):
    rounded = round(value, 2)
    whole = int(rounded)

    if rounded <= whole + 0.99:
        return round(whole + 0.99, 2)

    return round((whole + 1) + 0.99, 2)


def get_margin(cost_gbp):
    if cost_gbp <= 15:
        return 0.32
    elif cost_gbp <= 30:
        return 0.28
    elif cost_gbp <= 40:
        return 0.24
    else:
        return 0.20


def calculate_sale_price(cost_gbp):
    aud_base = cost_gbp * 2
    total_cost = aud_base * 1.12
    margin = get_margin(cost_gbp)
    pre_gst_sale = total_cost * (1 + margin)
    final_sale = pre_gst_sale * 1.10
    return round_up_to_99(final_sale)


def map_availability(stock_qty):
    return "supplier_stock" if stock_qty > 0 else "supplier_out"


def fetch_existing_moovies_rows(barcodes):
    existing = {}
    clean_barcodes = [normalize_barcode(b) for b in barcodes if normalize_barcode(b)]
    batch_size = 200

    for i in range(0, len(clean_barcodes), batch_size):
        batch = clean_barcodes[i:i + batch_size]

        response = (
            supabase.table("catalog_items")
            .select("""
                id,
                supplier,
                barcode,
                title,
                format,
                cost_price,
                calculated_sale_price,
                availability_status,
                supplier_stock_status,
                supplier_last_seen_at,
                active
            """)
            .eq("supplier", "Moovies")
            .in_("barcode", batch)
            .execute()
        )

        for row in response.data or []:
            existing[normalize_barcode(row.get("barcode"))] = row

    return existing


def load_stock_file(filepath):
    if filepath.lower().endswith(".xlsx"):
        df = pd.read_excel(filepath, dtype=str)
    elif filepath.lower().endswith(".txt"):
        df = pd.read_csv(filepath, sep="|", dtype=str)
    else:
        df = pd.read_csv(filepath, dtype=str)

    print(f"Rows found: {len(df)}")
    print("Columns detected:", list(df.columns))

    column_map = {
        "SKU": "barcode",
        "Qty": "qty",
        "Price": "price",
        "Format": "format",
    }

    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    required = ["barcode", "qty", "price"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in stock file: {missing}")

    return df

    df = df.rename(columns={k: v for k, v in column_map.items() if k in df.columns})

    required = ["barcode", "qty", "price"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in stock file: {missing}")

    return df


def build_update_row(source_row, existing_row):
    barcode = normalize_barcode(source_row.get("barcode"))
    stock_qty = parse_int(source_row.get("qty"))
    cost_price = parse_float(source_row.get("price"))
    file_format = clean_text(source_row.get("format"))

    sale_price = calculate_sale_price(cost_price) if cost_price is not None else existing_row.get("calculated_sale_price")

    return {
        "id": existing_row["id"],
        "barcode": barcode,
        "format": file_format or existing_row.get("format"),
        "cost_price": cost_price if cost_price is not None else existing_row.get("cost_price"),
        "pricing_source": "gbp_formula_v1",
        "calculated_sale_price": sale_price,
        "supplier_stock_status": stock_qty,
        "availability_status": map_availability(stock_qty),
        "supplier_last_seen_at": now_iso(),
        "active": True,
    }


def update_catalog_row(row_id, data):
    payload = dict(data)
    payload.pop("id", None)
    payload.pop("barcode", None)

    (
        supabase.table("catalog_items")
        .update(payload)
        .eq("id", row_id)
        .execute()
    )


def main(filepath):
    df = load_stock_file(filepath)

    rows = df.to_dict(orient="records")
    rows = [r for r in rows if normalize_barcode(r.get("barcode"))]

    print(f"Valid stock rows: {len(rows)}")

    incoming_barcodes = [normalize_barcode(r.get("barcode")) for r in rows]
    existing_rows = fetch_existing_moovies_rows(incoming_barcodes)

    rows_to_update = []
    unmatched_rows = []
    unchanged_rows = 0

    for source_row in rows:
        barcode = normalize_barcode(source_row.get("barcode"))
        existing_row = existing_rows.get(barcode)

        if not existing_row:
            unmatched_rows.append({
                "barcode": barcode,
                "qty": source_row.get("qty"),
                "price": source_row.get("price"),
                "format": source_row.get("format"),
            })
            continue


        update_row = build_update_row(source_row, existing_row)

        if has_meaningful_change(existing_row, update_row):
            rows_to_update.append(update_row)
        else:
            unchanged_rows += 1

    print(f"Rows to update: {len(rows_to_update)}")
    print(f"Rows unchanged: {unchanged_rows}")
    print(f"Unmatched rows: {len(unmatched_rows)}")

    batch_size = 100

    for i in range(0, len(rows_to_update), batch_size):
        batch = rows_to_update[i:i + batch_size]

        for row in batch:
            update_catalog_row(row["id"], row)

        print(f"Updated batch {i} - {i + len(batch)}")

    output_path = write_unmatched_to_csv(unmatched_rows, filepath)
    if output_path:
        print(f"Unmatched rows written to: {output_path}")

    print("Done syncing Moovies stock file into catalog_items")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sync_moovies_stock.py <file.csv|file.xlsx>")
        raise SystemExit(1)

    main(sys.argv[1])
