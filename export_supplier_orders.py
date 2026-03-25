import csv
import os
from datetime import datetime, timezone
from pathlib import Path

import paramiko
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USERNAME = os.getenv("SFTP_USERNAME")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_BASE_PATH = os.getenv("SFTP_BASE_PATH", "/supplier-feeds")

if not SUPABASE_URL:
    raise ValueError("Missing SUPABASE_URL in .env")

if not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_SERVICE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def slugify(value: str) -> str:
    safe = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    while "--" in safe:
        safe = safe.replace("--", "-")
    return safe.strip("-") or "supplier"


def fetch_pending_rows():
    response = (
        supabase.table("supplier_orders")
        .select("id,supplier,title,product_code,barcode,quantity")
        .eq("status", "pending")
        .eq("exported_to_supplier", False)
        .order("supplier")
        .order("title")
        .execute()
    )
    return response.data or []


def aggregate_rows(rows):
    grouped = {}
    ids_by_group = {}

    for row in rows:
        supplier = (row.get("supplier") or "unknown").strip()
        product_code = (row.get("product_code") or "").strip()
        title = (row.get("title") or "").strip()
        barcode = (row.get("barcode") or "").strip()
        quantity = int(row.get("quantity") or 0)

        key = (supplier, product_code, title, barcode)

        if key not in grouped:
            grouped[key] = {
                "supplier": supplier,
                "product_code": product_code,
                "title": title,
                "barcode": barcode,
                "quantity": 0,
            }
            ids_by_group[key] = []

        grouped[key]["quantity"] += quantity
        ids_by_group[key].append(row["id"])

    suppliers = {}
    supplier_ids = {}

    for key, item in grouped.items():
        supplier = item["supplier"]
        suppliers.setdefault(supplier, []).append(item)
        supplier_ids.setdefault(supplier, []).extend(ids_by_group[key])

    return suppliers, supplier_ids


def ensure_local_output_dir() -> Path:
    out_dir = Path("supplier_exports")
    out_dir.mkdir(exist_ok=True)
    return out_dir


def write_supplier_csv(supplier: str, items: list[dict], out_dir: Path) -> Path:
    date_str = datetime.now().strftime("%Y-%m-%d")
    supplier_slug = slugify(supplier)
    filename = f"{supplier_slug}_orders_{date_str}.csv"
    filepath = out_dir / filename

    with filepath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Product Code", "Title", "Barcode", "Quantity"],
        )
        writer.writeheader()

        for item in sorted(items, key=lambda x: (x["title"].lower(), x["product_code"].lower())):
            writer.writerow(
                {
                    "Product Code": item["product_code"],
                    "Title": item["title"],
                    "Barcode": item["barcode"],
                    "Quantity": item["quantity"],
                }
            )

    return filepath


def upload_file_sftp(local_path: Path, supplier: str):
    if not all([SFTP_HOST, SFTP_USERNAME, SFTP_PASSWORD]):
        raise ValueError("Missing SFTP credentials in .env")

    supplier_slug = slugify(supplier)
    remote_dir = f"supplier-feeds/{supplier_slug}/outgoing"
    remote_path = f"{remote_dir}/{local_path.name}"

    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)

    try:
        sftp = paramiko.SFTPClient.from_transport(transport)

        print("Uploading to remote path:", remote_path)
        sftp.put(str(local_path), remote_path)
        print(f"Uploaded to SFTP: {remote_path}")

    finally:
        transport.close()

def mark_rows_exported(row_ids: list[str]):
    if not row_ids:
        return

    now_iso = datetime.now(timezone.utc).isoformat()

    response = (
        supabase.table("supplier_orders")
        .update(
            {
                "exported_to_supplier": True,
                "exported_at": now_iso,
            }
        )
        .in_("id", row_ids)
        .execute()
    )

    return response


def main(upload: bool = False):
    rows = fetch_pending_rows()

    if not rows:
        print("No pending supplier orders to export.")
        return

    suppliers, supplier_ids = aggregate_rows(rows)
    out_dir = ensure_local_output_dir()

    print(f"Found {len(rows)} pending supplier order rows.")
    print(f"Creating {len(suppliers)} supplier export file(s).")

    for supplier, items in suppliers.items():
        csv_path = write_supplier_csv(supplier, items, out_dir)
        print(f"Created CSV: {csv_path}")

        if upload:
            upload_file_sftp(csv_path, supplier)

        mark_rows_exported(supplier_ids[supplier])
        print(f"Marked exported rows for supplier: {supplier}")


if __name__ == "__main__":
    import sys

    should_upload = "--upload" in sys.argv
    main(upload=should_upload)