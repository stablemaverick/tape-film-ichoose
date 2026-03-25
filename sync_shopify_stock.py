import os
import re
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(dotenv_path=".env")

SHOP = os.getenv("SHOPIFY_SHOP")
CLIENT_ID = os.getenv("SHOPIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SHOP:
    raise ValueError("Missing SHOPIFY_SHOP in .env")
if not CLIENT_ID:
    raise ValueError("Missing SHOPIFY_CLIENT_ID in .env")
if not CLIENT_SECRET:
    raise ValueError("Missing SHOPIFY_CLIENT_SECRET in .env")
if not SUPABASE_URL:
    raise ValueError("Missing SUPABASE_URL in .env")
if not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_SERVICE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

API_VERSION = "2026-04"
GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"


def get_admin_access_token():
    url = f"https://{SHOP}/admin/oauth/access_token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(url, headers=headers, data=data, timeout=30)
    response.raise_for_status()

    payload = response.json()
    token = payload.get("access_token")

    if not token:
        raise RuntimeError(f"No access token returned: {payload}")

    return token

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def clean_text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def clean_date(value):
    value = clean_text(value)
    return value or None


def update_catalog_row_by_id(row_id, data):
    cleaned = dict(data)
    cleaned.pop("id", None)

    (
        supabase.table("catalog_items")
        .update(cleaned)
        .eq("id", row_id)
        .execute()
    )

def dedupe_rows_by_variant_id(rows):
    deduped = {}
    no_variant_rows = []

    for row in rows:
        variant_id = row.get("shopify_variant_id")
        if variant_id:
            deduped[variant_id] = row
        else:
            no_variant_rows.append(row)

    return list(deduped.values()) + no_variant_rows


def insert_catalog_rows(rows):
    cleaned_rows = []
    for row in rows:
        cleaned = dict(row)
        cleaned.pop("id", None)
        cleaned_rows.append(cleaned)

    if cleaned_rows:
        (
            supabase.table("catalog_items")
            .upsert(cleaned_rows, on_conflict="shopify_variant_id")
            .execute()
        )


def fetch_existing_shopify_rows():
    existing = {}
    response = (
        supabase.table("catalog_items")
        .select("""
            shopify_variant_id,
            id,
            barcode,
            studio,
            cost_price,
            supplier_currency
        """)
        .eq("source_type", "shopify")
        .not_.is_("shopify_variant_id", "null")
        .execute()
    )

    for row in response.data or []:
        existing[row["shopify_variant_id"]] = row

    return existing

def fetch_existing_tape_rows_by_barcode(barcodes):
    existing = {}
    clean_barcodes = [b for b in barcodes if b]

    batch_size = 200
    for i in range(0, len(clean_barcodes), batch_size):
        batch = clean_barcodes[i:i + batch_size]

        response = (
            supabase.table("catalog_items")
            .select("id,barcode")
            .eq("supplier", "TAPE Film")
            .in_("barcode", batch)
            .execute()
        )

        for row in response.data or []:
            existing[row["barcode"]] = row["id"]

    return existing


def normalize_title(value):
    if not value:
        return ""

    text = str(value).lower().strip()
    text = re.sub(r"\b4k\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\buhd\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bblu[\s-]?ray\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdvd\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\blimited edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bcollector'?s edition\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bsteelbook\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bbox set\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_future_release(date_str):
    if not date_str:
        return False
    try:
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt > datetime.now(timezone.utc)
    except Exception:
        return False

def fetch_existing_shopify_variant_ids():
    response = (
        supabase.table("catalog_items")
        .select("shopify_variant_id")
        .eq("source_type", "shopify")
        .not_.is_("shopify_variant_id", "null")
        .execute()
    )

    return [row["shopify_variant_id"] for row in response.data or []]


def graph_ql(query, variables=None):
    access_token = get_admin_access_token()

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    response = requests.post(
        GRAPHQL_URL,
        headers=headers,
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    response.raise_for_status()

    payload = response.json()

    if payload.get("errors"):
        raise RuntimeError(payload["errors"])

    return payload["data"]


def fetch_products():
    query = """
    query GetProducts($cursor: String) {
      products(first: 50, after: $cursor, query: "status:active") {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          id
          title
          vendor
          status
          directorMeta: metafield(namespace: "custom", key: "director") { value }
          studioMeta: metafield(namespace: "custom", key: "studio") { value }
          filmReleasedMeta: metafield(namespace: "custom", key: "film_released") { value }
          mediaReleaseMeta: metafield(namespace: "custom", key: "media_release_date") { value }
          variants(first: 50) {
            nodes {
              id
              title
              sku
              barcode
              price
              inventoryQuantity
              inventoryPolicy
              inventoryItem {
                id
                unitCost {
                  amount
                  currencyCode
                }
              }
            }
          }
        }
      }
    }
    """

    all_products = []
    cursor = None

    while True:
        data = graph_ql(query, {"cursor": cursor})
        block = data["products"]
        all_products.extend(block["nodes"])

        if not block["pageInfo"]["hasNextPage"]:
            break

        cursor = block["pageInfo"]["endCursor"]

    return all_products


def fetch_existing_film_ids_by_barcode(barcodes):
    barcode_to_film_id = {}
    clean_barcodes = [b for b in barcodes if b]

    batch_size = 200
    for i in range(0, len(clean_barcodes), batch_size):
        batch = clean_barcodes[i:i + batch_size]

        response = (
            supabase.table("catalog_items")
            .select("barcode,film_id")
            .in_("barcode", batch)
            .not_.is_("film_id", "null")
            .execute()
        )

        for row in response.data or []:
            barcode_to_film_id[row["barcode"]] = row["film_id"]

    return barcode_to_film_id


def find_existing_film_id_by_title(title):
    normalized = normalize_title(title)
    if not normalized:
        return None

    response = (
        supabase.table("films")
        .select("id,title")
        .limit(5000)
        .execute()
    )

    for row in response.data or []:
        if normalize_title(row.get("title")) == normalized:
            return row["id"]

    return None


def map_variant_to_catalog_row(product, variant, barcode_to_film_id):
    barcode = clean_text(variant.get("barcode"))
    product_title = clean_text(product.get("title"))
    variant_title = clean_text(variant.get("title"))
    vendor = clean_text(product.get("vendor"))

    director = clean_text((product.get("directorMeta") or {}).get("value"))
    studio = clean_text((product.get("studioMeta") or {}).get("value")) or vendor
    film_released = clean_date((product.get("filmReleasedMeta") or {}).get("value"))
    media_release_date = clean_date((product.get("mediaReleaseMeta") or {}).get("value"))

    inventory_qty = int(variant.get("inventoryQuantity") or 0)
    inventory_policy = clean_text(variant.get("inventoryPolicy"))
    price = float(variant.get("price") or 0)

    inventory_item = variant.get("inventoryItem") or {}
    unit_cost = inventory_item.get("unitCost") or {}
    cost_amount = unit_cost.get("amount")
    cost_currency = unit_cost.get("currencyCode")

    if is_future_release(media_release_date):
        availability_status = "preorder"
    elif inventory_qty > 0:
        availability_status = "store_stock"
    else:
        availability_status = "store_out"

    title = product_title
    if variant_title and variant_title.lower() not in {"default title", "default"}:
        title = f"{product_title} — {variant_title}"

    film_id = barcode_to_film_id.get(barcode)
    if not film_id:
        film_id = find_existing_film_id_by_title(product_title)

    return {
        "title": title,
        "edition_title": None,
        "format": variant_title if variant_title and variant_title.lower() not in {"default title", "default"} else None,
        "director": director,
        "studio": studio,
        "film_released": film_released,
        "media_release_date": media_release_date,
        "barcode": barcode,
        "supplier": "TAPE Film",
        "supplier_sku": clean_text(variant.get("sku")),
        "supplier_currency": clean_text(cost_currency) or "AUD",
        "cost_price": float(cost_amount) if cost_amount not in (None, "") else None,
        "pricing_source": "shopify_live",
        "calculated_sale_price": price if price > 0 else None,
        "availability_status": availability_status,
        "supplier_stock_status": inventory_qty,
        "supplier_priority": 0,
        "source_type": "shopify",
        "active": True,
        "supplier_last_seen_at": now_iso(),
        "shopify_product_id": product.get("id"),
        "shopify_variant_id": variant.get("id"),
        "film_id": film_id,
    }


def main():
    products = fetch_products()
    print(f"Fetched {len(products)} Shopify products")

    all_barcodes = []
    for product in products:
        for variant in product.get("variants", {}).get("nodes", []):
            barcode = clean_text(variant.get("barcode"))
            if barcode:
                all_barcodes.append(barcode)

    barcode_to_film_id = fetch_existing_film_ids_by_barcode(all_barcodes)
    existing_tape_rows_by_barcode = fetch_existing_tape_rows_by_barcode(all_barcodes)
    existing_shopify_rows = fetch_existing_shopify_rows()
    existing_shopify_variant_ids = fetch_existing_shopify_variant_ids()

    rows_to_update = []
    rows_to_insert = []
    seen_variant_ids = set()

    for product in products:
        for variant in product.get("variants", {}).get("nodes", []):
            variant_id = variant.get("id")
            seen_variant_ids.add(variant_id)

            mapped = map_variant_to_catalog_row(product, variant, barcode_to_film_id)

            barcode = mapped.get("barcode")
            existing_id = None
            existing_row = None

            if barcode:
                existing_id = existing_tape_rows_by_barcode.get(barcode)

            existing_row = existing_shopify_rows.get(variant_id)

            if not existing_id and existing_row:
                existing_id = existing_row.get("id")

            if existing_id:
                mapped["id"] = existing_id

            if existing_row:
                if not mapped.get("barcode"):
                    mapped["barcode"] = clean_text(existing_row.get("barcode"))

                if not mapped.get("studio"):
                    mapped["studio"] = clean_text(existing_row.get("studio"))

                if mapped.get("cost_price") is None:
                    mapped["cost_price"] = existing_row.get("cost_price")

                if not mapped.get("supplier_currency"):
                    mapped["supplier_currency"] = clean_text(existing_row.get("supplier_currency"))

            if mapped.get("id"):
                rows_to_update.append(mapped)
            else:
                rows_to_insert.append(mapped)

    print(f"Prepared {len(rows_to_update)} Shopify rows to update")
    print(f"Prepared {len(rows_to_insert)} Shopify rows to insert")

    rows_to_update = dedupe_rows_by_variant_id(rows_to_update)
    rows_to_insert = dedupe_rows_by_variant_id(rows_to_insert)


    print(f"Deduped to {len(rows_to_update)} Shopify rows to update")
    print(f"Deduped to {len(rows_to_insert)} Shopify rows to insert")

    batch_size = 100

    # Update existing Shopify rows
    for i in range(0, len(rows_to_update), batch_size):
        batch = rows_to_update[i:i + batch_size]

        for row in batch:
            update_catalog_row_by_id(row["id"], row)

        print(f"Updated batch {i} - {i + len(batch)}")

    # Insert any genuinely new Shopify rows
    if rows_to_insert:
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i + batch_size]

            for row in batch:
                print("INSERT CANDIDATE:", row.get("shopify_variant_id"), row.get("title"))

            insert_catalog_rows(batch)
            print(f"Inserted batch {i} - {i + len(batch)}")

    # Mark Shopify rows inactive if they no longer exist in Shopify
    missing_variant_ids = [v for v in existing_shopify_variant_ids if v not in seen_variant_ids]

    for i in range(0, len(missing_variant_ids), batch_size):
        batch = missing_variant_ids[i:i + batch_size]
        (
            supabase.table("catalog_items")
            .update({
                "active": False,
                "availability_status": "store_out",
                "supplier_stock_status": 0,
                "supplier_last_seen_at": now_iso(),
            })
            .in_("shopify_variant_id", batch)
            .execute()
        )

    if missing_variant_ids:
        print(f"Marked {len(missing_variant_ids)} missing Shopify offers inactive")

    print("Done syncing Shopify stock into catalog_items")


if __name__ == "__main__":
    main()
