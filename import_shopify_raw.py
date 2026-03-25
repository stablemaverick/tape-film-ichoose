import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import requests
from dotenv import load_dotenv
from supabase import create_client


def die(msg: str) -> "None":
    print(msg, file=sys.stderr)
    raise SystemExit(1)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def chunked(items: Iterable[Dict[str, Any]], size: int) -> Iterable[list[Dict[str, Any]]]:
    batch: list[Dict[str, Any]] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def get_admin_access_token(shop: str, client_id: str, client_secret: str) -> str:
    url = f"https://{shop}/admin/oauth/access_token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    response = requests.post(url, headers=headers, data=data, timeout=30)
    response.raise_for_status()
    payload = response.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"No access token returned: {payload}")
    return token


def graph_ql(graphql_url: str, access_token: str, query: str, variables: dict | None = None) -> dict:
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }
    response = requests.post(
        graphql_url,
        headers=headers,
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError(payload["errors"])
    return payload["data"]


def fetch_products(graphql_url: str, access_token: str) -> List[dict]:
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

    all_products: list[dict] = []
    cursor = None
    while True:
        data = graph_ql(graphql_url, access_token, query, {"cursor": cursor})
        block = data["products"]
        all_products.extend(block["nodes"])
        if not block["pageInfo"]["hasNextPage"]:
            break
        cursor = block["pageInfo"]["endCursor"]
    return all_products


def import_shopify_raw(table: str = "staging_shopify_raw") -> str:
    load_dotenv(".env")

    shop = os.getenv("SHOPIFY_SHOP")
    client_id = os.getenv("SHOPIFY_CLIENT_ID")
    client_secret = os.getenv("SHOPIFY_CLIENT_SECRET")
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

    if not shop:
        die("Missing SHOPIFY_SHOP in .env")
    if not client_id:
        die("Missing SHOPIFY_CLIENT_ID in .env")
    if not client_secret:
        die("Missing SHOPIFY_CLIENT_SECRET in .env")
    if not supabase_url:
        die("Missing SUPABASE_URL in .env")
    if not supabase_key:
        die("Missing SUPABASE_SERVICE_KEY in .env")

    api_version = "2026-04"
    graphql_url = f"https://{shop}/admin/api/{api_version}/graphql.json"
    access_token = get_admin_access_token(shop, client_id, client_secret)
    supabase = create_client(supabase_url, supabase_key)

    products = fetch_products(graphql_url, access_token)
    batch_id = str(uuid.uuid4())
    rows: list[Dict[str, Any]] = []

    for product in products:
        product_id = product.get("id")
        product_title = clean_text(product.get("title"))
        vendor = clean_text(product.get("vendor"))
        director_meta = clean_text((product.get("directorMeta") or {}).get("value"))
        studio_meta = clean_text((product.get("studioMeta") or {}).get("value"))
        film_released_meta = clean_text((product.get("filmReleasedMeta") or {}).get("value"))
        media_release_meta = clean_text((product.get("mediaReleaseMeta") or {}).get("value"))

        for idx, variant in enumerate(product.get("variants", {}).get("nodes", []), start=1):
            variant_id = variant.get("id")
            if not variant_id:
                continue
            unit_cost = (variant.get("inventoryItem") or {}).get("unitCost") or {}
            rows.append(
                {
                    "import_batch_id": batch_id,
                    "imported_at": now_iso(),
                    "supplier": "Tape Film",
                    "source_filename": "shopify_graphql_api",
                    "row_number": idx,
                    "shopify_product_id": product_id,
                    "shopify_variant_id": variant_id,
                    "raw_title": product_title,
                    "raw_variant_title": clean_text(variant.get("title")),
                    "raw_barcode": clean_text(variant.get("barcode")),
                    "raw_sku": clean_text(variant.get("sku")),
                    "raw_price": clean_text(variant.get("price")),
                    "raw_inventory_qty": clean_text(variant.get("inventoryQuantity")),
                    "raw_inventory_policy": clean_text(variant.get("inventoryPolicy")),
                    "raw_vendor": vendor,
                    "raw_director": director_meta,
                    "raw_studio": studio_meta,
                    "raw_film_released": film_released_meta,
                    "raw_media_release_date": media_release_meta,
                    "raw_unit_cost_amount": clean_text(unit_cost.get("amount")),
                    "raw_unit_cost_currency": clean_text(unit_cost.get("currencyCode")),
                    "raw_payload": {
                        "product": product,
                        "variant": variant,
                    },
                }
            )

    for batch in chunked(rows, 500):
        supabase.table(table).upsert(batch, on_conflict="supplier,shopify_variant_id").execute()

    print(f"Imported {len(rows)} Shopify raw rows. Batch: {batch_id}")
    return batch_id


if __name__ == "__main__":
    import_shopify_raw()

