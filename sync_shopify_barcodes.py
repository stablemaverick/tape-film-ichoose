import os
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


def shopify_graphql(query, variables=None):
    admin_token = get_admin_access_token()

    headers = {
        "X-Shopify-Access-Token": admin_token,
        "Content-Type": "application/json",
    }
    payload = {
        "query": query,
        "variables": variables or {},
    }

    response = requests.post(GRAPHQL_URL, headers=headers, json=payload, timeout=60)
    response.raise_for_status()

    data = response.json()
    if "errors" in data:
        raise RuntimeError(data["errors"])

    return data


def fetch_all_variants():
    query = """
    query VariantsPage($cursor: String) {
      productVariants(first: 250, after: $cursor) {
        edges {
          cursor
          node {
            id
            barcode
            sku
            product {
              id
              title
            }
          }
        }
        pageInfo {
          hasNextPage
        }
      }
    }
    """

    all_variants = []
    cursor = None

    while True:
        data = shopify_graphql(query, {"cursor": cursor})
        variants = data["data"]["productVariants"]
        edges = variants["edges"]

        for edge in edges:
            all_variants.append(edge["node"])

        if not variants["pageInfo"]["hasNextPage"]:
            break

        cursor = edges[-1]["cursor"] if edges else None
        if not cursor:
            break

    return all_variants


def main():
    print("Fetching Shopify variants...")
    variants = fetch_all_variants()
    print(f"Fetched {len(variants)} variants")

    barcode_map = {}
    for variant in variants:
        barcode = (variant.get("barcode") or "").strip()
        if not barcode:
            continue
        barcode_map[barcode] = {
            "shopify_variant_id": variant["id"],
            "shopify_product_id": variant["product"]["id"],
            "shopify_title": variant["product"]["title"],
        }

    print(f"Variants with barcode: {len(barcode_map)}")

    page_size = 1000
    offset = 0
    matched = 0

    while True:
        response = (
            supabase.table("catalog_items")
            .select("id,barcode,supplier,title")
            .range(offset, offset + page_size - 1)
            .execute()
        )

        rows = response.data or []
        if not rows:
            break

        for row in rows:
            barcode = (row.get("barcode") or "").strip()
            if not barcode:
                continue

            match = barcode_map.get(barcode)
            if not match:
                continue

            (
                supabase.table("catalog_items")
                .update(
                    {
                        "shopify_variant_id": match["shopify_variant_id"],
                        "shopify_product_id": match["shopify_product_id"],
                    }
                )
                .eq("id", row["id"])
                .execute()
            )
            matched += 1

        offset += page_size
        print(f"Processed {offset} catalog rows...")

    print(f"Done. Matched {matched} catalog rows to Shopify by barcode.")


if __name__ == "__main__":
    main()