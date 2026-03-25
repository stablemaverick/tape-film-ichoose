import argparse
import os
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from supabase import create_client


def die(msg: str) -> "None":
    raise SystemExit(msg)


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def slugify(title: str) -> str:
    s = title.lower().strip()
    s = re.sub(r"['']+", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "product"


def parse_date(value: Any) -> Optional[date]:
    text = clean_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def is_preorder(row: Dict[str, Any]) -> bool:
    release = parse_date(row.get("media_release_date"))
    if not release:
        return False
    return release > date.today()


def derive_region(row: Dict[str, Any]) -> str:
    supplier = (clean_text(row.get("supplier")) or "").lower()
    if supplier in ("moovies", "lasgo", "tape film"):
        return "Region B"
    return "Region A"


def build_tags(row: Dict[str, Any]) -> List[str]:
    tags = ["auto-sync", "barcode-publish"]
    barcode = clean_text(row.get("barcode"))
    if barcode:
        tags.append(f"barcode:{barcode}")
    fmt = clean_text(row.get("format"))
    if fmt:
        tags.append(fmt)
    director = clean_text(row.get("director"))
    if director:
        tags.append(director)
    genres = clean_text(row.get("genres"))
    if genres:
        for g in genres.split(","):
            g = g.strip()
            if g:
                tags.append(g)
    return tags


def parse_barcodes(args: argparse.Namespace) -> List[str]:
    out: List[str] = []
    if args.barcodes:
        out.extend([b.strip() for b in args.barcodes.split(",") if b.strip()])
    if args.barcodes_file:
        with open(args.barcodes_file, "r", encoding="utf-8") as f:
            for line in f:
                b = line.strip()
                if b:
                    out.append(b)
    seen = set()
    uniq = []
    for b in out:
        if b in seen:
            continue
        seen.add(b)
        uniq.append(b)
    return uniq


def get_admin_access_token(shop: str, client_id: str, client_secret: str) -> str:
    url = f"https://{shop}/admin/oauth/access_token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    resp = requests.post(
        url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=payload,
        timeout=30,
    )
    resp.raise_for_status()
    token = (resp.json() or {}).get("access_token")
    if not token:
        raise RuntimeError(f"No access token returned: {resp.text}")
    return token


def graph_ql(graphql_url: str, access_token: str, query: str, variables: dict | None = None) -> dict:
    headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}
    resp = requests.post(
        graphql_url,
        headers=headers,
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise RuntimeError(payload["errors"])
    return payload["data"]


def variant_exists_by_barcode(graphql_url: str, access_token: str, barcode: str) -> Optional[Dict[str, Any]]:
    query = """
    query ($q: String!) {
      productVariants(first: 1, query: $q) {
        nodes {
          id
          barcode
          sku
          product { id title }
        }
      }
    }
    """
    data = graph_ql(graphql_url, access_token, query, {"q": f"barcode:{barcode}"})
    nodes = data.get("productVariants", {}).get("nodes", []) or []
    return nodes[0] if nodes else None


CATALOG_SELECT = (
    "id,title,barcode,sku,supplier,supplier_sku,source_type,"
    "supplier_stock_status,availability_status,"
    "cost_price,calculated_sale_price,"
    "director,studio,film_released,media_release_date,"
    "format,category,notes,supplier_priority,"
    "genres,top_cast,country_of_origin,"
    "tmdb_poster_path"
)


def fetch_catalog_rows_for_barcodes(supabase, barcodes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {b: [] for b in barcodes}
    for barcode in barcodes:
        rows = (
            supabase.table("catalog_items")
            .select(CATALOG_SELECT)
            .eq("barcode", barcode)
            .eq("active", True)
            .execute()
            .data
            or []
        )
        out[barcode] = rows
    return out


def pick_best_row(rows: List[Dict[str, Any]], supplier_preference: str = "best_offer") -> Optional[Dict[str, Any]]:
    if not rows:
        return None

    filtered = rows
    if supplier_preference != "best_offer":
        wanted = supplier_preference.strip().lower()
        filtered = [r for r in rows if (clean_text(r.get("supplier")) or "").strip().lower() == wanted]
        if not filtered:
            return None

    def avail_rank(v: str | None) -> int:
        t = (v or "").lower()
        if t == "in_stock":
            return 0
        if t == "supplier_stock":
            return 1
        if t == "supplier_preorder":
            return 2
        if t == "supplier_out":
            return 3
        return 9

    def sort_key(r: Dict[str, Any]):
        supplier = clean_text(r.get("supplier")) or ""
        stock = int(r.get("supplier_stock_status") or 0)
        price = float(r.get("calculated_sale_price") or 999999)
        src_pri = int(r.get("supplier_priority") or 99)
        return (
            0 if supplier == "Tape Film" else 1,
            avail_rank(clean_text(r.get("availability_status"))),
            -stock,
            src_pri,
            price,
            0 if clean_text(r.get("title")) else 1,
        )

    return sorted(filtered, key=sort_key)[0]


def build_metafields(row: Dict[str, Any]) -> List[Dict[str, str]]:
    mf: List[Dict[str, str]] = []

    def add(key: str, mf_type: str, value: Optional[str]):
        if value:
            mf.append({"namespace": "custom", "key": key, "type": mf_type, "value": value})

    add("director", "single_line_text_field", clean_text(row.get("director")))
    add("studio", "single_line_text_field", clean_text(row.get("studio")))
    add("format", "single_line_text_field", clean_text(row.get("format")))
    add("starring", "multi_line_text_field", clean_text(row.get("top_cast")))
    add("country_of_origin", "single_line_text_field", clean_text(row.get("country_of_origin")))
    add("region", "single_line_text_field", derive_region(row))

    film_released = parse_date(row.get("film_released"))
    if film_released:
        add("film_released", "date", film_released.isoformat())

    media_release = parse_date(row.get("media_release_date"))
    if media_release:
        add("media_release_date", "date", media_release.isoformat())

    preorder = is_preorder(row)
    add("pre_order", "boolean", "true" if preorder else "false")
    if preorder:
        add("po_flag", "single_line_text_field", "Pre-Order")

    return mf


def create_product_graphql(
    graphql_url: str,
    access_token: str,
    barcode: str,
    row: Dict[str, Any],
    status: str,
) -> Dict[str, Any]:
    title = clean_text(row.get("title")) or f"Film {barcode}"
    handle = slugify(title)
    sku = clean_text(row.get("supplier_sku")) or clean_text(row.get("sku")) or barcode

    price = str(row.get("calculated_sale_price")) if row.get("calculated_sale_price") is not None else "0.00"

    gbp_to_aud = float(os.getenv("GBP_AUD_RATE", "1.95"))
    landed_markup = float(os.getenv("LANDED_COST_MARKUP", "1.12"))
    raw_cost = float(row.get("cost_price") or 0) if row.get("cost_price") is not None else None
    cost = f"{raw_cost * gbp_to_aud * landed_markup:.2f}" if raw_cost is not None else None

    tags = build_tags(row)
    metafields = build_metafields(row)

    gql_status = status.upper()
    mutation = """
    mutation ProductSet($synchronous: Boolean!, $input: ProductSetInput!) {
      productSet(synchronous: $synchronous, input: $input) {
        product {
          id
          title
          handle
          status
          variants(first: 1) {
            nodes {
              id
              sku
              barcode
              price
            }
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    variant_input: Dict[str, Any] = {
        "sku": sku,
        "barcode": barcode,
        "price": price,
        "inventoryPolicy": "DENY",
        "optionValues": [{"optionName": "Title", "name": "Default Title"}],
        "inventoryItem": {
            "tracked": True,
            "measurement": {"weight": {"value": 0.25, "unit": "KILOGRAMS"}},
        },
    }
    if cost is not None:
        variant_input["inventoryItem"]["cost"] = cost

    product_input: Dict[str, Any] = {
        "title": title,
        "handle": handle,
        "vendor": "TAPE! FILM",
        "category": "gid://shopify/TaxonomyCategory/me-7-1",
        "status": gql_status,
        "tags": tags,
        "seo": {"title": title},
        "metafields": metafields,
        "productOptions": [
            {"name": "Title", "position": 1, "values": [{"name": "Default Title"}]},
        ],
        "variants": [variant_input],
    }

    data = graph_ql(graphql_url, access_token, mutation, {
        "synchronous": True,
        "input": product_input,
    })
    payload = data.get("productSet") or {}
    errs = payload.get("userErrors") or []
    if errs:
        raise RuntimeError(f"productSet userErrors: {errs}")
    return payload.get("product") or {}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish selected barcodes from catalog_items to Shopify as draft products."
    )
    parser.add_argument("--barcodes", default=None, help="Comma-separated list of barcodes")
    parser.add_argument("--barcodes-file", default=None, help="Path to newline-separated barcodes file")
    parser.add_argument(
        "--supplier",
        default="best_offer",
        help=(
            "Supplier selection mode: 'best_offer' (default) or a supplier name "
            "(e.g. 'moovies', 'lasgo', 'Tape Film')."
        ),
    )
    parser.add_argument(
        "--status",
        choices=["active", "draft", "archived"],
        default="draft",
        help="Shopify product status for created products",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--api-version", default="2026-04")
    parser.add_argument(
        "--env",
        default=".env",
        help="Path to env file (default: .env, use .env.prod for production)",
    )
    args = parser.parse_args()

    load_dotenv(args.env)
    barcodes = parse_barcodes(args)
    if not barcodes:
        die("Provide --barcodes and/or --barcodes-file")

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
    if not supabase_url or not supabase_key:
        die("Missing SUPABASE_URL/SUPABASE_SERVICE_KEY in .env")

    access_token = get_admin_access_token(shop, client_id, client_secret)
    graphql_url = f"https://{shop}/admin/api/{args.api_version}/graphql.json"
    supabase = create_client(supabase_url, supabase_key)

    catalog_rows_by_barcode = fetch_catalog_rows_for_barcodes(supabase, barcodes)

    created = 0
    skipped_exists = 0
    skipped_no_catalog = 0
    failed = 0

    for barcode in barcodes:
        existing_variant = variant_exists_by_barcode(graphql_url, access_token, barcode)
        if existing_variant:
            skipped_exists += 1
            product = (existing_variant.get("product") or {})
            print(f"SKIP exists barcode={barcode} product={product.get('title')} variant={existing_variant.get('id')}")
            continue

        best = pick_best_row(catalog_rows_by_barcode.get(barcode) or [], supplier_preference=args.supplier)
        if not best:
            skipped_no_catalog += 1
            print(f"SKIP no_catalog barcode={barcode} supplier_mode={args.supplier}")
            continue

        if args.dry_run:
            preorder = is_preorder(best)
            print(
                f"DRY-RUN create barcode={barcode} title={best.get('title')} "
                f"price={best.get('calculated_sale_price')} cost={best.get('cost_price')} "
                f"format={best.get('format')} preorder={preorder}"
            )
            continue

        try:
            product = create_product_graphql(
                graphql_url=graphql_url,
                access_token=access_token,
                barcode=barcode,
                row=best,
                status=args.status,
            )
            created += 1
            variant_nodes = (product.get("variants") or {}).get("nodes") or []
            variant_id = variant_nodes[0].get("id") if variant_nodes else None
            print(f"CREATED barcode={barcode} product_id={product.get('id')} variant_id={variant_id} title={product.get('title')}")
        except Exception as e:
            failed += 1
            print(f"ERROR barcode={barcode}: {e}")

    print(
        f"\nDone. input={len(barcodes)} created={created} skipped_exists={skipped_exists} "
        f"skipped_no_catalog={skipped_no_catalog} failed={failed}"
    )


if __name__ == "__main__":
    main()
