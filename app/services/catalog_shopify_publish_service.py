"""
Ad hoc publish: selected ``catalog_items`` rows → new Shopify products (draft by default).

This is intentionally separate from supplier catalog import, Shopify store sync, and
inventory sync. It uses ``ShopifyClient`` + Supabase service role like other jobs.

After a successful ``productSet`` create, the chosen catalog row is updated with
Shopify IDs and optional publish metadata columns, and the product is published to
configured sales channels via ``publishablePublish`` (requires ``read_publications``
and ``write_publications`` on the Admin API client).
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, TypedDict

from dotenv import load_dotenv
from supabase import Client

from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client
from app.helpers.text_helpers import chunked, clean_text, parse_date, slugify
from app.rules.pricing_rules import (
    DEFAULT_GBP_AUD_RATE,
    DEFAULT_LANDED_COST_MARKUP,
    calculate_shopify_cost_aud,
)

ShopifyProductStatus = Literal["draft", "active", "archived"]

CatalogPublishOutcome = Literal[
    "created",
    "skipped_exists_shopify",
    "skipped_no_catalog",
    "dry_run",
    "failed",
]


class PublishedChannelInfo(TypedDict):
    channel: str
    publication_id: str
    catalog_title: str


class CatalogBarcodePublishResult(TypedDict, total=False):
    barcode: str
    outcome: CatalogPublishOutcome
    message: str
    catalog_item_id: Optional[str]
    title: Optional[str]
    supplier: Optional[str]
    handle: Optional[str]
    handle_collision_avoided: bool
    inventory_policy: Optional[str]
    preorder: bool
    shopify_product_id: Optional[str]
    shopify_variant_id: Optional[str]
    existing_shopify_product_id: Optional[str]
    existing_shopify_variant_id: Optional[str]
    writeback_ok: bool
    writeback_error: Optional[str]
    published_channels: List[PublishedChannelInfo]
    missing_channels: List[str]
    publish_errors: List[str]


class CatalogShopifyPublishRunResult(TypedDict):
    results: List[CatalogBarcodePublishResult]
    summary: Dict[str, int]


# Production ``custom.genre`` (list.single_line_text_field) allowed choices; outputs are filtered to this set.
SHOPIFY_GENRE_ALLOWED: frozenset[str] = frozenset(
    {
        "Action",
        "Adventure",
        "Beautiful Vistas",
        "Big Screen",
        "Crime",
        "Dark Comedy",
        "Drama",
        "Fantasy",
        "Historic",
        "Horror",
        "Romance",
        "Thriller",
        "Mystery",
        "Science Fiction",
        "Vice",
        "War",
        "Western",
        "MCU",
        "Comedy",
        "Biographical",
        "Biography",
        "Musical",
        "Cult",
        "LGBTQ+",
        "Animation",
        "Anime",
        "Family",
        "Documentary",
    }
)

# Films/catalog genre labels (trimmed, exact match) → Shopify choice(s). Empty tuple = omit. Unknown label = omit.
FILM_CATALOG_GENRE_TO_SHOPIFY: Dict[str, tuple[str, ...]] = {
    "Action": ("Action",),
    "Action & Adventure": ("Action", "Adventure"),
    "Adventure": ("Adventure",),
    "Animation": ("Animation",),
    "Comedy": ("Comedy",),
    "Crime": ("Crime",),
    "Documentary": ("Documentary",),
    "Drama": ("Drama",),
    "Family": ("Family",),
    "Fantasy": ("Fantasy",),
    "History": ("Historic",),
    "Horror": ("Horror",),
    "Music": ("Musical",),
    "Mystery": ("Mystery",),
    "Romance": ("Romance",),
    "Sci-Fi & Fantasy": ("Science Fiction", "Fantasy"),
    "Science Fiction": ("Science Fiction",),
    "Thriller": ("Thriller",),
    "TV Movie": (),
    "War": ("War",),
    "Western": ("Western",),
}


def shopify_genre_list_from_catalog_genres(genres_raw: Any) -> List[str]:
    """
    Split comma-separated catalog/film genres, map via FILM_CATALOG_GENRE_TO_SHOPIFY,
    keep only SHOPIFY_GENRE_ALLOWED, dedupe preserving first-seen order.
    """
    text = clean_text(genres_raw)
    if not text:
        return []
    out: List[str] = []
    seen: set[str] = set()
    for part in text.split(","):
        label = part.strip()
        if not label:
            continue
        targets = FILM_CATALOG_GENRE_TO_SHOPIFY.get(label, ())
        for t in targets:
            if t in SHOPIFY_GENRE_ALLOWED and t not in seen:
                seen.add(t)
                out.append(t)
    return out


# ISO 3166-1 alpha-2 → display name (aligned with common Shopify / merchandising labels).
ISO_ALPHA2_TO_DISPLAY_NAME: Dict[str, str] = {
    "AD": "Andorra",
    "AE": "United Arab Emirates",
    "AF": "Afghanistan",
    "AG": "Antigua and Barbuda",
    "AL": "Albania",
    "AM": "Armenia",
    "AR": "Argentina",
    "AT": "Austria",
    "AU": "Australia",
    "BA": "Bosnia and Herzegovina",
    "BE": "Belgium",
    "BG": "Bulgaria",
    "BR": "Brazil",
    "BY": "Belarus",
    "CA": "Canada",
    "CH": "Switzerland",
    "CL": "Chile",
    "CN": "China",
    "CO": "Colombia",
    "CR": "Costa Rica",
    "CU": "Cuba",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "DK": "Denmark",
    "DO": "Dominican Republic",
    "DZ": "Algeria",
    "EC": "Ecuador",
    "EE": "Estonia",
    "EG": "Egypt",
    "ES": "Spain",
    "FI": "Finland",
    "FR": "France",
    "GB": "United Kingdom",
    "GE": "Georgia",
    "GR": "Greece",
    "GT": "Guatemala",
    "HK": "Hong Kong",
    "HR": "Croatia",
    "HU": "Hungary",
    "ID": "Indonesia",
    "IE": "Ireland",
    "IL": "Israel",
    "IN": "India",
    "IQ": "Iraq",
    "IR": "Iran",
    "IS": "Iceland",
    "IT": "Italy",
    "JM": "Jamaica",
    "JO": "Jordan",
    "JP": "Japan",
    "KE": "Kenya",
    "KR": "South Korea",
    "KW": "Kuwait",
    "KZ": "Kazakhstan",
    "LB": "Lebanon",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "LV": "Latvia",
    "MA": "Morocco",
    "MC": "Monaco",
    "MD": "Moldova",
    "MK": "North Macedonia",
    "MT": "Malta",
    "MX": "Mexico",
    "MY": "Malaysia",
    "NG": "Nigeria",
    "NL": "Netherlands",
    "NO": "Norway",
    "NZ": "New Zealand",
    "PE": "Peru",
    "PH": "Philippines",
    "PK": "Pakistan",
    "PL": "Poland",
    "PR": "Puerto Rico",
    "PT": "Portugal",
    "PY": "Paraguay",
    "QA": "Qatar",
    "RO": "Romania",
    "RS": "Serbia",
    "RU": "Russia",
    "SA": "Saudi Arabia",
    "SE": "Sweden",
    "SG": "Singapore",
    "SI": "Slovenia",
    "SK": "Slovakia",
    "TH": "Thailand",
    "TN": "Tunisia",
    "TR": "Turkey",
    "TW": "Taiwan",
    "UA": "Ukraine",
    "US": "United States",
    "UY": "Uruguay",
    "VE": "Venezuela",
    "VN": "Vietnam",
    "ZA": "South Africa",
}

ISO_ALPHA3_TO_DISPLAY_NAME: Dict[str, str] = {
    "USA": "United States",
    "GBR": "United Kingdom",
    "JPN": "Japan",
    "AUS": "Australia",
    "NZL": "New Zealand",
    "CAN": "Canada",
    "FRA": "France",
    "DEU": "Germany",
    "ITA": "Italy",
    "ESP": "Spain",
    "NLD": "Netherlands",
    "BEL": "Belgium",
    "CHE": "Switzerland",
    "AUT": "Austria",
    "SWE": "Sweden",
    "NOR": "Norway",
    "DNK": "Denmark",
    "FIN": "Finland",
    "IRL": "Ireland",
    "PRT": "Portugal",
    "GRC": "Greece",
    "POL": "Poland",
    "CZE": "Czech Republic",
    "HUN": "Hungary",
    "ROU": "Romania",
    "BGR": "Bulgaria",
    "HRV": "Croatia",
    "SRB": "Serbia",
    "UKR": "Ukraine",
    "RUS": "Russia",
    "CHN": "China",
    "HKG": "Hong Kong",
    "TWN": "Taiwan",
    "KOR": "South Korea",
    "IND": "India",
    "MEX": "Mexico",
    "BRA": "Brazil",
    "ARG": "Argentina",
    "ZAF": "South Africa",
    "ISR": "Israel",
    "TUR": "Turkey",
    "EGY": "Egypt",
    "SGP": "Singapore",
    "MYS": "Malaysia",
    "THA": "Thailand",
    "IDN": "Indonesia",
    "PHL": "Philippines",
    "VNM": "Vietnam",
}


def normalize_country_of_origin_for_shopify(value: Any) -> Optional[str]:
    """
    If value is a 2- or 3-letter ISO code (A–Z only), map to a full country name when known.
    Otherwise return the trimmed string unchanged (already a readable name or unknown code).
    """
    s = clean_text(value)
    if not s:
        return None
    if len(s) == 2 and s.isalpha():
        return ISO_ALPHA2_TO_DISPLAY_NAME.get(s.upper(), s)
    if len(s) == 3 and s.isalpha():
        return ISO_ALPHA3_TO_DISPLAY_NAME.get(s.upper(), s)
    return s


# Canonical channel labels (operator-facing) → normalized substrings / aliases for matching ``catalog.title``.
SALES_CHANNEL_TARGETS: tuple[str, ...] = (
    "Online Store",
    "Shop",
    "Point of Sale",
    "Inbox",
    "Facebook & Instagram",
    "Google & YouTube",
)

_SALES_CHANNEL_MATCH_ALIASES: Dict[str, frozenset[str]] = {
    "Online Store": frozenset({"online store"}),
    "Shop": frozenset({"shop"}),
    "Point of Sale": frozenset({"point of sale"}),
    "Inbox": frozenset({"inbox"}),
    "Facebook & Instagram": frozenset(
        {"facebook & instagram", "facebook and instagram"}
    ),
    "Google & YouTube": frozenset({"google & youtube", "google and youtube"}),
}


def _norm_sales_channel_label(label: str) -> str:
    t = label.strip().lower()
    t = t.replace(" and ", " & ")
    return " ".join(t.split())


def _publication_match_label(node: Dict[str, Any]) -> str:
    """Label for channel matching: ``catalog.title`` when set, else ``publication.name``."""
    cat = node.get("catalog")
    if cat:
        t = str(cat.get("title") or "").strip()
        if t:
            return t
    return str(node.get("name") or "").strip()


def _fetch_publication_nodes(client: ShopifyClient) -> tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Paginate ``publications`` and return raw nodes. On failure, ([], error message).
    Requires ``read_publications`` on the Admin API client.
    """
    nodes_out: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    q = """
    query Publications($cursor: String) {
      publications(first: 50, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          name
          catalog { title }
        }
      }
    }
    """
    try:
        while True:
            data = client.graphql(q, {"cursor": cursor})
            conn = data.get("publications") or {}
            for n in conn.get("nodes") or []:
                nodes_out.append(n)
            pi = conn.get("pageInfo") or {}
            if not pi.get("hasNextPage"):
                break
            cursor = pi.get("endCursor")
            if not cursor:
                break
        return nodes_out, None
    except Exception as e:
        return [], str(e)


def _resolve_sales_channel_publications(
    publication_nodes: List[Dict[str, Any]],
) -> tuple[Dict[str, tuple[str, str]], List[str]]:
    """
    Map canonical target name → (publication_id, match_label).
    ``match_label`` is ``catalog.title`` if present, else ``publication.name`` (for ``published_channels``).
    Returns (resolved, missing_canonical_names).
    """
    resolved: Dict[str, tuple[str, str]] = {}
    used_pub_ids: set[str] = set()

    norm_to_node: Dict[str, tuple[str, str]] = {}
    for node in publication_nodes:
        pid = node.get("id")
        label = _publication_match_label(node)
        if not pid or not label:
            continue
        key = _norm_sales_channel_label(label)
        if key not in norm_to_node:
            norm_to_node[key] = (str(pid), label)

    missing: List[str] = []
    for target in SALES_CHANNEL_TARGETS:
        aliases = _SALES_CHANNEL_MATCH_ALIASES.get(
            target, frozenset({_norm_sales_channel_label(target)})
        )
        hit: Optional[tuple[str, str]] = None
        for alias in aliases:
            if alias in norm_to_node:
                cand = norm_to_node[alias]
                if cand[0] not in used_pub_ids:
                    hit = cand
                    break
        if hit:
            resolved[target] = hit
            used_pub_ids.add(hit[0])
        else:
            missing.append(target)

    return resolved, missing


def _publishable_publish_product(
    client: ShopifyClient,
    product_id: str,
    publication_ids: List[str],
) -> List[str]:
    """Call ``publishablePublish`` for the given publication IDs. Returns user-facing error strings."""
    if not publication_ids:
        return []
    mut = """
    mutation PublishablePublish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors { field message }
      }
    }
    """
    input_rows = [{"publicationId": pid} for pid in publication_ids]
    errors: List[str] = []
    try:
        data = client.graphql(mut, {"id": product_id, "input": input_rows})
        payload = data.get("publishablePublish") or {}
        for ue in payload.get("userErrors") or []:
            msg = ue.get("message") or str(ue)
            errors.append(msg)
    except Exception as e:
        errors.append(str(e))
    return errors


def publish_product_to_required_sales_channels(
    client: ShopifyClient,
    product_id: str,
    publication_nodes: List[Dict[str, Any]],
    publications_fetch_error: Optional[str],
) -> tuple[List[PublishedChannelInfo], List[str], List[str]]:
    """
    Resolve required sales channels by publication label (``catalog.title`` or ``name``),
    publish the product, return (published_channels, missing_channels, publish_errors).
    """
    publish_errors: List[str] = []
    if publications_fetch_error:
        publish_errors.append(publications_fetch_error)
        return [], list(SALES_CHANNEL_TARGETS), publish_errors

    resolved, missing = _resolve_sales_channel_publications(publication_nodes)
    if not resolved:
        return [], missing, publish_errors

    ids_in_order = [resolved[t][0] for t in SALES_CHANNEL_TARGETS if t in resolved]
    batch_errs = _publishable_publish_product(client, product_id, ids_in_order)
    published: List[PublishedChannelInfo] = []

    if not batch_errs:
        for t in SALES_CHANNEL_TARGETS:
            if t in resolved:
                pid, ctitle = resolved[t]
                published.append(
                    {
                        "channel": t,
                        "publication_id": pid,
                        "catalog_title": ctitle,
                    }
                )
        return published, missing, publish_errors

    publish_errors.extend(batch_errs)
    for t in SALES_CHANNEL_TARGETS:
        if t not in resolved:
            continue
        pid, ctitle = resolved[t]
        one_errs = _publishable_publish_product(client, product_id, [pid])
        if not one_errs:
            published.append(
                {
                    "channel": t,
                    "publication_id": pid,
                    "catalog_title": ctitle,
                }
            )
        else:
            publish_errors.extend([f"{t}: {e}" for e in one_errs])

    return published, missing, publish_errors


CATALOG_SELECT = (
    "id,title,barcode,sku,supplier,supplier_sku,source_type,"
    "supplier_stock_status,availability_status,"
    "cost_price,calculated_sale_price,"
    "director,studio,film_released,media_release_date,"
    "format,category,notes,supplier_priority,"
    "genres,top_cast,country_of_origin,"
    "tmdb_poster_path"
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _env_path(env_file: str) -> Path:
    p = Path(env_file)
    if p.is_absolute():
        return p
    return _repo_root() / env_file


def normalize_barcodes(raw: Iterable[str]) -> List[str]:
    """Dedupe preserving order; strip whitespace; skip empties."""
    seen: set[str] = set()
    out: List[str] = []
    for line in raw:
        b = str(line).strip()
        if not b or b in seen:
            continue
        seen.add(b)
        out.append(b)
    return out


def _parse_media_release_as_date(value: Any) -> Optional[date]:
    iso = parse_date(value)
    if not iso:
        return None
    try:
        return datetime.strptime(iso, "%Y-%m-%d").date()
    except ValueError:
        return None


def is_preorder(row: Dict[str, Any]) -> bool:
    """Future media release → treat as pre-order for inventory policy."""
    release = _parse_media_release_as_date(row.get("media_release_date"))
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


def build_metafields(row: Dict[str, Any]) -> List[Dict[str, str]]:
    mf: List[Dict[str, str]] = []

    def add(key: str, mf_type: str, value: Optional[str]):
        if value:
            mf.append({"namespace": "custom", "key": key, "type": mf_type, "value": value})

    add("director", "single_line_text_field", clean_text(row.get("director")))
    add("studio", "single_line_text_field", clean_text(row.get("studio")))
    add("format", "single_line_text_field", clean_text(row.get("format")))
    add("starring", "multi_line_text_field", clean_text(row.get("top_cast")))
    add(
        "country_of_origin",
        "single_line_text_field",
        normalize_country_of_origin_for_shopify(row.get("country_of_origin")),
    )
    add("region", "single_line_text_field", derive_region(row))

    film_iso = parse_date(row.get("film_released"))
    if film_iso:
        add("film_released", "date", film_iso)

    media_iso = parse_date(row.get("media_release_date"))
    if media_iso:
        add("media_release_date", "date", media_iso)

    preorder = is_preorder(row)
    add("pre_order", "boolean", "true" if preorder else "false")
    if preorder:
        add("po_flag", "single_line_text_field", "Pre-Order")

    supplier_src = clean_text(row.get("supplier"))
    if supplier_src:
        add("source_supplier", "single_line_text_field", supplier_src.lower())
    add("source_supplier_sku", "single_line_text_field", clean_text(row.get("supplier_sku")))
    add("source_barcode", "single_line_text_field", clean_text(row.get("barcode")))
    cid = row.get("id")
    if cid is not None:
        cid_s = str(cid).strip()
        if cid_s:
            add("catalog_item_id", "single_line_text_field", cid_s)

    genre_list = shopify_genre_list_from_catalog_genres(row.get("genres"))
    if genre_list:
        add(
            "genre",
            "list.single_line_text_field",
            json.dumps(genre_list, ensure_ascii=False, separators=(",", ":")),
        )

    return mf


def fetch_catalog_rows_for_barcodes(client: Client, barcodes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {b: [] for b in barcodes}
    for batch in chunked(barcodes, 100):
        resp = (
            client.table("catalog_items")
            .select(CATALOG_SELECT)
            .in_("barcode", batch)
            .eq("active", True)
            .execute()
        )
        for row in resp.data or []:
            bc = clean_text(row.get("barcode"))
            if bc in out:
                out[bc].append(row)
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

    def avail_rank(v: Optional[str]) -> int:
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


def _barcode_handle_suffix(barcode: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "", barcode.lower())
    return s[-12:] if s else ""


def resolve_unique_product_handle(client: ShopifyClient, base_handle: str, barcode: str) -> tuple[str, bool]:
    """
    Pick a handle not already used in Shopify. Returns (handle, avoided_collision).
    """
    base = (base_handle or "product").strip("-")[:200]
    if not client.product_id_by_handle(base):
        return base, False
    tail = _barcode_handle_suffix(barcode)
    if tail:
        cand = f"{base}-{tail}"[:255]
        if not client.product_id_by_handle(cand):
            return cand, True
    n = 2
    while n < 200:
        cand = f"{base}-{n}"[:255]
        if not client.product_id_by_handle(cand):
            return cand, True
        n += 1
    raise RuntimeError(f"Could not allocate a unique handle for base={base_handle!r}")


def variant_inventory_policy_for_row(row: Dict[str, Any]) -> str:
    """Pre-order (future release) → CONTINUE; otherwise DENY."""
    return "CONTINUE" if is_preorder(row) else "DENY"


def _product_set_create(
    client: ShopifyClient,
    *,
    barcode: str,
    row: Dict[str, Any],
    status: ShopifyProductStatus,
    handle: str,
) -> Dict[str, Any]:
    title = clean_text(row.get("title")) or f"Film {barcode}"
    sku = clean_text(row.get("supplier_sku")) or clean_text(row.get("sku")) or barcode
    price = str(row.get("calculated_sale_price")) if row.get("calculated_sale_price") is not None else "0.00"

    gbp_aud = float(os.getenv("GBP_AUD_RATE", str(DEFAULT_GBP_AUD_RATE)))
    landed_markup = float(os.getenv("LANDED_COST_MARKUP", str(DEFAULT_LANDED_COST_MARKUP)))
    raw_cost = float(row["cost_price"]) if row.get("cost_price") is not None else None
    cost_val = calculate_shopify_cost_aud(raw_cost, gbp_aud_rate=gbp_aud, landed_cost_markup=landed_markup)
    cost = f"{cost_val:.2f}" if cost_val is not None else None

    tags = build_tags(row)
    metafields = build_metafields(row)
    inv_policy = variant_inventory_policy_for_row(row)

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
        "inventoryPolicy": inv_policy,
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
        "status": status.upper(),
        "tags": tags,
        "seo": {"title": title},
        "metafields": metafields,
        "productOptions": [
            {"name": "Title", "position": 1, "values": [{"name": "Default Title"}]},
        ],
        "variants": [variant_input],
    }

    data = client.graphql(
        mutation,
        {"synchronous": True, "input": product_input},
    )
    payload = data.get("productSet") or {}
    errs = payload.get("userErrors") or []
    if errs:
        raise RuntimeError(f"productSet userErrors: {errs}")
    return payload.get("product") or {}


def _writeback_catalog_row(
    supabase: Client,
    *,
    catalog_item_id: str,
    shopify_product_id: str,
    shopify_variant_id: str,
    set_publish_flags: bool,
) -> None:
    payload: Dict[str, Any] = {
        "shopify_product_id": shopify_product_id,
        "shopify_variant_id": shopify_variant_id,
    }
    if set_publish_flags:
        payload["published_to_shopify"] = True
        payload["shopify_published_at"] = datetime.now(timezone.utc).isoformat()
    supabase.table("catalog_items").update(payload).eq("id", catalog_item_id).execute()


def run_catalog_shopify_publish(
    *,
    barcodes: List[str],
    supplier_mode: str = "best_offer",
    shopify_status: ShopifyProductStatus = "draft",
    dry_run: bool = False,
    env_file: str = ".env",
    api_version: str = "2026-04",
    set_publish_flags: bool = True,
) -> CatalogShopifyPublishRunResult:
    """
    For each barcode: skip if Shopify already has that barcode; else pick catalog row,
    optionally create product, then write back Shopify IDs (+ optional flags) on success.

    ``supplier_mode``: ``best_offer`` or exact supplier name (case-insensitive).
    """
    path = _env_path(env_file)
    env_for_dotenv = str(path.resolve()) if path.is_file() else env_file
    if path.is_file():
        load_dotenv(path)
    else:
        load_dotenv(env_file)

    normalized = normalize_barcodes(barcodes)
    if not normalized:
        return CatalogShopifyPublishRunResult(
            results=[],
            summary={"input": 0, "created": 0, "skipped_exists": 0, "skipped_no_catalog": 0, "dry_run": 0, "failed": 0},
        )

    supabase = create_fresh_client(env_for_dotenv)
    shopify = ShopifyClient(api_version=api_version)

    by_barcode = fetch_catalog_rows_for_barcodes(supabase, normalized)
    results: List[CatalogBarcodePublishResult] = []
    pub_cache: Dict[str, Any] = {"done": False, "nodes": [], "err": None}

    def _get_publication_cache() -> tuple[List[Dict[str, Any]], Optional[str]]:
        if not pub_cache["done"]:
            n, e = _fetch_publication_nodes(shopify)
            pub_cache["nodes"] = n
            pub_cache["err"] = e
            pub_cache["done"] = True
        return pub_cache["nodes"], pub_cache["err"]

    summary = {
        "input": len(normalized),
        "created": 0,
        "skipped_exists": 0,
        "skipped_no_catalog": 0,
        "dry_run": 0,
        "failed": 0,
    }

    for barcode in normalized:
        existing = shopify.variant_exists_by_barcode(barcode)
        if existing:
            prod = existing.get("product") or {}
            results.append(
                {
                    "barcode": barcode,
                    "outcome": "skipped_exists_shopify",
                    "message": "Variant with this barcode already exists in Shopify",
                    "existing_shopify_variant_id": existing.get("id"),
                    "existing_shopify_product_id": prod.get("id"),
                    "writeback_ok": True,
                }
            )
            summary["skipped_exists"] += 1
            continue

        best = pick_best_row(by_barcode.get(barcode) or [], supplier_preference=supplier_mode)
        if not best:
            results.append(
                {
                    "barcode": barcode,
                    "outcome": "skipped_no_catalog",
                    "message": f"No active catalog row for supplier_mode={supplier_mode!r}",
                    "writeback_ok": True,
                }
            )
            summary["skipped_no_catalog"] += 1
            continue

        catalog_item_id = str(best["id"])
        title = clean_text(best.get("title"))
        preorder = is_preorder(best)
        inv_policy = variant_inventory_policy_for_row(best)
        base_handle = slugify(title or f"film-{barcode}")

        if dry_run:
            try:
                handle, avoided = resolve_unique_product_handle(shopify, base_handle, barcode)
            except Exception as e:
                results.append(
                    {
                        "barcode": barcode,
                        "outcome": "failed",
                        "message": f"Handle resolution failed: {e}",
                        "catalog_item_id": catalog_item_id,
                        "title": title,
                        "supplier": clean_text(best.get("supplier")),
                        "writeback_ok": True,
                    }
                )
                summary["failed"] += 1
                continue

            results.append(
                {
                    "barcode": barcode,
                    "outcome": "dry_run",
                    "message": "Would create Shopify product",
                    "catalog_item_id": catalog_item_id,
                    "title": title,
                    "supplier": clean_text(best.get("supplier")),
                    "handle": handle,
                    "handle_collision_avoided": avoided,
                    "inventory_policy": inv_policy,
                    "preorder": preorder,
                    "writeback_ok": True,
                }
            )
            summary["dry_run"] += 1
            continue

        try:
            handle, avoided = resolve_unique_product_handle(shopify, base_handle, barcode)
            product = _product_set_create(
                shopify,
                barcode=barcode,
                row=best,
                status=shopify_status,
                handle=handle,
            )
            variant_nodes = (product.get("variants") or {}).get("nodes") or []
            variant_id = variant_nodes[0].get("id") if variant_nodes else None
            product_id = product.get("id")
            if not product_id or not variant_id:
                raise RuntimeError(f"productSet missing ids: product={product_id!r} variant={variant_id!r}")

            writeback_err: Optional[str] = None
            try:
                _writeback_catalog_row(
                    supabase,
                    catalog_item_id=catalog_item_id,
                    shopify_product_id=product_id,
                    shopify_variant_id=variant_id,
                    set_publish_flags=set_publish_flags,
                )
            except Exception as wb:
                writeback_err = str(wb)

            pub_nodes, pub_err = _get_publication_cache()
            published_channels, missing_channels, publish_errors = (
                publish_product_to_required_sales_channels(
                    shopify, product_id, pub_nodes, pub_err
                )
            )

            results.append(
                {
                    "barcode": barcode,
                    "outcome": "created",
                    "message": "Created Shopify product and updated catalog row"
                    if not writeback_err
                    else f"Shopify created but catalog writeback failed: {writeback_err}",
                    "catalog_item_id": catalog_item_id,
                    "title": title or product.get("title"),
                    "supplier": clean_text(best.get("supplier")),
                    "handle": product.get("handle") or handle,
                    "handle_collision_avoided": avoided,
                    "inventory_policy": inv_policy,
                    "preorder": preorder,
                    "shopify_product_id": product_id,
                    "shopify_variant_id": variant_id,
                    "writeback_ok": writeback_err is None,
                    "writeback_error": writeback_err,
                    "published_channels": published_channels,
                    "missing_channels": missing_channels,
                    "publish_errors": publish_errors,
                }
            )
            summary["created"] += 1
        except Exception as e:
            results.append(
                {
                    "barcode": barcode,
                    "outcome": "failed",
                    "message": str(e),
                    "catalog_item_id": catalog_item_id,
                    "title": title,
                    "supplier": clean_text(best.get("supplier")),
                    "writeback_ok": True,
                }
            )
            summary["failed"] += 1

    return CatalogShopifyPublishRunResult(results=results, summary=summary)
