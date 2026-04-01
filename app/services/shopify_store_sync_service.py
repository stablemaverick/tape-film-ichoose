"""
Sync active Shopify products/variants into ``shopify_listings`` and link rows to ``catalog_items``.

Does **not** import supplier files, normalize offers, upsert catalog_items, or call the daily stock job.

**Metafield date parsing** (``_try_parse_date_string`` / ``_parse_shopify_metafield_date``), in order:

1. **ISO date or date-time** — ``datetime.fromisoformat`` after normalising a trailing ``Z`` to ``+00:00``
   (covers ``YYYY-MM-DD``, ``YYYY-MM-DDTHH:MM:SS``, offsets, etc.).
2. **DD/MM/YYYY** — ``strptime`` (day-first).
3. **DD-MM-YYYY** — ``strptime``.
4. **MM/DD/YYYY** — ``strptime`` (last; can disagree with day-first for ambiguous numeric dates).

Unparseable values still store verbatim in ``*_raw`` columns; typed ``*_date`` columns stay null.

``shopify_product_id``, ``shopify_variant_id``, and ``shopify_inventory_item_id`` are stored as **raw Shopify GID strings**
from the Admin API (no normalisation).
"""

from __future__ import annotations

import os
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from supabase import Client, create_client

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text

_MAX_ERR_LEN = 2000

MatchTuple = Tuple[Optional[str], str, str, str]  # catalog_item_id, match_method, match_status, match_value


def _repo_root() -> str:
    from pathlib import Path

    return str(Path(__file__).resolve().parents[2])


def _env_path(env_file: str) -> str:
    if os.path.isabs(env_file):
        return env_file
    return os.path.join(_repo_root(), env_file)


def _truncate_error(msg: object) -> str:
    s = str(msg).strip()
    if len(s) <= _MAX_ERR_LEN:
        return s
    return s[: _MAX_ERR_LEN - 3] + "..."


PRODUCTS_QUERY = """
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
      publishedAt
      productType
      directorMeta: metafield(namespace: "custom", key: "director") {
        value
      }
      studioMeta: metafield(namespace: "custom", key: "studio") {
        value
      }
      filmReleasedMeta: metafield(namespace: "custom", key: "film_released") {
        value
      }
      mediaReleaseMeta: metafield(namespace: "custom", key: "media_release_date") {
        value
      }
      variants(first: 100) {
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
            tracked
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

SHOP_CURRENCY_QUERY = """
query ShopCurrency {
  shop {
    currencyCode
  }
}
"""


def _parse_shopify_decimal(value: Any) -> Optional[float]:
    """Parse Shopify Money / price string into float for numeric columns (snapshot)."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"[^\d.\-]", "", text)
    if not text or text in (".", "-", "-."):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def fetch_shop_currency_code(client: ShopifyClient) -> Optional[str]:
    try:
        data = client.graphql(SHOP_CURRENCY_QUERY)
        return clean_text((data.get("shop") or {}).get("currencyCode"))
    except Exception:
        return None


def _try_parse_date_string(text: str) -> Optional[str]:
    """
    Parse a single calendar date for Postgres ``date`` (return ``YYYY-MM-DD``).

    **Precedence** (see module docstring): ISO date/datetime, then DD/MM/YYYY, DD-MM-YYYY, MM/DD/YYYY.
    """
    t = text.strip()
    if not t:
        return None

    iso_candidate = t.replace("Z", "+00:00") if t.endswith("Z") else t
    try:
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return date(dt.year, dt.month, dt.day).isoformat()
    except ValueError:
        pass

    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(t, fmt)
            return dt.date().isoformat()
        except ValueError:
            continue

    return None


def _parse_shopify_metafield_date(raw_value: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Shopify metafield string → (parsed ISO date or None, raw snapshot for *_raw columns).

    Raw is stored whenever the metafield is non-empty, even when parsing fails.
    """
    text = clean_text(raw_value)
    if text is None:
        return None, None
    parsed = _try_parse_date_string(text)
    return parsed, text


def _listing_should_be_preorder(media_release_date: Optional[str]) -> bool:
    if not media_release_date:
        return False
    try:
        d = date.fromisoformat(str(media_release_date)[:10])
        return d > datetime.now(timezone.utc).date()
    except Exception:
        return False


def derive_catalog_availability_from_listing(
    inventory_quantity: Any,
    inventory_policy: Optional[str],
    media_release_date: Optional[str],
) -> Tuple[str, int]:
    """
    Canonical Shopify-linked catalog availability snapshot from listing state.

    Returns:
      (availability_status, supplier_stock_status)
    """
    try:
        qty = int(inventory_quantity or 0)
    except Exception:
        qty = 0
    if _listing_should_be_preorder(media_release_date):
        return "preorder", qty
    if qty > 0:
        return "store_stock", qty
    return "store_out", qty


def _listing_ignored_for_matching(row: Dict[str, Any]) -> bool:
    pt = (clean_text(row.get("product_type")) or "").lower()
    return pt in ("gift card", "gift cards", "gift_card")


def _shopify_display_title(product_title: Optional[str], variant_title: Optional[str]) -> Optional[str]:
    """Same construction as legacy Shopify→catalog mapping: product + variant when variant is meaningful."""
    pt = clean_text(product_title)
    vt = clean_text(variant_title)
    if vt and vt.lower() not in ("default title", "default"):
        return f"{pt} — {vt}" if pt else vt
    return pt


def _ilike_exact_pattern(s: str) -> str:
    """Escape ``%``, ``_``, ``\\`` for PostgreSQL ILIKE without wildcards (exact string, case-insensitive)."""
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _normalize_title_match_key(s: str) -> str:
    t = (clean_text(s) or "").lower()
    t = t.replace("—", "-").replace("–", "-")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _core_title_match_key(s: Optional[str]) -> str:
    """
    Normalize a title to its core identity for conservative barcode tie-break matching.

    Strips common format/packaging suffixes and year parentheticals, then normalizes whitespace/punctuation.
    """
    t = (clean_text(s) or "").lower()
    if not t:
        return ""

    t = t.replace("—", " ").replace("–", " ")

    # Remove year parentheticals like "(1959)" anywhere in the listing/catalog title.
    t = re.sub(r"\(\s*(19|20)\d{2}\s*\)", " ", t)

    # Remove common format / packaging tokens.
    token_patterns = [
        r"\b4k\s+ultra\s+hd\b",
        r"\bultra\s+hd\b",
        r"\buhd\b",
        r"\bblu[\s-]?ray\b",
        r"\bdvd\b",
        r"\bsteelbook\b",
        r"\blimited\s+edition\b",
    ]
    for p in token_patterns:
        t = re.sub(p, " ", t, flags=re.IGNORECASE)

    # Remove common combo marker like "+ Blu-Ray".
    t = re.sub(r"\+\s*blu[\s-]?ray\b", " ", t, flags=re.IGNORECASE)

    # Remove leftover punctuation and normalize spacing.
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _availability_preference_rank(value: Optional[str]) -> int:
    v = (clean_text(value) or "").lower()
    if v == "store_stock":
        return 1
    if v == "preorder":
        return 2
    if v == "supplier_stock":
        return 3
    if v == "store_out":
        return 4
    return 9


def _pick_single_preferred_row(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Deterministic tie-break for identical core-title candidates.

    Preference:
      1) availability_status: store_stock > preorder > supplier_stock > store_out
      2) non-null shopify_variant_id
      3) otherwise no single winner (return None)
    """
    if not rows:
        return None
    if len(rows) == 1:
        return rows[0]

    best_rank = min(_availability_preference_rank(r.get("availability_status")) for r in rows)
    top = [r for r in rows if _availability_preference_rank(r.get("availability_status")) == best_rank]
    if len(top) == 1:
        return top[0]

    with_shopify = [r for r in top if clean_text(r.get("shopify_variant_id"))]
    if len(with_shopify) == 1:
        return with_shopify[0]

    return None


def _title_row_strong_match(row: Dict[str, Any], key_norm: str) -> bool:
    for f in ("title", "edition_title"):
        v = clean_text(row.get(f))
        if v and _normalize_title_match_key(v) == key_norm:
            return True
    return False


def _truncate_match_value_text(s: str, max_len: int = 120) -> str:
    t = s.strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


def _fetch_catalog_rows_for_display_title(
    supabase: Client,
    display_title: str,
    cache: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    if display_title in cache:
        return cache[display_title]
    pat = _ilike_exact_pattern(display_title)
    merged: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for col in ("title", "edition_title"):
        resp = (
            supabase.table("catalog_items")
            .select("id,title,edition_title")
            .eq("active", True)
            .ilike(col, pat)
            .execute()
        )
        for r in resp.data or []:
            rid = str(r["id"])
            if rid not in seen:
                seen.add(rid)
                merged.append(r)
    cache[display_title] = merged
    return merged


def _resolve_title_catalog_match(
    supabase: Client,
    display_title: Optional[str],
    cache: Dict[str, List[Dict[str, Any]]],
) -> MatchTuple:
    dt = clean_text(display_title)
    if not dt:
        return (None, "title", "unmatched", "")
    key_norm = _normalize_title_match_key(dt)
    if not key_norm:
        return (None, "title", "unmatched", "")

    rough = _fetch_catalog_rows_for_display_title(supabase, dt, cache)
    strong = [r for r in rough if _title_row_strong_match(r, key_norm)]
    if len(strong) == 1:
        cid = str(strong[0]["id"])
        return (cid, "title", "matched", cid)
    if len(strong) > 1:
        return (
            None,
            "title",
            "ambiguous",
            f"title:{_truncate_match_value_text(dt)}:n={len(strong)}",
        )
    return (None, "title", "unmatched", "")


def fetch_active_products(client: ShopifyClient) -> List[dict]:
    out: list[dict] = []
    cursor: Optional[str] = None
    while True:
        data = client.graphql(PRODUCTS_QUERY, {"cursor": cursor})
        block = data.get("products") or {}
        out.extend(block.get("nodes") or [])
        page = block.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
    return out


def _chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _resolve_catalog_matches(
    supabase: Client,
    flat_variants: List[Dict[str, Any]],
) -> Dict[str, MatchTuple]:
    """
    Map shopify_variant_id → (catalog_item_id, match_method, match_status, match_value).

    Precedence: **ignored** product types (gift card) → **shopify_variant_id** → **barcode** (exact) →
    **title** (conservative: exactly one strong normalized title match) → **unmatched**.

    SKU is not used. Multiple catalog rows for variant id / barcode yield **ambiguous** (no link).
    """
    by_vid_row: Dict[str, Dict[str, Any]] = {r["shopify_variant_id"]: r for r in flat_variants}
    result: Dict[str, MatchTuple] = {}
    title_cache: Dict[str, List[Dict[str, Any]]] = {}

    for vid, row in by_vid_row.items():
        if _listing_ignored_for_matching(row):
            result[vid] = (
                None,
                "ignored",
                "ignored",
                f"productType:{clean_text(row.get('product_type')) or ''}",
            )

    variant_ids = [v for v in by_vid_row if v not in result]
    cat_by_variant: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for batch in _chunked(variant_ids, 150):
        resp = (
            supabase.table("catalog_items")
            .select("id,shopify_variant_id")
            .in_("shopify_variant_id", batch)
            .execute()
        )
        for r in resp.data or []:
            sv = clean_text(r.get("shopify_variant_id"))
            if sv:
                cat_by_variant[sv].append(r)

    for vid in variant_ids:
        rows = cat_by_variant.get(vid, [])
        if len(rows) == 1:
            cid = str(rows[0]["id"])
            result[vid] = (cid, "shopify_variant_id", "matched", cid)
        elif len(rows) > 1:
            result[vid] = (
                None,
                "shopify_variant_id",
                "ambiguous",
                f"shopify_variant_id:{vid}:n={len(rows)}",
            )

    pending = [v for v in by_vid_row if v not in result]
    barcodes = list(
        {
            clean_text(by_vid_row[v].get("barcode"))
            for v in pending
            if clean_text(by_vid_row[v].get("barcode"))
        }
    )
    cat_by_barcode: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for batch in _chunked(barcodes, 200):
        resp = (
            supabase.table("catalog_items")
            .select("id,barcode,title,edition_title,source_type,shopify_variant_id,availability_status")
            .eq("active", True)
            .in_("barcode", batch)
            .execute()
        )
        for r in resp.data or []:
            bc = clean_text(r.get("barcode"))
            if bc:
                cat_by_barcode[bc].append(r)

    for vid in pending:
        bc = clean_text(by_vid_row[vid].get("barcode"))
        if not bc:
            continue
        rows = cat_by_barcode.get(bc, [])
        if len(rows) == 1:
            cid = str(rows[0]["id"])
            result[vid] = (cid, "barcode", "matched", cid)
        elif len(rows) > 1:
            display = clean_text(by_vid_row[vid].get("_match_display_title"))
            key_norm = _normalize_title_match_key(display or "")
            core_norm = _core_title_match_key(display)
            strong = [r for r in rows if key_norm and _title_row_strong_match(r, key_norm)]
            if not strong and core_norm:
                strong = [
                    r
                    for r in rows
                    if _core_title_match_key(clean_text(r.get("title")))
                    == core_norm
                    or _core_title_match_key(clean_text(r.get("edition_title")))
                    == core_norm
                ]
            preferred = _pick_single_preferred_row(strong)
            if preferred is not None:
                cid = str(preferred["id"])
                result[vid] = (cid, "barcode_title", "matched", cid)
            else:
                result[vid] = (None, "barcode", "ambiguous", f"barcode:{bc}:n={len(rows)}")

    pending = [v for v in by_vid_row if v not in result]
    for vid in pending:
        row = by_vid_row[vid]
        display = row.get("_match_display_title")
        if not clean_text(display):
            result[vid] = (None, "unmatched", "unmatched", "")
        else:
            result[vid] = _resolve_title_catalog_match(supabase, display, title_cache)

    for vid in by_vid_row:
        if vid not in result:
            result[vid] = (None, "unmatched", "unmatched", "")

    return result


def run_shopify_store_sync(*, env_file: str = ".env", dry_run: bool = False) -> Dict[str, Any]:
    path = _env_path(env_file)
    load_dotenv(path)

    shop = os.getenv("SHOPIFY_SHOP", "").strip()
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    if not shop:
        raise SystemExit("Missing SHOPIFY_SHOP")
    if not url or not key:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")

    supabase: Optional[Client] = None
    try:
        client = ShopifyClient()
        shop_currency = fetch_shop_currency_code(client)
        products = fetch_active_products(client)
        now = datetime.now(timezone.utc).isoformat()

        flat: List[Dict[str, Any]] = []
        for product in products:
            pid = product.get("id")
            ptitle = clean_text(product.get("title"))
            vendor = clean_text(product.get("vendor"))
            product_status = clean_text(product.get("status"))
            product_type = clean_text(product.get("productType"))
            published_at = product.get("publishedAt")
            published_to_online_store = bool(
                published_at is not None and str(published_at).strip() != ""
            )
            director_text = clean_text((product.get("directorMeta") or {}).get("value"))
            studio_text = clean_text((product.get("studioMeta") or {}).get("value"))
            film_released_date, film_released_raw = _parse_shopify_metafield_date(
                (product.get("filmReleasedMeta") or {}).get("value")
            )
            media_release_date, media_release_raw = _parse_shopify_metafield_date(
                (product.get("mediaReleaseMeta") or {}).get("value")
            )

            for variant in (product.get("variants") or {}).get("nodes") or []:
                vid = variant.get("id")
                if not vid:
                    continue
                v_title = clean_text(variant.get("title"))
                match_display_title = _shopify_display_title(ptitle, v_title)
                inv_item = (variant.get("inventoryItem") or {}) or {}
                unit_cost = (inv_item.get("unitCost") or {}) or {}
                price_raw = variant.get("price")
                price_amount = _parse_shopify_decimal(price_raw)
                price_cc = shop_currency
                uc_amt = _parse_shopify_decimal(unit_cost.get("amount"))
                uc_cc = clean_text(unit_cost.get("currencyCode"))
                tracked = inv_item.get("tracked")
                tracks_inventory = bool(tracked) if tracked is not None else None

                flat.append(
                    {
                        "shopify_product_id": pid,
                        "shopify_variant_id": vid,
                        "product_title": ptitle,
                        "vendor": vendor,
                        "product_status": product_status,
                        "product_type": product_type,
                        "published_to_online_store": published_to_online_store,
                        "director_text": director_text,
                        "studio_text": studio_text,
                        "film_released_raw": film_released_raw,
                        "film_released_date": film_released_date,
                        "media_release_raw": media_release_raw,
                        "media_release_date": media_release_date,
                        "variant_title": v_title,
                        "_match_display_title": match_display_title,
                        "sku": clean_text(variant.get("sku")),
                        "barcode": clean_text(variant.get("barcode")),
                        "price_amount": price_amount,
                        "price_currency_code": price_cc,
                        "inventory_quantity": int(variant.get("inventoryQuantity") or 0),
                        "inventory_policy": clean_text(variant.get("inventoryPolicy")),
                        "shopify_inventory_item_id": clean_text(inv_item.get("id")),
                        "tracks_inventory": tracks_inventory,
                        "unit_cost_amount": uc_amt,
                        "unit_cost_currency_code": uc_cc,
                    }
                )

        if not flat:
            return {
                "status": "ok",
                "job": "shopify_store_sync",
                "shop": shop,
                "variants": 0,
                "matched_to_catalog": 0,
                "match_status_counts": {"matched": 0, "unmatched": 0, "ambiguous": 0, "ignored": 0},
                "published_to_online_store_count": 0,
                "tracks_inventory_true_count": 0,
                "dry_run": dry_run,
                "db_writes": False,
            }

        supabase = create_client(url, key)
        matches = _resolve_catalog_matches(supabase, flat)

        rows: List[Dict[str, Any]] = []
        matched_to_catalog = 0
        for v in flat:
            vid = v["shopify_variant_id"]
            cid, match_method, mstatus, mvalue = matches[vid]
            if cid:
                matched_to_catalog += 1
            rows.append(
                {
                    "shop": shop,
                    "shopify_product_id": v["shopify_product_id"],
                    "shopify_variant_id": vid,
                    "product_title": v.get("product_title"),
                    "vendor": v.get("vendor"),
                    "product_status": v.get("product_status"),
                    "product_type": v.get("product_type"),
                    "published_to_online_store": v.get("published_to_online_store"),
                    "director_text": v.get("director_text"),
                    "studio_text": v.get("studio_text"),
                    "film_released_raw": v.get("film_released_raw"),
                    "film_released_date": v.get("film_released_date"),
                    "media_release_raw": v.get("media_release_raw"),
                    "media_release_date": v.get("media_release_date"),
                    "variant_title": v.get("variant_title"),
                    "sku": v.get("sku"),
                    "barcode": v.get("barcode"),
                    "price_amount": v.get("price_amount"),
                    "price_currency_code": v.get("price_currency_code"),
                    "inventory_quantity": v.get("inventory_quantity"),
                    "inventory_policy": v.get("inventory_policy"),
                    "shopify_inventory_item_id": v.get("shopify_inventory_item_id"),
                    "tracks_inventory": v.get("tracks_inventory"),
                    "unit_cost_amount": v.get("unit_cost_amount"),
                    "unit_cost_currency_code": v.get("unit_cost_currency_code"),
                    "catalog_item_id": cid,
                    "match_method": match_method,
                    "match_status": mstatus,
                    "match_value": mvalue or None,
                    "last_store_sync_at": now,
                    "last_store_sync_error": None,
                }
            )

        status_counts = Counter(str(r["match_status"]) for r in rows)
        match_status_counts = {
            "matched": int(status_counts.get("matched", 0)),
            "unmatched": int(status_counts.get("unmatched", 0)),
            "ambiguous": int(status_counts.get("ambiguous", 0)),
            "ignored": int(status_counts.get("ignored", 0)),
        }
        pub_true = sum(1 for r in rows if r.get("published_to_online_store") is True)
        track_true = sum(1 for r in rows if r.get("tracks_inventory") is True)

        if not dry_run:
            for batch in [rows[i : i + 200] for i in range(0, len(rows), 200)]:
                supabase.table("shopify_listings").upsert(
                    batch,
                    on_conflict="shop,shopify_variant_id",
                ).execute()

            # Keep existing Shopify-linked catalog_items commercially in sync with live Shopify sellability.
            # Only touch rows linked by exact shopify_variant_id match (not barcode/title fallbacks).
            catalog_updates: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                cid = clean_text(r.get("catalog_item_id"))
                if not cid:
                    continue
                if (r.get("match_status") or "") != "matched":
                    continue
                if (r.get("match_method") or "") != "shopify_variant_id":
                    continue
                av, stock_qty = derive_catalog_availability_from_listing(
                    r.get("inventory_quantity"),
                    clean_text(r.get("inventory_policy")),
                    clean_text(r.get("media_release_date")),
                )
                catalog_updates[cid] = {
                    "availability_status": av,
                    "supplier_stock_status": stock_qty,
                    "shopify_variant_id": clean_text(r.get("shopify_variant_id")),
                    "shopify_product_id": clean_text(r.get("shopify_product_id")),
                    "media_release_date": clean_text(r.get("media_release_date")),
                }

            if catalog_updates:
                for batch_ids in _chunked(list(catalog_updates.keys()), 200):
                    for cid in batch_ids:
                        supabase.table("catalog_items").update(catalog_updates[cid]).eq(
                            "id", cid
                        ).execute()

        return {
            "status": "ok",
            "job": "shopify_store_sync",
            "shop": shop,
            "variants": len(rows),
            "matched_to_catalog": matched_to_catalog,
            "match_status_counts": match_status_counts,
            "published_to_online_store_count": pub_true,
            "tracks_inventory_true_count": track_true,
            "dry_run": dry_run,
            "db_writes": not dry_run,
        }
    except Exception as exc:
        if supabase is not None and not dry_run:
            try:
                supabase.table("shopify_listings").update(
                    {"last_store_sync_error": _truncate_error(exc)}
                ).eq("shop", shop).execute()
            except Exception:
                pass
        raise
