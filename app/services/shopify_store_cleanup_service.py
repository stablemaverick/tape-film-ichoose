"""
Shopify store cleanup helpers: archive-candidate export, archive from CSV,
and batch metafield clears.

Read-only / dry-run by default. Mutations require explicit ``apply=True``.
"""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient, get_shopify_client
from app.helpers.text_helpers import clean_text
from app.services.shopify_inventory_settings_audit import (
    GIFTCARD_PRODUCT_TYPES,
    is_gift_card_product_type,
    parse_shopify_bool_metafield,
)
from app.services.catalog_shopify_publish_service import shopify_inventory_location_id

SYDNEY_TZ = ZoneInfo("Australia/Sydney")
DEFAULT_RELEASE_LOOKBACK_DAYS = 60

PRODUCTS_PAGE_QUERY = """
query StoreCleanupProducts($cursor: String, $locId: ID!, $q: String) {
  products(first: 25, after: $cursor, query: $q) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      handle
      title
      status
      productType
      updatedAt
      tags
      mediaReleaseDate: metafield(namespace: "custom", key: "media_release_date") {
        value
      }
      preOrder: metafield(namespace: "custom", key: "pre_order") {
        value
      }
      preorderAlt: metafield(namespace: "custom", key: "preorder") {
        value
      }
      backorder: metafield(namespace: "custom", key: "backorder") {
        value
      }
      poFlag: metafield(namespace: "custom", key: "po_flag") {
        value
      }
      metafields(first: 50) {
        nodes {
          namespace
          key
          type
          value
        }
      }
      variants(first: 100) {
        nodes {
          id
          sku
          barcode
          inventoryItem {
            id
            inventoryLevel(locationId: $locId) {
              quantities(names: ["available", "committed", "on_hand"]) {
                name
                quantity
              }
            }
          }
        }
      }
    }
  }
}
"""

PRODUCT_CHANGE_STATUS = """
mutation productChangeStatus($productId: ID!, $status: ProductStatus!) {
  productChangeStatus(productId: $productId, status: $status) {
    product {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
"""

METAFIELDS_SET = """
mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields {
      id
      namespace
      key
      value
    }
    userErrors {
      field
      message
    }
  }
}
"""

METAFIELD_DELETE = """
mutation metafieldDelete($input: MetafieldDeleteInput!) {
  metafieldDelete(input: $input) {
    deletedId
    userErrors {
      field
      message
    }
  }
}
"""

CANDIDATE_CSV_FIELDS = [
    "product_id",
    "handle",
    "title",
    "barcode",
    "sku",
    "available",
    "committed",
    "on_hand",
    "media_release_date",
    "pre_order",
    "backorder",
    "new_like_metafields",
    "tags",
    "reason",
]

EXCLUDED_CSV_FIELDS = CANDIDATE_CSV_FIELDS + ["exclude_reason"]


@dataclass
class ProductCleanupRow:
    product_id: str
    handle: str
    title: str
    barcode: str
    sku: str
    available: int
    committed: int
    on_hand: int
    media_release_date: str
    pre_order: bool
    backorder: bool
    new_like_metafields: str
    tags: str
    reason: str = ""
    exclude_reason: str = ""
    is_candidate: bool = False
    metafield_nodes: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class ExportSummary:
    products_scanned: int = 0
    candidates: int = 0
    excluded: int = 0
    skipped_gift_cards: int = 0
    candidates_csv: Optional[str] = None
    excluded_csv: Optional[str] = None
    new_like_keys_seen: List[str] = field(default_factory=list)


@dataclass
class MutateSummary:
    rows_read: int = 0
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    dry_run: bool = True
    failures: List[str] = field(default_factory=list)


def sydney_today() -> date:
    return datetime.now(SYDNEY_TZ).date()


def parse_iso_date(value: Any) -> Optional[date]:
    text = clean_text(value) or ""
    if not text:
        return None
    # Shopify date metafields are YYYY-MM-DD; tolerate datetime prefixes.
    text = text[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def qty_map_from_level(level: Optional[Dict[str, Any]]) -> Dict[str, int]:
    out = {"available": 0, "committed": 0, "on_hand": 0}
    if not level:
        return out
    for q in level.get("quantities") or []:
        name = clean_text(q.get("name")) or ""
        if name not in out:
            continue
        try:
            out[name] = int(q.get("quantity") or 0)
        except (TypeError, ValueError):
            out[name] = 0
    return out


def find_new_like_metafields(metafield_nodes: Sequence[Dict[str, Any]]) -> List[str]:
    """
    Return ``namespace.key`` entries that look like a New/badge flag.

    Matches key containing ``new`` (word-ish) or value equal to ``New`` / ``new``.
    """
    hits: List[str] = []
    for node in metafield_nodes or []:
        ns = clean_text(node.get("namespace")) or ""
        key = clean_text(node.get("key")) or ""
        val = clean_text(node.get("value")) or ""
        if not key:
            continue
        key_l = key.lower()
        val_l = val.lower()
        key_looks_new = (
            key_l == "new"
            or key_l.endswith("_new")
            or key_l.startswith("new_")
            or "new_release" in key_l
            or key_l in ("is_new", "badge_new", "new_badge")
        )
        val_looks_new = val_l in ("new", "true") and (
            "new" in key_l or "badge" in key_l or "flag" in key_l or "label" in key_l
        )
        if key_looks_new or val_looks_new or (val_l == "new" and ns == "custom"):
            hits.append(f"{ns}.{key}={val}")
    return hits


def classify_archive_candidate(
    *,
    status: str,
    product_type: Optional[str],
    available: int,
    committed: int,
    pre_order: bool,
    media_release: Optional[date],
    as_of: Optional[date] = None,
    release_lookback_days: int = DEFAULT_RELEASE_LOOKBACK_DAYS,
    include_gift_cards: bool = False,
) -> Tuple[bool, str, str]:
    """
    Returns ``(is_candidate, reason_or_empty, exclude_reason_or_empty)``.

    Candidate when ACTIVE, OOS, no committed demand, not preorder, and release
    date is missing or older than lookback (and not in the future).
    """
    today = as_of or sydney_today()
    status_u = (clean_text(status) or "").upper()
    if status_u != "ACTIVE":
        return False, "", f"status_{status_u or 'unknown'}"
    if not include_gift_cards and is_gift_card_product_type(product_type):
        return False, "", "gift_card_product_type"
    if available > 0:
        return False, "", "in_stock"
    if committed > 0:
        return False, "", "has_committed_demand"
    if pre_order:
        return False, "", "pre_order"
    if media_release is not None:
        if media_release > today:
            return False, "", "future_release"
        cutoff = today - timedelta(days=release_lookback_days)
        if media_release >= cutoff:
            return False, "", f"released_within_{release_lookback_days}d"
    reason = "active_oos_no_committed_not_recent_release"
    return True, reason, ""


def product_node_to_row(
    product: Dict[str, Any],
    *,
    as_of: Optional[date] = None,
    release_lookback_days: int = DEFAULT_RELEASE_LOOKBACK_DAYS,
    include_gift_cards: bool = False,
) -> ProductCleanupRow:
    variants = (product.get("variants") or {}).get("nodes") or []
    available = committed = on_hand = 0
    barcodes: List[str] = []
    skus: List[str] = []
    for v in variants:
        level = ((v.get("inventoryItem") or {}).get("inventoryLevel")) or None
        qtys = qty_map_from_level(level)
        available += qtys["available"]
        committed += qtys["committed"]
        on_hand += qtys["on_hand"]
        bc = clean_text(v.get("barcode"))
        sk = clean_text(v.get("sku"))
        if bc:
            barcodes.append(bc)
        if sk:
            skus.append(sk)

    pre_order = False
    for key in ("preOrder", "preorderAlt"):
        mf = product.get(key) or {}
        if parse_shopify_bool_metafield(mf.get("value")):
            pre_order = True
            break
    backorder = parse_shopify_bool_metafield((product.get("backorder") or {}).get("value"))
    media_raw = clean_text((product.get("mediaReleaseDate") or {}).get("value")) or ""
    media_dt = parse_iso_date(media_raw)

    mf_nodes = (product.get("metafields") or {}).get("nodes") or []
    new_like = find_new_like_metafields(mf_nodes)
    # Also surface po_flag when it looks like a badge
    po_flag = clean_text((product.get("poFlag") or {}).get("value")) or ""
    if po_flag and po_flag.lower() in ("new", "pre-order", "preorder"):
        token = f"custom.po_flag={po_flag}"
        if token not in new_like:
            new_like.append(token)

    tags = product.get("tags") or []
    if isinstance(tags, list):
        tags_s = ",".join(clean_text(t) or "" for t in tags if clean_text(t))
    else:
        tags_s = clean_text(tags) or ""

    is_cand, reason, excl = classify_archive_candidate(
        status=clean_text(product.get("status")) or "",
        product_type=product.get("productType"),
        available=available,
        committed=committed,
        pre_order=pre_order,
        media_release=media_dt,
        as_of=as_of,
        release_lookback_days=release_lookback_days,
        include_gift_cards=include_gift_cards,
    )

    return ProductCleanupRow(
        product_id=str(product.get("id") or ""),
        handle=clean_text(product.get("handle")) or "",
        title=clean_text(product.get("title")) or "",
        barcode=";".join(barcodes),
        sku=";".join(skus),
        available=available,
        committed=committed,
        on_hand=on_hand,
        media_release_date=media_raw,
        pre_order=pre_order,
        backorder=backorder,
        new_like_metafields="|".join(new_like),
        tags=tags_s,
        reason=reason,
        exclude_reason=excl,
        is_candidate=is_cand,
        metafield_nodes=[
            {
                "namespace": clean_text(n.get("namespace")) or "",
                "key": clean_text(n.get("key")) or "",
                "type": clean_text(n.get("type")) or "",
                "value": clean_text(n.get("value")) or "",
            }
            for n in mf_nodes
        ],
    )


def iter_active_products(
    client: ShopifyClient,
    *,
    location_id: str,
    product_query: str = "status:active",
    page_sleep_sec: float = 0.25,
) -> Iterable[Dict[str, Any]]:
    cursor: Optional[str] = None
    while True:
        data = client.graphql(
            PRODUCTS_PAGE_QUERY,
            {"cursor": cursor, "locId": location_id, "q": product_query},
        )
        block = data.get("products") or {}
        for node in block.get("nodes") or []:
            yield node
        page = block.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        if page_sleep_sec > 0:
            time.sleep(page_sleep_sec)


def _row_to_csv_dict(row: ProductCleanupRow, *, include_exclude: bool = False) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "product_id": row.product_id,
        "handle": row.handle,
        "title": row.title,
        "barcode": row.barcode,
        "sku": row.sku,
        "available": row.available,
        "committed": row.committed,
        "on_hand": row.on_hand,
        "media_release_date": row.media_release_date,
        "pre_order": row.pre_order,
        "backorder": row.backorder,
        "new_like_metafields": row.new_like_metafields,
        "tags": row.tags,
        "reason": row.reason,
    }
    if include_exclude:
        d["exclude_reason"] = row.exclude_reason
    return d


def write_rows_csv(
    rows: Sequence[ProductCleanupRow],
    path: Path,
    *,
    fieldnames: Sequence[str],
    include_exclude: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(_row_to_csv_dict(row, include_exclude=include_exclude))


def export_archive_candidates(
    *,
    env_file: str = ".env",
    api_version: str = "2026-04",
    out_dir: Optional[Path] = None,
    release_lookback_days: int = DEFAULT_RELEASE_LOOKBACK_DAYS,
    include_gift_cards: bool = False,
    product_query: str = "status:active",
    as_of: Optional[date] = None,
    client: Optional[ShopifyClient] = None,
) -> Tuple[List[ProductCleanupRow], List[ProductCleanupRow], ExportSummary]:
    load_dotenv(env_file)
    shopify = client or get_shopify_client(env_file, api_version=api_version)
    location_id = shopify_inventory_location_id()
    if not location_id:
        raise SystemExit("Missing SHOPIFY_INVENTORY_LOCATION_ID")

    stamp = (as_of or sydney_today()).strftime("%Y%m%d")
    base = out_dir or (Path("tmp") / "store_cleanup")
    candidates_path = base / f"archive_candidates_{stamp}.csv"
    excluded_path = base / f"archive_excluded_{stamp}.csv"

    candidates: List[ProductCleanupRow] = []
    excluded: List[ProductCleanupRow] = []
    new_keys: set[str] = set()
    scanned = 0
    skipped_gc = 0

    for product in iter_active_products(shopify, location_id=location_id, product_query=product_query):
        scanned += 1
        row = product_node_to_row(
            product,
            as_of=as_of,
            release_lookback_days=release_lookback_days,
            include_gift_cards=include_gift_cards,
        )
        if row.exclude_reason == "gift_card_product_type":
            skipped_gc += 1
        if row.new_like_metafields:
            for part in row.new_like_metafields.split("|"):
                if "=" in part:
                    new_keys.add(part.split("=", 1)[0])
                else:
                    new_keys.add(part)
        if row.is_candidate:
            candidates.append(row)
        else:
            # Near-misses: OOS ACTIVE that failed another gate, plus gift cards.
            status_u = (clean_text(product.get("status")) or "").upper()
            if status_u == "ACTIVE" and (
                row.available <= 0
                or row.exclude_reason in ("gift_card_product_type",)
            ):
                excluded.append(row)

    write_rows_csv(candidates, candidates_path, fieldnames=CANDIDATE_CSV_FIELDS)
    write_rows_csv(
        excluded,
        excluded_path,
        fieldnames=EXCLUDED_CSV_FIELDS,
        include_exclude=True,
    )

    summary = ExportSummary(
        products_scanned=scanned,
        candidates=len(candidates),
        excluded=len(excluded),
        skipped_gift_cards=skipped_gc,
        candidates_csv=str(candidates_path),
        excluded_csv=str(excluded_path),
        new_like_keys_seen=sorted(new_keys),
    )
    return candidates, excluded, summary


def read_product_ids_from_csv(path: Path) -> List[str]:
    ids: List[str] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "product_id" not in reader.fieldnames:
            raise ValueError(f"CSV must have a product_id column: {path}")
        for row in reader:
            pid = clean_text(row.get("product_id"))
            if pid:
                ids.append(pid)
    return ids


def archive_products_from_csv(
    *,
    csv_path: Path,
    env_file: str = ".env",
    api_version: str = "2026-04",
    apply: bool = False,
    sleep_sec: float = 0.2,
    client: Optional[ShopifyClient] = None,
) -> MutateSummary:
    load_dotenv(env_file)
    shopify = client or get_shopify_client(env_file, api_version=api_version)
    product_ids = read_product_ids_from_csv(csv_path)
    summary = MutateSummary(rows_read=len(product_ids), dry_run=not apply)

    for pid in product_ids:
        summary.attempted += 1
        if not apply:
            print(f"[dry-run] would archive {pid}")
            summary.succeeded += 1
            continue
        try:
            data = shopify.graphql(
                PRODUCT_CHANGE_STATUS,
                {"productId": pid, "status": "ARCHIVED"},
            )
            payload = data.get("productChangeStatus") or {}
            errors = payload.get("userErrors") or []
            if errors:
                msg = "; ".join(
                    f"{e.get('field')}: {e.get('message')}" for e in errors
                )
                summary.failed += 1
                summary.failures.append(f"{pid}: {msg}")
                print(f"FAIL archive {pid}: {msg}")
            else:
                status = ((payload.get("product") or {}).get("status")) or "?"
                summary.succeeded += 1
                print(f"OK archived {pid} -> {status}")
        except Exception as exc:
            summary.failed += 1
            summary.failures.append(f"{pid}: {exc}")
            print(f"FAIL archive {pid}: {exc}")
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    return summary


def _parse_namespace_key(spec: str) -> Tuple[str, str]:
    text = (clean_text(spec) or "").strip()
    if "." not in text:
        raise ValueError(f"Metafield key must be namespace.key, got: {spec!r}")
    ns, key = text.split(".", 1)
    ns, key = ns.strip(), key.strip()
    if not ns or not key:
        raise ValueError(f"Metafield key must be namespace.key, got: {spec!r}")
    return ns, key


def clear_metafields_from_csv(
    *,
    csv_path: Path,
    metafield_keys: Sequence[str],
    env_file: str = ".env",
    api_version: str = "2026-04",
    apply: bool = False,
    delete: bool = True,
    sleep_sec: float = 0.2,
    client: Optional[ShopifyClient] = None,
) -> MutateSummary:
    """
    Clear product metafields listed in ``metafield_keys`` (``namespace.key``).

    When ``delete`` is True, deletes the metafield via ``metafieldDelete`` after
    looking up the metafield id. Otherwise sets empty/false via ``metafieldsSet``
    (less preferred for badges).
    """
    load_dotenv(env_file)
    shopify = client or get_shopify_client(env_file, api_version=api_version)
    product_ids = read_product_ids_from_csv(csv_path)
    parsed = [_parse_namespace_key(k) for k in metafield_keys]
    summary = MutateSummary(rows_read=len(product_ids), dry_run=not apply)

    lookup_q = """
    query ProductMetafieldIds($id: ID!) {
      product(id: $id) {
        id
        metafields(first: 50) {
          nodes { id namespace key value }
        }
      }
    }
    """

    for pid in product_ids:
        for ns, key in parsed:
            label = f"{pid} {ns}.{key}"
            summary.attempted += 1
            if not apply:
                print(f"[dry-run] would clear {label}")
                summary.succeeded += 1
                continue
            try:
                data = shopify.graphql(lookup_q, {"id": pid})
                product = data.get("product") or {}
                nodes = (product.get("metafields") or {}).get("nodes") or []
                match = next(
                    (
                        n
                        for n in nodes
                        if (clean_text(n.get("namespace")) == ns and clean_text(n.get("key")) == key)
                    ),
                    None,
                )
                if not match:
                    print(f"SKIP {label}: metafield not present")
                    summary.succeeded += 1
                    continue
                if delete:
                    del_data = shopify.graphql(
                        METAFIELD_DELETE,
                        {"input": {"id": match["id"]}},
                    )
                    errors = (del_data.get("metafieldDelete") or {}).get("userErrors") or []
                else:
                    # Boolean → false; otherwise blank string (may fail type checks).
                    mf_type = "boolean" if str(match.get("value")).lower() in ("true", "false") else "single_line_text_field"
                    value = "false" if mf_type == "boolean" else ""
                    set_data = shopify.graphql(
                        METAFIELDS_SET,
                        {
                            "metafields": [
                                {
                                    "ownerId": pid,
                                    "namespace": ns,
                                    "key": key,
                                    "type": mf_type,
                                    "value": value,
                                }
                            ]
                        },
                    )
                    errors = (set_data.get("metafieldsSet") or {}).get("userErrors") or []
                if errors:
                    msg = "; ".join(
                        f"{e.get('field')}: {e.get('message')}" for e in errors
                    )
                    summary.failed += 1
                    summary.failures.append(f"{label}: {msg}")
                    print(f"FAIL clear {label}: {msg}")
                else:
                    summary.succeeded += 1
                    print(f"OK cleared {label}")
            except Exception as exc:
                summary.failed += 1
                summary.failures.append(f"{label}: {exc}")
                print(f"FAIL clear {label}: {exc}")
            if sleep_sec > 0:
                time.sleep(sleep_sec)
    return summary


def print_export_summary(summary: ExportSummary) -> None:
    print("\n=== Shopify archive candidate export ===")
    print(f"Products scanned:     {summary.products_scanned}")
    print(f"Candidates:           {summary.candidates}")
    print(f"Excluded (near-miss): {summary.excluded}")
    print(f"Gift cards skipped:   {summary.skipped_gift_cards}")
    if summary.candidates_csv:
        print(f"Candidates CSV:       {summary.candidates_csv}")
    if summary.excluded_csv:
        print(f"Excluded CSV:         {summary.excluded_csv}")
    if summary.new_like_keys_seen:
        print(f"New-like metafields:  {', '.join(summary.new_like_keys_seen)}")
    else:
        print("New-like metafields:  (none detected)")


# Silence unused import lint for GIFTCARD_PRODUCT_TYPES re-export convenience in tests.
_ = GIFTCARD_PRODUCT_TYPES
