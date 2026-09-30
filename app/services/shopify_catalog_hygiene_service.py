from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient, get_shopify_client
from app.helpers.text_helpers import clean_text
from app.services.shopify_inventory_settings_audit import parse_shopify_bool_metafield

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

PRODUCTS_QUERY = """
query CatalogHygieneProducts($cursor: String, $q: String) {
  products(first: 25, after: $cursor, query: $q) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      title
      handle
      status
      tags
      collections(first: 25) {
        nodes {
          id
          handle
          title
        }
      }
      preOrder: metafield(namespace: "custom", key: "pre_order") {
        id
        namespace
        key
        type
        value
      }
      preorderAlt: metafield(namespace: "custom", key: "preorder") {
        id
        namespace
        key
        type
        value
      }
      mediaReleaseDate: metafield(namespace: "custom", key: "media_release_date") {
        id
        namespace
        key
        type
        value
      }
      metafields(first: 80) {
        nodes {
          id
          namespace
          key
          type
          value
        }
      }
      variants(first: 100) {
        nodes {
          id
          title
          sku
          barcode
          inventoryQuantity
          inventoryItem {
            id
            tracked
          }
        }
      }
    }
  }
}
"""

PRODUCT_UPDATE_TAGS = """
mutation ProductUpdateTags($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id tags }
    userErrors { field message }
  }
}
"""

METAFIELDS_SET = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key type value }
    userErrors { field message }
  }
}
"""

METAFIELDS_DELETE = """
mutation MetafieldsDelete($metafields: [MetafieldIdentifierInput!]!) {
  metafieldsDelete(metafields: $metafields) {
    deletedMetafields {
      ownerId
      namespace
      key
    }
    userErrors { field message }
  }
}
"""

PRODUCT_CHANGE_STATUS = """
mutation ProductChangeStatus($productId: ID!, $status: ProductStatus!) {
  productChangeStatus(productId: $productId, status: $status) {
    product { id status }
    userErrors { field message }
  }
}
"""


def sydney_today() -> date:
    return datetime.now(SYDNEY_TZ).date()


def parse_iso_date(raw: Any) -> Optional[date]:
    text = clean_text(raw) or ""
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _variant_available_qty(variant: Dict[str, Any]) -> Optional[int]:
    fallback = variant.get("inventoryQuantity")
    if fallback is None:
        return None
    try:
        return int(fallback)
    except (TypeError, ValueError):
        return None


def is_valid_future_preorder(pre_order_value: bool, release_date: Optional[date], today: date) -> bool:
    return bool(pre_order_value and release_date and release_date > today)


def classify_zero_stock_candidate(
    *,
    product_status: str,
    variant_available_quantities: Sequence[Optional[int]],
    has_relevant_variants: bool,
    is_valid_preorder: bool,
) -> Tuple[bool, str]:
    status = (clean_text(product_status) or "").upper()
    if status != "ACTIVE":
        return False, "status_not_active"
    if is_valid_preorder:
        return False, "valid_future_preorder_protected"
    if not has_relevant_variants:
        return False, "no_relevant_sellable_variants"
    if any(q is None for q in variant_available_quantities):
        return False, "unknown_variant_stock"
    if any(int(q) > 0 for q in variant_available_quantities if q is not None):
        return False, "positive_stock_present"
    if any(int(q) < 0 for q in variant_available_quantities if q is not None):
        return False, "negative_stock_present"
    if all(int(q) == 0 for q in variant_available_quantities if q is not None):
        return True, "all_relevant_variants_exactly_zero"
    return False, "not_exactly_zero"


def _is_new_metafield(node: Dict[str, Any]) -> bool:
    ns = (clean_text(node.get("namespace")) or "").lower()
    key = (clean_text(node.get("key")) or "").lower()
    value = (clean_text(node.get("value")) or "").strip().lower()
    if ns != "custom":
        return False
    if key in {"new", "is_new", "new_badge", "badge_new"}:
        return True
    if key in {"po_flag", "badge", "label"} and value == "new":
        return True
    return False


def remove_exact_new_tag(tags: Sequence[str]) -> Tuple[List[str], bool]:
    clean_tags: List[str] = []
    removed = False
    for tag in tags:
        t = clean_text(tag)
        if not t:
            continue
        if t.casefold() == "new":
            removed = True
            continue
        clean_tags.append(t)
    return clean_tags, removed


@dataclass
class VariantSnapshot:
    id: str
    title: str
    sku: str
    barcode: str
    available: Optional[int]


@dataclass
class ProductAuditRecord:
    product_id: str
    title: str
    handle: str
    status: str
    pre_order_value: bool
    pre_order_field: str
    release_date: Optional[date]
    release_field: str
    variant_rows: List[VariantSnapshot]
    candidate: bool
    candidate_reason: str
    skip_stock_unknown: bool
    stale_preorder: bool
    preorder_missing_media_release_date: bool
    future_release_without_preorder: bool
    has_new_tag: bool
    has_new_metafield: bool
    has_new_collection: bool
    new_metafield_keys: List[str] = field(default_factory=list)


@dataclass
class HygieneAuditSummary:
    products_examined: int = 0
    active_products_examined: int = 0
    variants_examined: int = 0
    positive_stock_products: int = 0
    negative_stock_products_protected: int = 0
    valid_future_preorders_protected: int = 0
    zero_stock_candidate_products: int = 0
    products_skipped_unknown_stock: int = 0
    stale_preorders: int = 0
    preorder_missing_media_release_date: int = 0
    future_release_without_preorder: int = 0
    products_with_new: int = 0
    mutated_tags: int = 0
    mutated_preorder: int = 0
    mutated_new_metafields: int = 0
    failures: int = 0
    inventory_source_notes: List[str] = field(default_factory=list)
    observed_preorder_fields: List[str] = field(default_factory=list)
    observed_release_fields: List[str] = field(default_factory=list)
    observed_new_mechanisms: List[str] = field(default_factory=list)
    mutation_failures: List[str] = field(default_factory=list)


@dataclass
class DeactivationSummary:
    rows_read: int = 0
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    dry_run: bool = True
    failures: List[str] = field(default_factory=list)


def iter_products(
    client: ShopifyClient,
    *,
    product_query: str = "status:active",
    page_sleep_sec: float = 0.2,
) -> Iterable[Dict[str, Any]]:
    cursor: Optional[str] = None
    while True:
        tries = 0
        while True:
            tries += 1
            try:
                data = client.graphql(
                    PRODUCTS_QUERY,
                    {"cursor": cursor, "q": product_query},
                )
                break
            except Exception as exc:
                msg = str(exc)
                if "THROTTLED" in msg and tries < 6:
                    time.sleep(min(2.0 * tries, 8.0))
                    continue
                raise
        block = data.get("products") or {}
        for node in block.get("nodes") or []:
            yield node
        page_info = block.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if page_sleep_sec > 0:
            time.sleep(page_sleep_sec)


def _product_to_record(product: Dict[str, Any], *, today: date) -> ProductAuditRecord:
    variants = (product.get("variants") or {}).get("nodes") or []
    variant_rows: List[VariantSnapshot] = []
    qtys: List[Optional[int]] = []
    for v in variants:
        qty = _variant_available_qty(v)
        qtys.append(qty)
        variant_rows.append(
            VariantSnapshot(
                id=str(v.get("id") or ""),
                title=clean_text(v.get("title")) or "",
                sku=clean_text(v.get("sku")) or "",
                barcode=clean_text(v.get("barcode")) or "",
                available=qty,
            )
        )

    pre_order = parse_shopify_bool_metafield(((product.get("preOrder") or {}).get("value")))
    pre_order_field = "custom.pre_order"
    if not pre_order:
        alt = parse_shopify_bool_metafield(((product.get("preorderAlt") or {}).get("value")))
        if alt:
            pre_order = True
            pre_order_field = "custom.preorder"

    release_date = parse_iso_date(((product.get("mediaReleaseDate") or {}).get("value")))
    release_field = "custom.media_release_date" if release_date else ""

    valid_preorder = is_valid_future_preorder(pre_order, release_date, today)
    has_relevant_variants = len(variant_rows) > 0
    candidate, reason = classify_zero_stock_candidate(
        product_status=clean_text(product.get("status")) or "",
        variant_available_quantities=qtys,
        has_relevant_variants=has_relevant_variants,
        is_valid_preorder=valid_preorder,
    )
    skip_stock_unknown = any(v.available is None for v in variant_rows)

    stale_preorder = bool(pre_order and release_date is not None and release_date <= today)
    preorder_missing_media_release_date = bool(pre_order and release_date is None)
    future_release_without_preorder = bool((not pre_order) and release_date is not None and release_date > today)

    tags = product.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    _, has_new_tag = remove_exact_new_tag(tags)

    metafields = (product.get("metafields") or {}).get("nodes") or []
    new_mf = [n for n in metafields if _is_new_metafield(n)]
    new_mf_keys = [f"{n.get('namespace')}.{n.get('key')}" for n in new_mf]

    collections = ((product.get("collections") or {}).get("nodes")) or []
    has_new_collection = any(
        (clean_text(c.get("handle")) or "").casefold() == "new"
        or (clean_text(c.get("title")) or "").casefold() == "new"
        for c in collections
    )

    return ProductAuditRecord(
        product_id=str(product.get("id") or ""),
        title=clean_text(product.get("title")) or "",
        handle=clean_text(product.get("handle")) or "",
        status=(clean_text(product.get("status")) or "").upper(),
        pre_order_value=pre_order,
        pre_order_field=pre_order_field,
        release_date=release_date,
        release_field=release_field,
        variant_rows=variant_rows,
        candidate=candidate,
        candidate_reason=reason,
        skip_stock_unknown=skip_stock_unknown,
        stale_preorder=stale_preorder,
        preorder_missing_media_release_date=preorder_missing_media_release_date,
        future_release_without_preorder=future_release_without_preorder,
        has_new_tag=has_new_tag,
        has_new_metafield=bool(new_mf),
        has_new_collection=has_new_collection,
        new_metafield_keys=new_mf_keys,
    )


def _set_preorder_false(
    client: ShopifyClient,
    *,
    owner_id: str,
    namespace: str,
    key: str,
    mf_type: str,
) -> None:
    data = client.graphql(
        METAFIELDS_SET,
        {
            "metafields": [
                {
                    "ownerId": owner_id,
                    "namespace": namespace,
                    "key": key,
                    "type": mf_type or "boolean",
                    "value": "false",
                }
            ]
        },
    )
    errs = (data.get("metafieldsSet") or {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"metafieldsSet errors: {errs}")


def _update_product_tags(client: ShopifyClient, *, product_id: str, tags: Sequence[str]) -> None:
    data = client.graphql(PRODUCT_UPDATE_TAGS, {"input": {"id": product_id, "tags": list(tags)}})
    errs = (data.get("productUpdate") or {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"productUpdate errors: {errs}")


def _clear_new_metafield(client: ShopifyClient, *, owner_id: str, node: Dict[str, Any]) -> None:
    ns = clean_text(node.get("namespace")) or "custom"
    key = clean_text(node.get("key")) or ""
    if not key:
        return
    data = client.graphql(
        METAFIELDS_DELETE,
        {
            "metafields": [
                {
                    "ownerId": owner_id,
                    "namespace": ns,
                    "key": key,
                }
            ]
        },
    )
    errs = (data.get("metafieldsDelete") or {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"metafieldsDelete errors: {errs}")


def archive_product(client: ShopifyClient, *, product_id: str) -> None:
    data = client.graphql(
        PRODUCT_CHANGE_STATUS,
        {"productId": product_id, "status": "ARCHIVED"},
    )
    errs = (data.get("productChangeStatus") or {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"productChangeStatus errors: {errs}")


def run_catalog_hygiene_audit(
    *,
    env_file: str = ".env",
    api_version: str = "2026-04",
    apply_cleanup: bool = False,
    product_query: str = "status:active",
    client: Optional[ShopifyClient] = None,
) -> Tuple[List[ProductAuditRecord], HygieneAuditSummary]:
    load_dotenv(env_file)
    shopify = client or get_shopify_client(env_file=env_file, api_version=api_version)
    today = sydney_today()
    records: List[ProductAuditRecord] = []
    summary = HygieneAuditSummary(inventory_source_notes=["variant.inventoryQuantity"])

    observed_preorder = {"custom.pre_order", "custom.preorder"}
    observed_release = {"custom.media_release_date"}
    observed_new = set()

    for product in iter_products(shopify, product_query=product_query):
        rec = _product_to_record(product, today=today)
        records.append(rec)
        summary.products_examined += 1
        summary.variants_examined += len(rec.variant_rows)
        if rec.status == "ACTIVE":
            summary.active_products_examined += 1

        if rec.candidate:
            summary.zero_stock_candidate_products += 1
        if rec.skip_stock_unknown:
            summary.products_skipped_unknown_stock += 1
        if any((v.available or 0) > 0 for v in rec.variant_rows if v.available is not None):
            summary.positive_stock_products += 1
        if any((v.available or 0) < 0 for v in rec.variant_rows if v.available is not None):
            summary.negative_stock_products_protected += 1
        if is_valid_future_preorder(rec.pre_order_value, rec.release_date, today):
            summary.valid_future_preorders_protected += 1
        if rec.stale_preorder:
            summary.stale_preorders += 1
        if rec.preorder_missing_media_release_date:
            summary.preorder_missing_media_release_date += 1
        if rec.future_release_without_preorder:
            summary.future_release_without_preorder += 1

        if rec.has_new_tag or rec.has_new_metafield or rec.has_new_collection:
            summary.products_with_new += 1
        if rec.has_new_tag:
            observed_new.add("product_tag:New")
        if rec.has_new_metafield:
            observed_new.add("product_metafield:new-like")
        if rec.has_new_collection:
            observed_new.add("collection:New")

    summary.observed_preorder_fields = sorted(observed_preorder)
    summary.observed_release_fields = sorted(observed_release)
    summary.observed_new_mechanisms = sorted(observed_new)

    if not apply_cleanup:
        return records, summary

    for product in iter_products(shopify, product_query=product_query):
        rec = _product_to_record(product, today=today)
        try:
            if rec.stale_preorder or rec.preorder_missing_media_release_date:
                primary = product.get("preOrder") or {}
                _set_preorder_false(
                    shopify,
                    owner_id=rec.product_id,
                    namespace=clean_text(primary.get("namespace")) or "custom",
                    key=clean_text(primary.get("key")) or "pre_order",
                    mf_type=clean_text(primary.get("type")) or "boolean",
                )
                summary.mutated_preorder += 1

            if rec.has_new_metafield:
                mf_nodes = (product.get("metafields") or {}).get("nodes") or []
                for node in mf_nodes:
                    if _is_new_metafield(node):
                        _clear_new_metafield(shopify, owner_id=rec.product_id, node=node)
                        summary.mutated_new_metafields += 1

            tags = product.get("tags") or []
            if not isinstance(tags, list):
                tags = []
            new_tags, removed = remove_exact_new_tag(tags)
            if removed:
                _update_product_tags(shopify, product_id=rec.product_id, tags=new_tags)
                summary.mutated_tags += 1
        except Exception as exc:
            summary.failures += 1
            summary.mutation_failures.append(f"{rec.product_id}: {exc}")

    return records, summary


def serialize_records(records: Sequence[ProductAuditRecord]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in records:
        out.append(
            {
                "product_id": r.product_id,
                "title": r.title,
                "handle": r.handle,
                "status": r.status,
                "pre_order": r.pre_order_value,
                "pre_order_field": r.pre_order_field,
                "release_date": r.release_date.isoformat() if r.release_date else None,
                "release_field": r.release_field,
                "candidate": r.candidate,
                "candidate_reason": r.candidate_reason,
                "skip_stock_unknown": r.skip_stock_unknown,
                "stale_preorder": r.stale_preorder,
                "preorder_missing_media_release_date": r.preorder_missing_media_release_date,
                "future_release_without_preorder": r.future_release_without_preorder,
                "has_new_tag": r.has_new_tag,
                "has_new_metafield": r.has_new_metafield,
                "has_new_collection": r.has_new_collection,
                "new_metafield_keys": r.new_metafield_keys,
                "variants": [
                    {
                        "id": v.id,
                        "title": v.title,
                        "sku": v.sku,
                        "barcode": v.barcode,
                        "available": v.available,
                    }
                    for v in r.variant_rows
                ],
            }
        )
    return out


def write_json_report(path: str, *, records: Sequence[ProductAuditRecord], summary: HygieneAuditSummary) -> None:
    payload = {
        "generated_at_sydney": datetime.now(SYDNEY_TZ).isoformat(),
        "summary": summary.__dict__,
        "records": serialize_records(records),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def apply_deactivations_from_csv(
    *,
    csv_path: str,
    env_file: str = ".env",
    api_version: str = "2026-04",
    apply: bool = False,
    sleep_sec: float = 0.2,
    client: Optional[ShopifyClient] = None,
) -> DeactivationSummary:
    import csv

    load_dotenv(env_file)
    shopify = client or get_shopify_client(env_file=env_file, api_version=api_version)
    summary = DeactivationSummary(dry_run=not apply)
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        ids: List[str] = []
        for row in reader:
            pid = clean_text(row.get("product_id"))
            if pid:
                ids.append(pid)
    summary.rows_read = len(ids)
    for pid in ids:
        summary.attempted += 1
        if not apply:
            summary.succeeded += 1
            continue
        try:
            archive_product(shopify, product_id=pid)
            summary.succeeded += 1
        except Exception as exc:
            summary.failed += 1
            summary.failures.append(f"{pid}: {exc}")
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    return summary
