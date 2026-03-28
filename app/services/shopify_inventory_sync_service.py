"""
Compare Shopify on-hand inventory to ``catalog_items`` commercial fields; optionally push corrections.

Uses ``shopify_listings`` (from store sync) + direct ``catalog_items`` reads only.
Does not call supplier import, normalize, catalog upsert, or legacy stock sync.

With ``dry_run=True``, returns a ``drift_details`` list for every quantity mismatch where a target
quantity could be derived (so you can inspect before ``SHOPIFY_INVENTORY_APPLY=1``).

Each drift row includes ``drift_classification`` (expected preorder/backorder negative inventory,
true mismatch, or unclassified) for business interpretation — mutations stay opt-in via env + not dry-run.
"""

from __future__ import annotations

import os
from collections import Counter
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from supabase import create_client

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text

_MAX_INV_ERR_LEN = 2000


def _truncate_inventory_error(msg: object) -> str:
    s = str(msg).strip()
    if len(s) <= _MAX_INV_ERR_LEN:
        return s
    return s[: _MAX_INV_ERR_LEN - 3] + "..."


def _repo_root() -> str:
    from pathlib import Path

    return str(Path(__file__).resolve().parents[2])


def _env_path(env_file: str) -> str:
    if os.path.isabs(env_file):
        return env_file
    return os.path.join(_repo_root(), env_file)


def _apply_inventory() -> bool:
    return os.getenv("SHOPIFY_INVENTORY_APPLY", "0").strip().lower() in ("1", "true", "yes")


def _ignore_compare_quantity() -> bool:
    return os.getenv("SHOPIFY_INVENTORY_IGNORE_COMPARE", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _jsonish_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _parse_listing_media_release_date(lst: Dict[str, Any]) -> Optional[date]:
    """Calendar date from shopify_listings.media_release_date (Postgres date or ISO string)."""
    v = lst.get("media_release_date")
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).date() if v.tzinfo else v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        text = v.strip()
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                return None
    return None


def _inventory_policy_continue_when_oos(policy: Any) -> bool:
    """True when Shopify variant sells when out of stock (inventoryPolicy CONTINUE)."""
    p = (clean_text(policy) or "").upper()
    return p == "CONTINUE"


def _pre_order_from_catalog(cat: Dict[str, Any]) -> bool:
    """Business pre-order flag: catalog availability_status preorder."""
    return (cat.get("availability_status") or "").strip().lower() == "preorder"


def classify_inventory_drift(
    lst: Dict[str, Any],
    cat: Dict[str, Any],
    shop_qty: int,
) -> str:
    """
    Classify a quantity drift row for reporting (no mutation).

    expected_preorder_negative_inventory / expected_backorder_negative_inventory apply only when
    Shopify on-hand is negative, policy is CONTINUE, and media_release_date is parseable.
    """
    policy_continue = _inventory_policy_continue_when_oos(lst.get("inventory_policy"))
    media = _parse_listing_media_release_date(lst)
    today = _utc_today()
    preorder = _pre_order_from_catalog(cat)

    if shop_qty < 0 and policy_continue:
        if preorder:
            if media is not None and media > today:
                return "expected_preorder_negative_inventory"
            return "unclassified"
        if media is not None and media < today:
            return "expected_backorder_negative_inventory"
        return "unclassified"

    return "true_inventory_mismatch"


def desired_qty_from_catalog_item(row: Dict[str, Any]) -> Optional[int]:
    """
    Return a target on-hand quantity when we can infer it; otherwise None (skip push).
    """
    av = (row.get("availability_status") or "").strip().lower()
    if av in ("supplier_out", "store_out", "discontinued", "inactive"):
        return 0
    if av in ("preorder",):
        return 0

    ss = row.get("supplier_stock_status")
    if ss is None:
        return None
    if isinstance(ss, bool):
        return 0 if not ss else None
    if isinstance(ss, int):
        return max(0, ss)
    s = str(ss).strip()
    if s.isdigit():
        return int(s)
    low = s.lower()
    if any(x in low for x in ("out", "none", "n/a", "unavailable")):
        return 0
    if low in ("in stock", "instock", "available", "yes"):
        return None
    return None


def _drift_reason(
    cat: Dict[str, Any],
    desired: int,
    shop_qty: int,
    *,
    has_inventory_item: bool,
) -> str:
    if not has_inventory_item:
        return (
            f"catalog target_qty={desired} vs shopify_listings.inventory_quantity={shop_qty}; "
            "missing shopify_inventory_item_id (cannot apply)"
        )
    av_raw = (cat.get("availability_status") or "").strip()
    av_l = av_raw.lower()
    if av_l in ("supplier_out", "store_out", "discontinued", "inactive", "preorder"):
        return (
            f"availability_status={av_raw!r} → target {desired}; "
            f"shopify inventory_quantity={shop_qty}"
        )
    ss = cat.get("supplier_stock_status")
    return (
        f"supplier_stock_status={ss!r} → target_qty={desired}; "
        f"shopify inventory_quantity={shop_qty}"
    )


SET_QUANTITIES_MUTATION = """
mutation SetQty($input: InventorySetQuantitiesInput!) {
  inventorySetQuantities(input: $input) {
    userErrors {
      field
      message
    }
  }
}
"""


def run_shopify_inventory_sync(*, env_file: str = ".env", dry_run: bool = False) -> Dict[str, Any]:
    path = _env_path(env_file)
    load_dotenv(path)

    shop = os.getenv("SHOPIFY_SHOP", "").strip()
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    location_id = os.getenv("SHOPIFY_INVENTORY_LOCATION_ID", "").strip()

    if not shop:
        raise SystemExit("Missing SHOPIFY_SHOP")
    if not url or not key:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")

    do_apply = _apply_inventory() and not dry_run
    if do_apply and not location_id:
        raise SystemExit(
            "SHOPIFY_INVENTORY_APPLY is set but SHOPIFY_INVENTORY_LOCATION_ID is missing "
            "(gid://shopify/Location/...)"
        )

    supabase = create_client(url, key)
    sb = (
        supabase.table("shopify_listings")
        .select(
            "id,shopify_variant_id,shopify_inventory_item_id,inventory_quantity,catalog_item_id,shop,"
            "product_title,variant_title,sku,barcode,inventory_policy,media_release_date,"
            "match_method,match_status,match_value",
        )
        .eq("shop", shop)
        .not_.is_("catalog_item_id", "null")
        .execute()
    )
    listings = sb.data or []

    catalog_by_id: Dict[str, Dict[str, Any]] = {}
    ids = list({str(r["catalog_item_id"]) for r in listings if r.get("catalog_item_id")})
    for i in range(0, len(ids), 200):
        batch = ids[i : i + 200]
        resp = (
            supabase.table("catalog_items")
            .select(
                "id,supplier_stock_status,availability_status,barcode,shopify_variant_id",
            )
            .in_("id", batch)
            .execute()
        )
        for r in resp.data or []:
            catalog_by_id[str(r["id"])] = r

    now = datetime.now(timezone.utc).isoformat()
    compared = 0
    skipped_no_catalog_row = 0
    skipped_no_target = 0
    skipped_no_inventory_item = 0
    in_sync = 0
    to_fix: List[Dict[str, Any]] = []
    drift_details: List[Dict[str, Any]] = []

    for lst in listings:
        compared += 1
        cid = str(lst["catalog_item_id"])
        cat = catalog_by_id.get(cid)
        if not cat:
            skipped_no_catalog_row += 1
            continue
        desired = desired_qty_from_catalog_item(cat)
        if desired is None:
            skipped_no_target += 1
            continue
        shop_qty = int(lst.get("inventory_quantity") or 0)
        if shop_qty == desired:
            in_sync += 1
            continue

        inv_item = lst.get("shopify_inventory_item_id")
        inv_item = clean_text(inv_item) if inv_item else None
        has_inv = bool(inv_item)
        pre_order = _pre_order_from_catalog(cat)
        drift_classification = classify_inventory_drift(lst, cat, shop_qty)

        drift_details.append(
            {
                "shopify_variant_id": lst.get("shopify_variant_id"),
                "product_title": lst.get("product_title"),
                "variant_title": lst.get("variant_title"),
                "sku": lst.get("sku"),
                "barcode": lst.get("barcode"),
                "inventory_quantity_shopify": shop_qty,
                "target_quantity": desired,
                "supplier_stock_status": _jsonish_value(cat.get("supplier_stock_status")),
                "availability_status": cat.get("availability_status"),
                "pre_order": pre_order,
                "inventory_policy": lst.get("inventory_policy"),
                "media_release_date": _jsonish_value(lst.get("media_release_date")),
                "match_method": lst.get("match_method"),
                "match_status": lst.get("match_status"),
                "match_value": lst.get("match_value"),
                "drift_classification": drift_classification,
                "drift_reason": _drift_reason(
                    cat, desired, shop_qty, has_inventory_item=has_inv
                ),
                "can_apply_mutation": has_inv,
            }
        )

        if not inv_item:
            skipped_no_inventory_item += 1
            continue
        to_fix.append(
            {
                "listing_id": lst["id"],
                "shopify_inventory_item_id": inv_item,
                "shopify_variant_id": lst.get("shopify_variant_id"),
                "from_qty": shop_qty,
                "to_qty": desired,
            }
        )

    applied = 0
    errors: List[str] = []

    ignore_compare = _ignore_compare_quantity()

    if do_apply and to_fix:
        gql = ShopifyClient()
        for item in to_fix:
            try:
                qty_row: Dict[str, Any] = {
                    "inventoryItemId": item["shopify_inventory_item_id"],
                    "locationId": location_id,
                    "quantity": item["to_qty"],
                }
                if ignore_compare:
                    qty_row["ignoreCompareQuantity"] = True
                data = gql.graphql(
                    SET_QUANTITIES_MUTATION,
                    {
                        "input": {
                            "name": "available",
                            "reason": "correction",
                            "referenceDocumentUri": "tape-film://jobs/shopify_inventory_sync",
                            "quantities": [qty_row],
                        }
                    },
                )
                block = (data or {}).get("inventorySetQuantities") or {}
                uerr = block.get("userErrors") or []
                if uerr:
                    err_txt = _truncate_inventory_error(uerr)
                    errors.append(f"{item['shopify_variant_id']}: {uerr}")
                    if not dry_run:
                        supabase.table("shopify_listings").update(
                            {"last_inventory_apply_error": err_txt}
                        ).eq("id", item["listing_id"]).execute()
                    continue
                applied += 1
                if not dry_run:
                    supabase.table("shopify_listings").update(
                        {
                            "last_inventory_apply_at": now,
                            "inventory_quantity": item["to_qty"],
                            "last_inventory_apply_error": None,
                        }
                    ).eq("id", item["listing_id"]).execute()
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{item.get('shopify_variant_id')}: {exc!s}")
                if not dry_run:
                    supabase.table("shopify_listings").update(
                        {"last_inventory_apply_error": _truncate_inventory_error(exc)}
                    ).eq("id", item["listing_id"]).execute()

    if not dry_run:
        listing_ids = [str(x["id"]) for x in listings]
        for i in range(0, len(listing_ids), 100):
            chunk = listing_ids[i : i + 100]
            supabase.table("shopify_listings").update({"last_inventory_compare_at": now}).in_(
                "id", chunk
            ).execute()

    drift_detected = len(drift_details)
    drift_applyable = len(to_fix)
    class_counts = Counter(d.get("drift_classification", "unclassified") for d in drift_details)

    result: Dict[str, Any] = {
        "status": "ok",
        "job": "shopify_inventory_sync",
        "shop": shop,
        "listings_compared": compared,
        "in_sync": in_sync,
        "skipped_no_catalog_row": skipped_no_catalog_row,
        "skipped_no_target_qty": skipped_no_target,
        "skipped_no_inventory_item": skipped_no_inventory_item,
        "drift_detected": drift_detected,
        "drift_applyable_count": drift_applyable,
        "drift_expected_preorder_count": int(
            class_counts.get("expected_preorder_negative_inventory", 0)
        ),
        "drift_expected_backorder_count": int(
            class_counts.get("expected_backorder_negative_inventory", 0)
        ),
        "drift_true_mismatch_count": int(class_counts.get("true_inventory_mismatch", 0)),
        "drift_unclassified_count": int(class_counts.get("unclassified", 0)),
        "apply_enabled": do_apply,
        "ignore_compare_quantity": ignore_compare,
        "applied_mutations": applied,
        "dry_run": dry_run,
        "db_writes": (not dry_run) and len(listings) > 0,
        "errors": errors[:50],
    }

    if dry_run:
        result["drift_details"] = drift_details
    elif drift_detected > 0:
        result["drift_details"] = drift_details

    return result
