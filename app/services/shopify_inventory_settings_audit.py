"""
Audit (and optionally repair) Shopify variant inventory tracking / policy vs
``custom.pre_order`` / ``custom.backorder`` metafields.

Read-only by default. Repair is opt-in and must not change inventory quantities.
"""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.helpers.text_helpers import clean_text
from app.services.catalog_shopify_publish_service import shopify_inventory_location_id

ISSUE_TRACKING_DISABLED = "TRACKING_DISABLED"
ISSUE_POLICY_SHOULD_BE_CONTINUE = "POLICY_SHOULD_BE_CONTINUE"
ISSUE_POLICY_SHOULD_BE_DENY = "POLICY_SHOULD_BE_DENY"
ISSUE_MISSING_INVENTORY_ITEM = "MISSING_INVENTORY_ITEM"
ISSUE_MISSING_LOCATION_INVENTORY = "MISSING_LOCATION_INVENTORY"
ISSUE_MISSING_SKU_OR_BARCODE = "MISSING_SKU_OR_BARCODE"

GIFTCARD_PRODUCT_TYPES = frozenset({"gift card", "gift cards", "gift_card"})

PRODUCTS_PAGE_QUERY = """
query InventorySettingsAudit($cursor: String, $locId: ID!, $q: String) {
  products(first: 25, after: $cursor, query: $q) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      title
      status
      productType
      preOrder: metafield(namespace: "custom", key: "pre_order") {
        value
      }
      preorderAlt: metafield(namespace: "custom", key: "preorder") {
        value
      }
      backorder: metafield(namespace: "custom", key: "backorder") {
        value
      }
      variants(first: 100) {
        nodes {
          id
          title
          sku
          barcode
          inventoryPolicy
          inventoryItem {
            id
            tracked
            inventoryLevel(locationId: $locId) {
              quantities(names: ["available"]) {
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

INVENTORY_ITEM_UPDATE = """
mutation InventoryItemUpdate($id: ID!, $input: InventoryItemInput!) {
  inventoryItemUpdate(id: $id, input: $input) {
    inventoryItem {
      id
      tracked
    }
    userErrors {
      field
      message
    }
  }
}
"""

VARIANTS_BULK_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants {
      id
      inventoryPolicy
    }
    userErrors {
      field
      message
    }
  }
}
"""


def parse_shopify_bool_metafield(value: Any) -> bool:
    """
    Normalise Shopify boolean metafield values.

    Missing / empty → False. Accepts bool, and common string forms.
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("", "null", "none"):
        return False
    if text in ("true", "1", "yes"):
        return True
    if text in ("false", "0", "no"):
        return False
    return False


def expected_inventory_policy(*, pre_order: bool, backorder: bool) -> str:
    return "CONTINUE" if (pre_order or backorder) else "DENY"


def is_gift_card_product_type(product_type: Optional[str]) -> bool:
    return (clean_text(product_type) or "").lower() in GIFTCARD_PRODUCT_TYPES


@dataclass
class VariantAuditRow:
    product_id: str
    product_title: str
    product_status: str
    product_type: str
    variant_id: str
    variant_title: str
    sku: Optional[str]
    barcode: Optional[str]
    inventory_item_id: Optional[str]
    tracked: Optional[bool]
    inventory_policy: Optional[str]
    available: Optional[int]
    pre_order: bool
    backorder: bool
    expected_policy: str
    issues: List[str] = field(default_factory=list)
    skipped: bool = False
    skip_reason: Optional[str] = None

    @property
    def has_issues(self) -> bool:
        return bool(self.issues)


@dataclass
class ProposedRepair:
    product_id: str
    variant_id: str
    barcode: Optional[str]
    title: str
    enable_tracking: bool = False
    inventory_item_id: Optional[str] = None
    set_policy: Optional[str] = None
    current_policy: Optional[str] = None


@dataclass
class AuditSummary:
    products_checked: int = 0
    variants_checked: int = 0
    variants_correct: int = 0
    variants_with_issues: int = 0
    tracking_disabled: int = 0
    incorrect_continue: int = 0
    incorrect_deny: int = 0
    missing_inventory_or_location: int = 0
    skipped_products: int = 0
    csv_path: Optional[str] = None
    repairs_proposed: int = 0
    repairs_applied: int = 0
    repair_failures: int = 0


def classify_variant_issues(
    *,
    tracked: Optional[bool],
    inventory_policy: Optional[str],
    inventory_item_id: Optional[str],
    has_location_level: bool,
    expected_policy: str,
    sku: Optional[str],
    barcode: Optional[str],
) -> List[str]:
    issues: List[str] = []
    if not inventory_item_id:
        issues.append(ISSUE_MISSING_INVENTORY_ITEM)
    if tracked is False:
        issues.append(ISSUE_TRACKING_DISABLED)
    policy = (inventory_policy or "").upper() or None
    if policy and expected_policy == "CONTINUE" and policy != "CONTINUE":
        issues.append(ISSUE_POLICY_SHOULD_BE_CONTINUE)
    if policy and expected_policy == "DENY" and policy != "DENY":
        issues.append(ISSUE_POLICY_SHOULD_BE_DENY)
    if inventory_item_id and not has_location_level:
        issues.append(ISSUE_MISSING_LOCATION_INVENTORY)
    if not clean_text(sku) or not clean_text(barcode):
        issues.append(ISSUE_MISSING_SKU_OR_BARCODE)
    return issues


def _available_from_level(level: Optional[Dict[str, Any]]) -> Optional[int]:
    if not level:
        return None
    for q in level.get("quantities") or []:
        if clean_text(q.get("name")) == "available":
            try:
                return int(q.get("quantity") or 0)
            except (TypeError, ValueError):
                return None
    return None


def _metafield_bool(node: Dict[str, Any], *keys: str) -> bool:
    for key in keys:
        mf = node.get(key) or {}
        if parse_shopify_bool_metafield(mf.get("value")):
            return True
    # Explicit false / missing for all → false
    return False


def row_from_product_variant(
    product: Dict[str, Any],
    variant: Dict[str, Any],
) -> VariantAuditRow:
    inv_item = variant.get("inventoryItem") or {}
    inv_id = clean_text(inv_item.get("id"))
    level = inv_item.get("inventoryLevel")
    has_level = level is not None
    tracked = inv_item.get("tracked")
    if tracked is not None:
        tracked = bool(tracked)

    pre_order = _metafield_bool(product, "preOrder", "preorderAlt")
    backorder = _metafield_bool(product, "backorder")
    expected = expected_inventory_policy(pre_order=pre_order, backorder=backorder)
    policy = clean_text(variant.get("inventoryPolicy"))
    sku = clean_text(variant.get("sku"))
    barcode = clean_text(variant.get("barcode"))

    issues = classify_variant_issues(
        tracked=tracked,
        inventory_policy=policy,
        inventory_item_id=inv_id,
        has_location_level=has_level,
        expected_policy=expected,
        sku=sku,
        barcode=barcode,
    )

    return VariantAuditRow(
        product_id=str(product.get("id") or ""),
        product_title=clean_text(product.get("title")) or "",
        product_status=clean_text(product.get("status")) or "",
        product_type=clean_text(product.get("productType")) or "",
        variant_id=str(variant.get("id") or ""),
        variant_title=clean_text(variant.get("title")) or "",
        sku=sku,
        barcode=barcode,
        inventory_item_id=inv_id,
        tracked=tracked,
        inventory_policy=(policy.upper() if policy else None),
        available=_available_from_level(level),
        pre_order=pre_order,
        backorder=backorder,
        expected_policy=expected,
        issues=issues,
    )


def proposed_repairs_for_row(row: VariantAuditRow) -> Optional[ProposedRepair]:
    """Only deterministic config fixes — never quantity / metafield / location seeding."""
    if row.skipped:
        return None
    enable_tracking = ISSUE_TRACKING_DISABLED in row.issues and bool(row.inventory_item_id)
    set_policy: Optional[str] = None
    if ISSUE_POLICY_SHOULD_BE_CONTINUE in row.issues:
        set_policy = "CONTINUE"
    elif ISSUE_POLICY_SHOULD_BE_DENY in row.issues:
        set_policy = "DENY"
    if not enable_tracking and not set_policy:
        return None
    return ProposedRepair(
        product_id=row.product_id,
        variant_id=row.variant_id,
        barcode=row.barcode,
        title=row.product_title,
        enable_tracking=enable_tracking,
        inventory_item_id=row.inventory_item_id,
        set_policy=set_policy,
        current_policy=row.inventory_policy,
    )


def iter_catalog_products(
    client: ShopifyClient,
    *,
    location_id: str,
    product_query: str = "status:active OR status:draft",
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


def audit_products(
    products: Iterable[Dict[str, Any]],
    *,
    include_gift_cards: bool = False,
) -> List[VariantAuditRow]:
    rows: List[VariantAuditRow] = []
    for product in products:
        if not include_gift_cards and is_gift_card_product_type(product.get("productType")):
            rows.append(
                VariantAuditRow(
                    product_id=str(product.get("id") or ""),
                    product_title=clean_text(product.get("title")) or "",
                    product_status=clean_text(product.get("status")) or "",
                    product_type=clean_text(product.get("productType")) or "",
                    variant_id="",
                    variant_title="",
                    sku=None,
                    barcode=None,
                    inventory_item_id=None,
                    tracked=None,
                    inventory_policy=None,
                    available=None,
                    pre_order=False,
                    backorder=False,
                    expected_policy="DENY",
                    issues=[],
                    skipped=True,
                    skip_reason="gift_card_product_type",
                )
            )
            continue
        for variant in (product.get("variants") or {}).get("nodes") or []:
            rows.append(row_from_product_variant(product, variant))
    return rows


def summarize_rows(rows: Sequence[VariantAuditRow], *, csv_path: Optional[str] = None) -> AuditSummary:
    summary = AuditSummary(csv_path=csv_path)
    product_ids = {r.product_id for r in rows if r.product_id and not r.skipped}
    skipped_products = {r.product_id for r in rows if r.skipped}
    summary.products_checked = len(product_ids)
    summary.skipped_products = len(skipped_products)
    checked = [r for r in rows if not r.skipped]
    summary.variants_checked = len(checked)
    for r in checked:
        if r.has_issues:
            summary.variants_with_issues += 1
        else:
            summary.variants_correct += 1
        if ISSUE_TRACKING_DISABLED in r.issues:
            summary.tracking_disabled += 1
        if ISSUE_POLICY_SHOULD_BE_CONTINUE in r.issues:
            summary.incorrect_deny += 1  # expected CONTINUE, currently DENY (or other)
        if ISSUE_POLICY_SHOULD_BE_DENY in r.issues:
            summary.incorrect_continue += 1  # expected DENY, currently CONTINUE
        if ISSUE_MISSING_INVENTORY_ITEM in r.issues or ISSUE_MISSING_LOCATION_INVENTORY in r.issues:
            summary.missing_inventory_or_location += 1
    return summary


def write_csv(rows: Sequence[VariantAuditRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "product_id",
        "product_title",
        "product_status",
        "product_type",
        "variant_id",
        "variant_title",
        "sku",
        "barcode",
        "inventory_item_id",
        "tracked",
        "inventory_policy",
        "available",
        "pre_order",
        "backorder",
        "expected_policy",
        "issues",
        "skipped",
        "skip_reason",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(
                {
                    "product_id": r.product_id,
                    "product_title": r.product_title,
                    "product_status": r.product_status,
                    "product_type": r.product_type,
                    "variant_id": r.variant_id,
                    "variant_title": r.variant_title,
                    "sku": r.sku or "",
                    "barcode": r.barcode or "",
                    "inventory_item_id": r.inventory_item_id or "",
                    "tracked": "" if r.tracked is None else str(r.tracked),
                    "inventory_policy": r.inventory_policy or "",
                    "available": "" if r.available is None else r.available,
                    "pre_order": r.pre_order,
                    "backorder": r.backorder,
                    "expected_policy": r.expected_policy,
                    "issues": "|".join(r.issues),
                    "skipped": r.skipped,
                    "skip_reason": r.skip_reason or "",
                }
            )


def print_summary(summary: AuditSummary) -> None:
    print("\n=== Shopify inventory settings audit ===")
    print(f"Products checked:              {summary.products_checked}")
    print(f"Variants checked:              {summary.variants_checked}")
    print(f"Correct variants:              {summary.variants_correct}")
    print(f"Variants with issues:          {summary.variants_with_issues}")
    print(f"Tracking disabled:             {summary.tracking_disabled}")
    print(f"Incorrect CONTINUE (should DENY): {summary.incorrect_continue}")
    print(f"Incorrect DENY (should CONTINUE): {summary.incorrect_deny}")
    print(f"Missing inventory/location:    {summary.missing_inventory_or_location}")
    print(f"Skipped products (gift cards): {summary.skipped_products}")
    if summary.csv_path:
        print(f"CSV report:                    {summary.csv_path}")
    if summary.repairs_proposed:
        print(f"Repairs proposed:              {summary.repairs_proposed}")
    if summary.repairs_applied or summary.repair_failures:
        print(f"Repairs applied:               {summary.repairs_applied}")
        print(f"Repair failures:               {summary.repair_failures}")


def print_issue_rows(rows: Sequence[VariantAuditRow], *, issues_only: bool) -> None:
    """Print variant lines. Default / --issues-only: only rows with issues."""
    for r in rows:
        if r.skipped:
            continue
        if not r.has_issues:
            continue
        print(
            f"- {r.barcode or '(no barcode)'} | {r.product_title[:55]!r} | "
            f"policy={r.inventory_policy} expected={r.expected_policy} "
            f"tracked={r.tracked} available={r.available} "
            f"pre_order={r.pre_order} backorder={r.backorder} "
            f"issues={','.join(r.issues)}"
        )


def apply_repairs(
    client: ShopifyClient,
    repairs: Sequence[ProposedRepair],
    *,
    dry_run: bool,
    log: Callable[[str], None] = print,
) -> tuple[int, int]:
    """
    Apply deterministic repairs. Returns (applied, failed).

    Never mutates quantities, metafields, or product status.
    """
    applied = 0
    failed = 0
    for rep in repairs:
        log(
            f"PROPOSED repair barcode={rep.barcode} product={rep.product_id} "
            f"variant={rep.variant_id} enable_tracking={rep.enable_tracking} "
            f"set_policy={rep.set_policy} (was {rep.current_policy})"
        )
        if dry_run:
            continue
        try:
            if rep.enable_tracking and rep.inventory_item_id:
                data = client.graphql(
                    INVENTORY_ITEM_UPDATE,
                    {"id": rep.inventory_item_id, "input": {"tracked": True}},
                )
                block = data.get("inventoryItemUpdate") or {}
                errs = block.get("userErrors") or []
                if errs:
                    raise RuntimeError(f"inventoryItemUpdate userErrors: {errs}")
                log(f"APPLIED tracking=true inventory_item={rep.inventory_item_id}")

            if rep.set_policy:
                data = client.graphql(
                    VARIANTS_BULK_UPDATE,
                    {
                        "productId": rep.product_id,
                        "variants": [
                            {"id": rep.variant_id, "inventoryPolicy": rep.set_policy}
                        ],
                    },
                )
                block = data.get("productVariantsBulkUpdate") or {}
                errs = block.get("userErrors") or []
                if errs:
                    raise RuntimeError(f"productVariantsBulkUpdate userErrors: {errs}")
                log(f"APPLIED inventoryPolicy={rep.set_policy} variant={rep.variant_id}")

            applied += 1
        except Exception as exc:
            failed += 1
            log(f"FAILED repair barcode={rep.barcode} variant={rep.variant_id}: {exc}")
    return applied, failed


def run_audit(
    *,
    env_file: str = ".env",
    api_version: str = "2026-04",
    csv_path: Optional[Path] = None,
    issues_only: bool = False,
    fix: bool = False,
    dry_run: bool = False,
    confirm_token: Optional[str] = None,
    include_gift_cards: bool = False,
    product_query: str = "status:active OR status:draft",
    client: Optional[ShopifyClient] = None,
) -> tuple[List[VariantAuditRow], AuditSummary]:
    """
    Run catalogue audit. Mutations only when ``fix=True`` and ``dry_run=False``
    and ``confirm_token == "FIX"``.
    """
    path = Path(env_file)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[2] / env_file
    load_dotenv(path)

    location_id = shopify_inventory_location_id()
    if not location_id:
        raise SystemExit("Missing SHOPIFY_INVENTORY_LOCATION_ID")

    shopify = client or ShopifyClient(api_version=api_version)
    products = list(
        iter_catalog_products(shopify, location_id=location_id, product_query=product_query)
    )
    rows = audit_products(products, include_gift_cards=include_gift_cards)

    out_csv = csv_path or (Path(__file__).resolve().parents[2] / "tmp" / "shopify_inventory_audit.csv")
    write_csv(rows, out_csv)

    summary = summarize_rows(rows, csv_path=str(out_csv))
    print("\n=== Issue details ===")
    print_issue_rows(rows, issues_only=True)
    if not issues_only:
        print(f"(Full catalogue including {summary.variants_correct} correct variants is in the CSV.)")

    repairs = [p for r in rows if (p := proposed_repairs_for_row(r))]
    summary.repairs_proposed = len(repairs)

    if fix:
        print("\n=== Proposed repairs (config only; no quantity changes) ===")
        for rep in repairs:
            print(
                f"  {rep.barcode or '(no barcode)'} | {rep.title[:50]!r} | "
                f"tracking→on={rep.enable_tracking} policy {rep.current_policy}→{rep.set_policy}"
            )
        if not repairs:
            print("  (none)")
        elif dry_run:
            print("\n--dry-run: no Shopify mutations will be sent.")
            apply_repairs(shopify, repairs, dry_run=True)
        else:
            if confirm_token != "FIX":
                raise SystemExit(
                    'Repair aborted: confirmation token must be exactly "FIX".'
                )
            applied, failed = apply_repairs(shopify, repairs, dry_run=False)
            summary.repairs_applied = applied
            summary.repair_failures = failed

    print_summary(summary)
    return rows, summary
