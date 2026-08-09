"""
Deterministic Shopify listing → release_variant mapping (read/classify + shared resolve).

Does NOT create Shopify products. Dual-write remains a projection FROM Shopify INTO II.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping, Optional, Sequence

from app.helpers.text_helpers import clean_text

MappingStatus = Literal[
    "mapped_existing_listing",
    "mapped_catalog_item",
    "mapped_primary_barcode",
    "mapped_variant_identifier",
    "ambiguous_barcode",
    "missing_barcode",
    "unmapped",
]


@dataclass(frozen=True)
class ShopifyMappingResult:
    status: MappingStatus
    shopify_variant_id: str
    release_variant_id: Optional[str] = None
    barcode: Optional[str] = None
    catalog_item_id: Optional[str] = None
    candidate_release_ids: tuple[str, ...] = ()
    reason: str = ""


def classify_shopify_listing_mapping(
    supabase: Any,
    *,
    shop: str,
    shopify_variant_id: str,
    barcode: Optional[str] = None,
    catalog_item_id: Optional[str] = None,
) -> ShopifyMappingResult:
    """
    Classify how a Shopify variant would map to a release_variant.

    Order: existing listing → catalog_item_id → primary_barcode (unique) →
    variant_identifiers barcode (unique). Never fuzzy title match.
    """
    vid = clean_text(shopify_variant_id) or ""
    if not vid:
        return ShopifyMappingResult(
            status="unmapped",
            shopify_variant_id="",
            reason="missing_shopify_variant_id",
        )

    bc = clean_text(barcode)
    cid = clean_text(catalog_item_id)

    existing = (
        supabase.table("release_shopify_listings")
        .select("release_variant_id")
        .eq("shop", shop)
        .eq("shopify_variant_id", vid)
        .limit(1)
        .execute()
    )
    if existing.data:
        return ShopifyMappingResult(
            status="mapped_existing_listing",
            shopify_variant_id=vid,
            release_variant_id=str(existing.data[0]["release_variant_id"]),
            barcode=bc,
            catalog_item_id=cid,
        )

    if cid:
        by_cat = (
            supabase.table("release_variants")
            .select("id")
            .eq("catalog_item_id", cid)
            .eq("active", True)
            .limit(2)
            .execute()
        )
        rows = by_cat.data or []
        if len(rows) == 1:
            return ShopifyMappingResult(
                status="mapped_catalog_item",
                shopify_variant_id=vid,
                release_variant_id=str(rows[0]["id"]),
                barcode=bc,
                catalog_item_id=cid,
            )
        if len(rows) > 1:
            ids = tuple(str(r["id"]) for r in rows)
            return ShopifyMappingResult(
                status="ambiguous_barcode",
                shopify_variant_id=vid,
                barcode=bc,
                catalog_item_id=cid,
                candidate_release_ids=ids,
                reason="multiple_releases_for_catalog_item_id",
            )

    if not bc:
        return ShopifyMappingResult(
            status="missing_barcode",
            shopify_variant_id=vid,
            catalog_item_id=cid,
            reason="no_barcode_and_no_existing_mapping",
        )

    by_bc = (
        supabase.table("release_variants")
        .select("id")
        .eq("primary_barcode", bc)
        .eq("active", True)
        .limit(3)
        .execute()
    )
    primary_ids = [str(r["id"]) for r in (by_bc.data or [])]

    id_rows = (
        supabase.table("variant_identifiers")
        .select("release_variant_id")
        .in_("id_type", ["barcode", "ean", "upc"])
        .eq("id_value", bc)
        .eq("is_valid", True)
        .limit(10)
        .execute()
    )
    ident_ids = [str(r["release_variant_id"]) for r in (id_rows.data or []) if r.get("release_variant_id")]

    uniq = sorted(set(primary_ids) | set(ident_ids))
    if len(uniq) == 1:
        status: MappingStatus = (
            "mapped_primary_barcode" if primary_ids == uniq or (primary_ids and uniq[0] in primary_ids)
            else "mapped_variant_identifier"
        )
        if uniq[0] in primary_ids and uniq[0] in ident_ids:
            status = "mapped_primary_barcode"
        elif uniq[0] in ident_ids and uniq[0] not in primary_ids:
            status = "mapped_variant_identifier"
        else:
            status = "mapped_primary_barcode"
        return ShopifyMappingResult(
            status=status,
            shopify_variant_id=vid,
            release_variant_id=uniq[0],
            barcode=bc,
            catalog_item_id=cid,
        )
    if len(uniq) > 1:
        return ShopifyMappingResult(
            status="ambiguous_barcode",
            shopify_variant_id=vid,
            barcode=bc,
            catalog_item_id=cid,
            candidate_release_ids=tuple(uniq),
            reason="barcode_maps_to_multiple_releases",
        )

    return ShopifyMappingResult(
        status="unmapped",
        shopify_variant_id=vid,
        barcode=bc,
        catalog_item_id=cid,
        reason="no_release_for_barcode",
    )


def summarize_mapping_results(results: Sequence[ShopifyMappingResult]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    mapped = sum(
        counts.get(k, 0)
        for k in (
            "mapped_existing_listing",
            "mapped_catalog_item",
            "mapped_primary_barcode",
            "mapped_variant_identifier",
        )
    )
    return {
        "total_shopify_variants_inspected": len(results),
        "mapped": mapped,
        "unmapped": counts.get("unmapped", 0),
        "ambiguous": counts.get("ambiguous_barcode", 0),
        "missing_barcode": counts.get("missing_barcode", 0),
        "by_status": counts,
    }


# Guard: this module must never define Shopify product creation.
SHOPIFY_II_CREATES_SHOPIFY_PRODUCTS = False


def assert_shopify_ii_is_inbound_only() -> None:
    """Regression: Shopify II projects INTO intelligence, never publishes OUT."""
    assert SHOPIFY_II_CREATES_SHOPIFY_PRODUCTS is False
