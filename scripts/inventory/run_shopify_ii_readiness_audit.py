#!/usr/bin/env python3
"""
Full Shopify II mapping readiness audit (read-only).

Does not enable flags, mutate Shopify, or write II tables.
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter, defaultdict
from typing import Any

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client

from app.services.shopify_release_mapping import (
    classify_shopify_listing_mapping,
    summarize_mapping_results,
)


def _norm_barcode(raw: str | None) -> str:
    s = (raw or "").strip()
    s = s.replace(" ", "").replace("-", "")
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    # strip leading zeros only for comparison helper, keep original separately
    return s


def classify_missing_barcode_row(row: dict[str, Any]) -> str:
    title = f"{row.get('product_title') or ''} {row.get('variant_title') or ''}".lower()
    sku = (row.get("sku") or "").lower()
    vendor = (row.get("vendor") or "").lower()
    ptype = (row.get("product_type") or "").lower()
    blob = f"{title} {sku} {vendor} {ptype}"

    if "gift card" in blob or "giftcard" in blob:
        return "gift_card"
    if any(
        k in blob
        for k in (
            "t-shirt",
            "tshirt",
            "hoodie",
            "poster",
            "tote",
            "mug",
            "apparel",
            "merch",
            "sticker",
            "enamel pin",
        )
    ):
        return "merchandise"
    if any(k in blob for k in ("test product", "test variant", "do not buy", "sample")):
        return "test_product"
    if any(k in blob for k in ("shipping", "postage", "protection plan", "warranty")):
        return "non_film_service"
    if row.get("product_status") and str(row.get("product_status")).upper() == "DRAFT":
        return "draft_product"
    if row.get("product_status") and str(row.get("product_status")).upper() == "ARCHIVED":
        return "archived_product"
    # Physical media heuristics
    if any(
        k in blob
        for k in (
            "blu-ray",
            "bluray",
            "4k",
            "uhd",
            "dvd",
            "steelbook",
            "limited edition",
            "collector",
        )
    ):
        return "physical_media_missing_barcode"
    if (row.get("match_status") or "") == "matched":
        return "matched_catalog_but_no_barcode"
    return "other_or_unclear"


def diagnose_unmapped_barcode(
    sb: Any, barcode: str
) -> tuple[str, dict[str, Any]]:
    """Why barcode-bearing listing failed mapping."""
    bc = (barcode or "").strip()
    details: dict[str, Any] = {"barcode": bc}

    # Exact primary_barcode
    prim = (
        sb.table("release_variants")
        .select("id")
        .eq("primary_barcode", bc)
        .eq("active", True)
        .limit(5)
        .execute()
        .data
        or []
    )
    details["primary_barcode_hits"] = len(prim)

    # Identifiers
    ident = (
        sb.table("variant_identifiers")
        .select("release_variant_id,id_type,id_value")
        .in_("id_type", ["barcode", "ean", "upc"])
        .eq("id_value", bc)
        .limit(10)
        .execute()
        .data
        or []
    )
    details["variant_identifier_hits"] = len(ident)

    # Catalog
    cat = (
        sb.table("catalog_items")
        .select("id,title,supplier,active")
        .eq("barcode", bc)
        .limit(5)
        .execute()
        .data
        or []
    )
    details["catalog_barcode_hits"] = len(cat)

    # Normalisation variants
    alts = set()
    digits = re.sub(r"\D", "", bc)
    if digits:
        alts.add(digits)
        if len(digits) == 12:
            alts.add("0" + digits)  # UPC-A → EAN-13
        if len(digits) == 13 and digits.startswith("0"):
            alts.add(digits[1:])
        alts.add(digits.lstrip("0") or "0")
    alt_hits = []
    for a in sorted(alts - {bc}):
        r = (
            sb.table("release_variants")
            .select("id")
            .eq("primary_barcode", a)
            .eq("active", True)
            .limit(2)
            .execute()
            .data
            or []
        )
        i = (
            sb.table("variant_identifiers")
            .select("release_variant_id")
            .eq("id_value", a)
            .limit(2)
            .execute()
            .data
            or []
        )
        if r or i:
            alt_hits.append({"alt": a, "primary": len(r), "identifiers": len(i)})
    details["alt_format_hits"] = alt_hits

    if len(prim) > 1 or len({x["release_variant_id"] for x in ident}) > 1:
        return "would_be_ambiguous_if_matched", details
    if alt_hits:
        return "barcode_formatting_mismatch", details
    if cat and not prim and not ident:
        return "catalog_has_barcode_but_no_release_variant", details
    if not cat and not prim and not ident:
        return "barcode_absent_from_ii_and_catalog", details
    return "other", details


def page_all_listings(sb: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = (
            sb.table("shopify_listings")
            .select(
                "shop,shopify_product_id,shopify_variant_id,product_title,variant_title,"
                "barcode,sku,catalog_item_id,product_status,product_type,vendor,"
                "match_status,match_method,inventory_quantity,tracks_inventory,"
                "shopify_inventory_item_id,price_amount,inventory_policy"
            )
            .range(offset, offset + 999)
            .execute()
            .data
            or []
        )
        if not page:
            break
        out.extend(page)
        if len(page) < 1000:
            break
        offset += 1000
    return out


def main() -> int:
    load_dotenv(".env", override=True)
    load_dotenv(".env.prod", override=True)
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    listings = page_all_listings(sb)
    products = {r.get("shopify_product_id") for r in listings if r.get("shopify_product_id")}

    mapping_results = []
    missing_rows = []
    unmapped_bc_rows = []
    ambiguous_rows = []

    for r in listings:
        m = classify_shopify_listing_mapping(
            sb,
            shop=r.get("shop") or "",
            shopify_variant_id=r.get("shopify_variant_id") or "",
            barcode=r.get("barcode"),
            catalog_item_id=r.get("catalog_item_id"),
        )
        mapping_results.append(m)
        if m.status == "missing_barcode":
            missing_rows.append(r)
        elif m.status == "unmapped":
            unmapped_bc_rows.append((r, m))
        elif m.status == "ambiguous_barcode":
            ambiguous_rows.append((r, m))

    summary = summarize_mapping_results(mapping_results)
    with_bc = [m for m in mapping_results if m.barcode]
    mapped_with_bc = [
        m
        for m in with_bc
        if m.status
        in {
            "mapped_existing_listing",
            "mapped_catalog_item",
            "mapped_primary_barcode",
            "mapped_variant_identifier",
        }
    ]
    summary["total_shopify_products"] = len(products)
    summary["overall_mapping_pct"] = round(
        100.0 * summary["mapped"] / max(summary["total_shopify_variants_inspected"], 1), 2
    )
    summary["barcode_bearing_variants"] = len(with_bc)
    summary["barcode_bearing_mapped"] = len(mapped_with_bc)
    summary["barcode_bearing_mapping_pct"] = round(
        100.0 * len(mapped_with_bc) / max(len(with_bc), 1), 2
    )

    # Missing barcode classification
    miss_cat = Counter(classify_missing_barcode_row(r) for r in missing_rows)
    miss_examples = defaultdict(list)
    for r in missing_rows:
        cat = classify_missing_barcode_row(r)
        if len(miss_examples[cat]) < 3:
            miss_examples[cat].append(
                {
                    "product_title": r.get("product_title"),
                    "variant_title": r.get("variant_title"),
                    "shopify_variant_id": r.get("shopify_variant_id"),
                    "sku": r.get("sku"),
                    "match_status": r.get("match_status"),
                    "inventory_quantity": r.get("inventory_quantity"),
                }
            )

    # Unmapped barcode diagnosis
    unmapped_causes = Counter()
    unmapped_examples = []
    for r, m in unmapped_bc_rows:
        cause, details = diagnose_unmapped_barcode(sb, m.barcode or "")
        unmapped_causes[cause] += 1
        if len(unmapped_examples) < 25:
            rem = "create_release_on_first_dual_write"
            if cause == "barcode_formatting_mismatch":
                rem = "consider safe barcode normalisation before create"
            elif cause == "catalog_has_barcode_but_no_release_variant":
                rem = "link/create release_variant from catalog barcode (deterministic)"
            unmapped_examples.append(
                {
                    "product_title": r.get("product_title"),
                    "variant_title": r.get("variant_title"),
                    "shopify_variant_id": r.get("shopify_variant_id"),
                    "barcode": m.barcode,
                    "catalog_item_id": r.get("catalog_item_id"),
                    "match_status": r.get("match_status"),
                    "cause": cause,
                    "details": details,
                    "recommended_remediation": rem,
                }
            )

    # Baselines
    baselines = {}
    for t in (
        "release_shopify_listings",
        "tape_inventory_levels",
        "release_variants",
        "variant_identifiers",
        "supplier_offers",
        "shopify_listings",
    ):
        baselines[t] = sb.table(t).select("id", count="exact").limit(0).execute().count

    # First-run impact estimates
    # Dual-write creates listing for every non-ambiguous row; creates tape level when location set
    # and tracks inventory / has inventory item
    skip_ambiguous = summary.get("ambiguous", 0)
    expected_channels = summary["total_shopify_variants_inspected"] - skip_ambiguous
    trackable = sum(
        1
        for r in listings
        if r.get("shopify_inventory_item_id") or r.get("tracks_inventory") is not False
    )

    report = {
        "scope_note": (
            "shopify_listings is populated by store sync with query status:active only. "
            "Draft/archived Shopify products are not in this table."
        ),
        "catalogue_stats": summary,
        "product_status_in_listings": dict(
            Counter((r.get("product_status") or "") for r in listings)
        ),
        "missing_barcode": {
            "total": len(missing_rows),
            "by_category": dict(miss_cat),
            "should_map_estimate": miss_cat.get("physical_media_missing_barcode", 0)
            + miss_cat.get("matched_catalog_but_no_barcode", 0),
            "examples": {k: v for k, v in miss_examples.items()},
        },
        "unmapped_with_barcode": {
            "total": len(unmapped_bc_rows),
            "by_cause": dict(unmapped_causes),
            "examples": unmapped_examples,
        },
        "ambiguous": {
            "total": len(ambiguous_rows),
            "examples": [
                {
                    "product_title": r.get("product_title"),
                    "barcode": m.barcode,
                    "candidates": list(m.candidate_release_ids),
                    "reason": m.reason,
                }
                for r, m in ambiguous_rows[:20]
            ],
        },
        "baselines": baselines,
        "first_run_impact_estimate": {
            "expected_release_shopify_listings_upserts": expected_channels,
            "expected_tape_inventory_levels_approx": trackable,
            "expected_skipped_ambiguous": skip_ambiguous,
            "expected_new_release_variants_approx": summary.get("unmapped", 0)
            + summary.get("missing_barcode", 0),
            "note": (
                "Unmapped + missing-barcode variants create NEW release_variants on first "
                "dual-write (curated Shopify inbound). Ambiguous are skipped. Tape levels "
                "require SHOPIFY_INVENTORY_LOCATION_ID and tracked inventory item."
            ),
        },
        "location_env": os.getenv("SHOPIFY_INVENTORY_LOCATION_ID"),
    }

    out_path = os.path.join(
        ROOT, "docs/inventory-intelligence/shopify-ii-full-mapping-audit.json"
    )
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str)[:12000])
    print(f"\n... full JSON written to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
