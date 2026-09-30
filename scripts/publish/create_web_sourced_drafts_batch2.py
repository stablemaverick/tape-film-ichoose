#!/usr/bin/env python3
"""Create 3 more web-sourced draft products."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from dotenv import load_dotenv

load_dotenv(".env.prod", override=True)

from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client
from app.helpers.text_helpers import slugify
from app.rules.pricing_rules import (
    DEFAULT_GBP_AUD_RATE,
    DEFAULT_LANDED_COST_MARKUP,
    DEFAULT_MARGIN_FLOOR_RATIO,
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    calculate_shopify_cost_aud,
)
from app.services.catalog_shopify_publish_service import (
    build_metafields,
    build_tags,
    initial_variant_inventory_quantities,
    resolve_new_listing_price,
    resolve_unique_product_handle,
    shopify_inventory_location_id,
    variant_inventory_policy_for_row,
)

PRODUCTS = [
    {
        "barcode": "5061088923761",
        "title": "Shallow Grave Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 28.95,
        "director": "Danny Boyle",
        "studio": "StudioCanal",
        "format": "4K Ultra HD",
        "genres": "Crime, Thriller",
        "top_cast": "Ewan McGregor, Christopher Eccleston, Kerry Fox, Keith Allen, Ken Stott, Peter Mullan",
        "film_released": "1994-12-22",
        "media_release_date": None,
        "country_of_origin": "United Kingdom",
        "description": (
            "The acclaimed debut film from visionary director Danny Boyle (Trainspotting, Slumdog Millionaire, "
            "28 Days Later) that helped redefine British cinema in the 1990s. Darkly comic and relentlessly "
            "suspenseful, the film follows three young professionals — Alex (Ewan McGregor), David "
            "(Christopher Eccleston) and Juliet (Kerry Fox) — whose seemingly perfect friendship is thrown "
            "into chaos when they discover a suitcase full of cash. As greed, suspicion and paranoia take "
            "hold, their close-knit circle begins to unravel with deadly consequences."
        ),
    },
    {
        "barcode": "5061088923778",
        "title": "Shallow Grave Limited Collector's Edition 4K Ultra HD",
        "cost_price": 45.29,
        "director": "Danny Boyle",
        "studio": "StudioCanal",
        "format": "4K Ultra HD",
        "genres": "Crime, Thriller",
        "top_cast": "Ewan McGregor, Christopher Eccleston, Kerry Fox, Keith Allen, Ken Stott, Peter Mullan",
        "film_released": "1994-12-22",
        "media_release_date": None,
        "country_of_origin": "United Kingdom",
        "description": (
            "The acclaimed debut film from visionary director Danny Boyle (Trainspotting, Slumdog Millionaire, "
            "28 Days Later) that helped redefine British cinema in the 1990s. Darkly comic and relentlessly "
            "suspenseful, the film follows three young professionals — Alex (Ewan McGregor), David "
            "(Christopher Eccleston) and Juliet (Kerry Fox) — whose seemingly perfect friendship is thrown "
            "into chaos when they discover a suitcase full of cash. As greed, suspicion and paranoia take "
            "hold, their close-knit circle begins to unravel with deadly consequences."
        ),
    },
    {
        "barcode": "5055201855374",
        "title": "Blood Simple: Director's Cut Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 29.09,
        "director": "Joel Coen",
        "studio": "StudioCanal",
        "format": "4K Ultra HD",
        "genres": "Crime, Thriller",
        "top_cast": "Frances McDormand, John Getz, Dan Hedaya, M. Emmet Walsh, Samm-Art Williams",
        "film_released": "1984-09-07",
        "media_release_date": "2026-10-05",
        "country_of_origin": "United States",
        "description": (
            "A stylish, imaginative and hard-boiled neo-noir, Blood Simple announced Joel and Ethan Coen as "
            "vital and distinctive new cinematic voices on its release. M. Emmet Walsh is sleazy Texas private "
            "eye Visser, hired by bar owner Marty (Dan Hedaya) to kill his unfaithful wife (Frances McDormand) "
            "and her lover (John Getz). Given a plan to work from, he decides to modify it without warning; "
            "and matters quickly spiral out of control."
        ),
    },
]

MUTATION = """
mutation ProductSet($synchronous: Boolean!, $input: ProductSetInput!) {
  productSet(synchronous: $synchronous, input: $input) {
    product {
      id title handle status
      variants(first: 1) { nodes { id sku barcode price } }
    }
    userErrors { field message }
  }
}
"""


def _html(text: str) -> str:
    return "<p>" + text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") + "</p>"


def main() -> int:
    shopify = ShopifyClient(api_version="2026-04")
    sb = create_fresh_client(".env.prod")
    gbp_aud = float(os.getenv("GBP_AUD_RATE", str(DEFAULT_GBP_AUD_RATE)))
    landed_markup = float(os.getenv("LANDED_COST_MARKUP", str(DEFAULT_LANDED_COST_MARKUP)))
    margin_floor = float(os.getenv("DEFAULT_MARGIN_FLOOR_RATIO", str(DEFAULT_MARGIN_FLOOR_RATIO)))
    location_id = shopify_inventory_location_id()
    sample = sb.table("catalog_items").select("*").limit(1).execute().data[0]
    ok = failed = skipped = 0

    for p in PRODUCTS:
        barcode = p["barcode"]
        existing = shopify.variant_exists_by_barcode(barcode)
        if existing:
            print("SKIP exists", barcode, existing.get("id"))
            skipped += 1
            continue

        floor = calculate_sale_price_with_margin_floor_from_gbp_cost(p["cost_price"])
        row = {
            "id": None,
            "title": p["title"],
            "barcode": barcode,
            "sku": barcode,
            "supplier": "moovies",
            "supplier_sku": barcode,
            "cost_price": p["cost_price"],
            "calculated_sale_price": floor,
            "director": p["director"],
            "studio": p["studio"],
            "format": p["format"],
            "genres": p["genres"],
            "top_cast": p["top_cast"],
            "film_released": p["film_released"],
            "media_release_date": p.get("media_release_date"),
            "country_of_origin": p["country_of_origin"],
            "availability_status": "supplier_stock",
            "supplier_stock_status": 0,
        }
        price = resolve_new_listing_price(
            row=row,
            gbp_aud_rate=gbp_aud,
            landed_cost_markup=landed_markup,
            margin_floor_ratio=margin_floor,
        )
        cost_val = calculate_shopify_cost_aud(
            p["cost_price"], gbp_aud_rate=gbp_aud, landed_cost_markup=landed_markup
        )
        handle, _ = resolve_unique_product_handle(shopify, slugify(p["title"]), barcode)
        tags = build_tags(row) + ["web-sourced"]
        metafields = build_metafields(row)
        if not any(m.get("key") == "region" for m in metafields):
            metafields.append(
                {
                    "namespace": "custom",
                    "key": "region",
                    "type": "single_line_text_field",
                    "value": "Region B",
                }
            )
        inv_policy = variant_inventory_policy_for_row(row)
        variant_input = {
            "sku": barcode,
            "barcode": barcode,
            "price": f"{price:.2f}",
            "inventoryPolicy": inv_policy,
            "optionValues": [{"optionName": "Title", "name": "Default Title"}],
            "inventoryItem": {
                "tracked": True,
                "cost": f"{cost_val:.2f}",
                "measurement": {"weight": {"value": 0.25, "unit": "KILOGRAMS"}},
            },
        }
        if location_id:
            variant_input["inventoryQuantities"] = initial_variant_inventory_quantities(
                location_id
            )
        product_input = {
            "title": p["title"],
            "handle": handle,
            "descriptionHtml": _html(p["description"]),
            "vendor": "TAPE! FILM",
            "category": "gid://shopify/TaxonomyCategory/me-7-1",
            "status": "DRAFT",
            "tags": tags,
            "seo": {"title": p["title"], "description": p["description"][:320]},
            "metafields": metafields,
            "productOptions": [
                {"name": "Title", "position": 1, "values": [{"name": "Default Title"}]},
            ],
            "variants": [variant_input],
        }
        try:
            data = shopify.graphql(MUTATION, {"synchronous": True, "input": product_input})
            payload = data.get("productSet") or {}
            errs = payload.get("userErrors") or []
            if errs:
                raise RuntimeError(str(errs))
            product = payload.get("product") or {}
            variant = ((product.get("variants") or {}).get("nodes") or [{}])[0]
            product_id = product.get("id")
            variant_id = variant.get("id")
            cat = {
                "supplier": "moovies",
                "barcode": barcode,
                "title": p["title"],
                "supplier_sku": barcode,
                "cost_price": p["cost_price"],
                "calculated_sale_price": price,
                "director": p["director"],
                "studio": p["studio"],
                "format": p["format"],
                "genres": p["genres"],
                "top_cast": p["top_cast"],
                "film_released": p["film_released"],
                "media_release_date": p.get("media_release_date"),
                "country_of_origin": p["country_of_origin"],
                "availability_status": "supplier_stock",
                "supplier_stock_status": 0,
                "active": True,
                "source_type": "catalog",
                "media_type": "film",
                "shopify_product_id": product_id,
                "shopify_variant_id": variant_id,
                "notes": "web-sourced draft; supplier cost from operator",
            }
            cat = {k: v for k, v in cat.items() if k in sample}
            sb.table("catalog_items").upsert(cat, on_conflict="supplier,barcode").execute()
            print(
                "CREATED DRAFT",
                barcode,
                "price=" + f"{price:.2f}",
                "product=" + str(product_id),
                "|",
                p["title"],
            )
            ok += 1
            time.sleep(0.15)
        except Exception as exc:
            print("FAIL", barcode, exc)
            failed += 1
            time.sleep(0.3)

    print("Done created=%s skipped=%s failed=%s" % (ok, skipped, failed))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
