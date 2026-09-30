#!/usr/bin/env python3
"""One-off: create draft Shopify products from web metadata + user GBP costs."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from dotenv import load_dotenv

load_dotenv(".env.prod", override=True)

from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client
from app.helpers.text_helpers import clean_text, slugify
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

# Source: Rarewaves / iMusic UK listings (Sep 2026). Costs from operator.
PRODUCTS = [
    {
        "barcode": "5061088923709",
        "title": "Van Helsing Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 23.52,
        "director": "Stephen Sommers",
        "studio": "Universal Pictures",
        "format": "4K Ultra HD",
        "genres": "Action & Adventure",
        "top_cast": "Hugh Jackman, Kate Beckinsale, Richard Roxburgh, David Wenham, Shuler Hensley, Will Kemp",
        "film_released": "2004-05-07",
        "media_release_date": "2026-10-05",
        "country_of_origin": "United States",
        "description": (
            "ADVENTURE LIVES FOREVER. The director of The Mummy and The Mummy Returns brings three of "
            "Universal's classic monsters back to life like never before in the action-packed Van Helsing! "
            "Legendary monster hunter Van Helsing must rely on the help of the beautiful and mysterious "
            "Anna Valerious as he engages in an epic battle with the ultimate forces of darkness — "
            "Dracula, the Wolf Man and Frankenstein's Monster! Get ready for non-stop action and mythic "
            "adventure in this pulse-pounding thrill ride!"
        ),
    },
    {
        "barcode": "5061088924010",
        "title": "The Terminator Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 23.52,
        "director": "James Cameron",
        "studio": "MGM",
        "format": "4K Ultra HD",
        "genres": "Science Fiction, Action",
        "top_cast": "Arnold Schwarzenegger, Linda Hamilton, Michael Biehn, Paul Winfield, Lance Henriksen, Bill Paxton",
        "film_released": "1984-10-26",
        "media_release_date": None,  # Rarewaves: Coming Soon
        "country_of_origin": "United States",
        "description": (
            "Arnold Schwarzenegger stars as the most fierce and relentless killing machine ever to threaten "
            "the survival of mankind. An indestructible cyborg — a Terminator — is sent back in time to kill "
            "Sarah Connor, the woman whose unborn son will become humanity's only hope in a future war against "
            "machines. This legendary sci-fi thriller from Academy Award-winning director James Cameron fires "
            "an arsenal of action and heart-stopping suspense that never lets up."
        ),
    },
    {
        "barcode": "5055761915372",
        "title": "Saw Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 22.88,
        "director": "James Wan",
        "studio": "Lionsgate",
        "format": "4K Ultra HD",
        "genres": "Horror, Thriller",
        "top_cast": "Cary Elwes, Leigh Whannell, Danny Glover, Tobin Bell, Dina Meyer, Ken Leung",
        "film_released": "2004-10-29",
        "media_release_date": "2026-11-09",
        "country_of_origin": "United States",
        "description": (
            "Would you die to live? That's what two men, Adam (Leigh Whannell) and Gordon (Cary Elwes), "
            "have to ask themselves when they're paired up in a deadly situation. Abducted by a serial killer, "
            "they're trapped in a prison constructed with such ingenuity that they may not be able to escape "
            "before their captor decides it's time to dismantle their bodies in his signature way. Attempting "
            "to break free may kill them, but staying definitely will."
        ),
    },
    {
        "barcode": "5055761917451",
        "title": "Saw II Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 22.88,
        "director": "Darren Lynn Bousman",
        "studio": "Lionsgate",
        "format": "4K Ultra HD",
        "genres": "Horror, Thriller",
        "top_cast": "Donnie Wahlberg, Tobin Bell, Shawnee Smith, Beverley Mitchell, Dina Meyer, Emmanuelle Vaugier",
        "film_released": "2005-10-28",
        "media_release_date": "2026-11-09",
        "country_of_origin": "United States",
        "description": (
            "Jigsaw is back. The brilliant, disturbed mastermind returns for another round of horrifying "
            "life-or-death games. When a new murder victim is discovered with all the signs of Jigsaw's hand, "
            "Detective Eric Matthews begins a full investigation and apprehends Jigsaw with little effort. "
            "But for Jigsaw, getting caught is just another part of his plan. Eight more of his victims are "
            "already fighting for their lives and now it's time for Matthews to join the game."
        ),
    },
    {
        "barcode": "5055761917468",
        "title": "Saw III Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 22.88,
        "director": "Darren Lynn Bousman",
        "studio": "Lionsgate",
        "format": "4K Ultra HD",
        "genres": "Horror, Thriller",
        "top_cast": "Tobin Bell, Shawnee Smith, Angus Macfadyen, Bahar Soomekh, Donnie Wahlberg, Dina Meyer",
        "film_released": "2006-10-27",
        "media_release_date": "2026-11-09",
        "country_of_origin": "United States",
        "description": (
            "Jigsaw has disappeared. Along with his new apprentice Amanda (Shawnee Smith), the puppet master "
            "behind the cruel, intricate games that have terrified a community and baffled police has once "
            "again eluded capture and vanished. While city detectives scramble to locate him, Doctor Lynn "
            "Denlon (Bahar Soomekh) and Jeff Reinhart (Angus Macfadyen) are unaware that they are about to "
            "become the latest pawns on his vicious chessboard."
        ),
    },
    {
        "barcode": "5055761917475",
        "title": "Saw IV Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 22.88,
        "director": "Darren Lynn Bousman",
        "studio": "Lionsgate",
        "format": "4K Ultra HD",
        "genres": "Horror, Thriller",
        "top_cast": "Tobin Bell, Costas Mandylor, Scott Patterson, Betsy Russell, Lyriq Bent, Shawnee Smith",
        "film_released": "2007-10-26",
        "media_release_date": "2026-11-09",
        "country_of_origin": "United States",
        "description": (
            "When SWAT Commander Rigg is abducted and thrust into a game, the last officer untouched by "
            "Jigsaw has but ninety minutes to overcome a series of demented traps and save an old friend — "
            "or face the deadly consequences."
        ),
    },
    {
        "barcode": "5061088924270",
        "title": "Rocky 50th Anniversary Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 23.52,
        "director": "John G. Avildsen",
        "studio": "MGM",
        "format": "4K Ultra HD",
        "genres": "Drama, Sport",
        "top_cast": "Sylvester Stallone, Talia Shire, Burt Young, Carl Weathers, Burgess Meredith",
        "film_released": "1976-11-21",
        "media_release_date": None,  # Rarewaves: Coming Soon
        "country_of_origin": "United States",
        "description": (
            "Nominated for 10 Academy Awards, this 1976 Best Picture winner inspired a nation. A struggling "
            "Philadelphia club fighter (Sylvester Stallone) gets a once-in-a-lifetime opportunity to fight "
            "for love, glory and self-respect. Featuring a legendary musical score and thrilling fight "
            "sequences, this rousing crowd-pleaser scores a knockout!"
        ),
    },
    {
        "barcode": "5056719202421",
        "title": "Dead Poets Society 4K Ultra HD + Blu-Ray",
        "cost_price": 18.90,
        "director": "Peter Weir",
        "studio": "Walt Disney Studios",
        "format": "4K Ultra HD",
        "genres": "Drama",
        "top_cast": "Robin Williams, Robert Sean Leonard, Ethan Hawke, Josh Charles, Gale Hansen, Dylan Kussman",
        "film_released": "1989-06-02",
        "media_release_date": "2026-11-02",
        "country_of_origin": "United States",
        "description": (
            "For generations, Welton Academy students have been groomed to lives of conformity and tradition "
            "— until new professor John Keating (Robin Williams) inspires them to think for themselves, live "
            "life to the fullest and 'Carpe Diem'. This unconventional approach awakens the spirits of the "
            "students, but draws the wrath of a disapproving faculty when an unexpected tragedy strikes the "
            "school. With unforgettable characters and beautiful cinematography, Dead Poets Society will "
            "captivate and inspire you time and time again."
        ),
    },
    {
        "barcode": "5056719202438",
        "title": "Dead Poets Society Limited Edition 4K Ultra HD Steelbook",
        "cost_price": 29.42,
        "director": "Peter Weir",
        "studio": "Walt Disney Studios",
        "format": "4K Ultra HD",
        "genres": "Drama",
        "top_cast": "Robin Williams, Robert Sean Leonard, Ethan Hawke, Josh Charles, Gale Hansen, Dylan Kussman",
        "film_released": "1989-06-02",
        "media_release_date": "2026-11-02",
        "country_of_origin": "United States",
        "description": (
            "For generations, Welton Academy students have been groomed to lives of conformity and tradition "
            "— until new professor John Keating (Robin Williams) inspires them to think for themselves, live "
            "life to the fullest and 'Carpe Diem'. This unconventional approach awakens the spirits of the "
            "students, but draws the wrath of a disapproving faculty when an unexpected tragedy strikes the "
            "school. With unforgettable characters and beautiful cinematography, Dead Poets Society will "
            "captivate and inspire you time and time again."
        ),
    },
]


MUTATION = """
mutation ProductSet($synchronous: Boolean!, $input: ProductSetInput!) {
  productSet(synchronous: $synchronous, input: $input) {
    product {
      id
      title
      handle
      status
      descriptionHtml
      variants(first: 1) {
        nodes { id sku barcode price }
      }
    }
    userErrors { field message }
  }
}
"""


def _html_description(text: str) -> str:
    escaped = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return f"<p>{escaped}</p>"


def _row_for_publish(p: dict) -> dict:
    floor = calculate_sale_price_with_margin_floor_from_gbp_cost(p["cost_price"])
    return {
        "id": None,  # no catalog_item_id yet
        "title": p["title"],
        "barcode": p["barcode"],
        "sku": p["barcode"],
        "supplier": "moovies",  # UK Region B source family for derive_region
        "supplier_sku": p["barcode"],
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
        "notes": "web-sourced draft create Sep 2026",
    }


def main() -> int:
    shopify = ShopifyClient(api_version="2026-04")
    sb = create_fresh_client(".env.prod")
    gbp_aud = float(os.getenv("GBP_AUD_RATE", str(DEFAULT_GBP_AUD_RATE)))
    landed_markup = float(os.getenv("LANDED_COST_MARKUP", str(DEFAULT_LANDED_COST_MARKUP)))
    margin_floor = float(os.getenv("DEFAULT_MARGIN_FLOOR_RATIO", str(DEFAULT_MARGIN_FLOOR_RATIO)))
    location_id = shopify_inventory_location_id()

    ok = failed = skipped = 0
    for p in PRODUCTS:
        barcode = p["barcode"]
        existing = shopify.variant_exists_by_barcode(barcode)
        if existing:
            print(f"SKIP exists {barcode} {existing.get('id')}")
            skipped += 1
            continue

        row = _row_for_publish(p)
        title = row["title"]
        price = resolve_new_listing_price(
            row=row,
            gbp_aud_rate=gbp_aud,
            landed_cost_markup=landed_markup,
            margin_floor_ratio=margin_floor,
        )
        cost_val = calculate_shopify_cost_aud(
            row["cost_price"], gbp_aud_rate=gbp_aud, landed_cost_markup=landed_markup
        )
        handle, _ = resolve_unique_product_handle(shopify, slugify(title), barcode)
        tags = build_tags(row)
        tags.append("web-sourced")
        metafields = build_metafields(row)
        # ensure Region B even if supplier mapping changes
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
            "title": title,
            "handle": handle,
            "descriptionHtml": _html_description(p["description"]),
            "vendor": "TAPE! FILM",
            "category": "gid://shopify/TaxonomyCategory/me-7-1",
            "status": "DRAFT",
            "tags": tags,
            "seo": {"title": title, "description": p["description"][:320]},
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
            variants = ((product.get("variants") or {}).get("nodes") or [])
            variant = variants[0] if variants else {}
            product_id = product.get("id")
            variant_id = variant.get("id")

            # Seed catalog_items for later ops (active=true, not published flags)
            cat_row = {
                "supplier": "moovies",
                "barcode": barcode,
                "title": title,
                "supplier_sku": barcode,
                "cost_price": row["cost_price"],
                "calculated_sale_price": price,
                "director": row["director"],
                "studio": row["studio"],
                "format": row["format"],
                "genres": row["genres"],
                "top_cast": row["top_cast"],
                "film_released": row["film_released"],
                "media_release_date": row.get("media_release_date"),
                "country_of_origin": row["country_of_origin"],
                "availability_status": "supplier_stock",
                "supplier_stock_status": 0,
                "active": True,
                "source_type": "catalog",
                "source_priority": 1,
                "media_type": "film",
                "shopify_product_id": product_id,
                "shopify_variant_id": variant_id,
                "notes": "web-sourced draft; supplier cost from operator",
            }
            try:
                sb.table("catalog_items").upsert(
                    cat_row, on_conflict="supplier,barcode"
                ).execute()
            except Exception as cat_exc:
                print(f"WARN catalog upsert {barcode}: {cat_exc}")

            print(
                f"CREATED DRAFT barcode={barcode} price={price:.2f} "
                f"product={product_id} variant={variant_id} handle={handle} | {title}"
            )
            ok += 1
            time.sleep(0.15)
        except Exception as exc:
            print(f"FAIL {barcode}: {exc}")
            failed += 1
            time.sleep(0.3)

    print(f"\nDone. created={ok} skipped_exists={skipped} failed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
