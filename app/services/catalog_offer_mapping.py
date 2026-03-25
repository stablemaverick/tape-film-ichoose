"""
Map staging_supplier_offers rows to catalog_items insert payloads.

Used by catalog_upsert_service and publish_supplier_offers_to_catalog (shim).
"""

from typing import Any, Dict

from app.helpers.text_helpers import clean_text, now_iso
from app.rules.pricing_rules import pricing_source_for_supplier


def map_offer_to_catalog_row(offer: Dict[str, Any]) -> Dict[str, Any]:
    supplier = clean_text(offer.get("supplier")) or "Unknown"
    shopify_product_id = clean_text(offer.get("shopify_product_id"))
    shopify_variant_id = clean_text(offer.get("shopify_variant_id"))

    title = clean_text(offer.get("title")) or clean_text(offer.get("harmonized_title"))
    format_value = clean_text(offer.get("format")) or clean_text(offer.get("harmonized_format"))
    studio = clean_text(offer.get("studio")) or clean_text(offer.get("harmonized_studio"))

    return {
        "title": title,
        "edition_title": clean_text(offer.get("edition_title")),
        "format": format_value,
        "director": clean_text(offer.get("director")) or clean_text(offer.get("harmonized_director")),
        "studio": studio,
        "film_released": None,
        "media_release_date": offer.get("media_release_date"),
        "barcode": clean_text(offer.get("barcode")),
        "sku": None,
        "supplier": supplier,
        "supplier_sku": clean_text(offer.get("supplier_sku")),
        "supplier_currency": clean_text(offer.get("supplier_currency")) or (
            "AUD" if supplier == "Tape Film" else "GBP"
        ),
        "cost_price": offer.get("cost_price"),
        "pricing_source": pricing_source_for_supplier(supplier),
        "calculated_sale_price": offer.get("calculated_sale_price"),
        "availability_status": clean_text(offer.get("availability_status")),
        "supplier_stock_status": offer.get("supplier_stock_status") or 0,
        "supplier_priority": offer.get("source_priority")
        if offer.get("source_priority") is not None
        else 9,
        "country_of_origin": None,
        "category": None,
        "source_type": clean_text(offer.get("source_type")) or "catalog",
        "active": bool(offer.get("active", True)),
        "supplier_last_seen_at": now_iso(),
        "shopify_product_id": shopify_product_id,
        "shopify_variant_id": shopify_variant_id,
        "film_id": None,
        "film_link_status": None,
        "film_link_method": None,
        "tmdb_id": None,
        "tmdb_title": None,
        "tmdb_match_status": None,
        "top_cast": None,
        "genres": None,
        "tmdb_poster_path": None,
        "tmdb_backdrop_path": None,
        "tmdb_vote_average": None,
        "tmdb_vote_count": None,
        "tmdb_popularity": None,
        "tmdb_last_refreshed_at": None,
        "media_type": clean_text(offer.get("media_type")) or "film",
    }


def make_offer_key(offer: Dict[str, Any]) -> str:
    supplier = clean_text(offer.get("supplier")) or ""
    if supplier == "Tape Film":
        variant = clean_text(offer.get("shopify_variant_id"))
        if variant:
            return f"{supplier}|variant:{variant}"
    barcode = clean_text(offer.get("barcode"))
    if barcode:
        return f"{supplier}|barcode:{barcode}"
    title = clean_text(offer.get("title")) or ""
    return f"{supplier}|title:{title.lower()}"
