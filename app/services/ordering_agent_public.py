"""
Ordering Agent V1 — customer-safe response serializers.

Privacy is enforced here. Never serialize internal StockAvailability or
CommerceOffer objects to browser clients.
"""

from __future__ import annotations

from typing import Any, Optional

from app.services.commerce_offer_service import (
    PUBLIC_FORBIDDEN_KEYS,
    assert_public_offer_has_no_supplier_leak,
)


CUSTOMER_STATUS_LABELS = {
    "in_stock": "In Stock",
    "available_from_supplier": "Available from Supplier",
    "available_to_order": "Available to Order",
    "out_of_stock": "Out of Stock",
    "preorder": "Preorder",
    "unavailable": "Unavailable",
    "unavailable_for_supplier_order": "Unavailable",
}


ORDERING_PUBLIC_FORBIDDEN = PUBLIC_FORBIDDEN_KEYS | frozenset(
    {
        "costGbp",
        "cost_gbp",
        "supplierStock",
        "supplier_stock",
        "unit_cost",
        "preferred_supplier_id",
        "preferred_supplier_sku",
        "fulfillment",
        "quote",
        "margin_gate_passed",
        "stock_summary",
        "warnings",
        "internal",
        "suppliers",
        "tape",
        "summary",
    }
)


def format_price_aud(price: Any) -> Optional[str]:
    if price is None:
        return None
    try:
        return f"A${float(price):.2f}"
    except (TypeError, ValueError):
        return None


def customer_status_phrase(status: Optional[str], price: Any = None) -> str:
    label = CUSTOMER_STATUS_LABELS.get(status or "", "Unavailable")
    money = format_price_aud(price)
    if status in {
        "in_stock",
        "available_from_supplier",
        "available_to_order",
        "preorder",
    } and money:
        return f"{label} — {money}"
    return label


def assert_ordering_public_safe(payload: dict[str, Any]) -> None:
    def walk(obj: Any, path: str = "") -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in ORDERING_PUBLIC_FORBIDDEN:
                    raise AssertionError(f"forbidden key in public payload: {path}.{k}".strip("."))
                walk(v, f"{path}.{k}" if path else k)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                walk(item, f"{path}[{i}]")

    walk(payload)
    blob = str(payload).lower()
    for token in ("lasgo", "moovies", "supplier_sku", "unit_cost", "preferred_supplier"):
        if token in blob:
            raise AssertionError(f"forbidden token in public payload: {token}")


def public_release_card(
    *,
    release_variant_id: str,
    title: str,
    availability: str,
    price: Any,
    currency: str = "AUD",
    format: Optional[str] = None,
    shopify_listed: bool = False,
    product_url: Optional[str] = None,
) -> dict[str, Any]:
    card = {
        "release_variant_id": release_variant_id,
        "title": title,
        "format": format,
        "availability": availability,
        "price": price,
        "currency": currency or "AUD",
        "shopify_listed": bool(shopify_listed),
        "product_url": product_url,
        "availability_label": customer_status_phrase(availability, price),
    }
    assert_ordering_public_safe(card)
    return card


def public_offer_from_commerce(public_offer: dict[str, Any], *, format: Optional[str] = None) -> dict[str, Any]:
    """Map CommerceOffer public contract → Ordering Agent release card."""
    assert_public_offer_has_no_supplier_leak(public_offer)
    return public_release_card(
        release_variant_id=str(public_offer.get("release_variant_id") or ""),
        title=str(public_offer.get("title") or ""),
        availability=str(public_offer.get("availability") or "unavailable"),
        price=public_offer.get("price"),
        currency=str(public_offer.get("currency") or "AUD"),
        format=format,
        shopify_listed=public_offer.get("listing_type") == "shopify",
        product_url=None,
    )


def public_choice(candidate: dict[str, Any]) -> dict[str, Any]:
    """Customer-safe clarification choice (no commerce internals)."""
    out = {
        "release_variant_id": candidate.get("release_variant_id") or candidate.get("id"),
        "title": candidate.get("title"),
        "format": candidate.get("format"),
        "label": customer_choice_label_safe(candidate),
    }
    assert_ordering_public_safe(out)
    return out


def customer_choice_label_safe(candidate: dict[str, Any]) -> str:
    title = str(candidate.get("title") or "Untitled").strip()
    fmt = str(candidate.get("format") or "").strip()
    if fmt and fmt.lower() not in title.lower():
        return f"{title} ({fmt})"
    return title
