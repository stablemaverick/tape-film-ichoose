"""
Commerce Offer V1 — customer sellability + pricing authority.

Paths:
  A) Shopify-listed → retail price from Shopify; availability from Stock Availability
  B) Agent-only → retail price from TAPE pricing_rules.calculate_sale_price

Customers never see supplier identity, SKU, cost, or quantity.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from uuid import uuid4

from app.rules.pricing_rules import (
    DEFAULT_GBP_AUD_RATE,
    DEFAULT_LANDED_COST_MARKUP,
    DEFAULT_MARGIN_FLOOR_RATIO,
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    calculate_shopify_cost_aud,
)
from app.services.stock_availability_service import (
    StockAvailabilityError,
    StockAvailabilityService,
    pick_preferred_supplier,
)
from app.services.shopify_release_mapping import assert_shopify_ii_is_inbound_only

PRICING_POLICY_VERSION = "gbp_formula_28_floor_v2"
DEFAULT_MIN_MARGIN_RATIO = 0.12
DEFAULT_QUOTE_TTL_MINUTES = 30

# Fields that must never appear in customer-safe payloads
PUBLIC_FORBIDDEN_KEYS = frozenset(
    {
        "supplier_id",
        "supplier",
        "supplier_sku",
        "unit_cost",
        "quantity",
        "quantity_type",
        "preferred_supplier_id",
        "preferred_supplier_name",
        "preferred_supplier_sku",
        "suppliers",
        "margin",
        "landed_cost",
        "Lasgo",
        "Moovies",
        "lasgo",
        "moovies",
    }
)


def _float_env(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def pricing_rates() -> tuple[float, float]:
    return (
        _float_env("GBP_AUD_RATE", DEFAULT_GBP_AUD_RATE),
        _float_env("LANDED_COST_MARKUP", DEFAULT_LANDED_COST_MARKUP),
    )


def pricing_margin_floor_ratio() -> float:
    return _float_env("DEFAULT_MARGIN_FLOOR_RATIO", DEFAULT_MARGIN_FLOOR_RATIO)


def min_margin_ratio() -> float:
    return _float_env("COMMERCE_MIN_MARGIN_RATIO", DEFAULT_MIN_MARGIN_RATIO)


def margin_ok(*, retail_aud: Optional[float], cost_gbp: Optional[float]) -> bool:
    """True when retail leaves at least min margin over landed cost."""
    if retail_aud is None or cost_gbp is None:
        return False
    rate, markup = pricing_rates()
    landed = calculate_shopify_cost_aud(cost_gbp, rate, markup)
    if landed is None or retail_aud <= 0:
        return False
    margin = (retail_aud - landed) / retail_aud
    return margin >= min_margin_ratio()


def to_public_commerce_offer(internal: dict[str, Any]) -> dict[str, Any]:
    """Strip supplier-sensitive fields for customer / Ordering Agent presentation."""
    assert_shopify_ii_is_inbound_only()
    public = {
        "release_variant_id": internal.get("release_variant_id"),
        "title": internal.get("title"),
        "listing_type": internal.get("listing_type"),
        "sellable": bool(internal.get("sellable")),
        "availability": internal.get("customer_status"),
        "price": internal.get("retail_price"),
        "currency": internal.get("currency") or "AUD",
    }
    assert_public_offer_has_no_supplier_leak(public)
    return public


def assert_public_offer_has_no_supplier_leak(public: dict[str, Any]) -> None:
    for key in PUBLIC_FORBIDDEN_KEYS:
        if key in public:
            raise AssertionError(f"supplier-sensitive key in public offer: {key}")
    for k, v in public.items():
        if isinstance(v, dict):
            assert_public_offer_has_no_supplier_leak(v)
        if isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    assert_public_offer_has_no_supplier_leak(item)


class CommerceOfferService:
    def __init__(self, supabase: Any, *, now: Optional[datetime] = None):
        self.sb = supabase
        self.now = now or datetime.now(timezone.utc)
        self.stock = StockAvailabilityService(supabase, now=self.now)

    def get_commerce_offer(
        self,
        *,
        release_variant_id: Optional[str] = None,
        barcode: Optional[str] = None,
        shopify_variant_id: Optional[str] = None,
        include_internal: bool = True,
    ) -> dict[str, Any]:
        stock = self.stock.get_stock_availability(
            release_variant_id=release_variant_id,
            barcode=barcode,
            shopify_variant_id=shopify_variant_id,
            include_costs=True,
        )
        rid = stock["release"]["release_variant_id"]
        shopify_listing = self._find_shopify_listing(rid)
        preferred = pick_preferred_supplier(stock.get("suppliers") or [])
        fresh_supplier = bool(
            preferred
            and preferred.get("availability_status") == "available"
            and not preferred.get("is_stale")
        )

        if shopify_listing:
            internal = self._shopify_path(stock, shopify_listing, preferred, fresh_supplier)
        else:
            internal = self._agent_only_path(stock, preferred, fresh_supplier)

        public = to_public_commerce_offer(internal)
        assert_public_offer_has_no_supplier_leak(public)
        out = {"public": public}
        if include_internal:
            out["internal"] = internal
        return out

    def _find_shopify_listing(self, release_variant_id: str) -> Optional[dict[str, Any]]:
        resp = (
            self.sb.table("release_shopify_listings")
            .select("shop,shopify_variant_id,shopify_product_id,is_primary")
            .eq("release_variant_id", release_variant_id)
            .limit(5)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            return None
        primary = next((r for r in rows if r.get("is_primary")), rows[0])
        # Retail price from operational shopify_listings (authoritative)
        vid = primary.get("shopify_variant_id")
        shop = primary.get("shop")
        price = None
        currency = "AUD"
        if vid:
            sl = (
                self.sb.table("shopify_listings")
                .select("price_amount,price_currency_code,product_title,barcode")
                .eq("shopify_variant_id", vid)
                .limit(1)
                .execute()
            )
            srow = (sl.data or [None])[0]
            if srow:
                try:
                    price = float(srow["price_amount"]) if srow.get("price_amount") is not None else None
                except (TypeError, ValueError):
                    price = None
                currency = srow.get("price_currency_code") or "AUD"
        return {
            **primary,
            "retail_price": price,
            "currency": currency,
        }

    def _shopify_path(
        self,
        stock: dict[str, Any],
        listing: dict[str, Any],
        preferred: Optional[dict[str, Any]],
        fresh_supplier: bool,
    ) -> dict[str, Any]:
        tape = stock.get("tape") or {}
        tape_avail = tape.get("available")
        retail = listing.get("retail_price")
        currency = listing.get("currency") or "AUD"

        # Shopify price never changes with supplier cost
        pricing_source = "shopify"
        customer_status = "out_of_stock"
        sellable = False
        margin_pass = True
        warnings: list[str] = list(stock.get("warnings") or [])

        if stock["release"].get("preorder"):
            customer_status = "preorder"
            sellable = True
        elif isinstance(tape_avail, int) and tape_avail > 0:
            customer_status = "in_stock"
            sellable = True
        elif fresh_supplier and preferred:
            cost = preferred.get("unit_cost")
            try:
                cost_f = float(cost) if cost is not None else None
            except (TypeError, ValueError):
                cost_f = None
            margin_pass = margin_ok(retail_aud=retail, cost_gbp=cost_f) if retail is not None else False
            if retail is None:
                warnings.append("SHOPIFY_PRICE_MISSING")
                customer_status = "unavailable"
                sellable = False
            elif not margin_pass:
                customer_status = "unavailable_for_supplier_order"
                sellable = False
                warnings.append("MARGIN_GATE_FAILED")
            else:
                customer_status = "available_from_supplier"
                sellable = True
        else:
            customer_status = "out_of_stock"
            sellable = False

        return {
            "release_variant_id": stock["release"]["release_variant_id"],
            "title": stock["release"].get("title"),
            "listing_type": "shopify",
            "sellable": sellable,
            "customer_status": customer_status,
            "retail_price": retail,
            "currency": currency,
            "pricing_source": pricing_source,
            "fulfilment": {
                "type": "tape" if customer_status == "in_stock" else ("supplier" if sellable else "none"),
                "preferred_supplier_id": (preferred or {}).get("supplier_id"),
                "preferred_supplier_sku": (preferred or {}).get("supplier_sku"),
                "unit_cost": (preferred or {}).get("unit_cost"),
            },
            "stock_summary": {
                "tape_available": tape_avail,
                "tape_status": tape.get("status"),
                "supplier_available": stock.get("summary", {}).get("supplier_available"),
            },
            "margin_gate_passed": margin_pass,
            "warnings": warnings,
            "shopify_variant_id": listing.get("shopify_variant_id"),
        }

    def _agent_only_path(
        self,
        stock: dict[str, Any],
        preferred: Optional[dict[str, Any]],
        fresh_supplier: bool,
    ) -> dict[str, Any]:
        warnings: list[str] = list(stock.get("warnings") or [])
        rate, markup = pricing_rates()
        retail = None
        sellable = False
        customer_status = "unavailable"
        quote = None

        if fresh_supplier and preferred:
            try:
                cost_f = float(preferred["unit_cost"]) if preferred.get("unit_cost") is not None else None
            except (TypeError, ValueError):
                cost_f = None
            retail = calculate_sale_price_with_margin_floor_from_gbp_cost(
                cost_f,
                gbp_aud_rate=rate,
                landed_cost_markup=markup,
                margin_floor_ratio=pricing_margin_floor_ratio(),
            )
            if retail is None:
                warnings.append("PRICING_FAILED")
                customer_status = "unavailable"
            else:
                sellable = True
                customer_status = "available_to_order"
                quote = {
                    "quoted_price": retail,
                    "currency": "AUD",
                    "supplier_offer_ref": {
                        "supplier_id": preferred.get("supplier_id"),
                        "supplier_sku": preferred.get("supplier_sku"),
                    },
                    "created_at": self.now.isoformat(),
                    "expires_at": (self.now + timedelta(minutes=DEFAULT_QUOTE_TTL_MINUTES)).isoformat(),
                    "pricing_policy_version": PRICING_POLICY_VERSION,
                    "quote_id": str(uuid4()),
                }
        else:
            warnings.append("NO_ELIGIBLE_SUPPLIER")

        return {
            "release_variant_id": stock["release"]["release_variant_id"],
            "title": stock["release"].get("title"),
            "listing_type": "agent_only",
            "sellable": sellable,
            "customer_status": customer_status,
            "retail_price": retail,
            "currency": "AUD",
            "pricing_source": "supplier_pricing_policy",
            "fulfilment": {
                "type": "supplier" if sellable else "none",
                "preferred_supplier_id": (preferred or {}).get("supplier_id"),
                "preferred_supplier_sku": (preferred or {}).get("supplier_sku"),
                "unit_cost": (preferred or {}).get("unit_cost"),
            },
            "quote": quote,
            "stock_summary": {
                "tape_available": (stock.get("tape") or {}).get("available"),
                "tape_status": (stock.get("tape") or {}).get("status"),
                "supplier_available": stock.get("summary", {}).get("supplier_available"),
            },
            "warnings": warnings,
        }


def tool_get_commerce_offer(supabase: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return CommerceOfferService(supabase).get_commerce_offer(**kwargs)
    except StockAvailabilityError as exc:
        return exc.to_dict()
