"""
Feature flags for inventory intelligence dual-writes (Phase 3b).

Defaults are OFF — production behaviour unchanged until explicitly enabled.
Master switch must be on for any stream to run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _flag(name: str, default: str = "0") -> bool:
    raw = (os.getenv(name) or default).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _float_env(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class InventoryDualWriteFlags:
    enabled: bool
    shopify: bool
    supplier: bool
    purchase_orders: bool
    auto_accept_min_confidence: float
    fresh_max_hours: float
    aging_max_hours: float
    create_supplier_only_releases: bool

    @property
    def shopify_enabled(self) -> bool:
        return self.enabled and self.shopify

    @property
    def supplier_enabled(self) -> bool:
        return self.enabled and self.supplier

    @property
    def po_enabled(self) -> bool:
        return self.enabled and self.purchase_orders


def load_inventory_dual_write_flags() -> InventoryDualWriteFlags:
    return InventoryDualWriteFlags(
        enabled=_flag("INVENTORY_DUAL_WRITE_ENABLED", "0"),
        shopify=_flag("INVENTORY_DUAL_WRITE_SHOPIFY", "0"),
        supplier=_flag("INVENTORY_DUAL_WRITE_SUPPLIER", "0"),
        purchase_orders=_flag("INVENTORY_DUAL_WRITE_PO", "0"),
        auto_accept_min_confidence=_float_env(
            "SUPPLIER_RESOLUTION_AUTO_ACCEPT_MIN_CONFIDENCE", 0.95
        ),
        fresh_max_hours=_float_env("AVAILABILITY_FEED_FRESH_MAX_HOURS", 36.0),
        aging_max_hours=_float_env("AVAILABILITY_FEED_AGING_MAX_HOURS", 72.0),
        create_supplier_only_releases=_flag(
            "INVENTORY_CREATE_SUPPLIER_ONLY_RELEASES", "1"
        ),
    )


def normalize_supplier_id(supplier: str | None) -> str:
    """Map catalog/staging supplier labels to suppliers.id."""
    raw = (supplier or "").strip()
    low = raw.casefold()
    if low in {"moovies"}:
        return "moovies"
    if low in {"lasgo"}:
        return "lasgo"
    if low in {"tape film", "tape_film", "tapefilm"}:
        return "tape_film"
    return low.replace(" ", "_") if low else "unknown"


def supplier_sku_identity(*, supplier_sku: str | None, raw_barcode: str | None) -> str | None:
    """
    Primary supplier offer identity component.

    Prefer native supplier_sku; if missing, fall back to barcode:{ean} so
    (supplier_id, supplier_sku) remains unique when feeds only provide EAN.
    """
    sku = (supplier_sku or "").strip()
    if sku:
        return sku
    barcode = (raw_barcode or "").strip()
    if barcode:
        return f"barcode:{barcode}"
    return None
