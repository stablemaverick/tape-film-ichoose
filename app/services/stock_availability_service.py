"""
Stock Availability V1 — canonical deterministic inventory read layer.

TAPE inventory and supplier inventory are separate domains and must never be
combined into a single quantity.
"""

from __future__ import annotations

import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from app.rules.availability_rules import (
    DEFAULT_AGING_MAX_HOURS,
    DEFAULT_FRESH_MAX_HOURS,
    derive_feed_freshness,
)
from app.rules.supplier_precedence_rules import supplier_rank


class StockAvailabilityError(Exception):
    """Structured inventory read error."""

    code: str = "INVENTORY_ERROR"

    def __init__(self, message: str, *, details: Optional[dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {"error": self.code, "message": self.message, "details": self.details}


class InvalidIdentifier(StockAvailabilityError):
    code = "INVALID_IDENTIFIER"


class ReleaseNotFound(StockAvailabilityError):
    code = "RELEASE_NOT_FOUND"


class AmbiguousIdentifier(StockAvailabilityError):
    code = "AMBIGUOUS_IDENTIFIER"


class ShopifyListingNotFound(StockAvailabilityError):
    code = "SHOPIFY_LISTING_NOT_FOUND"


class SupplierMappingUnresolved(StockAvailabilityError):
    code = "SUPPLIER_MAPPING_UNRESOLVED"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _float_env(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def freshness_hours_for_supplier(supplier_id: str) -> tuple[float, float]:
    """Return (fresh_max_hours, aging_max_hours) for a supplier."""
    sid = (supplier_id or "").strip().lower()
    global_fresh = _float_env("AVAILABILITY_FEED_FRESH_MAX_HOURS", DEFAULT_FRESH_MAX_HOURS)
    global_aging = _float_env("AVAILABILITY_FEED_AGING_MAX_HOURS", DEFAULT_AGING_MAX_HOURS)
    if sid == "moovies":
        return (
            _float_env("INVENTORY_FRESHNESS_MOOVIES_HOURS", global_fresh),
            _float_env("INVENTORY_FRESHNESS_MOOVIES_AGING_HOURS", global_aging),
        )
    if sid == "lasgo":
        return (
            _float_env("INVENTORY_FRESHNESS_LASGO_HOURS", global_fresh),
            _float_env("INVENTORY_FRESHNESS_LASGO_AGING_HOURS", global_aging),
        )
    if sid in {"tape_film", "shopify"}:
        return (
            _float_env("INVENTORY_FRESHNESS_SHOPIFY_HOURS", global_fresh),
            _float_env("INVENTORY_FRESHNESS_SHOPIFY_AGING_HOURS", global_aging),
        )
    return global_fresh, global_aging


# ---------------------------------------------------------------------------
# Pure derivation helpers (unit-tested without Supabase)
# ---------------------------------------------------------------------------


def map_supplier_api_availability(
    *,
    offer_status: str,
    feed_freshness: str,
) -> tuple[str, Optional[str]]:
    """
    Map II offer status + freshness → API availability_status.

    Returns (availability_status, last_known_availability_status|None).

    API statuses: available | unavailable | unknown | stale
    """
    status = (offer_status or "unknown").strip().lower()
    fresh = (feed_freshness or "unknown").strip().lower()

    if status in {"in_stock", "low_stock", "preorder", "backorder"}:
        known = "available"
    elif status in {"unavailable", "discontinued"}:
        known = "unavailable"
    else:
        known = "unknown"

    if fresh == "stale" and known in {"available", "unavailable"}:
        return "stale", known
    if fresh == "unknown" and known == "unknown":
        return "unknown", None
    return known, None


def quantity_type_for_offer(*, reported_quantity: Any, quantity_is_exact: bool) -> str:
    if reported_quantity is None:
        return "boolean_only" if quantity_is_exact is False else "unknown"
    if quantity_is_exact:
        return "exact"
    return "capped"


def derive_tape_status(*, on_hand: int, committed: int, available: int, is_stale: bool) -> str:
    if is_stale:
        return "stale"
    if available > 0:
        return "available"
    if available < 0:
        return "oversold"
    # available == 0
    if on_hand <= 0 and committed <= 0:
        return "sold_out"
    return "sold_out"


def pick_preferred_supplier(suppliers: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """
    Deterministic preferred supplier among current supplier positions.

    Priority:
      1. fresh available (availability_status == available, not stale)
      2. usable identity (has supplier_sku)
      3. lowest valid unit_cost
      4. suppliers.priority / supplier_rank
      5. supplier_id stable tie-break
    """
    if not suppliers:
        return None

    def is_fresh_available(s: dict[str, Any]) -> bool:
        return s.get("availability_status") == "available" and not s.get("is_stale")

    candidates = [s for s in suppliers if is_fresh_available(s)]
    pool = candidates if candidates else []
    if not pool:
        # Fall back: any non-unknown with identity, else any
        pool = [s for s in suppliers if s.get("availability_status") != "unknown"] or list(suppliers)

    def sort_key(s: dict[str, Any]):
        fresh_avail = 0 if is_fresh_available(s) else 1
        has_sku = 0 if (s.get("supplier_sku") or "").strip() else 1
        cost = s.get("unit_cost")
        try:
            cost_key = float(cost) if cost is not None else float("inf")
        except (TypeError, ValueError):
            cost_key = float("inf")
        sid = str(s.get("supplier_id") or s.get("supplier") or "")
        return (
            fresh_avail,
            has_sku,
            cost_key,
            supplier_rank(sid) if sid else 9,
            sid,
        )

    return sorted(pool, key=sort_key)[0]


def assert_no_combined_quantity(summary: dict[str, Any]) -> None:
    """Regression guard: summary must never expose combined tape+supplier qty."""
    forbidden = {
        "total_available",
        "combined_available",
        "total_quantity",
        "aggregate_available",
        "available_total",
    }
    for key in forbidden:
        if key in summary:
            raise AssertionError(f"combined inventory key forbidden in summary: {key}")
    # Also forbid numeric sum pattern if both present as a single field
    if "tape_plus_supplier" in summary:
        raise AssertionError("tape_plus_supplier forbidden")


@dataclass
class StockAvailabilityResult:
    release: dict[str, Any]
    tape: dict[str, Any]
    suppliers: list[dict[str, Any]]
    summary: dict[str, Any]
    freshness: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    latency_ms: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        assert_no_combined_quantity(self.summary)
        out = {
            "release": self.release,
            "tape": self.tape,
            "suppliers": self.suppliers,
            "summary": self.summary,
            "freshness": self.freshness,
            "warnings": self.warnings,
        }
        if self.latency_ms is not None:
            out["latency_ms"] = self.latency_ms
        return out


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class StockAvailabilityService:
    """Canonical inventory read service backed by Inventory Intelligence tables."""

    def __init__(self, supabase: Any, *, now: Optional[datetime] = None):
        self.sb = supabase
        self.now = now or datetime.now(timezone.utc)

    # -- identity -----------------------------------------------------------

    def resolve_release_variant_id(
        self,
        *,
        release_variant_id: Optional[str] = None,
        barcode: Optional[str] = None,
        shopify_variant_id: Optional[str] = None,
        supplier_id: Optional[str] = None,
        supplier_sku: Optional[str] = None,
    ) -> str:
        provided = [
            ("release_variant_id", (release_variant_id or "").strip()),
            ("barcode", (barcode or "").strip()),
            ("shopify_variant_id", (shopify_variant_id or "").strip()),
            (
                "supplier",
                f"{(supplier_id or '').strip()}|{(supplier_sku or '').strip()}"
                if (supplier_id or "").strip() and (supplier_sku or "").strip()
                else "",
            ),
        ]
        non_empty = [(k, v) for k, v in provided if v]
        if not non_empty:
            raise InvalidIdentifier("Provide release_variant_id, barcode, shopify_variant_id, or supplier_id+supplier_sku")
        if len(non_empty) > 1:
            raise InvalidIdentifier(
                "Provide exactly one primary identifier",
                details={"provided": [k for k, _ in non_empty]},
            )

        kind, value = non_empty[0]
        if kind == "release_variant_id":
            row = (
                self.sb.table("release_variants")
                .select("id")
                .eq("id", value)
                .limit(1)
                .execute()
            )
            if not (row.data or []):
                raise ReleaseNotFound(f"release_variant_id not found: {value}")
            return value

        if kind == "barcode":
            return self._resolve_barcode(value)

        if kind == "shopify_variant_id":
            return self._resolve_shopify_variant(value)

        sid, sku = value.split("|", 1)
        return self._resolve_supplier_sku(sid, sku)

    def _resolve_barcode(self, barcode: str) -> str:
        ids: list[str] = []
        # variant_identifiers
        resp = (
            self.sb.table("variant_identifiers")
            .select("release_variant_id")
            .in_("id_type", ["barcode", "ean", "upc"])
            .eq("id_value", barcode)
            .eq("is_valid", True)
            .execute()
        )
        for r in resp.data or []:
            rid = r.get("release_variant_id")
            if rid:
                ids.append(str(rid))
        # primary_barcode fallback
        resp2 = (
            self.sb.table("release_variants")
            .select("id")
            .eq("primary_barcode", barcode)
            .execute()
        )
        for r in resp2.data or []:
            rid = r.get("id")
            if rid:
                ids.append(str(rid))
        uniq = sorted(set(ids))
        if not uniq:
            raise ReleaseNotFound(f"No release_variant for barcode={barcode}")
        if len(uniq) > 1:
            raise AmbiguousIdentifier(
                f"Barcode maps to {len(uniq)} release variants",
                details={"barcode": barcode, "release_variant_ids": uniq},
            )
        return uniq[0]

    def _resolve_shopify_variant(self, shopify_variant_id: str) -> str:
        resp = (
            self.sb.table("release_shopify_listings")
            .select("release_variant_id")
            .eq("shopify_variant_id", shopify_variant_id)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            raise ShopifyListingNotFound(
                f"No release_shopify_listings row for shopify_variant_id={shopify_variant_id}"
            )
        uniq = sorted({str(r["release_variant_id"]) for r in rows if r.get("release_variant_id")})
        if len(uniq) > 1:
            raise AmbiguousIdentifier(
                "Shopify variant maps to multiple releases",
                details={"shopify_variant_id": shopify_variant_id, "release_variant_ids": uniq},
            )
        return uniq[0]

    def _resolve_supplier_sku(self, supplier_id: str, supplier_sku: str) -> str:
        sid = supplier_id.strip().lower()
        # Prefer active resolution
        resp = (
            self.sb.table("supplier_sku_resolutions")
            .select("resolved_release_variant_id")
            .eq("supplier_id", sid)
            .eq("supplier_sku", supplier_sku)
            .eq("active", True)
            .limit(5)
            .execute()
        )
        resolved = [
            str(r["resolved_release_variant_id"])
            for r in (resp.data or [])
            if r.get("resolved_release_variant_id")
        ]
        if len(resolved) > 1:
            raise AmbiguousIdentifier(
                "Supplier SKU resolves to multiple releases",
                details={"supplier_id": sid, "supplier_sku": supplier_sku, "ids": resolved},
            )
        if len(resolved) == 1:
            return resolved[0]

        resp2 = (
            self.sb.table("supplier_offers")
            .select("release_variant_id")
            .eq("supplier_id", sid)
            .eq("supplier_sku", supplier_sku)
            .eq("active", True)
            .limit(5)
            .execute()
        )
        offers = [
            str(r["release_variant_id"])
            for r in (resp2.data or [])
            if r.get("release_variant_id")
        ]
        uniq = sorted(set(offers))
        if not uniq:
            raise SupplierMappingUnresolved(
                f"Unresolved supplier mapping {sid}/{supplier_sku}"
            )
        if len(uniq) > 1:
            raise AmbiguousIdentifier(
                "Supplier offer maps to multiple releases",
                details={"supplier_id": sid, "supplier_sku": supplier_sku, "ids": uniq},
            )
        return uniq[0]

    # -- reads --------------------------------------------------------------

    def get_stock_availability(
        self,
        *,
        release_variant_id: Optional[str] = None,
        barcode: Optional[str] = None,
        shopify_variant_id: Optional[str] = None,
        supplier_id: Optional[str] = None,
        supplier_sku: Optional[str] = None,
        include_costs: bool = True,
    ) -> dict[str, Any]:
        t0 = time.perf_counter()
        warnings: list[str] = []
        rid = self.resolve_release_variant_id(
            release_variant_id=release_variant_id,
            barcode=barcode,
            shopify_variant_id=shopify_variant_id,
            supplier_id=supplier_id,
            supplier_sku=supplier_sku,
        )

        release = self._load_release(rid, warnings)
        tape = self._load_tape_inventory(rid, warnings)
        suppliers = self._load_suppliers(rid, include_costs=include_costs, warnings=warnings)
        preferred = pick_preferred_supplier(suppliers)
        for s in suppliers:
            s["preferred"] = bool(
                preferred
                and s.get("supplier_id") == preferred.get("supplier_id")
                and s.get("supplier_sku") == preferred.get("supplier_sku")
            )

        summary = {
            "tape_available": tape.get("available"),
            "tape_status": tape.get("status"),
            "supplier_available": any(s.get("availability_status") == "available" for s in suppliers),
            "preferred_supplier_id": (preferred or {}).get("supplier_id"),
            "preferred_supplier_name": (preferred or {}).get("supplier"),
            "preferred_supplier_sku": (preferred or {}).get("supplier_sku"),
        }
        assert_no_combined_quantity(summary)

        result = StockAvailabilityResult(
            release=release,
            tape=tape,
            suppliers=suppliers,
            summary=summary,
            freshness={"generated_at": self.now.isoformat()},
            warnings=warnings,
            latency_ms=round((time.perf_counter() - t0) * 1000, 2),
        )
        return result.to_dict()

    def _load_release(self, rid: str, warnings: list[str]) -> dict[str, Any]:
        resp = (
            self.sb.table("release_variants")
            .select(
                "id,title,format,primary_barcode,catalog_item_id,publication_status,active"
            )
            .eq("id", rid)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            raise ReleaseNotFound(f"release_variant_id not found: {rid}")
        rv = rows[0]
        media_release_date = None
        preorder = False
        catalog_item_id = rv.get("catalog_item_id")
        if catalog_item_id:
            cat = (
                self.sb.table("catalog_items")
                .select("media_release_date,availability_status,title,format,barcode")
                .eq("id", catalog_item_id)
                .limit(1)
                .execute()
            )
            crow = (cat.data or [None])[0]
            if crow:
                media_release_date = crow.get("media_release_date")
                if (crow.get("availability_status") or "").lower() == "preorder":
                    preorder = True
                elif media_release_date:
                    try:
                        dt = datetime.fromisoformat(str(media_release_date).replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        preorder = dt > self.now
                    except ValueError:
                        pass
                if not rv.get("title"):
                    rv["title"] = crow.get("title")
                if not rv.get("format"):
                    rv["format"] = crow.get("format")
                if not rv.get("primary_barcode"):
                    rv["primary_barcode"] = crow.get("barcode")

        return {
            "release_variant_id": rid,
            "title": rv.get("title"),
            "format": rv.get("format"),
            "edition": None,
            "barcode": rv.get("primary_barcode"),
            "release_date": media_release_date,
            "preorder": preorder,
            "publication_status": rv.get("publication_status"),
            "active": rv.get("active"),
            "catalog_item_id": catalog_item_id,
        }

    def _load_tape_inventory(self, rid: str, warnings: list[str]) -> dict[str, Any]:
        fresh_h, aging_h = freshness_hours_for_supplier("shopify")
        resp = (
            self.sb.table("tape_inventory_levels")
            .select(
                "on_hand,committed,available,po_incoming_confirmed,shopify_incoming_reported,"
                "damaged_or_unavailable,last_synced_at,shopify_location_id"
            )
            .eq("release_variant_id", rid)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            warnings.append("TAPE_INVENTORY_ABSENT")
            return {
                "status": "unknown",
                "on_hand": None,
                "committed": None,
                "available": None,
                "incoming": None,
                "observed_at": None,
                "is_stale": None,
                "age_hours": None,
                "shopify_location_id": None,
                "present": False,
            }

        # Aggregate across locations (sum) — available may be negative
        on_hand = sum(int(r.get("on_hand") or 0) for r in rows)
        committed = sum(int(r.get("committed") or 0) for r in rows)
        # Prefer sum of stored available; also recompute for consistency check
        available_stored = sum(int(r.get("available") or 0) for r in rows)
        available = on_hand - committed
        if available != available_stored:
            warnings.append("TAPE_AVAILABLE_RECOMPUTED")
        incoming = sum(int(r.get("shopify_incoming_reported") or 0) for r in rows)
        # newest sync
        observed_at = None
        for r in rows:
            ts = r.get("last_synced_at")
            if ts and (observed_at is None or str(ts) > str(observed_at)):
                observed_at = ts
        fres = derive_feed_freshness(
            last_seen_at=observed_at,
            now=self.now,
            fresh_max_hours=fresh_h,
            aging_max_hours=aging_h,
        )
        is_stale = fres.status == "stale"
        status = derive_tape_status(
            on_hand=on_hand, committed=committed, available=available, is_stale=is_stale
        )
        return {
            "status": status,
            "on_hand": on_hand,
            "committed": committed,
            "available": available,  # may be negative — never clamp
            "incoming": incoming,
            "observed_at": observed_at,
            "is_stale": is_stale,
            "age_hours": fres.age_hours,
            "shopify_location_id": rows[0].get("shopify_location_id") if len(rows) == 1 else None,
            "locations": len(rows),
            "present": True,
        }

    def _load_suppliers(
        self, rid: str, *, include_costs: bool, warnings: list[str]
    ) -> list[dict[str, Any]]:
        resp = (
            self.sb.table("supplier_offers")
            .select(
                "id,supplier_id,supplier_sku,raw_barcode,availability_status,reported_quantity,"
                "quantity_is_exact,supplier_can_supply,unit_cost,currency,last_seen_at,"
                "source_feed_at,pipeline_completed_at,active"
            )
            .eq("release_variant_id", rid)
            .eq("active", True)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            warnings.append("NO_SUPPLIER_OFFERS")

        # supplier display names
        names: dict[str, str] = {}
        try:
            sresp = self.sb.table("suppliers").select("id,display_name").execute()
            for s in sresp.data or []:
                names[str(s["id"])] = s.get("display_name") or s["id"]
        except Exception:  # noqa: BLE001
            pass

        out: list[dict[str, Any]] = []
        for r in rows:
            sid = str(r.get("supplier_id") or "")
            fresh_h, aging_h = freshness_hours_for_supplier(sid)
            fres = derive_feed_freshness(
                last_seen_at=r.get("last_seen_at"),
                source_feed_at=r.get("source_feed_at"),
                pipeline_completed_at=r.get("pipeline_completed_at"),
                now=self.now,
                fresh_max_hours=fresh_h,
                aging_max_hours=aging_h,
            )
            api_status, last_known = map_supplier_api_availability(
                offer_status=str(r.get("availability_status") or "unknown"),
                feed_freshness=fres.status,
            )
            qty = r.get("reported_quantity")
            qexact = bool(r.get("quantity_is_exact"))
            item: dict[str, Any] = {
                "supplier_id": sid,
                "supplier": names.get(sid, sid),
                "supplier_sku": r.get("supplier_sku"),
                "availability_status": api_status,
                "last_known_availability_status": last_known,
                "offer_status": r.get("availability_status"),
                "quantity": qty,
                "quantity_type": quantity_type_for_offer(
                    reported_quantity=qty, quantity_is_exact=qexact
                ),
                "currency": r.get("currency"),
                "observed_at": r.get("last_seen_at") or r.get("source_feed_at"),
                "is_stale": fres.status == "stale",
                "feed_freshness": fres.status,
                "age_hours": fres.age_hours,
                "preferred": False,
            }
            if include_costs:
                item["unit_cost"] = r.get("unit_cost")
            out.append(item)
        # stable order by supplier rank then sku
        out.sort(key=lambda s: (supplier_rank(str(s.get("supplier_id") or "")), str(s.get("supplier_sku") or "")))
        return out

    def search_inventory(self, query: str, *, limit: int = 20) -> dict[str, Any]:
        """Title search → film release candidates only (no stock synthesis)."""
        from app.services.shopify_ii_product_domain import is_film_inventory_release

        q = (query or "").strip()
        if len(q) < 2:
            raise InvalidIdentifier("search query too short")
        # Over-fetch then filter music/vinyl domain so film limit stays full.
        fetch_n = max(1, min(limit * 3, 100))
        try:
            resp = (
                self.sb.table("release_variants")
                .select(
                    "id,title,format,primary_barcode,publication_status,active,product_domain"
                )
                .ilike("title", f"%{q}%")
                .eq("active", True)
                .limit(fetch_n)
                .execute()
            )
        except Exception:
            resp = (
                self.sb.table("release_variants")
                .select("id,title,format,primary_barcode,publication_status,active")
                .ilike("title", f"%{q}%")
                .eq("active", True)
                .limit(fetch_n)
                .execute()
            )
        raw_rows = resp.data or []
        music_ids = self._music_domain_release_ids(
            [r.get("id") for r in raw_rows if r.get("id")]
        )
        candidates: list[dict[str, Any]] = []
        for r in raw_rows:
            row = {
                "release_variant_id": r.get("id"),
                "title": r.get("title"),
                "format": r.get("format"),
                "barcode": r.get("primary_barcode"),
                "publication_status": r.get("publication_status"),
                "product_domain": r.get("product_domain"),
            }
            if not is_film_inventory_release(row):
                continue
            rid = row["release_variant_id"]
            if rid and rid in music_ids:
                continue
            candidates.append(
                {
                    "release_variant_id": row["release_variant_id"],
                    "title": row["title"],
                    "format": row["format"],
                    "barcode": row["barcode"],
                    "publication_status": row["publication_status"],
                }
            )
            if len(candidates) >= max(1, min(limit, 50)):
                break
        return {"query": q, "candidates": candidates, "count": len(candidates)}

    def _music_domain_release_ids(self, release_ids: list[str]) -> set[str]:
        """Releases linked to Shopify soundtrack / vinyl listings (film search exclusion)."""
        from app.services.shopify_ii_product_domain import (
            MUSIC_MEDIA_FORMATS,
            SOUNDTRACKS_COLLECTION_HANDLE,
            normalize_collection_handles,
        )

        out: set[str] = set()
        ids = [i for i in release_ids if i]
        if not ids:
            return out
        try:
            listings = (
                self.sb.table("release_shopify_listings")
                .select("release_variant_id,shopify_variant_id")
                .in_("release_variant_id", ids[:100])
                .execute()
                .data
                or []
            )
        except Exception:
            return out
        if not listings:
            return out
        vids = [r.get("shopify_variant_id") for r in listings if r.get("shopify_variant_id")]
        listing_meta: dict[str, dict[str, Any]] = {}
        if vids:
            try:
                snap = (
                    self.sb.table("shopify_listings")
                    .select("shopify_variant_id,media_format,collection_handles")
                    .in_("shopify_variant_id", vids[:100])
                    .execute()
                    .data
                    or []
                )
                listing_meta = {
                    r["shopify_variant_id"]: r
                    for r in snap
                    if r.get("shopify_variant_id")
                }
            except Exception:
                listing_meta = {}
        for ch in listings:
            rid = ch.get("release_variant_id")
            meta = listing_meta.get(ch.get("shopify_variant_id") or "") or {}
            handles = normalize_collection_handles(meta.get("collection_handles"))
            fmt = (str(meta.get("media_format") or "")).strip().casefold()
            if SOUNDTRACKS_COLLECTION_HANDLE in handles or fmt in MUSIC_MEDIA_FORMATS:
                if rid:
                    out.add(str(rid))
        return out

    def get_inventory_history(
        self, *, release_variant_id: str, limit: int = 50
    ) -> dict[str, Any]:
        """Recent observations/events for a release (Phase 1 — straightforward)."""
        rid = (release_variant_id or "").strip()
        if not rid:
            raise InvalidIdentifier("release_variant_id required")
        offers = (
            self.sb.table("supplier_offers")
            .select("id,supplier_id,supplier_sku")
            .eq("release_variant_id", rid)
            .execute()
        )
        offer_ids = [r["id"] for r in (offers.data or []) if r.get("id")]
        observations: list[dict[str, Any]] = []
        if offer_ids:
            # PostgREST in_ filter
            obs = (
                self.sb.table("supplier_offer_observations")
                .select(
                    "id,supplier_offer_id,observed_at,availability_status,reported_quantity,unit_cost"
                )
                .in_("supplier_offer_id", offer_ids[:100])
                .order("observed_at", desc=True)
                .limit(max(1, min(limit, 200)))
                .execute()
            )
            observations = obs.data or []
        events = (
            self.sb.table("inventory_events")
            .select("id,event_type,observed_at,release_variant_id,supplier_offer_id")
            .eq("release_variant_id", rid)
            .order("observed_at", desc=True)
            .limit(max(1, min(limit, 200)))
            .execute()
        )
        return {
            "release_variant_id": rid,
            "observations": observations,
            "events": events.data or [],
        }


# Tool-layer wrappers (deterministic; no LLM)

def tool_get_inventory(supabase: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return StockAvailabilityService(supabase).get_stock_availability(**kwargs)
    except StockAvailabilityError as exc:
        return exc.to_dict()


def tool_search_inventory(supabase: Any, query: str, **kwargs: Any) -> dict[str, Any]:
    try:
        return StockAvailabilityService(supabase).search_inventory(query, **kwargs)
    except StockAvailabilityError as exc:
        return exc.to_dict()


def tool_get_inventory_history(supabase: Any, **kwargs: Any) -> dict[str, Any]:
    try:
        return StockAvailabilityService(supabase).get_inventory_history(**kwargs)
    except StockAvailabilityError as exc:
        return exc.to_dict()
