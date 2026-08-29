"""
Controlled Region B existing-film catalogue repricing.

Policy (see docs/inventory-intelligence/region-b-film-pricing-policy.md):
  * Replacement cost determines the economic 28% floor (canonical calculator unchanged).
  * Commercial judgement decides whether a material existing-price increase applies.
  * Tiered mutation eligibility:
      ≤$5   → PRICE_INCREASE_AUTO_ELIGIBLE
      $6–$10 → REVIEW_PRICE_INCREASE (commercial review; not auto-applied)
      >$10  → REVIEW_LARGE_INCREASE (mandatory manual review; no partial increases)
  * Optional reviewed artifact may set approved_retail_price (with exception reason
    if below floor). Never auto-generate below-floor overrides.
  * Competitor prices are informational only; not used in the calculator.
  * Film classification beats Region B / GBP / allowlist eligibility.
  * Region A / USD products are out of scope for this GBP path.

Uses canonical GBP calculator only:
  calculate_sale_price_with_margin_floor_from_gbp_cost
  (app/rules/pricing_rules.py — do not duplicate the formula)

Safety:
  * Dry-run by default.
  * --apply alone = zero mutations (explicit allowlist required).
  * Film-only gate runs in dry-run AND apply; allowlist cannot override non-film.
  * Region A blocked from GBP calculator.
  * Never auto-reduce retail (KEEP_CURRENT_PRICE when already above floor).
  * Apply revalidates live Shopify price, film class, Region B, GBP cost, freshness.
  * Mutation writes price only (no inventoryPolicy / quantity changes).

Reserved: REVIEW_MARKET_PRICE_OUTLIER / MARKET_POSITIONING_EXCEPTION —
human review signals only; never auto price-match competitors.

Barcode uniqueness is handled as follows:
- Apply mutations always target Shopify variant ID.
- Allowlist CSVs with variant_id require variant-ID match (no barcode-only win).
- HARD_REVIEW_BARCODES includes unresolved identity/cost issues (Easy Rider/Moonrise,
  The Mask LE, Gladiator II steelbook drift, Poltergeist Film Vault LE drift).
- Florida Project 5028836042709 is NOT hard-reviewed: active barcode is correct;
  inactive/unlisted siblings are excluded by status:active scan + variant-ID targeting.
- Barcode-only supplier resolution mapping to multiple release_variant_ids →
  REVIEW_COST_ANOMALY / apply skip (no first-match-wins).
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Sequence, Set, Tuple

from dotenv import load_dotenv

from app.clients.shopify_client import ShopifyClient
from app.clients.supabase_client import create_fresh_client
from app.helpers.text_helpers import clean_text
from app.rules.pricing_rules import (
    DEFAULT_GBP_AUD_RATE,
    DEFAULT_LANDED_COST_MARKUP,
    DEFAULT_MARGIN_FLOOR_RATIO,
    GST_RATE,
    calculate_sale_price_with_margin_floor_from_gbp_cost,
    effective_pricing_assumptions,
    exact_ex_gst_margin_ratio,
    log_pricing_assumptions,
    replacement_landed_cost_aud,
)
from app.services.arrow_inventory_policy_sync_service import (
    WHOLESALE_SUPPLIER_IDS,
    _eval_offers,
    _load_supplier_context,
    normalize_studio_label,
    resolve_variant_region,
    supplier_is_usable,
)
from app.services.film_product_class import (
    ACTION_OUT_OF_SCOPE_NON_FILM,
    ACTION_SKIP_AMBIGUOUS_PRODUCT,
    PRODUCT_CLASS_FILM,
    classify_product_class,
    product_class_to_action,
)
from app.services.shopify_ii_product_domain import fetch_soundtracks_collection_product_ids
from app.services.stock_availability_service import pick_preferred_supplier
from app.services.supplier_margin_protection_service import ApplyAllowlist, parse_apply_allowlist

logger = logging.getLogger(__name__)

ACTION_PRICE_INCREASE_AUTO_ELIGIBLE = "PRICE_INCREASE_AUTO_ELIGIBLE"
# Backward-compatible alias used by older artifacts/tests.
ACTION_PRICE_INCREASE = ACTION_PRICE_INCREASE_AUTO_ELIGIBLE
ACTION_REVIEW_PRICE_INCREASE = "REVIEW_PRICE_INCREASE"
ACTION_KEEP_CURRENT_PRICE = "KEEP_CURRENT_PRICE"
ACTION_NO_CHANGE = "NO_CHANGE"
ACTION_REVIEW_LARGE_INCREASE = "REVIEW_LARGE_INCREASE"
ACTION_REVIEW_COST_ANOMALY = "REVIEW_COST_ANOMALY"
# Live retail matches an explicit below-floor merchandising approval.
ACTION_APPROVED_PRICING_EXCEPTION = "APPROVED_PRICING_EXCEPTION"
# Reserved (not emitted yet): market-price outlier → human review only.
ACTION_REVIEW_MARKET_PRICE_OUTLIER = "REVIEW_MARKET_PRICE_OUTLIER"
# Reserved / used exception reason for audited below-floor approvals.
REASON_MARKET_POSITIONING_EXCEPTION = "MARKET_POSITIONING_EXCEPTION"
ACTION_NO_CURRENT_COST = "NO_CURRENT_COST"
ACTION_STALE_SUPPLIER = "STALE_SUPPLIER"
ACTION_AMBIGUOUS_MAPPING = "AMBIGUOUS_MAPPING"
ACTION_REGION_OR_CURRENCY_AMBIGUOUS = "REGION_OR_CURRENCY_AMBIGUOUS"
ACTION_REGION_A_BLOCKED = "REGION_A_BLOCKED"
ACTION_ERROR = "ERROR"
ACTION_APPLY_SKIP_DRIFT = "APPLY_SKIP_DRIFT"
ACTION_APPLIED = "APPLIED"

# Auto cohort: increases of $1–$5 inclusive (and fractional amounts ≤ $5).
DEFAULT_MAX_AUTO_INCREASE = 5.0
# Commercial review band: >$5 and ≤$10.
DEFAULT_COMMERCIAL_REVIEW_MAX = 10.0
# Re-open an approved below-floor exception when GBP cost rises by this much.
DEFAULT_EXCEPTION_GBP_INCREASE_PCT = 0.05
DEFAULT_EXCEPTION_GBP_INCREASE_ABS = 0.50
DEFAULT_COST_ANOMALY_PCT = 0.25
PRICE_MATCH_TOLERANCE = 0.02
FLOOR_MATCH_TOLERANCE = 0.02

PRICING_EXCEPTIONS_REL_PATH = Path("app/config/region_b_pricing_exceptions.json")
# Deterministic dashboard-facing current-state output (not tmp/).
PRICING_HEALTH_DIR_REL = Path("var/pricing_health")
PRICING_HEALTH_LATEST_JSON = "region_b_pricing_health_latest.json"
PRICING_HEALTH_LATEST_CSV = "region_b_pricing_health_latest.csv"
PRICING_HEALTH_META_JSON = "region_b_pricing_health_meta.json"

# Known barcode / cost anomalies — never auto-apply.
HARD_REVIEW_BARCODES = frozenset(
    {
        # Easy Rider incorrectly shares Moonrise Kingdom barcode.
        "5050629184334",
        # The Mask LE: Shopify unitCost materially drifts from GBP replacement.
        "5027035029276",
        # Cost-drift anomalies — blocked until reconciled.
        "5056453207812",  # Gladiator II Limited Edition Steelbook
        "5051892252850",  # Poltergeist Film Vault LE Steelbook
    }
)

# Florida Project 5028836042709 is NOT hard-reviewed: barcode is correct for the
# ACTIVE listing. Safety = status:active scan + variant-ID allowlist + RSL mapping.
# The UNLISTED sibling must never enter candidates (inactive products are not fetched).


PRODUCTS_QUERY = """
query RegionBFilmRepricing($cursor: String, $q: String) {
  products(first: 50, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      title
      handle
      status
      productType
      tags
      studio: metafield(namespace: "custom", key: "studio") { value }
      region: metafield(namespace: "custom", key: "region") { value }
      formatMeta: metafield(namespace: "custom", key: "format") { value }
      mediaFormat: metafield(namespace: "custom", key: "media_format") { value }
      collections(first: 30) { nodes { handle } }
      variants(first: 25) {
        nodes {
          id
          title
          sku
          barcode
          price
          region: metafield(namespace: "custom", key: "region") { value }
          inventoryItem { unitCost { amount currencyCode } }
        }
      }
    }
  }
}
"""

LIVE_VARIANT_QUERY = """
query LiveVariantPrice($id: ID!) {
  node(id: $id) {
    ... on ProductVariant {
      id
      price
      barcode
      sku
      product {
        id
        title
        productType
        tags
        studio: metafield(namespace: "custom", key: "studio") { value }
        region: metafield(namespace: "custom", key: "region") { value }
        formatMeta: metafield(namespace: "custom", key: "format") { value }
        collections(first: 30) { nodes { handle } }
      }
      region: metafield(namespace: "custom", key: "region") { value }
      inventoryItem { unitCost { amount currencyCode } }
    }
  }
}
"""

VARIANT_PRICE_UPDATE = """
mutation ProductVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id price }
    userErrors { field message }
  }
}
"""


@dataclass
class RepriceRow:
    title: str
    variant_title: str
    barcode: str
    sku: str
    product_id: str
    variant_id: str
    studio_raw: str
    studio_norm: str
    region_raw: str
    normalized_region: str
    product_type: str
    media_format: str
    product_class: str
    class_reason: str
    collection_handles: str
    preferred_supplier: str
    source_currency: str
    source_cost_gbp: Optional[float]
    gbp_aud_rate: Optional[float]
    landed_aud_cost: Optional[float]
    shopify_unit_cost_aud: Optional[float]
    current_retail: Optional[float]
    current_gp_pct: Optional[float]
    floor_retail: Optional[float]
    proposed_retail: Optional[float]
    proposed_gp_pct: Optional[float]
    dollar_change: Optional[float]
    pct_change: Optional[float]
    action: str
    reason: str
    # Calculated economic floor (always the canonical 28% .99 price when known).
    calculated_floor_price: Optional[float] = None
    # Explicit reviewed apply price. Normal auto candidates: equals floor.
    # Below-floor values are allowed ONLY when set in a reviewed artifact with reason.
    approved_retail_price: Optional[float] = None
    approved_gp_percent: Optional[float] = None
    pricing_exception_reason: str = ""
    # Exception registry observability (populated when a registry record exists).
    source_cost_gbp_at_approval: Optional[float] = None
    gbp_movement_from_approval: Optional[float] = None
    calculated_floor_at_approval: Optional[float] = None
    supplier_freshness: str = ""
    resolution_method: str = ""
    release_variant_id: str = ""
    data_notes: str = ""
    apply_status: str = ""
    apply_error: str = ""
    live_price_at_apply: Optional[float] = None
    evaluated_at: str = ""


@dataclass
class RepriceSummary:
    products_scanned: int = 0
    variants_examined: int = 0
    films_assessed: int = 0
    non_film_excluded: int = 0
    ambiguous_skipped: int = 0
    region_a_blocked: int = 0
    price_increase: int = 0  # AUTO_ELIGIBLE count
    review_price_increase: int = 0  # $6–$10 commercial review
    review_large_increase: int = 0
    approved_pricing_exception: int = 0
    exception_reopened: int = 0
    keep_current: int = 0
    no_change: int = 0
    review_anomaly: int = 0
    stale_supplier: int = 0
    no_current_cost: int = 0
    ambiguous_mapping: int = 0
    applied_ok: int = 0
    applied_failed: int = 0
    apply_skipped: int = 0
    dry_run: bool = True
    monitoring_only: bool = True
    labels: List[str] = field(default_factory=list)
    csv_path: str = ""
    json_path: str = ""
    candidates_csv_path: str = ""
    apply_report_path: str = ""


def pricing_exceptions_path(root: Optional[Path] = None) -> Path:
    base = root or _repo_root()
    return base / PRICING_EXCEPTIONS_REL_PATH


def load_pricing_exceptions(path: Optional[Path] = None) -> Dict[str, dict[str, Any]]:
    """variant_id → exception record. Missing file → empty."""
    p = path or pricing_exceptions_path()
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to load pricing exceptions from %s", p)
        return {}
    exceptions = raw.get("exceptions") if isinstance(raw, dict) else None
    if not isinstance(exceptions, dict):
        return {}
    out: Dict[str, dict[str, Any]] = {}
    for vid, rec in exceptions.items():
        key = clean_text(vid) or ""
        if key and isinstance(rec, dict):
            out[key] = rec
    return out


def upsert_pricing_exceptions(
    records: Sequence[dict[str, Any]],
    *,
    path: Optional[Path] = None,
) -> Path:
    """Merge exception records keyed by variant_id. Does not delete others."""
    p = path or pricing_exceptions_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if p.is_file():
        try:
            existing = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
    exceptions = dict(existing.get("exceptions") or {})
    now = datetime.now(timezone.utc).isoformat()
    for rec in records:
        vid = clean_text(rec.get("variant_id")) or ""
        if not vid:
            continue
        exceptions[vid] = {
            "variant_id": vid,
            "barcode": clean_text(rec.get("barcode")) or "",
            "title": rec.get("title") or "",
            "approved_retail_price": rec.get("approved_retail_price"),
            "calculated_floor_at_approval": rec.get("calculated_floor_at_approval")
            or rec.get("calculated_floor_price"),
            "source_cost_gbp_at_approval": rec.get("source_cost_gbp_at_approval")
            or rec.get("source_cost_gbp"),
            "pricing_exception_reason": rec.get("pricing_exception_reason")
            or REASON_MARKET_POSITIONING_EXCEPTION,
            "approved_gp_percent": rec.get("approved_gp_percent"),
            "preferred_supplier": rec.get("preferred_supplier") or "",
            "approved_at": rec.get("approved_at") or now,
            "updated_at": now,
        }
    payload = {
        "version": 1,
        "updated_at": now,
        "exceptions": exceptions,
    }
    p.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return p


def gbp_cost_increased_materially(
    current_gbp: Optional[float],
    approved_gbp: Optional[float],
    *,
    pct: float = DEFAULT_EXCEPTION_GBP_INCREASE_PCT,
    abs_gbp: float = DEFAULT_EXCEPTION_GBP_INCREASE_ABS,
) -> bool:
    if current_gbp is None or approved_gbp is None:
        return False
    delta = float(current_gbp) - float(approved_gbp)
    if delta <= 0:
        return False
    return delta >= abs_gbp or (approved_gbp > 0 and (delta / approved_gbp) >= pct)


def load_reviewed_artifact_overrides(csv_path: Optional[str]) -> Dict[str, dict[str, Any]]:
    """Load per-variant reviewed fields from an allowlist CSV (variant_id keyed)."""
    if not csv_path:
        return {}
    path = Path(csv_path)
    if not path.is_file():
        return {}
    out: Dict[str, dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            vid = clean_text(row.get("variant_id")) or ""
            if not vid:
                continue
            out[vid] = {
                "approved_retail_price": _f(row.get("approved_retail_price")),
                "calculated_floor_price": _f(
                    row.get("calculated_floor_price") or row.get("floor_retail")
                ),
                "approved_gp_percent": _f(row.get("approved_gp_percent")),
                "pricing_exception_reason": clean_text(row.get("pricing_exception_reason"))
                or "",
                "source_cost_gbp": _f(row.get("source_cost_gbp")),
                "current_retail": _f(row.get("current_retail")),
            }
    return out


def apply_reviewed_overrides_to_rows(
    rows: Sequence[RepriceRow],
    overrides: Dict[str, dict[str, Any]],
) -> None:
    """Mutate rows in place with explicit reviewed artifact overrides."""
    for r in rows:
        ov = overrides.get(r.variant_id)
        if not ov:
            continue
        if ov.get("calculated_floor_price") is not None:
            r.calculated_floor_price = ov["calculated_floor_price"]
        if ov.get("approved_retail_price") is not None:
            r.approved_retail_price = ov["approved_retail_price"]
        if ov.get("approved_gp_percent") is not None:
            r.approved_gp_percent = ov["approved_gp_percent"]
        if ov.get("pricing_exception_reason"):
            r.pricing_exception_reason = ov["pricing_exception_reason"]
        # For apply targeting of commercial exceptions, treat as reviewed increase.
        if (
            r.approved_retail_price is not None
            and (r.pricing_exception_reason or "").strip()
            and r.action
            in {
                ACTION_REVIEW_PRICE_INCREASE,
                ACTION_REVIEW_LARGE_INCREASE,
                ACTION_PRICE_INCREASE_AUTO_ELIGIBLE,
            }
        ):
            r.proposed_retail = r.approved_retail_price
            if r.current_retail is not None:
                r.dollar_change = round(r.approved_retail_price - r.current_retail, 2)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _f(v: Any) -> Optional[float]:
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _margin_pct(price: Optional[float], landed: Optional[float]) -> Optional[float]:
    ratio = exact_ex_gst_margin_ratio(price, landed)
    if ratio is None:
        return None
    return float(ratio * Decimal("100"))


def normalize_label(raw: Optional[str]) -> str:
    canon = normalize_studio_label(raw)
    if canon in {"Arrow", "Second Sight", "Criterion Collection"}:
        return canon
    text = re.sub(r"\s+", " ", (clean_text(raw) or "")).strip()
    if not text:
        return "(blank)"
    low = text.casefold()
    if "88" in low and "film" in low:
        return "88 Films"
    if "radiance" in low:
        return "Radiance Films"
    if "eureka" in low:
        return "Eureka"
    return text


def decide_action(
    *,
    current: Optional[float],
    floor: Optional[float],
    max_auto_increase: float = DEFAULT_MAX_AUTO_INCREASE,
    commercial_review_max: float = DEFAULT_COMMERCIAL_REVIEW_MAX,
) -> tuple[str, str, Optional[float], Optional[float], Optional[float]]:
    """Returns action, reason, proposed, dollar_change, pct_change.

    The calculated floor is always the economic authority (proposed = floor when
    an increase is indicated). Mutation eligibility is tiered separately:

      ≤ max_auto_increase          → PRICE_INCREASE_AUTO_ELIGIBLE
      > max_auto … ≤ commercial_max → REVIEW_PRICE_INCREASE
      > commercial_max             → REVIEW_LARGE_INCREASE
    """
    if current is None or floor is None:
        return ACTION_ERROR, "missing_price_or_floor", None, None, None
    delta = round(floor - current, 2)
    pct = round((delta / current) * 100, 2) if current else None
    if abs(delta) < 0.005:
        return ACTION_NO_CHANGE, "price_already_at_floor", current, delta, pct
    if delta < 0:
        return (
            ACTION_KEEP_CURRENT_PRICE,
            "proposed_below_current_keep_margin",
            current,
            delta,
            pct,
        )
    if delta > commercial_review_max:
        return (
            ACTION_REVIEW_LARGE_INCREASE,
            "increase_above_commercial_review_threshold",
            floor,
            delta,
            pct,
        )
    if delta > max_auto_increase:
        return (
            ACTION_REVIEW_PRICE_INCREASE,
            "increase_requires_commercial_review",
            floor,
            delta,
            pct,
        )
    return (
        ACTION_PRICE_INCREASE_AUTO_ELIGIBLE,
        "current_below_28pct_floor_auto_eligible",
        floor,
        delta,
        pct,
    )


def to_preferred_pool(evaluated: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    pool = []
    for e in evaluated:
        sid = str(e.get("supplier_id") or "").strip().lower()
        if sid not in WHOLESALE_SUPPLIER_IDS:
            continue
        pool.append(
            {
                "supplier_id": sid,
                "supplier": e.get("supplier"),
                "supplier_sku": e.get("supplier_sku"),
                "availability_status": e.get("api_status"),
                "is_stale": str(e.get("freshness") or "") == "stale",
                "unit_cost": e.get("unit_cost"),
                "qty": e.get("qty"),
                "freshness": e.get("freshness"),
                "observed_at": e.get("observed_at"),
            }
        )
    return pool


def _gql(client: ShopifyClient, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    tries = 0
    while True:
        tries += 1
        try:
            return client.graphql(query, variables)
        except Exception as exc:
            if "THROTTLED" in str(exc) and tries < 8:
                time.sleep(min(2 * tries, 10))
                continue
            raise


def fetch_active_products(client: ShopifyClient) -> Tuple[int, List[dict[str, Any]]]:
    out: List[dict[str, Any]] = []
    scanned = 0
    cursor = None
    while True:
        data = _gql(client, PRODUCTS_QUERY, {"cursor": cursor, "q": "status:active"})
        block = data["products"]
        nodes = block.get("nodes") or []
        scanned += len(nodes)
        out.extend(nodes)
        page = block.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        time.sleep(0.08)
    return scanned, out


def build_candidate_rows(
    products: Sequence[dict[str, Any]],
    *,
    supabase: Any,
    soundtrack_product_ids: Optional[Set[str]] = None,
    labels: Optional[Sequence[str]] = None,
    gbp_aud: float = DEFAULT_GBP_AUD_RATE,
    landed_markup: float = DEFAULT_LANDED_COST_MARKUP,
    margin_floor: float = DEFAULT_MARGIN_FLOOR_RATIO,
    max_auto_increase: float = DEFAULT_MAX_AUTO_INCREASE,
    commercial_review_max: float = DEFAULT_COMMERCIAL_REVIEW_MAX,
    anomaly_pct: float = DEFAULT_COST_ANOMALY_PCT,
    now: Optional[datetime] = None,
    pricing_exceptions: Optional[Dict[str, dict[str, Any]]] = None,
) -> List[RepriceRow]:
    now = now or datetime.now(timezone.utc)
    label_filter = {str(x).strip() for x in (labels or []) if str(x).strip()} or None
    soundtrack_product_ids = soundtrack_product_ids or set()
    pricing_exceptions = pricing_exceptions if pricing_exceptions is not None else load_pricing_exceptions()

    flat: List[dict[str, Any]] = []
    for p in products:
        studio_raw = clean_text((p.get("studio") or {}).get("value")) or ""
        studio_norm = normalize_label(studio_raw)
        if label_filter and studio_norm not in label_filter:
            continue
        product_region = clean_text((p.get("region") or {}).get("value")) or ""
        format_value = clean_text((p.get("formatMeta") or {}).get("value")) or ""
        media_format = clean_text((p.get("mediaFormat") or {}).get("value")) or ""
        effective_format = format_value or media_format
        product_type = clean_text(p.get("productType")) or ""
        tags = list(p.get("tags") or [])
        handles = {
            (clean_text(n.get("handle")) or "").casefold()
            for n in ((p.get("collections") or {}).get("nodes") or [])
            if clean_text((n or {}).get("handle"))
        }
        product_class, class_reason = classify_product_class(
            title=p.get("title") or "",
            product_id=p.get("id") or "",
            product_type=product_type,
            format_value=format_value,
            media_format=media_format,
            tags=tags,
            collection_handles=handles,
            soundtrack_product_ids=soundtrack_product_ids,
        )
        for v in ((p.get("variants") or {}).get("nodes") or []):
            variant_region = clean_text((v.get("region") or {}).get("value")) or ""
            region_code = resolve_variant_region(
                product_region=product_region, variant_region=variant_region
            )
            cost_obj = (v.get("inventoryItem") or {}).get("unitCost") or {}
            flat.append(
                {
                    "title": p.get("title") or "",
                    "variant_title": v.get("title") or "",
                    "barcode": clean_text(v.get("barcode")) or "",
                    "sku": clean_text(v.get("sku")) or "",
                    "product_id": p.get("id") or "",
                    "variant_id": v.get("id") or "",
                    "studio_raw": studio_raw,
                    "studio_norm": studio_norm,
                    "region_raw": variant_region or product_region,
                    "normalized_region": region_code,
                    "product_type": product_type,
                    "media_format": effective_format,
                    "product_class": product_class,
                    "class_reason": class_reason,
                    "collection_handles": ",".join(sorted(handles)),
                    "current_retail": _f(v.get("price")),
                    "shopify_unit_cost_aud": _f(cost_obj.get("amount"))
                    if (cost_obj.get("currencyCode") or "AUD") == "AUD"
                    else None,
                }
            )

    ctx = _load_supplier_context(
        supabase,
        variant_ids=[r["variant_id"] for r in flat if r["variant_id"]],
        barcodes=sorted({r["barcode"] for r in flat if r["barcode"]}),
    )

    # Within this (ACTIVE) scan: barcodes attached to more than one variant.
    active_barcode_dupes: Dict[str, int] = {}
    for r in flat:
        bc = r.get("barcode") or ""
        if bc:
            active_barcode_dupes[bc] = active_barcode_dupes.get(bc, 0) + 1

    rows: List[RepriceRow] = []
    for row in flat:
        gate_action, gate_reason = product_class_to_action(
            row["product_class"], row["class_reason"]
        )
        # Non-film / ambiguous always excluded — even with GBP offers present.
        if gate_action:
            rows.append(
                RepriceRow(
                    title=row["title"],
                    variant_title=row["variant_title"],
                    barcode=row["barcode"],
                    sku=row["sku"],
                    product_id=row["product_id"],
                    variant_id=row["variant_id"],
                    studio_raw=row["studio_raw"],
                    studio_norm=row["studio_norm"],
                    region_raw=row["region_raw"],
                    normalized_region=row["normalized_region"],
                    product_type=row["product_type"],
                    media_format=row["media_format"],
                    product_class=row["product_class"],
                    class_reason=row["class_reason"],
                    collection_handles=row["collection_handles"],
                    preferred_supplier="",
                    source_currency="",
                    source_cost_gbp=None,
                    gbp_aud_rate=None,
                    landed_aud_cost=None,
                    shopify_unit_cost_aud=row["shopify_unit_cost_aud"],
                    current_retail=row["current_retail"],
                    current_gp_pct=None,
                    floor_retail=None,
                    proposed_retail=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=gate_action,
                    reason=gate_reason,
                )
            )
            continue

        if row["normalized_region"] == "A":
            rows.append(
                RepriceRow(
                    title=row["title"],
                    variant_title=row["variant_title"],
                    barcode=row["barcode"],
                    sku=row["sku"],
                    product_id=row["product_id"],
                    variant_id=row["variant_id"],
                    studio_raw=row["studio_raw"],
                    studio_norm=row["studio_norm"],
                    region_raw=row["region_raw"],
                    normalized_region=row["normalized_region"],
                    product_type=row["product_type"],
                    media_format=row["media_format"],
                    product_class=row["product_class"],
                    class_reason=row["class_reason"],
                    collection_handles=row["collection_handles"],
                    preferred_supplier="",
                    source_currency="USD_BLOCKED",
                    source_cost_gbp=None,
                    gbp_aud_rate=None,
                    landed_aud_cost=row["shopify_unit_cost_aud"],
                    shopify_unit_cost_aud=row["shopify_unit_cost_aud"],
                    current_retail=row["current_retail"],
                    current_gp_pct=_margin_pct(
                        row["current_retail"], row["shopify_unit_cost_aud"]
                    ),
                    floor_retail=None,
                    proposed_retail=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_REGION_A_BLOCKED,
                    reason="region_a_usd_path_required_gbp_calculator_blocked",
                )
            )
            continue

        if row["normalized_region"] != "B":
            rows.append(
                RepriceRow(
                    title=row["title"],
                    variant_title=row["variant_title"],
                    barcode=row["barcode"],
                    sku=row["sku"],
                    product_id=row["product_id"],
                    variant_id=row["variant_id"],
                    studio_raw=row["studio_raw"],
                    studio_norm=row["studio_norm"],
                    region_raw=row["region_raw"],
                    normalized_region=row["normalized_region"],
                    product_type=row["product_type"],
                    media_format=row["media_format"],
                    product_class=row["product_class"],
                    class_reason=row["class_reason"],
                    collection_handles=row["collection_handles"],
                    preferred_supplier="",
                    source_currency="",
                    source_cost_gbp=None,
                    gbp_aud_rate=None,
                    landed_aud_cost=None,
                    shopify_unit_cost_aud=row["shopify_unit_cost_aud"],
                    current_retail=row["current_retail"],
                    current_gp_pct=None,
                    floor_retail=None,
                    proposed_retail=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_REGION_OR_CURRENCY_AMBIGUOUS,
                    reason="region_missing_or_not_b",
                )
            )
            continue

        vid = row["variant_id"]
        rid = ctx["rsl"].get(vid)
        offers: List[dict[str, Any]] = []
        resolution = ""
        if rid:
            offers = list(ctx["offers_by_release"].get(str(rid), []))
            if offers:
                resolution = "release_shopify_listings"
        if not offers and row["barcode"]:
            barcode_offers = list(ctx["offers_by_barcode"].get(row["barcode"], []))
            release_ids = sorted(
                {
                    str(o.get("release_variant_id") or "")
                    for o in barcode_offers
                    if o.get("release_variant_id")
                }
            )
            # Never let barcode-only resolution silently win across multiple releases.
            if len(release_ids) > 1:
                rows.append(
                    RepriceRow(
                        title=row["title"],
                        variant_title=row["variant_title"],
                        barcode=row["barcode"],
                        sku=row["sku"],
                        product_id=row["product_id"],
                        variant_id=row["variant_id"],
                        studio_raw=row["studio_raw"],
                        studio_norm=row["studio_norm"],
                        region_raw=row["region_raw"],
                        normalized_region=row["normalized_region"],
                        product_type=row["product_type"],
                        media_format=row["media_format"],
                        product_class=row["product_class"],
                        class_reason=row["class_reason"],
                        collection_handles=row["collection_handles"],
                        preferred_supplier="",
                        source_currency="GBP",
                        source_cost_gbp=None,
                        gbp_aud_rate=gbp_aud,
                        landed_aud_cost=None,
                        shopify_unit_cost_aud=row["shopify_unit_cost_aud"],
                        current_retail=row["current_retail"],
                        current_gp_pct=None,
                        floor_retail=None,
                        proposed_retail=None,
                        proposed_gp_pct=None,
                        dollar_change=None,
                        pct_change=None,
                        action=ACTION_REVIEW_COST_ANOMALY,
                        reason="ambiguous_barcode_supplier_resolution",
                        resolution_method="supplier_offers_by_barcode_ambiguous",
                    )
                )
                continue
            if barcode_offers:
                offers = barcode_offers
                resolution = "supplier_offers_by_barcode"

        if not rid and not offers:
            rows.append(
                RepriceRow(
                    title=row["title"],
                    variant_title=row["variant_title"],
                    barcode=row["barcode"],
                    sku=row["sku"],
                    product_id=row["product_id"],
                    variant_id=row["variant_id"],
                    studio_raw=row["studio_raw"],
                    studio_norm=row["studio_norm"],
                    region_raw=row["region_raw"],
                    normalized_region=row["normalized_region"],
                    product_type=row["product_type"],
                    media_format=row["media_format"],
                    product_class=row["product_class"],
                    class_reason=row["class_reason"],
                    collection_handles=row["collection_handles"],
                    preferred_supplier="",
                    source_currency="GBP",
                    source_cost_gbp=None,
                    gbp_aud_rate=gbp_aud,
                    landed_aud_cost=None,
                    shopify_unit_cost_aud=row["shopify_unit_cost_aud"],
                    current_retail=row["current_retail"],
                    current_gp_pct=None,
                    floor_retail=None,
                    proposed_retail=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_AMBIGUOUS_MAPPING,
                    reason="no_release_or_supplier_mapping",
                )
            )
            continue

        evaluated = _eval_offers(offers, suppliers=ctx["suppliers"], now=now)
        if not supplier_is_usable(evaluated):
            stale = any(str(e.get("freshness") or "") == "stale" for e in evaluated)
            rows.append(
                RepriceRow(
                    title=row["title"],
                    variant_title=row["variant_title"],
                    barcode=row["barcode"],
                    sku=row["sku"],
                    product_id=row["product_id"],
                    variant_id=row["variant_id"],
                    studio_raw=row["studio_raw"],
                    studio_norm=row["studio_norm"],
                    region_raw=row["region_raw"],
                    normalized_region=row["normalized_region"],
                    product_type=row["product_type"],
                    media_format=row["media_format"],
                    product_class=row["product_class"],
                    class_reason=row["class_reason"],
                    collection_handles=row["collection_handles"],
                    preferred_supplier="",
                    source_currency="GBP",
                    source_cost_gbp=None,
                    gbp_aud_rate=gbp_aud,
                    landed_aud_cost=None,
                    shopify_unit_cost_aud=row["shopify_unit_cost_aud"],
                    current_retail=row["current_retail"],
                    current_gp_pct=None,
                    floor_retail=None,
                    proposed_retail=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_STALE_SUPPLIER if stale else ACTION_NO_CURRENT_COST,
                    reason="supplier_not_usable",
                    resolution_method=resolution,
                    release_variant_id=str(rid or ""),
                )
            )
            continue

        preferred = pick_preferred_supplier(to_preferred_pool(evaluated))
        cost_gbp = _f((preferred or {}).get("unit_cost"))
        if not preferred or cost_gbp is None or cost_gbp <= 0:
            rows.append(
                RepriceRow(
                    title=row["title"],
                    variant_title=row["variant_title"],
                    barcode=row["barcode"],
                    sku=row["sku"],
                    product_id=row["product_id"],
                    variant_id=row["variant_id"],
                    studio_raw=row["studio_raw"],
                    studio_norm=row["studio_norm"],
                    region_raw=row["region_raw"],
                    normalized_region=row["normalized_region"],
                    product_type=row["product_type"],
                    media_format=row["media_format"],
                    product_class=row["product_class"],
                    class_reason=row["class_reason"],
                    collection_handles=row["collection_handles"],
                    preferred_supplier=str((preferred or {}).get("supplier_id") or ""),
                    source_currency="GBP",
                    source_cost_gbp=cost_gbp,
                    gbp_aud_rate=gbp_aud,
                    landed_aud_cost=None,
                    shopify_unit_cost_aud=row["shopify_unit_cost_aud"],
                    current_retail=row["current_retail"],
                    current_gp_pct=None,
                    floor_retail=None,
                    proposed_retail=None,
                    proposed_gp_pct=None,
                    dollar_change=None,
                    pct_change=None,
                    action=ACTION_NO_CURRENT_COST,
                    reason="invalid_or_missing_gbp_cost",
                    resolution_method=resolution,
                    release_variant_id=str(rid or ""),
                )
            )
            continue

        landed = replacement_landed_cost_aud(
            cost_gbp, gbp_aud_rate=gbp_aud, landed_cost_markup=landed_markup
        )
        floor = calculate_sale_price_with_margin_floor_from_gbp_cost(
            cost_gbp,
            gbp_aud_rate=gbp_aud,
            landed_cost_markup=landed_markup,
            margin_floor_ratio=margin_floor,
        )
        current = row["current_retail"]
        shop_cost = row["shopify_unit_cost_aud"]
        notes: List[str] = []
        if shop_cost and landed and landed > 0:
            drift = abs(shop_cost - landed) / landed
            if drift >= anomaly_pct:
                notes.append(f"shopify_cost_vs_replacement_drift={drift:.0%}")
        # Active-scan duplicate barcodes (two ACTIVE variants sharing a barcode).
        if row["barcode"] and active_barcode_dupes.get(row["barcode"], 0) > 1:
            notes.append("shopify_active_duplicate_barcode")
        if row["barcode"] in HARD_REVIEW_BARCODES:
            notes.append("hard_review_barcode")

        if row["barcode"] in HARD_REVIEW_BARCODES or (
            notes and floor is not None and current is not None and floor > current
        ) or "shopify_active_duplicate_barcode" in notes:
            action = ACTION_REVIEW_COST_ANOMALY
            reason = "cost_anomaly;" + ";".join(notes) if notes else "cost_anomaly"
            proposed = floor
            delta = round((floor or 0) - (current or 0), 2) if floor and current else None
            pct = round((delta / current) * 100, 2) if delta is not None and current else None
        else:
            action, reason, proposed, delta, pct = decide_action(
                current=current,
                floor=floor,
                max_auto_increase=max_auto_increase,
                commercial_review_max=commercial_review_max,
            )

        # Economic floor is always recorded. Auto-eligible rows approve the floor.
        # Commercial/large reviews leave approved_* empty until a human-reviewed artifact.
        approved_price: Optional[float] = None
        approved_gp: Optional[float] = None
        exception_reason = ""
        if action == ACTION_PRICE_INCREASE_AUTO_ELIGIBLE and floor is not None:
            approved_price = floor
            approved_gp = _margin_pct(floor, landed)

        # Recognise explicit below-floor merchandising approvals (do not re-queue as errors).
        exc = pricing_exceptions.get(row["variant_id"]) if pricing_exceptions else None
        exc_gbp_at_approval: Optional[float] = None
        gbp_movement: Optional[float] = None
        floor_at_approval: Optional[float] = None
        if exc:
            exc_gbp_at_approval = _f(exc.get("source_cost_gbp_at_approval"))
            floor_at_approval = _f(exc.get("calculated_floor_at_approval"))
            if cost_gbp is not None and exc_gbp_at_approval is not None:
                gbp_movement = round(float(cost_gbp) - float(exc_gbp_at_approval), 4)
        if (
            exc
            and action
            in {
                ACTION_PRICE_INCREASE_AUTO_ELIGIBLE,
                ACTION_REVIEW_PRICE_INCREASE,
                ACTION_REVIEW_LARGE_INCREASE,
                ACTION_KEEP_CURRENT_PRICE,
                ACTION_NO_CHANGE,
            }
            and current is not None
            and floor is not None
        ):
            exc_approved = _f(exc.get("approved_retail_price"))
            if exc_approved is not None and abs(current - exc_approved) <= PRICE_MATCH_TOLERANCE:
                if gbp_cost_increased_materially(cost_gbp, exc_gbp_at_approval):
                    action = ACTION_REVIEW_PRICE_INCREASE
                    reason = "exception_reopen_gbp_cost_increased"
                    proposed = floor
                    delta = round(floor - current, 2)
                    pct = round((delta / current) * 100, 2) if current else None
                    approved_price = None
                    approved_gp = None
                    exception_reason = ""
                    notes.append(
                        f"exception_gbp_at_approval={exc_gbp_at_approval};current_gbp={cost_gbp}"
                    )
                else:
                    action = ACTION_APPROVED_PRICING_EXCEPTION
                    reason = "approved_market_positioning_exception"
                    proposed = current
                    delta = round(floor - current, 2)
                    pct = round((delta / current) * 100, 2) if current else None
                    approved_price = exc_approved
                    approved_gp = _margin_pct(current, landed)
                    exception_reason = (
                        clean_text(exc.get("pricing_exception_reason"))
                        or REASON_MARKET_POSITIONING_EXCEPTION
                    )
            elif (
                exc_approved is not None
                and action
                in {
                    ACTION_PRICE_INCREASE_AUTO_ELIGIBLE,
                    ACTION_REVIEW_PRICE_INCREASE,
                    ACTION_REVIEW_LARGE_INCREASE,
                }
            ):
                # Registry exists but live retail no longer matches the approved exception.
                # Do not treat the registry as authoritative for auto-apply; force review.
                action = ACTION_REVIEW_PRICE_INCREASE
                reason = "exception_live_price_mismatch"
                proposed = floor
                delta = round(floor - current, 2)
                pct = round((delta / current) * 100, 2) if current else None
                approved_price = None
                approved_gp = None
                exception_reason = (
                    clean_text(exc.get("pricing_exception_reason"))
                    or REASON_MARKET_POSITIONING_EXCEPTION
                )
                notes.append(
                    f"exception_approved_retail={exc_approved};live_retail={current}"
                )

        evaluated_at = datetime.now(timezone.utc).isoformat()
        rows.append(
            RepriceRow(
                title=row["title"],
                variant_title=row["variant_title"],
                barcode=row["barcode"],
                sku=row["sku"],
                product_id=row["product_id"],
                variant_id=row["variant_id"],
                studio_raw=row["studio_raw"],
                studio_norm=row["studio_norm"],
                region_raw=row["region_raw"],
                normalized_region=row["normalized_region"],
                product_type=row["product_type"],
                media_format=row["media_format"],
                product_class=row["product_class"],
                class_reason=row["class_reason"],
                collection_handles=row["collection_handles"],
                preferred_supplier=str(preferred.get("supplier_id") or ""),
                source_currency="GBP",
                source_cost_gbp=cost_gbp,
                gbp_aud_rate=gbp_aud,
                landed_aud_cost=landed,
                shopify_unit_cost_aud=shop_cost,
                current_retail=current,
                current_gp_pct=_margin_pct(current, landed),
                floor_retail=floor,
                proposed_retail=proposed,
                proposed_gp_pct=_margin_pct(
                    current
                    if action
                    in {ACTION_KEEP_CURRENT_PRICE, ACTION_APPROVED_PRICING_EXCEPTION}
                    else proposed,
                    landed,
                ),
                dollar_change=delta,
                pct_change=pct,
                action=action,
                reason=reason,
                calculated_floor_price=floor,
                approved_retail_price=approved_price,
                approved_gp_percent=approved_gp,
                pricing_exception_reason=exception_reason,
                source_cost_gbp_at_approval=exc_gbp_at_approval,
                gbp_movement_from_approval=gbp_movement,
                calculated_floor_at_approval=floor_at_approval,
                supplier_freshness=str(preferred.get("freshness") or ""),
                resolution_method=resolution,
                release_variant_id=str(rid or ""),
                data_notes=";".join(notes),
                evaluated_at=evaluated_at,
            )
        )
    return rows


def summarize_rows(
    rows: Sequence[RepriceRow],
    *,
    products_scanned: int,
    dry_run: bool,
    monitoring_only: bool,
    labels: Sequence[str],
) -> RepriceSummary:
    s = RepriceSummary(
        products_scanned=products_scanned,
        variants_examined=len(rows),
        films_assessed=sum(1 for r in rows if r.product_class == PRODUCT_CLASS_FILM),
        non_film_excluded=sum(1 for r in rows if r.action == ACTION_OUT_OF_SCOPE_NON_FILM),
        ambiguous_skipped=sum(1 for r in rows if r.action == ACTION_SKIP_AMBIGUOUS_PRODUCT),
        region_a_blocked=sum(1 for r in rows if r.action == ACTION_REGION_A_BLOCKED),
        dry_run=dry_run,
        monitoring_only=monitoring_only,
        labels=list(labels),
    )
    for r in rows:
        if r.action == ACTION_PRICE_INCREASE_AUTO_ELIGIBLE:
            s.price_increase += 1
        elif r.action == ACTION_REVIEW_PRICE_INCREASE:
            s.review_price_increase += 1
            if (r.reason or "") == "exception_reopen_gbp_cost_increased":
                s.exception_reopened += 1
        elif r.action == ACTION_REVIEW_LARGE_INCREASE:
            s.review_large_increase += 1
        elif r.action == ACTION_APPROVED_PRICING_EXCEPTION:
            s.approved_pricing_exception += 1
        elif r.action == ACTION_KEEP_CURRENT_PRICE:
            s.keep_current += 1
        elif r.action == ACTION_NO_CHANGE:
            s.no_change += 1
        elif r.action == ACTION_REVIEW_COST_ANOMALY:
            s.review_anomaly += 1
        elif r.action == ACTION_STALE_SUPPLIER:
            s.stale_supplier += 1
        elif r.action == ACTION_NO_CURRENT_COST:
            s.no_current_cost += 1
        elif r.action == ACTION_AMBIGUOUS_MAPPING:
            s.ambiguous_mapping += 1
        if r.apply_status == "ok":
            s.applied_ok += 1
        elif r.apply_status == "failed":
            s.applied_failed += 1
        elif r.apply_status.startswith("skip"):
            s.apply_skipped += 1
    return s


def write_rows_csv(path: Path, rows: Sequence[RepriceRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(RepriceRow.__dataclass_fields__.keys())
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))


def write_candidates_csv(path: Path, rows: Sequence[RepriceRow]) -> None:
    """AUTO_ELIGIBLE only — default reviewed allowlist source for automatic apply."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "title",
        "studio_norm",
        "barcode",
        "sku",
        "variant_id",
        "product_id",
        "preferred_supplier",
        "source_cost_gbp",
        "landed_aud_cost",
        "current_retail",
        "floor_retail",
        "calculated_floor_price",
        "proposed_retail",
        "approved_retail_price",
        "approved_gp_percent",
        "pricing_exception_reason",
        "dollar_change",
        "current_gp_pct",
        "proposed_gp_pct",
        "action",
        "reason",
        "supplier_freshness",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in rows:
            if r.action != ACTION_PRICE_INCREASE_AUTO_ELIGIBLE:
                continue
            w.writerow({k: getattr(r, k) for k in fields})


def auto_apply_targets(
    rows: Sequence[RepriceRow],
    allowlist: ApplyAllowlist,
) -> List[RepriceRow]:
    """Allowlisted apply targets: AUTO_ELIGIBLE or explicit reviewed price overrides.

    Below-floor overrides require pricing_exception_reason on the row (from a
    reviewed artifact merge). Non-film / non-Region-B never included.
    """
    if allowlist.empty:
        return []
    out = []
    for r in rows:
        if r.product_class != PRODUCT_CLASS_FILM:
            continue
        if r.normalized_region != "B":
            continue
        if not allowlist.matches(
            {"variant_id": r.variant_id, "barcode": r.barcode}
        ):
            continue
        has_reviewed_override = (
            r.approved_retail_price is not None
            and bool((r.pricing_exception_reason or "").strip())
        )
        if r.action == ACTION_PRICE_INCREASE_AUTO_ELIGIBLE or has_reviewed_override:
            out.append(r)
    return out


def effective_apply_price(row: RepriceRow, floor: Optional[float]) -> Optional[float]:
    """Price to mutate: explicit reviewed override if present, else calculated floor.

    Below-floor overrides require pricing_exception_reason and are never invented here.
    """
    approved = row.approved_retail_price
    if approved is not None:
        if floor is not None and approved + PRICE_MATCH_TOLERANCE < floor:
            if not (row.pricing_exception_reason or "").strip():
                return None  # refuse silent below-floor override
        return float(approved)
    return floor if floor is not None else row.proposed_retail


def revalidate_for_apply(
    client: ShopifyClient,
    row: RepriceRow,
    *,
    supabase: Any,
    soundtrack_product_ids: Set[str],
    gbp_aud: float,
    landed_markup: float,
    margin_floor: float,
    max_auto_increase: float,
    anomaly_pct: float,
    now: datetime,
) -> tuple[bool, str, Optional[float], Optional[float]]:
    """
    Live revalidation at apply time.

    Returns (ok, reason, live_price, recomputed_floor).
    """
    data = _gql(client, LIVE_VARIANT_QUERY, {"id": row.variant_id})
    node = data.get("node") or {}
    if not node:
        return False, "live_variant_missing", None, None

    product = node.get("product") or {}
    live_price = _f(node.get("price"))
    live_barcode = clean_text(node.get("barcode")) or ""
    title = clean_text(product.get("title")) or row.title
    product_id = clean_text(product.get("id")) or row.product_id
    product_type = clean_text(product.get("productType")) or ""
    format_value = clean_text((product.get("formatMeta") or {}).get("value")) or ""
    tags = list(product.get("tags") or [])
    handles = {
        (clean_text(n.get("handle")) or "").casefold()
        for n in ((product.get("collections") or {}).get("nodes") or [])
        if clean_text((n or {}).get("handle"))
    }
    product_region = clean_text((product.get("region") or {}).get("value")) or ""
    variant_region = clean_text((node.get("region") or {}).get("value")) or ""
    region = resolve_variant_region(
        product_region=product_region, variant_region=variant_region
    )

    product_class, class_reason = classify_product_class(
        title=title,
        product_id=product_id,
        product_type=product_type,
        format_value=format_value,
        media_format="",
        tags=tags,
        collection_handles=handles,
        soundtrack_product_ids=soundtrack_product_ids,
    )
    if product_class != PRODUCT_CLASS_FILM:
        action, _ = product_class_to_action(product_class, class_reason)
        return False, f"apply_blocked_{action}:{class_reason}", live_price, None

    if region != "B":
        return False, f"apply_blocked_region_{region or 'blank'}", live_price, None

    if live_barcode and row.barcode and live_barcode != row.barcode:
        return False, "apply_skip_barcode_drift", live_price, None

    if row.current_retail is not None and live_price is not None:
        if abs(live_price - row.current_retail) > PRICE_MATCH_TOLERANCE:
            # Idempotent: already at/above proposed floor
            if row.proposed_retail is not None and live_price + PRICE_MATCH_TOLERANCE >= row.proposed_retail:
                return False, "apply_skip_already_at_or_above_proposed", live_price, None
            return False, "apply_skip_live_price_drift", live_price, None

    ctx = _load_supplier_context(
        supabase, variant_ids=[row.variant_id], barcodes=[live_barcode or row.barcode]
    )
    rid = ctx["rsl"].get(row.variant_id)
    offers: List[dict[str, Any]] = []
    if rid:
        offers = list(ctx["offers_by_release"].get(str(rid), []))
    if not offers and (live_barcode or row.barcode):
        barcode_offers = list(
            ctx["offers_by_barcode"].get(live_barcode or row.barcode, [])
        )
        release_ids = sorted(
            {
                str(o.get("release_variant_id") or "")
                for o in barcode_offers
                if o.get("release_variant_id")
            }
        )
        if len(release_ids) > 1:
            return False, "apply_skip_ambiguous_barcode_supplier_resolution", live_price, None
        offers = barcode_offers
    if not offers:
        return False, "apply_skip_no_supplier_offer", live_price, None

    evaluated = _eval_offers(offers, suppliers=ctx["suppliers"], now=now)
    if not supplier_is_usable(evaluated):
        return False, "apply_skip_supplier_not_usable", live_price, None
    preferred = pick_preferred_supplier(to_preferred_pool(evaluated))
    cost_gbp = _f((preferred or {}).get("unit_cost"))
    if cost_gbp is None or cost_gbp <= 0:
        return False, "apply_skip_invalid_gbp_cost", live_price, None
    if row.source_cost_gbp is not None and abs(cost_gbp - row.source_cost_gbp) >= 0.01:
        return False, "apply_skip_gbp_cost_drift", live_price, None

    floor = calculate_sale_price_with_margin_floor_from_gbp_cost(
        cost_gbp,
        gbp_aud_rate=gbp_aud,
        landed_cost_markup=landed_markup,
        margin_floor_ratio=margin_floor,
    )
    if floor is None:
        return False, "apply_skip_floor_missing", live_price, floor
    # Calculated floor must still match the artifact's recorded floor (economic authority).
    artifact_floor = row.calculated_floor_price if row.calculated_floor_price is not None else row.proposed_retail
    if artifact_floor is None or abs(floor - artifact_floor) > FLOOR_MATCH_TOLERANCE:
        return False, "apply_skip_floor_drift", live_price, floor

    apply_price = effective_apply_price(row, floor)
    if apply_price is None:
        return False, "apply_skip_invalid_below_floor_override", live_price, floor

    if live_price is None:
        return False, "apply_skip_missing_live_price", live_price, floor
    delta = round(apply_price - live_price, 2)
    if delta <= 0:
        return False, "apply_skip_no_increase_needed", live_price, floor
    # Auto path: never mutate more than max_auto from live without commercial review.
    # Explicit below-floor exceptions still require delta ≤ max_auto unless reason is set
    # and approved was human-reviewed (commercial band handled outside auto_apply_targets).
    if delta > max_auto_increase and not (row.pricing_exception_reason or "").strip():
        return False, "apply_skip_above_max_auto_increase", live_price, floor

    barcode = live_barcode or row.barcode
    if barcode in HARD_REVIEW_BARCODES:
        return False, "apply_skip_hard_review_barcode", live_price, floor

    landed = replacement_landed_cost_aud(
        cost_gbp, gbp_aud_rate=gbp_aud, landed_cost_markup=landed_markup
    )
    cost_obj = (node.get("inventoryItem") or {}).get("unitCost") or {}
    shop_cost = _f(cost_obj.get("amount")) if (cost_obj.get("currencyCode") or "AUD") == "AUD" else None
    if shop_cost and landed and landed > 0:
        drift = abs(shop_cost - landed) / landed
        if drift >= anomaly_pct:
            return False, "apply_skip_cost_anomaly", live_price, floor

    return True, "ok", live_price, floor


def apply_price_updates(
    client: ShopifyClient,
    rows: Sequence[RepriceRow],
    *,
    supabase: Any,
    soundtrack_product_ids: Set[str],
    gbp_aud: float,
    landed_markup: float,
    margin_floor: float,
    max_auto_increase: float,
    anomaly_pct: float,
) -> Tuple[int, int, int]:
    if os.getenv("REGION_B_PRICING_HEALTH_READONLY", "").strip() == "1":
        raise RuntimeError(
            "REGION_B_PRICING_HEALTH_READONLY=1 — refuse Shopify price mutations"
        )
    ok = failed = skipped = 0
    now = datetime.now(timezone.utc)
    for r in rows:
        # Defense in depth: refuse non-film even if caller mis-filtered.
        if r.product_class != PRODUCT_CLASS_FILM:
            r.apply_status = "skip_non_film"
            r.apply_error = r.class_reason
            skipped += 1
            continue
        passed, reason, live_price, floor = revalidate_for_apply(
            client,
            r,
            supabase=supabase,
            soundtrack_product_ids=soundtrack_product_ids,
            gbp_aud=gbp_aud,
            landed_markup=landed_markup,
            margin_floor=margin_floor,
            max_auto_increase=max_auto_increase,
            anomaly_pct=anomaly_pct,
            now=now,
        )
        r.live_price_at_apply = live_price
        if not passed:
            r.apply_status = f"skip:{reason}"
            r.apply_error = reason
            skipped += 1
            continue
        new_price_val = effective_apply_price(r, floor)
        if new_price_val is None:
            r.apply_status = "skip:apply_skip_invalid_below_floor_override"
            r.apply_error = "invalid_below_floor_override"
            skipped += 1
            continue
        new_price = f"{new_price_val:.2f}"
        try:
            data = _gql(
                client,
                VARIANT_PRICE_UPDATE,
                {
                    "productId": r.product_id,
                    "variants": [{"id": r.variant_id, "price": new_price}],
                },
            )
            block = data.get("productVariantsBulkUpdate") or {}
            errs = block.get("userErrors") or []
            if errs:
                raise RuntimeError(str(errs))
            r.apply_status = "ok"
            r.action = ACTION_APPLIED
            r.current_retail = float(new_price)
            ok += 1
            time.sleep(0.08)
        except Exception as exc:
            r.apply_status = "failed"
            r.apply_error = str(exc)
            failed += 1
            time.sleep(0.2)
    return ok, failed, skipped


def run_region_b_film_repricing(
    *,
    env_file: str = ".env",
    api_version: str = "2026-04",
    apply: bool = False,
    allowlist: Optional[ApplyAllowlist] = None,
    labels: Optional[Sequence[str]] = None,
    max_auto_increase: float = DEFAULT_MAX_AUTO_INCREASE,
    commercial_review_max: float = DEFAULT_COMMERCIAL_REVIEW_MAX,
    reviewed_artifact_csv: Optional[str] = None,
    csv_path: Optional[str] = None,
    json_path: Optional[str] = None,
    candidates_csv_path: Optional[str] = None,
    client: Optional[ShopifyClient] = None,
    supabase: Any = None,
) -> Tuple[List[RepriceRow], RepriceSummary]:
    root = _repo_root()
    path = Path(env_file)
    if not path.is_absolute():
        path = root / path
    load_dotenv(path, override=True)
    cfg = log_pricing_assumptions(logger)
    gbp_aud = float(cfg["gbp_aud_rate"])
    landed_markup = float(cfg["landed_cost_markup"])
    margin_floor = float(cfg["margin_floor_ratio"])
    anomaly_pct = float(cfg.get("supplier_cost_anomaly_pct") or DEFAULT_COST_ANOMALY_PCT)

    allowlist = allowlist or parse_apply_allowlist()
    scoped_apply = bool(apply and not allowlist.empty)
    shopify = client or ShopifyClient(api_version=api_version)
    if supabase is None:
        supabase = create_fresh_client()

    soundtrack_ids = fetch_soundtracks_collection_product_ids(shopify)
    scanned, products = fetch_active_products(shopify)
    rows = build_candidate_rows(
        products,
        supabase=supabase,
        soundtrack_product_ids=soundtrack_ids,
        labels=labels,
        gbp_aud=gbp_aud,
        landed_markup=landed_markup,
        margin_floor=margin_floor,
        max_auto_increase=max_auto_increase,
        commercial_review_max=commercial_review_max,
        anomaly_pct=anomaly_pct,
    )

    overrides = load_reviewed_artifact_overrides(reviewed_artifact_csv)
    if overrides:
        apply_reviewed_overrides_to_rows(rows, overrides)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = Path(csv_path or root / "tmp" / f"region_b_film_repricing_{stamp}.csv")
    out_json = Path(json_path or root / "tmp" / f"region_b_film_repricing_{stamp}.json")
    out_cand = Path(
        candidates_csv_path or root / "tmp" / f"region_b_film_repricing_candidates_{stamp}.csv"
    )
    write_rows_csv(out_csv, rows)
    write_candidates_csv(out_cand, rows)

    applied_ok = applied_failed = apply_skipped = 0
    if apply and allowlist.empty:
        logger.warning(
            "REGION_B_FILM_REPRICING --apply requested but allowlist empty — zero mutations"
        )
        print("REGION_B_FILM_REPRICING apply_skipped=allowlist_empty", flush=True)
    elif scoped_apply:
        targets = auto_apply_targets(rows, allowlist)
        # Also refuse allowlisted non-film rows that somehow appear (defense).
        for r in rows:
            if allowlist.matches({"variant_id": r.variant_id, "barcode": r.barcode}):
                if r.product_class != PRODUCT_CLASS_FILM:
                    r.apply_status = "skip_non_film_allowlist_ignored"
                    r.apply_error = r.class_reason
                    apply_skipped += 1
        applied_ok, applied_failed, skipped = apply_price_updates(
            shopify,
            targets,
            supabase=supabase,
            soundtrack_product_ids=soundtrack_ids,
            gbp_aud=gbp_aud,
            landed_markup=landed_markup,
            margin_floor=margin_floor,
            max_auto_increase=max_auto_increase,
            anomaly_pct=anomaly_pct,
        )
        apply_skipped += skipped
        # Persist successful below-floor merchandising exceptions for future scans.
        exception_records = []
        for r in targets:
            if r.apply_status != "ok":
                continue
            if not (r.pricing_exception_reason or "").strip():
                continue
            if r.approved_retail_price is None:
                continue
            exception_records.append(
                {
                    "variant_id": r.variant_id,
                    "barcode": r.barcode,
                    "title": r.title,
                    "approved_retail_price": r.approved_retail_price,
                    "calculated_floor_at_approval": r.calculated_floor_price or r.floor_retail,
                    "source_cost_gbp_at_approval": r.source_cost_gbp,
                    "pricing_exception_reason": r.pricing_exception_reason,
                    "approved_gp_percent": r.approved_gp_percent,
                    "preferred_supplier": r.preferred_supplier,
                }
            )
        if exception_records:
            upsert_pricing_exceptions(exception_records, path=pricing_exceptions_path(root))
        write_rows_csv(out_csv, rows)

    summary = summarize_rows(
        rows,
        products_scanned=scanned,
        dry_run=not scoped_apply,
        monitoring_only=not scoped_apply,
        labels=labels or [],
    )
    summary.applied_ok = applied_ok
    summary.applied_failed = applied_failed
    summary.apply_skipped = apply_skipped
    summary.csv_path = str(out_csv)
    summary.json_path = str(out_json)
    summary.candidates_csv_path = str(out_cand)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": summary.dry_run,
        "mutations": scoped_apply,
        "pricing_assumptions": cfg,
        "max_auto_increase": max_auto_increase,
        "commercial_review_max": commercial_review_max,
        "labels": list(labels or []),
        "hard_review_barcodes": sorted(HARD_REVIEW_BARCODES),
        "summary": asdict(summary),
        "action_counts": {
            ACTION_PRICE_INCREASE_AUTO_ELIGIBLE: summary.price_increase,
            ACTION_REVIEW_PRICE_INCREASE: summary.review_price_increase,
            ACTION_REVIEW_LARGE_INCREASE: summary.review_large_increase,
            ACTION_APPROVED_PRICING_EXCEPTION: summary.approved_pricing_exception,
            ACTION_KEEP_CURRENT_PRICE: summary.keep_current,
            ACTION_NO_CHANGE: summary.no_change,
            ACTION_REVIEW_COST_ANOMALY: summary.review_anomaly,
            ACTION_OUT_OF_SCOPE_NON_FILM: summary.non_film_excluded,
            ACTION_SKIP_AMBIGUOUS_PRODUCT: summary.ambiguous_skipped,
            ACTION_REGION_A_BLOCKED: summary.region_a_blocked,
        },
        "price_increase_auto_eligible": [
            asdict(r) for r in rows if r.action == ACTION_PRICE_INCREASE_AUTO_ELIGIBLE
        ],
        "review_price_increase": [
            asdict(r) for r in rows if r.action == ACTION_REVIEW_PRICE_INCREASE
        ],
        "approved_pricing_exceptions": [
            asdict(r) for r in rows if r.action == ACTION_APPROVED_PRICING_EXCEPTION
        ],
        "review_large_increase": [
            asdict(r) for r in rows if r.action == ACTION_REVIEW_LARGE_INCREASE
        ],
        "review_anomalies": [
            asdict(r) for r in rows if r.action == ACTION_REVIEW_COST_ANOMALY
        ],
    }
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return rows, summary


def pricing_health_dir(root: Optional[Path] = None) -> Path:
    return (root or _repo_root()) / PRICING_HEALTH_DIR_REL


def format_pricing_health_status_line(summary: RepriceSummary) -> str:
    return (
        "REGION_B_PRICING_HEALTH_STATUS="
        f"ok evaluated={summary.variants_examined} "
        f"films={summary.films_assessed} "
        f"no_change={summary.no_change} "
        f"keep_current={summary.keep_current} "
        f"auto_eligible={summary.price_increase} "
        f"review={summary.review_price_increase} "
        f"large_review={summary.review_large_increase} "
        f"approved_exception={summary.approved_pricing_exception} "
        f"exception_reopened={summary.exception_reopened} "
        f"anomaly={summary.review_anomaly} "
        f"stale={summary.stale_supplier} "
        f"no_cost={summary.no_current_cost} "
        f"ambiguous={summary.ambiguous_mapping} "
        f"region_a={summary.region_a_blocked} "
        f"non_film={summary.non_film_excluded} "
        f"shopify_mutations=0"
    )


def run_region_b_pricing_health(
    *,
    env_file: str = ".env.prod",
    api_version: str = "2026-04",
    labels: Optional[Sequence[str]] = None,
    max_auto_increase: float = DEFAULT_MAX_AUTO_INCREASE,
    commercial_review_max: float = DEFAULT_COMMERCIAL_REVIEW_MAX,
    client: Optional[ShopifyClient] = None,
    supabase: Any = None,
) -> Tuple[List[RepriceRow], RepriceSummary, dict[str, Any]]:
    """Read-only Region B film pricing-health evaluation.

    Explicitly incapable of Shopify mutations:
      * no --apply path
      * sets REGION_B_PRICING_HEALTH_READONLY=1 for the process
      * never calls apply_price_updates
    """
    os.environ["REGION_B_PRICING_HEALTH_READONLY"] = "1"
    root = _repo_root()
    path = Path(env_file)
    if not path.is_absolute():
        path = root / path
    load_dotenv(path, override=True)
    cfg = log_pricing_assumptions(logger)
    gbp_aud = float(cfg["gbp_aud_rate"])
    landed_markup = float(cfg["landed_cost_markup"])
    margin_floor = float(cfg["margin_floor_ratio"])
    anomaly_pct = float(cfg.get("supplier_cost_anomaly_pct") or DEFAULT_COST_ANOMALY_PCT)

    shopify = client or ShopifyClient(api_version=api_version)
    if supabase is None:
        supabase = create_fresh_client()

    soundtrack_ids = fetch_soundtracks_collection_product_ids(shopify)
    scanned, products = fetch_active_products(shopify)
    rows = build_candidate_rows(
        products,
        supabase=supabase,
        soundtrack_product_ids=soundtrack_ids,
        labels=labels,
        gbp_aud=gbp_aud,
        landed_markup=landed_markup,
        margin_floor=margin_floor,
        max_auto_increase=max_auto_increase,
        commercial_review_max=commercial_review_max,
        anomaly_pct=anomaly_pct,
    )
    summary = summarize_rows(
        rows,
        products_scanned=scanned,
        dry_run=True,
        monitoring_only=True,
        labels=labels or [],
    )

    evaluated_at = datetime.now(timezone.utc).isoformat()
    health_dir = pricing_health_dir(root)
    health_dir.mkdir(parents=True, exist_ok=True)
    history_dir = health_dir / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    gbp_costs = [r.source_cost_gbp for r in rows if r.source_cost_gbp is not None]
    exceptions_loaded = load_pricing_exceptions(pricing_exceptions_path(root))

    action_counts = {
        ACTION_NO_CHANGE: summary.no_change,
        ACTION_KEEP_CURRENT_PRICE: summary.keep_current,
        ACTION_PRICE_INCREASE_AUTO_ELIGIBLE: summary.price_increase,
        ACTION_REVIEW_PRICE_INCREASE: summary.review_price_increase,
        ACTION_REVIEW_LARGE_INCREASE: summary.review_large_increase,
        ACTION_APPROVED_PRICING_EXCEPTION: summary.approved_pricing_exception,
        "EXCEPTION_REOPENED": summary.exception_reopened,
        ACTION_REVIEW_COST_ANOMALY: summary.review_anomaly,
        ACTION_STALE_SUPPLIER: summary.stale_supplier,
        ACTION_NO_CURRENT_COST: summary.no_current_cost,
        ACTION_AMBIGUOUS_MAPPING: summary.ambiguous_mapping,
        ACTION_REGION_A_BLOCKED: summary.region_a_blocked,
        ACTION_OUT_OF_SCOPE_NON_FILM: summary.non_film_excluded,
        ACTION_SKIP_AMBIGUOUS_PRODUCT: summary.ambiguous_skipped,
    }

    payload: dict[str, Any] = {
        "schema_version": 1,
        "job": "region_b_pricing_health",
        "read_only": True,
        "shopify_mutations": 0,
        "inventory_policy_mutations": 0,
        "inventory_quantity_mutations": 0,
        "evaluated_at": evaluated_at,
        "pricing_assumptions": cfg,
        "exception_registry_path": str(pricing_exceptions_path(root)),
        "exception_registry_count": len(exceptions_loaded),
        "summary": asdict(summary),
        "action_counts": action_counts,
        "rows": [asdict(r) for r in rows],
        "approved_pricing_exceptions": [
            asdict(r) for r in rows if r.action == ACTION_APPROVED_PRICING_EXCEPTION
        ],
        "exception_reopened": [
            asdict(r)
            for r in rows
            if (r.reason or "") == "exception_reopen_gbp_cost_increased"
        ],
        "metrics": {
            "products_scanned": summary.products_scanned,
            "variants_examined": summary.variants_examined,
            "eligible_region_b_films": summary.films_assessed,
            "supplier_gbp_observations": len(gbp_costs),
        },
    }

    latest_json = health_dir / PRICING_HEALTH_LATEST_JSON
    latest_csv = health_dir / PRICING_HEALTH_LATEST_CSV
    meta_json = health_dir / PRICING_HEALTH_META_JSON
    hist_json = history_dir / f"region_b_pricing_health_{stamp}.json"

    latest_json.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    hist_json.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    write_rows_csv(latest_csv, rows)
    meta = {
        "last_successful_evaluation_at": evaluated_at,
        "latest_json": str(latest_json),
        "latest_csv": str(latest_csv),
        "history_json": str(hist_json),
        "action_counts": action_counts,
        "shopify_mutations": 0,
        "read_only": True,
    }
    meta_json.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    summary.json_path = str(latest_json)
    summary.csv_path = str(latest_csv)
    print(format_pricing_health_status_line(summary), flush=True)
    print(f"REGION_B_PRICING_HEALTH_OUTPUT={latest_json}", flush=True)
    return rows, summary, payload
