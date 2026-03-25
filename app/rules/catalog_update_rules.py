"""
Catalog update whitelists and protection rules.

Controls which fields may be written to catalog_items during the upsert step,
depending on whether the pipeline is running in stock-sync or catalog-sync mode.

Write boundary:
  - Only fields listed in the active whitelist may be written to catalog_items
  - TMDB fields and film linkage fields are NEVER written by the upsert step

Two modes:
  Stock Sync  (--existing-only)  -> commercial fields only, **excluding** `media_release_date`
                                   (stock sync does not run harmonization; release dates are
                                   refreshed on catalog sync only)
  Catalog Sync (default)         -> COMMERCIAL_FIELDS + IDENTITY_FIELDS (includes `media_release_date`)

Protected fields (never touched by upsert, regardless of mode):
  TMDB_PROTECTED_FIELDS    — owned exclusively by enrich_catalog_with_tmdb
  FILM_LINK_PROTECTED_FIELDS — owned exclusively by build_films_from_catalog
"""

from typing import Any, Dict


COMMERCIAL_FIELDS = frozenset({
    "supplier_stock_status",
    "availability_status",
    "cost_price",
    "calculated_sale_price",
    "media_release_date",
    "supplier_last_seen_at",
})

IDENTITY_FIELDS = frozenset({
    "title",
    "format",
    "director",
    "studio",
})

# Stock sync: prices, qty, availability, last_seen — not release dates (no step-04 harmonization).
STOCK_SYNC_WHITELIST = frozenset(COMMERCIAL_FIELDS - {"media_release_date"})
CATALOG_SYNC_WHITELIST = COMMERCIAL_FIELDS | IDENTITY_FIELDS

TMDB_PROTECTED_FIELDS = frozenset({
    "tmdb_id",
    "tmdb_title",
    "tmdb_match_status",
    "tmdb_last_refreshed_at",
    "tmdb_poster_path",
    "tmdb_backdrop_path",
    "tmdb_vote_average",
    "tmdb_vote_count",
    "tmdb_popularity",
    "genres",
    "top_cast",
    "country_of_origin",
    "film_released",
})

FILM_LINK_PROTECTED_FIELDS = frozenset({
    "film_id",
    "film_link_status",
    "film_link_method",
    "film_linked_at",
})

ALL_PROTECTED_FIELDS = TMDB_PROTECTED_FIELDS | FILM_LINK_PROTECTED_FIELDS


def get_update_whitelist(*, existing_only: bool) -> frozenset[str]:
    """Return the correct field whitelist for the current sync mode."""
    return STOCK_SYNC_WHITELIST if existing_only else CATALOG_SYNC_WHITELIST


def filter_update_payload(
    payload: Dict[str, Any], whitelist: frozenset[str]
) -> Dict[str, Any]:
    """Strip a payload down to only whitelisted fields."""
    return {k: v for k, v in payload.items() if k in whitelist}


def validate_payload_safety(payload: Dict[str, Any]) -> list[str]:
    """Return a list of field names that violate protection rules (should be empty)."""
    return [k for k in payload if k in ALL_PROTECTED_FIELDS]
