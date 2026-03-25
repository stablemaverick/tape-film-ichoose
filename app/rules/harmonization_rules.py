"""
Cross-supplier harmonization rules.

These rules define which supplier's data wins for each identity field
when multiple suppliers share the same barcode.

Write boundary:
  - Only harmonized_* fields and media_release_date in staging_supplier_offers
  - Never touches commercial fields, catalog_items, or films

Field ownership:
  harmonized_title         <- Lasgo  (cleaner, more accurate titles)
  harmonized_format        <- Moovies (richer format descriptions)
  harmonized_studio        <- Moovies (more complete studio data)
  harmonized_director      <- Moovies (more complete director data)
  media_release_date       <- Lasgo  (trusted daily source)

Fields NEVER touched by harmonization:
  supplier, supplier_sku, supplier_stock_status, availability_status,
  cost_price, calculated_sale_price, supplier_currency, source_priority,
  source_type, active, barcode, title (native), format (native),
  director (native), studio (native), normalized_title, edition_title
"""

from typing import Any, Dict, List, Optional


HARMONIZE_WRITABLE_FIELDS = frozenset({
    "harmonized_title",
    "harmonized_format",
    "harmonized_studio",
    "harmonized_director",
    "harmonized_from_supplier",
    "harmonized_at",
    "media_release_date",
})

HARMONIZE_NEVER_TOUCH = frozenset({
    "supplier",
    "supplier_sku",
    "supplier_stock_status",
    "availability_status",
    "cost_price",
    "calculated_sale_price",
    "supplier_currency",
    "source_priority",
    "source_type",
    "active",
    "barcode",
    "title",
    "format",
    "director",
    "studio",
    "normalized_title",
    "edition_title",
})


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _supplier_map(group: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {(_clean(r.get("supplier")) or "").lower(): r for r in group}


def pick_best_title(group: List[Dict[str, Any]]) -> Optional[str]:
    """Lasgo title wins if present (cleaner titles), else Moovies, else any."""
    by = _supplier_map(group)
    lasgo = by.get("lasgo")
    if lasgo and _clean(lasgo.get("title")):
        return _clean(lasgo["title"])
    moovies = by.get("moovies")
    if moovies and _clean(moovies.get("title")):
        return _clean(moovies["title"])
    for row in group:
        t = _clean(row.get("title"))
        if t:
            return t
    return None


def pick_best_format(group: List[Dict[str, Any]]) -> Optional[str]:
    """Moovies format wins if present (richer format data), else Lasgo, else any."""
    by = _supplier_map(group)
    moovies = by.get("moovies")
    if moovies and _clean(moovies.get("format")):
        return _clean(moovies["format"])
    lasgo = by.get("lasgo")
    if lasgo and _clean(lasgo.get("format")):
        return _clean(lasgo["format"])
    for row in group:
        f = _clean(row.get("format"))
        if f:
            return f
    return None


def pick_best_studio(group: List[Dict[str, Any]]) -> Optional[str]:
    """Moovies studio wins if present, else Lasgo, else any."""
    by = _supplier_map(group)
    moovies = by.get("moovies")
    if moovies and _clean(moovies.get("studio")):
        return _clean(moovies["studio"])
    lasgo = by.get("lasgo")
    if lasgo and _clean(lasgo.get("studio")):
        return _clean(lasgo["studio"])
    for row in group:
        s = _clean(row.get("studio"))
        if s:
            return s
    return None


def pick_best_director(group: List[Dict[str, Any]]) -> Optional[str]:
    """Moovies director wins if present, else Lasgo, else any."""
    by = _supplier_map(group)
    moovies = by.get("moovies")
    if moovies and _clean(moovies.get("director")):
        return _clean(moovies["director"])
    lasgo = by.get("lasgo")
    if lasgo and _clean(lasgo.get("director")):
        return _clean(lasgo["director"])
    for row in group:
        d = _clean(row.get("director"))
        if d:
            return d
    return None


def pick_best_release_date(group: List[Dict[str, Any]]) -> Optional[str]:
    """Lasgo release date wins if present (trusted daily source), else Moovies, else any."""
    by = _supplier_map(group)
    lasgo = by.get("lasgo")
    if lasgo and lasgo.get("media_release_date"):
        return lasgo["media_release_date"]
    moovies = by.get("moovies")
    if moovies and moovies.get("media_release_date"):
        return moovies["media_release_date"]
    for row in group:
        if row.get("media_release_date"):
            return row["media_release_date"]
    return None


def determine_leader_supplier(group: List[Dict[str, Any]]) -> str:
    """Which supplier contributed the most harmonized fields."""
    by = _supplier_map(group)
    if "moovies" in by:
        return "moovies"
    if "lasgo" in by:
        return "lasgo"
    return _clean(group[0].get("supplier")) or "unknown"


def compute_harmonized_update(
    row: Dict[str, Any],
    best_title: Optional[str],
    best_format: Optional[str],
    best_studio: Optional[str],
    best_director: Optional[str],
    best_release_date: Optional[str],
    leader_supplier: str,
    timestamp: str,
) -> Optional[Dict[str, Any]]:
    """Return an update payload if any harmonized field differs, else None."""
    changes: Dict[str, Any] = {}

    if best_title and _clean(row.get("harmonized_title")) != best_title:
        changes["harmonized_title"] = best_title
    if best_format and _clean(row.get("harmonized_format")) != best_format:
        changes["harmonized_format"] = best_format
    if best_studio and _clean(row.get("harmonized_studio")) != best_studio:
        changes["harmonized_studio"] = best_studio
    if best_director and _clean(row.get("harmonized_director")) != best_director:
        changes["harmonized_director"] = best_director
    if best_release_date and row.get("media_release_date") != best_release_date:
        changes["media_release_date"] = best_release_date

    if not changes:
        return None

    changes["harmonized_from_supplier"] = leader_supplier
    changes["harmonized_at"] = timestamp
    return changes
