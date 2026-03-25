"""
Supplier precedence and best-offer selection rules.

Used in two contexts:
  1. Shopify publisher — pick_best_row selects which supplier's offer to publish
  2. Film builder — pick_representative selects which catalog row provides
     metadata for the canonical films record

Precedence hierarchy:
  Tape Film (own stock)  -> 0 (highest)
  Moovies                -> 1
  Lasgo                  -> 2
  Unknown / other        -> 9
"""

from typing import Any, Dict, List, Optional


SUPPLIER_PRIORITY = {
    "Tape Film": 0,
    "moovies": 1,
    "Moovies": 1,
    "lasgo": 2,
    "Lasgo": 2,
}


def supplier_rank(supplier: str) -> int:
    return SUPPLIER_PRIORITY.get(supplier, 9)


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _avail_rank(value: Optional[str]) -> int:
    t = (value or "").lower()
    if t == "in_stock":
        return 0
    if t == "supplier_stock":
        return 1
    if t == "supplier_preorder":
        return 2
    if t == "supplier_out":
        return 3
    return 9


def pick_best_row(
    rows: List[Dict[str, Any]],
    supplier_preference: str = "best_offer",
) -> Optional[Dict[str, Any]]:
    """
    Select the best supplier offer for a given barcode.

    Ranking criteria (in order):
      1. Tape Film stock preferred over third-party
      2. Availability status (in_stock > supplier_stock > preorder > out)
      3. Higher stock quantity
      4. Lower source_priority number
      5. Lower price
      6. Has a title
    """
    if not rows:
        return None

    filtered = rows
    if supplier_preference != "best_offer":
        wanted = supplier_preference.strip().lower()
        filtered = [
            r for r in rows
            if (_clean(r.get("supplier")) or "").strip().lower() == wanted
        ]
        if not filtered:
            return None

    def sort_key(r: Dict[str, Any]):
        supplier = _clean(r.get("supplier")) or ""
        stock = int(r.get("supplier_stock_status") or 0)
        price = float(r.get("calculated_sale_price") or 999999)
        src_pri = int(r.get("supplier_priority") or 99)
        return (
            0 if supplier == "Tape Film" else 1,
            _avail_rank(_clean(r.get("availability_status"))),
            -stock,
            src_pri,
            price,
            0 if _clean(r.get("title")) else 1,
        )

    return sorted(filtered, key=sort_key)[0]


def pick_representative(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Select the most metadata-rich catalog row to represent a film.
    Used by build_films_from_catalog to populate the films table.

    Ranking criteria:
      1. Metadata richness score (tmdb_id, title, genres, cast, etc.)
      2. Supplier priority
      3. Has a title
    """
    def richness_score(row: Dict[str, Any]) -> int:
        score = 0
        if row.get("tmdb_id"):
            score += 100
        if _clean(row.get("tmdb_title")):
            score += 50
        if _clean(row.get("genres")):
            score += 10
        if _clean(row.get("top_cast")):
            score += 10
        if _clean(row.get("country_of_origin")):
            score += 10
        if row.get("tmdb_vote_count") is not None:
            score += 5
        if row.get("tmdb_vote_average") is not None:
            score += 5
        if row.get("tmdb_popularity") is not None:
            score += 5
        if _clean(row.get("tmdb_poster_path")):
            score += 3
        if _clean(row.get("tmdb_backdrop_path")):
            score += 3
        return score

    return sorted(
        rows,
        key=lambda r: (
            -richness_score(r),
            supplier_rank(_clean(r.get("supplier")) or ""),
            0 if _clean(r.get("title")) else 1,
        ),
    )[0]
