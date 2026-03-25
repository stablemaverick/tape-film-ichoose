"""
Live catalog + film metrics for health reports and pipeline run history.

Thresholds are read from the environment on each call (after load_dotenv).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.rules.content_classification_rules import classify_content_type

EXIT_OK = 0
EXIT_WARNING = 1
EXIT_CRITICAL = 2


def compute_film_link_pct(linked: int, total_catalog: int) -> float:
    """Percentage of active catalog rows with a non-null film_id (rounded 1 decimal)."""
    return round(linked / total_catalog * 100, 1) if total_catalog > 0 else 0.0


def compute_tmdb_match_rate_pct(matched: int, not_found: int) -> float:
    """TMDB match rate among rows that have a resolved matched/not_found status."""
    denom = matched + not_found
    return round(matched / denom * 100, 1) if denom > 0 else 0.0


def compute_missing_price_pct(missing_price: int, total_catalog: int) -> float:
    return round(missing_price / total_catalog * 100, 1) if total_catalog > 0 else 0.0


def resolve_exit_code_from_alerts(alerts: List[Dict[str, str]]) -> int:
    worst = EXIT_OK
    for a in alerts:
        if a["level"] == "CRITICAL":
            worst = EXIT_CRITICAL
        elif a["level"] == "WARNING" and worst < EXIT_CRITICAL:
            worst = EXIT_WARNING
    return worst


def _count(supabase, table: str, filters=None) -> int:
    q = supabase.table(table).select("id", count="exact")
    if filters:
        q = filters(q)
    resp = q.limit(1).execute()
    return resp.count or 0


def _count_catalog(supabase, filters=None) -> int:
    return _count(supabase, "catalog_items", filters)


def _count_films(supabase, filters=None) -> int:
    return _count(supabase, "films", filters)


def _fetch_page(supabase, table, select, filters=None, limit=1000) -> List[Dict]:
    q = supabase.table(table).select(select).limit(limit)
    if filters:
        q = filters(q)
    return q.execute().data or []


# catalog_items only — no harmonized_* (those live on staging_supplier_offers).
_CLASSIFICATION_SELECT = (
    "title,edition_title,media_type,category,format,notes,source_type,"
    "film_id,tmdb_match_status"
)

# PostgREST default max-rows is often 1000; requesting a larger window still returns ≤1000
# rows, which previously caused early exit (len(page) < page_size). Keep page_size at or
# below the server cap and advance offset by len(page) until exhaustion.
_CLASSIFICATION_PAGE_SIZE = int(os.getenv("HEALTH_CLASSIFICATION_PAGE_SIZE", "1000"))
_CLASSIFICATION_MAX_ROWS = int(os.getenv("HEALTH_CLASSIFICATION_MAX_ROWS", "250000"))


def _fetch_active_rows_for_classification(supabase) -> List[Dict[str, Any]]:
    """Paginate all active catalog_items rows for content classification (health KPIs)."""
    out: List[Dict[str, Any]] = []
    page_size = max(1, _CLASSIFICATION_PAGE_SIZE)
    offset = 0
    while offset < _CLASSIFICATION_MAX_ROWS:
        resp = (
            supabase.table("catalog_items")
            .select(_CLASSIFICATION_SELECT)
            .eq("active", True)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        page = resp.data or []
        if not page:
            break
        out.extend(page)
        offset += len(page)
        if len(page) < page_size:
            break
    return out


def _assert_classification_counts_match_active_catalog(
    total_active: int,
    class_counts: Dict[str, int],
    rows_fetched: int,
) -> None:
    """
    film + tv + unknown must equal every active row we classified.
    Raises RuntimeError if fetch was truncated or counts are inconsistent.
    """
    summed = class_counts["film"] + class_counts["tv"] + class_counts["unknown"]
    if rows_fetched != summed:
        raise RuntimeError(
            "Internal error: classification bucket sum does not match rows fetched: "
            f"rows_fetched={rows_fetched}, film+tv+unknown={summed}, buckets={class_counts}"
        )
    if total_active != rows_fetched:
        raise RuntimeError(
            "Content classification did not cover all active catalog_items: "
            f"active_catalog_items (count)={total_active}, rows_fetched={rows_fetched}. "
            f"If you have more than {_CLASSIFICATION_MAX_ROWS} active rows, raise "
            "HEALTH_CLASSIFICATION_MAX_ROWS. Otherwise check PostgREST pagination."
        )


def _classification_counts(rows: List[Dict[str, Any]]) -> Tuple[Dict[str, int], int, int]:
    """
    Returns (classification_counts, film_linked, film_total).
    film_link stats are only for rows classified as film.
    """
    counts = {"film": 0, "tv": 0, "unknown": 0}
    film_linked = 0
    for row in rows:
        kind = classify_content_type(row)
        counts[kind] = counts.get(kind, 0) + 1
        if kind == "film":
            if row.get("film_id") is not None:
                film_linked += 1
    film_total = counts["film"]
    return counts, film_linked, film_total


def gather_metrics(supabase, since_days: Optional[int] = None) -> Dict[str, Any]:
    """
    since_days reserved for future windowed metrics (currently unused).
    """
    del since_days  # API compatibility
    film_link_min = float(os.getenv("HEALTH_FILM_LINK_MIN_PCT", "85"))
    film_link_critical = float(os.getenv("HEALTH_FILM_LINK_CRITICAL_PCT", "70"))
    tmdb_stale_days = int(os.getenv("HEALTH_TMDB_STALE_DAYS", "7"))
    tmdb_stale_max = int(os.getenv("HEALTH_TMDB_STALE_MAX", "50"))
    duplicate_films_max = int(os.getenv("HEALTH_DUPLICATE_FILMS_MAX", "0"))
    null_barcode_max = int(os.getenv("HEALTH_NULL_BARCODE_MAX", "0"))
    missing_price_max_pct = float(os.getenv("HEALTH_MISSING_PRICE_MAX_PCT", "5"))

    metrics: Dict[str, Any] = {}
    alerts: List[Dict[str, str]] = []

    # ── Coverage ──────────────────────────────────────────────────────────
    total_catalog = _count_catalog(supabase, lambda q: q.eq("active", True))
    total_films = _count_films(supabase)

    by_supplier_rows = _fetch_page(
        supabase,
        "catalog_items",
        "supplier",
        filters=lambda q: q.eq("active", True),
        limit=50000,
    )
    supplier_counts: Dict[str, int] = {}
    for r in by_supplier_rows:
        s = (r.get("supplier") or "Unknown").strip()
        supplier_counts[s] = supplier_counts.get(s, 0) + 1

    metrics["coverage"] = {
        "total_catalog_items_active": total_catalog,
        "total_films": total_films,
        "catalog_by_supplier": supplier_counts,
    }

    # ── Linkage ───────────────────────────────────────────────────────────
    linked_all = _count_catalog(supabase, lambda q: q.eq("active", True).not_.is_("film_id", "null"))
    unlinked_all = total_catalog - linked_all
    link_pct_all_active = compute_film_link_pct(linked_all, total_catalog)

    classification_rows = _fetch_active_rows_for_classification(supabase)
    class_counts, film_linked, film_total = _classification_counts(classification_rows)
    _assert_classification_counts_match_active_catalog(
        total_catalog, class_counts, len(classification_rows)
    )
    film_unlinked = film_total - film_linked
    film_link_pct = compute_film_link_pct(film_linked, film_total)

    tmdb_matched = _count_catalog(
        supabase, lambda q: q.eq("active", True).eq("tmdb_match_status", "matched")
    )
    tmdb_not_found = _count_catalog(
        supabase, lambda q: q.eq("active", True).eq("tmdb_match_status", "not_found")
    )
    tmdb_pending = _count_catalog(
        supabase, lambda q: q.eq("active", True).is_("tmdb_last_refreshed_at", "null")
    )
    tmdb_match_rate = compute_tmdb_match_rate_pct(tmdb_matched, tmdb_not_found)

    metrics["linkage"] = {
        "content_classification": {
            "film_rows": class_counts["film"],
            "tv_rows": class_counts["tv"],
            "unknown_rows": class_counts["unknown"],
        },
        # Primary KPI: film_id coverage among rows classified as theatrical / physical film SKUs
        "film_linked": film_linked,
        "film_unlinked": film_unlinked,
        "film_link_pct": film_link_pct,
        "film_classified_rows": film_total,
        # Whole-catalog attachment (TV/unknown included — not used for alerts)
        "all_active_with_film_id": linked_all,
        "all_active_without_film_id": unlinked_all,
        "all_active_film_id_pct": link_pct_all_active,
        "tmdb_matched": tmdb_matched,
        "tmdb_not_found": tmdb_not_found,
        "tmdb_pending": tmdb_pending,
        "tmdb_match_rate_pct": tmdb_match_rate,
    }

    # Primary KPI: film linkage among film-classified rows only (TV/unknown excluded)
    if film_total > 0:
        if film_link_pct < film_link_critical:
            alerts.append(
                {
                    "level": "CRITICAL",
                    "metric": "film_link_pct",
                    "value": str(film_link_pct),
                    "threshold": str(film_link_critical),
                    "message": (
                        f"Film linkage {film_link_pct}% among film-classified rows is CRITICAL "
                        f"(below {film_link_critical}%); entity graph / TMDB / film build may be broken"
                    ),
                }
            )
        elif film_link_pct < film_link_min:
            alerts.append(
                {
                    "level": "WARNING",
                    "metric": "film_link_pct",
                    "value": str(film_link_pct),
                    "threshold": str(film_link_min),
                    "message": (
                        f"Film linkage {film_link_pct}% among film-classified rows is below "
                        f"{film_link_min}% threshold"
                    ),
                }
            )

    # ── Stale TMDB pending ────────────────────────────────────────────────
    if tmdb_stale_days > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=tmdb_stale_days)).isoformat()
        stale_pending = _count_catalog(
            supabase,
            lambda q: q.eq("active", True)
            .is_("tmdb_last_refreshed_at", "null")
            .lt("created_at", cutoff),
        )
        metrics["linkage"]["tmdb_stale_pending"] = stale_pending
        metrics["linkage"]["tmdb_stale_days_threshold"] = tmdb_stale_days

        if stale_pending > tmdb_stale_max:
            alerts.append(
                {
                    "level": "WARNING",
                    "metric": "tmdb_stale_pending",
                    "value": str(stale_pending),
                    "threshold": str(tmdb_stale_max),
                    "message": (
                        f"{stale_pending} rows pending TMDB enrichment for >{tmdb_stale_days} days"
                    ),
                }
            )

    # ── Commercial ────────────────────────────────────────────────────────
    missing_price = _count_catalog(
        supabase, lambda q: q.eq("active", True).is_("calculated_sale_price", "null")
    )
    missing_cost = _count_catalog(
        supabase, lambda q: q.eq("active", True).is_("cost_price", "null")
    )
    missing_price_pct = compute_missing_price_pct(missing_price, total_catalog)

    metrics["commercial"] = {
        "missing_sale_price": missing_price,
        "missing_sale_price_pct": missing_price_pct,
        "missing_cost_price": missing_cost,
    }

    if missing_price_pct > missing_price_max_pct:
        alerts.append(
            {
                "level": "WARNING",
                "metric": "missing_sale_price_pct",
                "value": str(missing_price_pct),
                "threshold": str(missing_price_max_pct),
                "message": f"{missing_price_pct}% of catalog rows missing sale price",
            }
        )

    # ── Freshness ─────────────────────────────────────────────────────────
    recent_rows = _fetch_page(
        supabase,
        "catalog_items",
        "supplier_last_seen_at",
        filters=lambda q: q.eq("active", True)
        .not_.is_("supplier_last_seen_at", "null")
        .order("supplier_last_seen_at", desc=True),
        limit=1,
    )
    oldest_rows = _fetch_page(
        supabase,
        "catalog_items",
        "supplier_last_seen_at",
        filters=lambda q: q.eq("active", True)
        .not_.is_("supplier_last_seen_at", "null")
        .order("supplier_last_seen_at", desc=False),
        limit=1,
    )
    metrics["freshness"] = {
        "latest_supplier_seen": recent_rows[0]["supplier_last_seen_at"] if recent_rows else None,
        "oldest_supplier_seen": oldest_rows[0]["supplier_last_seen_at"] if oldest_rows else None,
    }

    # ── Exceptions (data quality) ─────────────────────────────────────────
    null_barcode = _count_catalog(supabase, lambda q: q.eq("active", True).is_("barcode", "null"))

    dup_film_rows = _fetch_page(
        supabase,
        "films",
        "tmdb_id",
        filters=lambda q: q.not_.is_("tmdb_id", "null"),
        limit=50000,
    )
    tmdb_id_counts: Dict[int, int] = {}
    for r in dup_film_rows:
        tid = r.get("tmdb_id")
        if tid is not None:
            tmdb_id_counts[tid] = tmdb_id_counts.get(tid, 0) + 1
    duplicate_films = sum(1 for c in tmdb_id_counts.values() if c > 1)

    null_title = _count_catalog(supabase, lambda q: q.eq("active", True).is_("title", "null"))

    metrics["exceptions"] = {
        "null_barcode_rows": null_barcode,
        "duplicate_films_by_tmdb_id": duplicate_films,
        "null_title_rows": null_title,
    }

    if null_barcode > null_barcode_max:
        alerts.append(
            {
                "level": "CRITICAL",
                "metric": "null_barcode_rows",
                "value": str(null_barcode),
                "threshold": str(null_barcode_max),
                "message": f"{null_barcode} active catalog rows have NULL barcode",
            }
        )

    if duplicate_films > duplicate_films_max:
        alerts.append(
            {
                "level": "CRITICAL",
                "metric": "duplicate_films_by_tmdb_id",
                "value": str(duplicate_films),
                "threshold": str(duplicate_films_max),
                "message": f"{duplicate_films} films share a tmdb_id (duplicates)",
            }
        )

    metrics["alerts"] = alerts
    metrics["generated_at"] = datetime.now(timezone.utc).isoformat()
    metrics["exit_code"] = resolve_exit_code_from_alerts(alerts)

    return metrics
