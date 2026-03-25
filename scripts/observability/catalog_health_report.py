#!/usr/bin/env python3
"""
Catalog Health Report — comprehensive health observability for the data catalog.

Queries catalog_items and films to produce coverage, linkage, commercial,
freshness, and exception metrics. Supports text, JSON, and CSV output.
Returns non-zero exit code if any threshold is breached.

Usage:
  venv/bin/python scripts/observability/catalog_health_report.py
  venv/bin/python scripts/observability/catalog_health_report.py --format json
  venv/bin/python scripts/observability/catalog_health_report.py --format csv --output health.csv
  venv/bin/python scripts/observability/catalog_health_report.py --since-days 7

Thresholds (configurable via env or CLI):
  HEALTH_FILM_LINK_CRITICAL_PCT   = 70   (film linkage below this % = CRITICAL — among film-classified rows only)
  HEALTH_FILM_LINK_MIN_PCT        = 85   (film linkage below this % = WARNING — among film-classified rows only)
  HEALTH_TMDB_STALE_DAYS          = 7    (pending rows older than N days = WARNING)
  HEALTH_TMDB_STALE_MAX           = 50   (max stale pending rows before WARNING)
  HEALTH_DUPLICATE_FILMS_MAX      = 0    (duplicate tmdb_id in films = CRITICAL)
  HEALTH_NULL_BARCODE_MAX         = 0    (null barcode rows = CRITICAL)
  HEALTH_MISSING_PRICE_MAX_PCT    = 5    (catalog rows missing sale price % = WARNING)
  HEALTH_CLASSIFICATION_PAGE_SIZE = 1000 (PostgREST page size for classifying all active rows)
  HEALTH_CLASSIFICATION_MAX_ROWS  = 250000 (safety cap; exceed → RuntimeError, not silent truncation)

If active row count does not match classification buckets, gather_metrics raises RuntimeError
instead of emitting misleading film-link percentages.
"""

import argparse
import csv
import io
import json
import os
import sys
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from supabase import create_client

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.observability.catalog_metrics import (
    EXIT_CRITICAL,
    EXIT_OK,
    EXIT_WARNING,
    gather_metrics,
)


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------

def format_text(metrics: Dict[str, Any]) -> str:
    cov = metrics["coverage"]
    lnk = metrics["linkage"]
    com = metrics["commercial"]
    exc = metrics["exceptions"]
    cc = lnk.get("content_classification") or {}
    status = (
        "HEALTHY"
        if metrics["exit_code"] == EXIT_OK
        else ("WARNING" if metrics["exit_code"] == EXIT_WARNING else "CRITICAL")
    )
    lines = [
        f"SYSTEM STATUS: {status}",
        "",
        "Catalog",
        f"- Rows: {cov['total_catalog_items_active']:,}",
        f"- Content (classifier): film {cc.get('film_rows', 0):,} | "
        f"tv {cc.get('tv_rows', 0):,} | unknown {cc.get('unknown_rows', 0):,}",
        f"- TMDB matched (rate): {lnk['tmdb_match_rate_pct']}%",
        f"- Film linked (film-classified only): {lnk['film_link_pct']}%",
        f"- Rows w/ film_id (all active): {lnk.get('all_active_film_id_pct', 0)}%",
        "",
        "Commercial",
        f"- Missing sale price: {com['missing_sale_price_pct']}%",
        "",
        "Exceptions",
        f"- Null barcodes: {exc['null_barcode_rows']}",
        f"- Duplicate films: {exc['duplicate_films_by_tmdb_id']}",
        "",
        "=" * 70,
        "CATALOG HEALTH REPORT",
        f"Generated: {metrics['generated_at']}",
        "=" * 70,
        "",
        "── Coverage ──────────────────────────────────────────",
    ]
    lines.append(f"  Active catalog items:  {cov['total_catalog_items_active']:,}")
    lines.append(f"  Total films:           {cov['total_films']:,}")
    for supplier, count in sorted(cov["catalog_by_supplier"].items()):
        lines.append(f"    {supplier:<20} {count:>8,}")

    lines.append("")
    lines.append("── Linkage ───────────────────────────────────────────")
    lines.append(
        f"  Content classification:  film {cc.get('film_rows', 0):,}  |  "
        f"tv {cc.get('tv_rows', 0):,}  |  unknown {cc.get('unknown_rows', 0):,}"
    )
    lines.append(
        f"  Film linked (film SKUs): {lnk['film_linked']:,}  ({lnk['film_link_pct']}%)  "
        f"[denominator: {lnk.get('film_classified_rows', 0):,} film-classified rows]"
    )
    lines.append(f"  Film unlinked (film SKUs): {lnk['film_unlinked']:,}")
    lines.append(
        f"  All active w/ film_id:   {lnk.get('all_active_with_film_id', 0):,}  "
        f"({lnk.get('all_active_film_id_pct', 0)}% of all active)"
    )
    lines.append(f"  TMDB matched:          {lnk['tmdb_matched']:,}")
    lines.append(f"  TMDB not_found:        {lnk['tmdb_not_found']:,}")
    lines.append(f"  TMDB pending:          {lnk['tmdb_pending']:,}")
    lines.append(f"  TMDB match rate:       {lnk['tmdb_match_rate_pct']}%")
    if "tmdb_stale_pending" in lnk:
        lines.append(f"  TMDB stale (>{lnk['tmdb_stale_days_threshold']}d):   {lnk['tmdb_stale_pending']:,}")

    lines.append("")
    lines.append("── Commercial ────────────────────────────────────────")
    lines.append(f"  Missing sale price:    {com['missing_sale_price']:,}  ({com['missing_sale_price_pct']}%)")
    lines.append(f"  Missing cost price:    {com['missing_cost_price']:,}")

    lines.append("")
    lines.append("── Freshness ─────────────────────────────────────────")
    fsh = metrics["freshness"]
    lines.append(f"  Latest supplier seen:  {fsh['latest_supplier_seen'] or 'N/A'}")
    lines.append(f"  Oldest supplier seen:  {fsh['oldest_supplier_seen'] or 'N/A'}")

    lines.append("")
    lines.append("── Exceptions ────────────────────────────────────────")
    lines.append(f"  Null barcode rows:     {exc['null_barcode_rows']}")
    lines.append(f"  Duplicate films:       {exc['duplicate_films_by_tmdb_id']}")
    lines.append(f"  Null title rows:       {exc['null_title_rows']}")

    alerts = metrics.get("alerts", [])
    if alerts:
        lines.append("")
        lines.append("── Alerts ────────────────────────────────────────────")
        for a in alerts:
            lines.append(f"  [{a['level']}] {a['message']}")

    lines.append("")
    lines.append(f"Status: {status}")
    lines.append("=" * 70)
    return "\n".join(lines)


def format_json(metrics: Dict[str, Any]) -> str:
    return json.dumps(metrics, indent=2, default=str)


def format_csv(metrics: Dict[str, Any]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["section", "metric", "value"])

    for section in ("coverage", "linkage", "commercial", "freshness", "exceptions"):
        data = metrics.get(section, {})
        for key, value in data.items():
            if isinstance(value, dict):
                for sub_key, sub_val in value.items():
                    writer.writerow([section, f"{key}.{sub_key}", sub_val])
            else:
                writer.writerow([section, key, value])

    for i, alert in enumerate(metrics.get("alerts", [])):
        writer.writerow(["alert", f"alert_{i}", f"[{alert['level']}] {alert['message']}"])

    writer.writerow(["meta", "exit_code", metrics["exit_code"]])
    writer.writerow(["meta", "generated_at", metrics["generated_at"]])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Catalog health report with threshold alerting.")
    parser.add_argument(
        "--format", choices=["text", "json", "csv"], default="text",
        help="Output format (default: text)",
    )
    parser.add_argument("--output", default=None, help="Write output to file instead of stdout")
    parser.add_argument("--since-days", type=int, default=None, help="Only consider rows seen in last N days")
    parser.add_argument("--env", default=".env", help="Path to env file")
    args = parser.parse_args()

    load_dotenv(args.env)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    if not supabase_url or not supabase_key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY", file=sys.stderr)
        sys.exit(2)

    supabase = create_client(supabase_url, supabase_key)
    metrics = gather_metrics(supabase, since_days=args.since_days)

    if args.format == "json":
        output = format_json(metrics)
    elif args.format == "csv":
        output = format_csv(metrics)
    else:
        output = format_text(metrics)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"Report written to {args.output}")
    else:
        print(output)

    sys.exit(metrics["exit_code"])


if __name__ == "__main__":
    main()
