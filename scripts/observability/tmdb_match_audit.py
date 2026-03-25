#!/usr/bin/env python3
"""
TMDB Match Audit — deep observability for TMDB enrichment quality.

Queries catalog_items to produce TMDB-specific metrics: match rates,
not_found patterns, stale pending rows, and rematch candidates.

Usage:
  venv/bin/python scripts/observability/tmdb_match_audit.py
  venv/bin/python scripts/observability/tmdb_match_audit.py --format json
  venv/bin/python scripts/observability/tmdb_match_audit.py --since-days 30
  venv/bin/python scripts/observability/tmdb_match_audit.py --format csv --output tmdb_audit.csv

Metrics:
  - Total matched / not_found / pending / no_clean_title
  - Match rate (overall, last 7 days, last 30 days)
  - Top title patterns among not_found rows
  - Rows that deserve manual rematch review (not_found with high stock)
  - Stale pending rows (created > N days ago, never enriched)
  - Matched rows missing key metadata (potential low-quality matches)

Thresholds:
  TMDB_MATCH_RATE_MIN_PCT  = 70  (match rate below this = WARNING)
  TMDB_PENDING_STALE_DAYS  = 7   (pending rows older than N days)
  TMDB_PENDING_STALE_MAX   = 50  (max stale rows before WARNING)
  TMDB_LOW_QUALITY_MAX     = 20  (matched but missing poster/genres = WARNING)
"""

import argparse
import csv
import io
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from supabase import create_client

MATCH_RATE_MIN_PCT = float(os.getenv("TMDB_MATCH_RATE_MIN_PCT", "70"))
PENDING_STALE_DAYS = int(os.getenv("TMDB_PENDING_STALE_DAYS", "7"))
PENDING_STALE_MAX = int(os.getenv("TMDB_PENDING_STALE_MAX", "50"))
LOW_QUALITY_MAX = int(os.getenv("TMDB_LOW_QUALITY_MAX", "20"))

EXIT_OK = 0
EXIT_WARNING = 1
EXIT_CRITICAL = 2


def _count(supabase, filters=None) -> int:
    q = supabase.table("catalog_items").select("id", count="exact")
    if filters:
        q = filters(q)
    return (q.limit(1).execute()).count or 0


def _fetch(supabase, select, filters=None, limit=1000) -> List[Dict]:
    q = supabase.table("catalog_items").select(select).limit(limit)
    if filters:
        q = filters(q)
    return q.execute().data or []


def _paginated_fetch(supabase, select, filters=None, page_size=1000, max_rows=50000) -> List[Dict]:
    out: List[Dict] = []
    offset = 0
    while len(out) < max_rows:
        q = supabase.table("catalog_items").select(select).range(offset, offset + page_size - 1)
        if filters:
            q = filters(q)
        page = q.execute().data or []
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return out


def _extract_title_pattern(title: str) -> str:
    """Extract a simplified pattern from a title for grouping."""
    t = (title or "").strip().lower()
    t = re.sub(r"\b4k\b|\buhd\b|\bblu[\s-]?ray\b|\bdvd\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\blimited edition\b|\bspecial edition\b|\bcollector.?s edition\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\bsteelbook\b|\bbox set\b|\bdeluxe\b", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\(.*?\)|\[.*?\]", "", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def gather_metrics(supabase, since_days: Optional[int] = None) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {}
    alerts: List[Dict[str, str]] = []
    now = datetime.now(timezone.utc)

    # ── Overall counts ────────────────────────────────────────────────────
    total_active = _count(supabase, lambda q: q.eq("active", True))
    matched = _count(supabase, lambda q: q.eq("active", True).eq("tmdb_match_status", "matched"))
    not_found = _count(supabase, lambda q: q.eq("active", True).eq("tmdb_match_status", "not_found"))
    no_clean = _count(supabase, lambda q: q.eq("active", True).eq("tmdb_match_status", "no_clean_title"))
    pending = _count(supabase, lambda q: q.eq("active", True).is_("tmdb_last_refreshed_at", "null"))
    attempted = matched + not_found + no_clean
    match_rate = round(matched / attempted * 100, 1) if attempted > 0 else 0

    metrics["totals"] = {
        "active_catalog_items": total_active,
        "tmdb_matched": matched,
        "tmdb_not_found": not_found,
        "tmdb_no_clean_title": no_clean,
        "tmdb_pending": pending,
        "tmdb_attempted": attempted,
        "tmdb_match_rate_pct": match_rate,
    }

    if match_rate < MATCH_RATE_MIN_PCT and attempted > 100:
        alerts.append({
            "level": "WARNING",
            "metric": "tmdb_match_rate_pct",
            "value": str(match_rate),
            "threshold": str(MATCH_RATE_MIN_PCT),
            "message": f"TMDB match rate {match_rate}% is below {MATCH_RATE_MIN_PCT}% threshold",
        })

    # ── Match rate by time window ─────────────────────────────────────────
    windows: Dict[str, Dict[str, Any]] = {}
    for label, days in [("last_7_days", 7), ("last_30_days", 30)]:
        cutoff = (now - timedelta(days=days)).isoformat()
        w_matched = _count(
            supabase,
            lambda q, c=cutoff: q.eq("active", True).eq("tmdb_match_status", "matched").gte("tmdb_last_refreshed_at", c)
        )
        w_not_found = _count(
            supabase,
            lambda q, c=cutoff: q.eq("active", True).eq("tmdb_match_status", "not_found").gte("tmdb_last_refreshed_at", c)
        )
        w_total = w_matched + w_not_found
        w_rate = round(w_matched / w_total * 100, 1) if w_total > 0 else 0
        windows[label] = {
            "matched": w_matched,
            "not_found": w_not_found,
            "attempted": w_total,
            "match_rate_pct": w_rate,
        }

    metrics["match_rate_by_window"] = windows

    # ── Stale pending ─────────────────────────────────────────────────────
    stale_cutoff = (now - timedelta(days=PENDING_STALE_DAYS)).isoformat()
    stale_pending = _count(
        supabase,
        lambda q: q.eq("active", True).is_("tmdb_last_refreshed_at", "null").lt("created_at", stale_cutoff)
    )
    metrics["stale_pending"] = {
        "count": stale_pending,
        "threshold_days": PENDING_STALE_DAYS,
        "threshold_max": PENDING_STALE_MAX,
    }

    if stale_pending > PENDING_STALE_MAX:
        alerts.append({
            "level": "WARNING",
            "metric": "stale_pending",
            "value": str(stale_pending),
            "threshold": str(PENDING_STALE_MAX),
            "message": f"{stale_pending} rows pending TMDB enrichment for >{PENDING_STALE_DAYS} days",
        })

    # ── Not-found title patterns ──────────────────────────────────────────
    not_found_rows = _paginated_fetch(
        supabase,
        "title,supplier_stock_status,availability_status,barcode",
        filters=lambda q: q.eq("active", True).eq("tmdb_match_status", "not_found"),
        max_rows=5000,
    )

    pattern_counter = Counter()
    for r in not_found_rows:
        pattern = _extract_title_pattern(r.get("title") or "")
        if pattern:
            pattern_counter[pattern] += 1

    metrics["not_found_title_patterns"] = [
        {"pattern": p, "count": c} for p, c in pattern_counter.most_common(20)
    ]

    # ── Rematch candidates ────────────────────────────────────────────────
    # not_found rows with stock > 0 are the most valuable to manually review
    rematch_candidates = [
        {
            "barcode": r.get("barcode"),
            "title": r.get("title"),
            "stock": r.get("supplier_stock_status", 0),
            "availability": r.get("availability_status"),
        }
        for r in not_found_rows
        if (r.get("supplier_stock_status") or 0) > 0
    ]
    rematch_candidates.sort(key=lambda x: -(x.get("stock") or 0))
    metrics["rematch_candidates"] = {
        "count": len(rematch_candidates),
        "top_20": rematch_candidates[:20],
    }

    # ── Low-quality matches ───────────────────────────────────────────────
    low_quality_rows = _paginated_fetch(
        supabase,
        "title,barcode,tmdb_id,tmdb_title,genres,top_cast,tmdb_poster_path",
        filters=lambda q: q.eq("active", True)
                           .eq("tmdb_match_status", "matched")
                           .is_("tmdb_poster_path", "null"),
        max_rows=1000,
    )
    missing_genres = [r for r in low_quality_rows if not r.get("genres")]
    missing_cast = [r for r in low_quality_rows if not r.get("top_cast")]

    metrics["low_quality_matches"] = {
        "missing_poster": len(low_quality_rows),
        "missing_genres": len(missing_genres),
        "missing_cast": len(missing_cast),
        "total_low_quality": len(low_quality_rows),
    }

    if len(low_quality_rows) > LOW_QUALITY_MAX:
        alerts.append({
            "level": "WARNING",
            "metric": "low_quality_matches",
            "value": str(len(low_quality_rows)),
            "threshold": str(LOW_QUALITY_MAX),
            "message": f"{len(low_quality_rows)} matched rows missing poster (possible low-quality matches)",
        })

    metrics["alerts"] = alerts
    metrics["generated_at"] = now.isoformat()

    worst = EXIT_OK
    for a in alerts:
        if a["level"] == "CRITICAL":
            worst = EXIT_CRITICAL
        elif a["level"] == "WARNING" and worst < EXIT_CRITICAL:
            worst = EXIT_WARNING
    metrics["exit_code"] = worst

    return metrics


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------

def format_text(metrics: Dict[str, Any]) -> str:
    lines = [
        "=" * 70,
        "TMDB MATCH AUDIT",
        f"Generated: {metrics['generated_at']}",
        "=" * 70,
        "",
        "── Overall ───────────────────────────────────────────",
    ]
    t = metrics["totals"]
    lines.append(f"  Active catalog items:  {t['active_catalog_items']:,}")
    lines.append(f"  TMDB matched:          {t['tmdb_matched']:,}")
    lines.append(f"  TMDB not_found:        {t['tmdb_not_found']:,}")
    lines.append(f"  TMDB no_clean_title:   {t['tmdb_no_clean_title']:,}")
    lines.append(f"  TMDB pending:          {t['tmdb_pending']:,}")
    lines.append(f"  Match rate:            {t['tmdb_match_rate_pct']}%")

    lines.append("")
    lines.append("── Match Rate by Window ──────────────────────────────")
    for label, data in metrics["match_rate_by_window"].items():
        lines.append(
            f"  {label:<16} matched={data['matched']:,}  "
            f"not_found={data['not_found']:,}  rate={data['match_rate_pct']}%"
        )

    lines.append("")
    lines.append("── Stale Pending ─────────────────────────────────────")
    sp = metrics["stale_pending"]
    lines.append(f"  Pending >{sp['threshold_days']} days:  {sp['count']:,}  (threshold: {sp['threshold_max']})")

    lines.append("")
    lines.append("── Top Not-Found Title Patterns ──────────────────────")
    for entry in metrics["not_found_title_patterns"][:15]:
        lines.append(f"  {entry['count']:>5}x  {entry['pattern']}")

    lines.append("")
    lines.append("── Rematch Candidates (not_found + in stock) ────────")
    rc = metrics["rematch_candidates"]
    lines.append(f"  Total candidates:      {rc['count']}")
    if rc["top_20"]:
        lines.append(f"  Top {len(rc['top_20'])}:")
        for r in rc["top_20"]:
            lines.append(
                f"    barcode={r['barcode']}  stock={r['stock']}  "
                f"title={r['title']}"
            )

    lines.append("")
    lines.append("── Low-Quality Matches ───────────────────────────────")
    lq = metrics["low_quality_matches"]
    lines.append(f"  Missing poster:        {lq['missing_poster']}")
    lines.append(f"  Missing genres:        {lq['missing_genres']}")
    lines.append(f"  Missing cast:          {lq['missing_cast']}")

    alerts = metrics.get("alerts", [])
    if alerts:
        lines.append("")
        lines.append("── Alerts ────────────────────────────────────────────")
        for a in alerts:
            lines.append(f"  [{a['level']}] {a['message']}")

    lines.append("")
    status = "HEALTHY" if metrics["exit_code"] == EXIT_OK else (
        "WARNING" if metrics["exit_code"] == EXIT_WARNING else "CRITICAL"
    )
    lines.append(f"Status: {status}")
    lines.append("=" * 70)
    return "\n".join(lines)


def format_json(metrics: Dict[str, Any]) -> str:
    return json.dumps(metrics, indent=2, default=str)


def format_csv(metrics: Dict[str, Any]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["section", "metric", "value"])

    for key, val in metrics["totals"].items():
        writer.writerow(["totals", key, val])

    for label, data in metrics["match_rate_by_window"].items():
        for key, val in data.items():
            writer.writerow(["match_rate", f"{label}.{key}", val])

    for key, val in metrics["stale_pending"].items():
        writer.writerow(["stale_pending", key, val])

    for entry in metrics["not_found_title_patterns"]:
        writer.writerow(["not_found_pattern", entry["pattern"], entry["count"]])

    writer.writerow(["rematch_candidates", "count", metrics["rematch_candidates"]["count"]])

    for key, val in metrics["low_quality_matches"].items():
        writer.writerow(["low_quality", key, val])

    for i, alert in enumerate(metrics.get("alerts", [])):
        writer.writerow(["alert", f"alert_{i}", f"[{alert['level']}] {alert['message']}"])

    writer.writerow(["meta", "exit_code", metrics["exit_code"]])
    writer.writerow(["meta", "generated_at", metrics["generated_at"]])
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="TMDB match quality audit with threshold alerting.")
    parser.add_argument(
        "--format", choices=["text", "json", "csv"], default="text",
        help="Output format (default: text)",
    )
    parser.add_argument("--output", default=None, help="Write output to file instead of stdout")
    parser.add_argument("--since-days", type=int, default=None, help="Focus window for trending metrics")
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
        print(f"Audit written to {args.output}")
    else:
        print(output)

    sys.exit(metrics["exit_code"])


if __name__ == "__main__":
    main()
