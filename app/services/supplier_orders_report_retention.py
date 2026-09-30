"""
D-5 retention for the supplier-orders FTP report series only.

Retain today (Australia/Sydney) plus the previous 5 calendar days of dated
report CSVs. Delete older dated files that match known filename prefixes.

Safety:
  - Exact report directory only (never FTP root)
  - Exact known filename patterns only
  - Unsuffixed "latest" files are always retained
  - Malformed / unclassifiable names are retained and reported
  - Prefer authoritative YYYYMMDD in filename over mtime
  - Dry-run support (no deletes)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Sequence
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

SYDNEY = ZoneInfo("Australia/Sydney")

# Dated report series written by supplier_orders_report_service.
REPORT_SERIES_PREFIXES: tuple[str, ...] = (
    "supplier_orders_needed_",
    "supplier_orders_preorder_",
    "supplier_orders_continue_oos_",
    "supplier_orders_delta_",
    "supplier_po_unmatched_",
)

# Always retain these "latest" aliases (no date suffix).
LATEST_FILENAMES: frozenset[str] = frozenset(
    {
        "supplier_orders_needed.csv",
        "supplier_orders_preorder.csv",
        "supplier_orders_continue_oos.csv",
        "supplier_orders_delta.csv",
        "supplier_po_unmatched.csv",
    }
)

_DATED_RE = re.compile(
    r"^(?P<prefix>"
    + "|".join(re.escape(p) for p in REPORT_SERIES_PREFIXES)
    + r")(?P<ymd>\d{8})\.csv$"
)


@dataclass(frozen=True)
class RetentionDecision:
    filename: str
    parsed_report_date: Optional[date]
    age_days: Optional[int]
    matches_report_series: bool
    action: str  # KEEP | DELETE | IGNORE | UNCLASSIFIED
    reason: str


@dataclass
class RetentionSummary:
    directory: str
    as_of: date
    keep_from: date
    dry_run: bool
    decisions: List[RetentionDecision] = field(default_factory=list)
    deleted: List[str] = field(default_factory=list)
    retained: List[str] = field(default_factory=list)
    unclassified: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def would_delete(self) -> List[str]:
        return [d.filename for d in self.decisions if d.action == "DELETE"]


def sydney_today(now: Optional[datetime] = None) -> date:
    if now is None:
        return datetime.now(SYDNEY).date()
    if now.tzinfo is None:
        now = now.replace(tzinfo=SYDNEY)
    return now.astimezone(SYDNEY).date()


def parse_report_date_from_filename(name: str) -> Optional[date]:
    m = _DATED_RE.match(name)
    if not m:
        return None
    raw = m.group("ymd")
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError:
        return None


def classify_report_file(
    name: str,
    *,
    as_of: date,
    retain_days: int = 5,
) -> RetentionDecision:
    """
    retain_days=5 means keep today plus previous 5 calendar days (6 dates total).
    """
    if name in LATEST_FILENAMES:
        return RetentionDecision(
            filename=name,
            parsed_report_date=None,
            age_days=None,
            matches_report_series=True,
            action="KEEP",
            reason="latest_alias",
        )

    # Directories / unrelated must never be deleted by this helper.
    if name == "inbound" or name.endswith("/"):
        return RetentionDecision(
            filename=name,
            parsed_report_date=None,
            age_days=None,
            matches_report_series=False,
            action="IGNORE",
            reason="directory_or_non_file",
        )

    parsed = parse_report_date_from_filename(name)
    if parsed is None:
        # Known prefix but bad date, or unknown name.
        for prefix in REPORT_SERIES_PREFIXES:
            if name.startswith(prefix):
                return RetentionDecision(
                    filename=name,
                    parsed_report_date=None,
                    age_days=None,
                    matches_report_series=True,
                    action="UNCLASSIFIED",
                    reason="malformed_dated_filename",
                )
        return RetentionDecision(
            filename=name,
            parsed_report_date=None,
            age_days=None,
            matches_report_series=False,
            action="IGNORE",
            reason="unrelated_filename",
        )

    keep_from = as_of - timedelta(days=retain_days)
    age = (as_of - parsed).days
    if parsed > as_of:
        return RetentionDecision(
            filename=name,
            parsed_report_date=parsed,
            age_days=age,
            matches_report_series=True,
            action="KEEP",
            reason="future_dated_filename",
        )
    if parsed >= keep_from:
        return RetentionDecision(
            filename=name,
            parsed_report_date=parsed,
            age_days=age,
            matches_report_series=True,
            action="KEEP",
            reason=f"within_d{retain_days}_window",
        )
    return RetentionDecision(
        filename=name,
        parsed_report_date=parsed,
        age_days=age,
        matches_report_series=True,
        action="DELETE",
        reason=f"older_than_d{retain_days}",
    )


def plan_retention(
    directory: Path | str,
    *,
    as_of: Optional[date] = None,
    retain_days: int = 5,
    filenames: Optional[Sequence[str]] = None,
) -> RetentionSummary:
    root = Path(directory)
    as_of = as_of or sydney_today()
    keep_from = as_of - timedelta(days=retain_days)
    summary = RetentionSummary(
        directory=str(root),
        as_of=as_of,
        keep_from=keep_from,
        dry_run=True,
    )
    if filenames is None:
        if not root.is_dir():
            summary.errors.append(f"not_a_directory:{root}")
            return summary
        names = sorted(p.name for p in root.iterdir() if p.is_file() or p.name == "inbound")
        # Also surface inbound directory as IGNORE if present
        if (root / "inbound").is_dir() and "inbound" not in names:
            names.append("inbound")
            names.sort()
    else:
        names = list(filenames)

    for name in names:
        decision = classify_report_file(name, as_of=as_of, retain_days=retain_days)
        summary.decisions.append(decision)
        if decision.action == "KEEP":
            summary.retained.append(name)
        elif decision.action == "UNCLASSIFIED":
            summary.unclassified.append(name)
            summary.retained.append(name)
        elif decision.action == "IGNORE":
            summary.retained.append(name)
    return summary


def apply_retention(
    directory: Path | str,
    *,
    as_of: Optional[date] = None,
    retain_days: int = 5,
    dry_run: bool = True,
) -> RetentionSummary:
    root = Path(directory).resolve()
    summary = plan_retention(root, as_of=as_of, retain_days=retain_days)
    summary.dry_run = dry_run

    for decision in summary.decisions:
        if decision.action != "DELETE":
            continue
        target = root / decision.filename
        # Hard safety: must be a file directly under the report directory.
        if target.parent != root or not target.is_file():
            summary.errors.append(f"skip_unsafe_path:{decision.filename}")
            continue
        if dry_run:
            logger.info(
                "SUPPLIER_ORDERS_RETENTION dry_run would_delete file=%s date=%s reason=%s",
                decision.filename,
                decision.parsed_report_date,
                decision.reason,
            )
            continue
        try:
            target.unlink()
            summary.deleted.append(decision.filename)
            logger.info(
                "SUPPLIER_ORDERS_RETENTION deleted file=%s date=%s reason=%s",
                decision.filename,
                decision.parsed_report_date,
                decision.reason,
            )
        except OSError as exc:
            summary.errors.append(f"delete_failed:{decision.filename}:{exc}")
    return summary


def format_retention_status(summary: RetentionSummary) -> str:
    return (
        "SUPPLIER_ORDERS_RETENTION_STATUS="
        f"{'dry_run' if summary.dry_run else 'applied'} "
        f"as_of={summary.as_of.isoformat()} "
        f"keep_from={summary.keep_from.isoformat()} "
        f"would_delete={len(summary.would_delete)} "
        f"deleted={len(summary.deleted)} "
        f"retained={len(summary.retained)} "
        f"unclassified={len(summary.unclassified)} "
        f"errors={len(summary.errors)}"
    )
