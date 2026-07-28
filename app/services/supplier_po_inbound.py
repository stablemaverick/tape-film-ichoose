"""
Load weekly open supplier PO CSVs and match lines to Shopify variants.

Latest ``*.csv`` in the inbound folder is a full snapshot of open POs.
Match order: exact normalized SKU, then unique exact normalized title.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from app.helpers.text_helpers import clean_text

OPEN_PO_STATUSES = frozenset(
    {
        "pre-order",
        "preorder",
        "picking",
        "awaiting stock",
    }
)

REQUIRED_HEADERS = frozenset({"sku", "title", "qty", "status"})

UNMATCHED_CSV_FIELDS = [
    "order_id",
    "sku",
    "title",
    "qty",
    "unit_cost",
    "line_total",
    "status",
    "reason",
]


@dataclass
class SupplierPoLine:
    order_id: str
    sku: str
    title: str
    qty: int
    unit_cost: str
    line_total: str
    status: str
    sku_key: str
    title_key: str


@dataclass
class PoBucket:
    qty: int = 0
    order_ids: List[str] = field(default_factory=list)
    lines: List[SupplierPoLine] = field(default_factory=list)

    def add(self, line: SupplierPoLine) -> None:
        self.qty += max(0, line.qty)
        if line.order_id and line.order_id not in self.order_ids:
            self.order_ids.append(line.order_id)
        self.lines.append(line)


@dataclass
class PoInboundSnapshot:
    path: Optional[Path]
    lines: List[SupplierPoLine]
    by_sku: Dict[str, PoBucket]
    by_title: Dict[str, PoBucket]
    skipped_status: int
    parse_errors: List[str]


def normalize_match_key(value: Any) -> str:
    """Trim, casefold, collapse whitespace for exact SKU/title matching."""
    text = clean_text(value)
    if not text:
        return ""
    text = text.casefold()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def is_open_po_status(status: Any) -> bool:
    key = normalize_match_key(status)
    return key in OPEN_PO_STATUSES


def find_latest_inbound_csv(inbound_dir: Path) -> Optional[Path]:
    if not inbound_dir.is_dir():
        return None
    files = [p for p in inbound_dir.glob("*.csv") if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)

def find_latest_readable_inbound_csv(
    inbound_dir: Path,
) -> Tuple[Optional[Path], Optional[Path], Optional[str]]:
    """
    Return the newest readable CSV, plus info about the latest unreadable one.
    """
    if not inbound_dir.is_dir():
        return None, None, None
    files = sorted(
        (p for p in inbound_dir.glob("*.csv") if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        return None, None, None
    first_unreadable: Optional[Path] = None
    first_err: Optional[str] = None
    for path in files:
        try:
            with path.open("rb"):
                pass
            return path, first_unreadable, first_err
        except (OSError, PermissionError) as exc:
            if first_unreadable is None:
                first_unreadable = path
                first_err = str(exc)
            continue
    return None, first_unreadable, first_err


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    text = str(value).replace(",", "").strip()
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return default


def _row_get(row: Dict[str, Any], *names: str) -> str:
    lower = {normalize_match_key(k): v for k, v in row.items() if k is not None}
    for name in names:
        key = normalize_match_key(name)
        if key in lower and lower[key] is not None:
            return str(lower[key]).strip()
    return ""


def parse_po_csv(path: Path) -> Tuple[List[SupplierPoLine], List[str], int]:
    """
    Parse a PO CSV. Returns (open_lines, parse_errors, skipped_status_count).
    """
    errors: List[str] = []
    lines: List[SupplierPoLine] = []
    skipped_status = 0

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return [], [f"{path.name}: missing header row"], 0
        headers = {normalize_match_key(h) for h in reader.fieldnames if h}
        missing = REQUIRED_HEADERS - headers
        if missing:
            return (
                [],
                [f"{path.name}: missing required columns: {', '.join(sorted(missing))}"],
                0,
            )

        for i, row in enumerate(reader, start=2):
            status = _row_get(row, "status")
            if not is_open_po_status(status):
                skipped_status += 1
                continue
            sku = _row_get(row, "sku")
            title = _row_get(row, "title")
            qty = _safe_int(_row_get(row, "qty"), 0)
            if qty <= 0:
                errors.append(f"{path.name}:line {i}: qty must be > 0")
                continue
            if not sku and not title:
                errors.append(f"{path.name}:line {i}: need sku or title")
                continue
            line = SupplierPoLine(
                order_id=_row_get(row, "order_id"),
                sku=sku,
                title=title,
                qty=qty,
                unit_cost=_row_get(row, "unit_cost"),
                line_total=_row_get(row, "line_total"),
                status=status,
                sku_key=normalize_match_key(sku),
                title_key=normalize_match_key(title),
            )
            lines.append(line)

    return lines, errors, skipped_status


def aggregate_po_lines(lines: Sequence[SupplierPoLine]) -> Tuple[Dict[str, PoBucket], Dict[str, PoBucket]]:
    by_sku: Dict[str, PoBucket] = {}
    by_title: Dict[str, PoBucket] = {}
    for line in lines:
        if line.sku_key:
            by_sku.setdefault(line.sku_key, PoBucket()).add(line)
        if line.title_key:
            by_title.setdefault(line.title_key, PoBucket()).add(line)
    return by_sku, by_title


def load_po_inbound_snapshot(inbound_dir: Optional[Path]) -> PoInboundSnapshot:
    if inbound_dir is None:
        return PoInboundSnapshot(
            path=None,
            lines=[],
            by_sku={},
            by_title={},
            skipped_status=0,
            parse_errors=[],
        )
    path, unreadable_path, unreadable_err = find_latest_readable_inbound_csv(inbound_dir)
    if path is None:
        parse_errors: List[str] = []
        if unreadable_path is not None:
            parse_errors.append(
                f"{unreadable_path.name}: unreadable ({unreadable_err or 'permission denied'})"
            )
        return PoInboundSnapshot(
            path=None,
            lines=[],
            by_sku={},
            by_title={},
            skipped_status=0,
            parse_errors=parse_errors,
        )
    try:
        lines, errors, skipped = parse_po_csv(path)
    except (OSError, PermissionError) as exc:
        return PoInboundSnapshot(
            path=path,
            lines=[],
            by_sku={},
            by_title={},
            skipped_status=0,
            parse_errors=[f"{path.name}: unreadable ({exc})"],
        )
    by_sku, by_title = aggregate_po_lines(lines)
    return PoInboundSnapshot(
        path=path,
        lines=list(lines),
        by_sku=by_sku,
        by_title=by_title,
        skipped_status=skipped,
        parse_errors=errors,
    )


def build_shopify_match_indexes(
    variants: Iterable[Dict[str, str]],
) -> Tuple[Dict[str, str], Dict[str, str], set[str]]:
    """
    Build sku_key -> variant_id and title_key -> variant_id.

    Titles that map to more than one variant_id are recorded in ``ambiguous_titles``
    and omitted from the title index (SKU-only for those).
    """
    by_sku: Dict[str, str] = {}
    title_candidates: Dict[str, set[str]] = {}
    for v in variants:
        vid = clean_text(v.get("shopify_variant_id")) or ""
        if not vid:
            continue
        sku_key = normalize_match_key(v.get("sku"))
        if sku_key and sku_key not in by_sku:
            by_sku[sku_key] = vid
        title_key = normalize_match_key(v.get("product_title") or v.get("title"))
        if title_key:
            title_candidates.setdefault(title_key, set()).add(vid)

    by_title: Dict[str, str] = {}
    ambiguous: set[str] = set()
    for title_key, vids in title_candidates.items():
        if len(vids) == 1:
            by_title[title_key] = next(iter(vids))
        else:
            ambiguous.add(title_key)
    return by_sku, by_title, ambiguous


def format_po_order_ids(order_ids: Sequence[str], *, max_len: int = 120) -> str:
    text = ";".join(oid for oid in order_ids if oid)
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def allocate_po_cover_for_variant(
    *,
    sku: str,
    product_title: str,
    shopify_need: int,
    remaining_sku_qty: Dict[str, int],
    remaining_title_qty: Dict[str, int],
    by_sku: Dict[str, PoBucket],
    by_title: Dict[str, PoBucket],
    ambiguous_titles: set[str],
) -> Tuple[int, str, str]:
    """
    Consume open PO qty against one Shopify variant need.

    Returns (open_po_qty_applied, po_match, po_order_ids).
    """
    if shopify_need <= 0:
        return 0, "", ""

    sku_key = normalize_match_key(sku)
    title_key = normalize_match_key(product_title)

    if sku_key and remaining_sku_qty.get(sku_key, 0) > 0:
        available = remaining_sku_qty[sku_key]
        applied = min(shopify_need, available)
        remaining_sku_qty[sku_key] = available - applied
        # Keep title bucket in sync when same lines contribute to both indexes.
        if title_key and title_key in remaining_title_qty:
            remaining_title_qty[title_key] = max(0, remaining_title_qty[title_key] - applied)
        bucket = by_sku.get(sku_key)
        return applied, "sku", format_po_order_ids(bucket.order_ids if bucket else [])

    if (
        title_key
        and title_key not in ambiguous_titles
        and remaining_title_qty.get(title_key, 0) > 0
    ):
        available = remaining_title_qty[title_key]
        applied = min(shopify_need, available)
        remaining_title_qty[title_key] = available - applied
        bucket = by_title.get(title_key)
        return applied, "title", format_po_order_ids(bucket.order_ids if bucket else [])

    return 0, "", ""


def remaining_qty_maps(
    snapshot: PoInboundSnapshot,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    return (
        {k: b.qty for k, b in snapshot.by_sku.items()},
        {k: b.qty for k, b in snapshot.by_title.items()},
    )


def collect_unmatched_po_lines(
    snapshot: PoInboundSnapshot,
    *,
    matched_sku_keys: set[str],
    matched_title_keys: set[str],
    shopify_sku_keys: set[str],
    shopify_title_keys: set[str],
    ambiguous_titles: set[str],
) -> List[Dict[str, Any]]:
    """
    PO lines that never matched a Shopify variant (or were title-ambiguous).

    A line is matched if its sku_key is in matched_sku_keys, or (no usable sku match
    path) its title_key is in matched_title_keys.
    """
    rows: List[Dict[str, Any]] = []
    for line in snapshot.lines:
        if line.sku_key and line.sku_key in matched_sku_keys:
            continue
        if (
            line.sku_key
            and line.sku_key in shopify_sku_keys
            and line.sku_key in matched_sku_keys
        ):
            continue
        # SKU exists on Shopify but cover may have been fully consumed by another row —
        # still considered matched for unmatched reporting if SKU is known on Shopify.
        if line.sku_key and line.sku_key in shopify_sku_keys:
            continue
        if (
            not line.sku_key
            and line.title_key
            and line.title_key in matched_title_keys
        ):
            continue
        if (
            not line.sku_key
            and line.title_key
            and line.title_key in shopify_title_keys
            and line.title_key not in ambiguous_titles
        ):
            continue

        reason = "no_match"
        if line.title_key and line.title_key in ambiguous_titles and (
            not line.sku_key or line.sku_key not in shopify_sku_keys
        ):
            reason = "ambiguous_title"
        rows.append(
            {
                "order_id": line.order_id,
                "sku": line.sku,
                "title": line.title,
                "qty": line.qty,
                "unit_cost": line.unit_cost,
                "line_total": line.line_total,
                "status": line.status,
                "reason": reason,
            }
        )
    return rows


def write_unmatched_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=UNMATCHED_CSV_FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow(row)
