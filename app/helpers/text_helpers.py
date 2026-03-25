"""
Shared text/value parsing helpers used across the pipeline.
"""

import re
from datetime import datetime, timezone
from typing import Any, Iterable, Optional


def clean_text(value: Any) -> Optional[str]:
    """Strip whitespace, return None for empty/null values."""
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def now_iso() -> str:
    """UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def slugify(title: str) -> str:
    """Convert a product title to a URL-safe handle."""
    s = title.lower().strip()
    s = re.sub(r"['']+", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s or "product"


def parse_int(value: Any) -> int:
    """Parse an integer from a messy string (e.g. '1,200+')."""
    if value in (None, ""):
        return 0
    text = str(value).replace(",", "").replace("+", "").strip()
    m = re.search(r"\d+", text)
    return int(m.group(0)) if m else 0


def parse_price_gbp(value: Any) -> Optional[float]:
    """Parse a GBP price string (handles £ prefix, commas)."""
    if value in (None, ""):
        return None
    text = str(value).replace("£", "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(value: Any) -> Optional[str]:
    """Parse various date formats into ISO date string (YYYY-MM-DD)."""
    if not value:
        return None
    text = str(value).strip()
    for fmt in (
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def normalize_key(s: str) -> str:
    """Normalize a column header key for case-insensitive lookup."""
    return " ".join(str(s).strip().lower().split())


def lower_keys(record: dict) -> dict:
    """Convert all keys in a record to normalized lowercase form."""
    return {normalize_key(k): v for k, v in (record or {}).items()}


def pick(record_lc: dict, *candidates: str, default: str = "") -> str:
    """Pick the first non-empty value from candidate column names."""
    for name in candidates:
        key = normalize_key(name)
        val = record_lc.get(key)
        if val is None:
            continue
        s = str(val).strip()
        if s != "":
            return s
    return default


def chunked(items, size: int) -> Iterable[list]:
    """Yield successive chunks of the given size from items."""
    batch: list = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
