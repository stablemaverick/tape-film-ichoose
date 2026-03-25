"""
Validate pipeline_run_history.json envelope and per-run records.

Expected file shape::

    {"schema_version": 1, "runs": [ {...}, ... ]}
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Keep in sync with app.observability.pipeline_run_history.SCHEMA_VERSION
EXPECTED_SCHEMA_VERSION = 1

# Keys that must be present on each run record (as written by build_history_record)
REQUIRED_RUN_KEYS = (
    "timestamp",
    "pipeline_type",
    "log_file",
    "duration_seconds",
    "completed",
    "inserts",
    "updates",
    "failures",
    "health_exit_code",
    "lock_encountered",
)

# key -> allowed Python types (None allowed in addition for optional metrics)
RUN_KEY_TYPES: Dict[str, Tuple[type, ...]] = {
    "timestamp": (str,),
    "pipeline_type": (str,),
    "log_file": (str,),
    "started_at": (str, type(None)),
    "ended_at": (str, type(None)),
    "duration_seconds": (int, float),
    "completed": (bool,),
    "inserts": (int,),
    "updates": (int,),
    "tmdb_matched_pct": (int, float, type(None)),
    "film_linked_pct": (int, float, type(None)),
    "catalog_rows_active": (int, type(None)),
    "missing_sale_price_pct": (int, float, type(None)),
    "null_barcode_rows": (int, type(None)),
    "duplicate_films": (int, type(None)),
    "failures": (int,),
    "health_exit_code": (int,),
    "lock_encountered": (bool,),
}


def validate_history_record(record: Dict[str, Any], *, path_hint: str = "") -> List[str]:
    """
    Return a list of human-readable errors; empty means the record is valid.
    Unknown extra keys are allowed (forward compatibility).
    """
    errors: List[str] = []
    prefix = f"{path_hint}: " if path_hint else ""

    if not isinstance(record, dict):
        return [f"{prefix}record must be a JSON object"]

    for key in REQUIRED_RUN_KEYS:
        if key not in record:
            errors.append(f"{prefix}missing required key {key!r}")

    for key, value in record.items():
        if key not in RUN_KEY_TYPES:
            continue
        allowed = RUN_KEY_TYPES[key]
        if value is not None and not isinstance(value, allowed):
            errors.append(
                f"{prefix}key {key!r} must be one of {allowed}, got {type(value).__name__}"
            )

    hec = record.get("health_exit_code")
    if hec is not None and hec not in (0, 1, 2):
        errors.append(f"{prefix}health_exit_code must be 0, 1, or 2, got {hec!r}")

    ptype = record.get("pipeline_type")
    if ptype is not None and not isinstance(ptype, str):
        errors.append(f"{prefix}pipeline_type must be str")

    return errors


def validate_history_envelope(data: Any, *, path_hint: str = "") -> List[str]:
    """Validate top-level JSON object (schema_version + runs)."""
    errors: List[str] = []
    prefix = f"{path_hint}: " if path_hint else ""

    if not isinstance(data, dict):
        return [f"{prefix}root must be a JSON object"]

    ver = data.get("schema_version")
    if ver != EXPECTED_SCHEMA_VERSION:
        errors.append(
            f"{prefix}schema_version must be {EXPECTED_SCHEMA_VERSION}, got {ver!r}"
        )

    runs = data.get("runs")
    if not isinstance(runs, list):
        errors.append(f"{prefix}runs must be a list")
        return errors

    for i, run in enumerate(runs):
        errors.extend(validate_history_record(run, path_hint=f"runs[{i}]"))

    return errors


def load_and_validate_history_file(path: Union[str, Path]) -> Tuple[Optional[Dict], List[str]]:
    """
    Load JSON from path and validate. Returns (data, errors).
    On JSON decode error, data is None.
    """
    p = Path(path)
    if not p.is_file():
        return None, [f"file not found: {p}"]

    try:
        raw = p.read_text(encoding="utf-8")
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, [f"invalid JSON: {exc}"]

    return data, validate_history_envelope(data, path_hint=str(p))
