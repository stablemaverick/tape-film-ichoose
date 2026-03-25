"""
Tests for pipeline run history append and truncation.
"""

import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.observability.pipeline_run_history import append_pipeline_run_record  # noqa: E402
from app.observability.pipeline_history_schema import (  # noqa: E402
    EXPECTED_SCHEMA_VERSION,
    validate_history_envelope,
    validate_history_record,
)


def _minimal_record(i: int) -> dict:
    return {
        "timestamp": f"2026-01-{i:02d}T00:00:00+00:00",
        "pipeline_type": "catalog_sync",
        "log_file": f"/tmp/run{i}.log",
        "started_at": None,
        "ended_at": None,
        "duration_seconds": float(i),
        "completed": True,
        "inserts": i,
        "updates": 100,
        "tmdb_matched_pct": 90.0,
        "film_linked_pct": 88.0,
        "catalog_rows_active": 1000,
        "missing_sale_price_pct": 1.0,
        "null_barcode_rows": 0,
        "duplicate_films": 0,
        "failures": 0,
        "health_exit_code": 0,
        "lock_encountered": False,
    }


class TestAppendAndTruncate:
    def test_append_and_truncate_max_runs(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.unlink(path)
        try:
            for i in range(1, 6):
                append_pipeline_run_record(_minimal_record(i), path, max_runs=3)
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            assert data["schema_version"] == EXPECTED_SCHEMA_VERSION
            assert len(data["runs"]) == 3
            assert data["runs"][0]["inserts"] == 3
            assert data["runs"][-1]["inserts"] == 5
            assert validate_history_envelope(data) == []
        finally:
            if os.path.isfile(path):
                os.unlink(path)
            lockp = Path(path).with_suffix(Path(path).suffix + ".lock")
            if lockp.is_file():
                lockp.unlink()

    def test_invalid_record_raises(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.unlink(path)
        try:
            bad = _minimal_record(1)
            del bad["timestamp"]
            try:
                append_pipeline_run_record(bad, path, max_runs=10)
            except ValueError as exc:
                assert "timestamp" in str(exc).lower() or "required" in str(exc).lower()
            else:
                raise AssertionError("expected ValueError")
        finally:
            if os.path.isfile(path):
                os.unlink(path)
            lockp = Path(path).with_suffix(Path(path).suffix + ".lock")
            if lockp.is_file():
                lockp.unlink()


class TestSchema:
    def test_validate_record_rejects_bad_health_code(self):
        r = _minimal_record(1)
        r["health_exit_code"] = 99
        errs = validate_history_record(r)
        assert any("health_exit_code" in e for e in errs)
