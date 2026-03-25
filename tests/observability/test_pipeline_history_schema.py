"""
Tests for pipeline history JSON schema validation.
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.observability.pipeline_history_schema import (  # noqa: E402
    EXPECTED_SCHEMA_VERSION,
    load_and_validate_history_file,
    validate_history_envelope,
    validate_history_record,
)


def test_validate_envelope_wrong_version():
    errs = validate_history_envelope({"schema_version": 999, "runs": []})
    assert any("schema_version" in e for e in errs)


def test_validate_envelope_runs_not_list():
    errs = validate_history_envelope({"schema_version": EXPECTED_SCHEMA_VERSION, "runs": {}})
    assert any("runs" in e.lower() for e in errs)


def test_load_invalid_json():
    fd, path = tempfile.mkstemp(suffix=".json", text=True)
    os.write(fd, b"{not json")
    os.close(fd)
    try:
        data, errs = load_and_validate_history_file(path)
        assert data is None
        assert errs
    finally:
        os.unlink(path)


def test_round_trip_valid_file():
    payload = {
        "schema_version": EXPECTED_SCHEMA_VERSION,
        "runs": [
            {
                "timestamp": "2026-01-01T00:00:00+00:00",
                "pipeline_type": "stock_sync",
                "log_file": "/x.log",
                "duration_seconds": 12.5,
                "completed": True,
                "inserts": 0,
                "updates": 50,
                "failures": 0,
                "health_exit_code": 0,
                "lock_encountered": False,
            }
        ],
    }
    fd, path = tempfile.mkstemp(suffix=".json", text=True)
    os.close(fd)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        data, errs = load_and_validate_history_file(path)
        assert errs == []
        assert data == payload
    finally:
        os.unlink(path)


def test_record_extra_keys_allowed():
    r = {
        "timestamp": "2026-01-01T00:00:00+00:00",
        "pipeline_type": "catalog_sync",
        "log_file": "/a.log",
        "duration_seconds": 1,
        "completed": True,
        "inserts": 0,
        "updates": 0,
        "failures": 0,
        "health_exit_code": 0,
        "lock_encountered": False,
        "future_field": {"nested": True},
    }
    assert validate_history_record(r) == []
