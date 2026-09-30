"""Regression: local history PermissionError must not fail after DB persist."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts" / "observability"))


def test_append_history_permission_error_nonfatal_when_db_ok(monkeypatch, tmp_path):
    import append_pipeline_run_history as mod

    log_file = tmp_path / "stock.log"
    log_file.write_text(
        "[2026-08-07 00:00:00] Starting STOCK SYNC\n"
        "Operational sync complete. inserted=0 updated=10\n"
        "OPERATIONAL_STOCK_SYNC_STATUS=success\n"
        "INVENTORY_INTELLIGENCE_PROJECTION_STATUS=skipped\n"
        "[2026-08-07 00:05:00] STOCK SYNC complete\n",
        encoding="utf-8",
    )
    history = tmp_path / "pipeline_run_history.json"

    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")

    monkeypatch.setattr(mod, "load_dotenv", lambda *_a, **_k: None)
    monkeypatch.setattr(mod, "create_client", lambda *_a, **_k: object())
    monkeypatch.setattr(
        mod,
        "gather_metrics",
        lambda *_a, **_k: {
            "generated_at": "2026-08-07T00:05:00+00:00",
            "exit_code": 0,
            "linkage": {},
            "coverage": {},
            "commercial": {},
            "exceptions": {},
        },
    )
    monkeypatch.setattr(
        mod,
        "persist_pipeline_observability_safe",
        lambda *_a, **_k: "run-db-id",
    )

    def boom(*_a, **_k):
        raise PermissionError(13, "Permission denied", str(history))

    monkeypatch.setattr(mod, "append_pipeline_run_record", boom)

    argv = [
        "--log-file",
        str(log_file),
        "--history-file",
        str(history),
        "--pipeline-type",
        "stock_sync",
        "--env",
        str(tmp_path / "missing.env"),
    ]
    monkeypatch.setattr(sys, "argv", ["append_pipeline_run_history.py", *argv])
    code = mod.main()
    assert code == 0


def test_log_parser_reads_separated_status_lines(tmp_path):
    from app.observability.pipeline_log_parser import parse_log_file

    log_file = tmp_path / "stock.log"
    log_file.write_text(
        "[2026-08-07 00:00:00] Starting STOCK SYNC\n"
        "Operational sync complete. inserted=0 updated=12\n"
        "OPERATIONAL_STOCK_SYNC_STATUS=success\n"
        "INVENTORY_INTELLIGENCE_PROJECTION_STATUS=failed\n"
        "[2026-08-07 00:05:00] STOCK SYNC complete\n",
        encoding="utf-8",
    )
    run = parse_log_file(str(log_file))
    assert run.operational_status == "success"
    assert run.inventory_intelligence_projection_status == "failed"
    assert run.completed is True
    assert run.operational_updated == 12
