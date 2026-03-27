"""
Catalog sync: no-files per source is a clean no-op (not a pipeline failure).
"""

from __future__ import annotations

import os
import sys
import tempfile
from unittest.mock import patch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.observability.pipeline_log_parser import (  # noqa: E402
    PipelineRun,
    parse_log_file,
)
from app.observability.pipeline_run_history import build_history_record  # noqa: E402
from app.services.supplier_fetch_service import (  # noqa: E402
    catalog_sync_source_summary_message,
    run_catalog_archive_from_env,
)


def test_summary_both_success():
    s = catalog_sync_source_summary_message(lasgo="success", moovies="success")
    assert "Catalog sync completed." in s
    assert "Lasgo catalog processed successfully." in s
    assert "Moovies catalog processed successfully." in s


def test_summary_mixed_skipped_moovies():
    s = catalog_sync_source_summary_message(lasgo="success", moovies="skipped_no_files")
    assert "Lasgo catalog processed successfully." in s
    assert "No Moovies catalog files were available to process." in s


def test_summary_both_skipped():
    s = catalog_sync_source_summary_message(lasgo="skipped_no_files", moovies="skipped_no_files")
    assert "No Lasgo catalog files were available to process." in s
    assert "No Moovies catalog files were available to process." in s


def test_parse_log_catalog_source_and_summary():
    p = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".log",
        delete=False,
        encoding="utf-8",
    )
    try:
        p.write(
            "[2026-03-24 08:00:00] Starting CATALOG SYNC\n"
            "[step 00] Fetch\n"
            "CATALOG_SYNC_SOURCE_STATUS lasgo=success moovies=skipped_no_files\n"
            "[step 01] Import Moovies — SKIPPED\n"
            "[step 02] Import Lasgo raw (full, Blu-ray only)\n"
            "CATALOG_SYNC_SUMMARY: Catalog sync completed. Lasgo catalog processed successfully. "
            "No Moovies catalog files were available to process.\n"
            "[2026-03-24 08:30:00] CATALOG SYNC complete\n"
        )
        p.close()
        run = parse_log_file(p.name)
        assert run.completed is True
        assert run.catalog_source_lasgo_status == "success"
        assert run.catalog_source_moovies_status == "skipped_no_files"
        assert run.catalog_sync_summary is not None
        assert "No Moovies catalog files" in run.catalog_sync_summary
    finally:
        os.unlink(p.name)


def test_build_history_record_includes_catalog_source_fields():
    run = PipelineRun(log_file="/nonexistent/catalog.log")
    run.completed = True
    run.has_operational_totals = True
    run.operational_inserted = 0
    run.operational_updated = 0
    run.catalog_sync_summary = "Catalog sync completed. Lasgo ok. Moovies skipped."
    run.catalog_source_lasgo_status = "success"
    run.catalog_source_moovies_status = "skipped_no_files"
    rec = build_history_record(
        run=run,
        metrics={"generated_at": "2026-03-24T12:00:00Z", "exit_code": 0},
        pipeline_type="catalog_sync",
    )
    assert rec["catalog_sync_summary"] == run.catalog_sync_summary
    assert rec["catalog_source_lasgo_status"] == "success"
    assert rec["catalog_source_moovies_status"] == "skipped_no_files"


def test_archive_from_env_no_ftp_keys_is_noop():
    manifest = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".env",
        delete=False,
        encoding="utf-8",
    )
    try:
        manifest.write(
            "MOOVIES_FILE=\n"
            "LASGO_FILE=\n"
            "CATALOG_SOURCE_MOOVIES_STATUS=skipped_no_files\n"
            "CATALOG_SOURCE_LASGO_STATUS=skipped_no_files\n"
        )
        manifest.close()
        with patch("app.services.supplier_fetch_service.build_ftp_client") as mock_ftp:
            run_catalog_archive_from_env(
                fetch_env_path=manifest.name,
                env_file=os.path.join(ROOT, ".env.nosuch"),
            )
            mock_ftp.assert_not_called()
    finally:
        os.unlink(manifest.name)
