"""
Tests for app.observability.pipeline_log_parser
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.observability.pipeline_log_parser import (  # noqa: E402
    extract_catalog_upsert_counts,
    parse_log_file,
    parse_log_runs,
    total_failures,
)


def _write_log(content: str) -> str:
    fd, path = tempfile.mkstemp(suffix=".log", text=True)
    os.close(fd)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


class TestParseLogFile:
    def test_catalog_sync_complete(self):
        p = _write_log(
            "[2026-03-22 07:00:00] Starting CATALOG SYNC\n"
            "[step 05] Upsert supplier offers -> catalog_items\n"
            "Operational sync complete. inserted=130 updated=22119\n"
            "[2026-03-22 07:15:00] CATALOG SYNC complete\n"
        )
        try:
            run = parse_log_file(p)
            assert run.completed is True
            assert run.pipeline_type == "catalog_sync"
            assert run.started_at == "2026-03-22 07:00:00"
            assert run.ended_at == "2026-03-22 07:15:00"
            assert len(run.steps) >= 1
        finally:
            os.unlink(p)

    def test_append_without_completion_banner_marks_completed(self):
        """Older runs: step 08 ran before the shell wrote SYNC complete (duration may stay 0)."""
        p = _write_log(
            "[2026-03-25 10:32:20] Starting CATALOG SYNC\n"
            "[step 01] Import\n"
            "append_pipeline_run_history: appended run to logs/pipeline_run_history.json\n"
        )
        try:
            run = parse_log_file(p)
            assert run.completed is True
            assert run.started_at == "2026-03-25 10:32:20"
            assert run.ended_at == "2026-03-25 10:32:20"
            assert run.duration_seconds == 0
        finally:
            os.unlink(p)

    def test_completion_banner_after_append_still_sets_ended_at(self):
        p = _write_log(
            "[2026-03-25 10:00:00] Starting CATALOG SYNC\n"
            "append_pipeline_run_history: appended run to x.json\n"
            "[2026-03-25 11:00:00] CATALOG SYNC complete\n"
        )
        try:
            run = parse_log_file(p)
            assert run.completed is True
            assert run.ended_at == "2026-03-25 11:00:00"
            assert run.duration_seconds == 3600.0
        finally:
            os.unlink(p)

    def test_extract_operational_line(self):
        p = _write_log(
            "Operational sync complete. inserted=10 updated=2000\n"
        )
        try:
            run = parse_log_file(p)
            assert extract_catalog_upsert_counts(run) == (10, 2000)
        finally:
            os.unlink(p)

    def test_failures_increment_on_error(self):
        p = _write_log(
            "[2026-01-01 00:00:00] Starting CATALOG SYNC\n"
            "[step 03] Normalize\n"
            "ERROR: something failed\n"
        )
        try:
            run = parse_log_file(p)
            assert total_failures(run) >= 1
        finally:
            os.unlink(p)

    def test_last_run_only_when_multiple_starts_in_file(self):
        p = _write_log(
            "[2026-03-22 10:00:00] Starting CATALOG SYNC\n"
            "[step 05] Upsert supplier offers -> catalog_items\n"
            "Operational sync complete. inserted=100 updated=200\n"
            "[2026-03-22 10:30:00] CATALOG SYNC complete\n"
            "[2026-03-22 20:00:00] Starting CATALOG SYNC\n"
            "[step 00] Fetch\n"
            "Traceback (most recent call last):\n"
            "ERROR: EOFError\n"
        )
        try:
            run = parse_log_file(p)
            assert run.completed is False
            assert run.started_at == "2026-03-22 20:00:00"
            assert run.operational_inserted == 0 and run.operational_updated == 0
            assert run.has_operational_totals is False
            runs = parse_log_runs(p)
            assert len(runs) == 2
            assert runs[0].completed is True
            assert runs[0].operational_inserted == 100
            assert extract_catalog_upsert_counts(runs[0]) == (100, 200)
        finally:
            os.unlink(p)
