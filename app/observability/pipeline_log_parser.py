"""
Parse pipeline log files into structured runs (used by pipeline_run_report + history).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class StepResult:
    name: str
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    duration_seconds: float = 0
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    retries: int = 0
    failures: int = 0
    completed: bool = False


@dataclass
class PipelineRun:
    log_file: str
    pipeline_type: str = ""
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    duration_seconds: float = 0
    steps: List[StepResult] = field(default_factory=list)
    lock_encountered: bool = False
    completed: bool = False
    supplier_files: List[str] = field(default_factory=list)
    # Set when "Operational sync complete. inserted=X updated=Y" appears in this run chunk
    has_operational_totals: bool = False
    operational_inserted: int = 0
    operational_updated: int = 0
    # Catalog sync per-source fetch outcome (from step 00 / final summary)
    catalog_source_lasgo_status: Optional[str] = None
    catalog_source_moovies_status: Optional[str] = None
    catalog_sync_summary: Optional[str] = None


STEP_PATTERN = re.compile(r"\[step (\d+)\]\s*(.*)")
TIMESTAMP_PATTERN = re.compile(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]")
INSERTED_PATTERN = re.compile(r"inserted?=(\d+)")
UPDATED_PATTERN = re.compile(r"updated?=(\d+)")
ROWS_PATTERN = re.compile(r"(?:rows?|total)[=: ]*(\d+)", re.IGNORECASE)
RETRY_PATTERN = re.compile(r"\b(?:RETRY|retry)\s+\d+/\d+", re.IGNORECASE)
# Supplier paths from fetch step only (avoid tracebacks / "args.env" false positives)
DOWNLOADED_BASENAME_PATTERN = re.compile(r"Downloaded:\s+\S+[/\\]([^/\\\s]+)\s*->")
ENV_CATALOG_FILE_PATTERN = re.compile(r"^(?:MOOVIES|LASGO)_FILE=(.+)$")
LOCK_PATTERN = re.compile(
    r"Another\s+(?:catalog|stock)\s+sync\s+is\s+already\s+running",
    re.IGNORECASE,
)
# Prefer this for ended_at / completed — timestamp is authoritative (not "last [ts]" in file).
SYNC_COMPLETE_BANNER_PATTERN = re.compile(
    r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s*(CATALOG|STOCK)\s+SYNC\s+complete\b",
    re.IGNORECASE,
)
COMPLETE_PATTERN = re.compile(r"(?:CATALOG|STOCK) SYNC complete", re.IGNORECASE)
# When append ran before the completion banner was written (older runs), treat success as completed.
APPEND_HISTORY_OK_PATTERN = re.compile(
    r"append_pipeline_run_history:\s+appended run to\s+",
    re.IGNORECASE,
)
# Catalog upsert final line (logged by both pipelines; stock sync logs it as step 04)
OPERATIONAL_SYNC_PATTERN = re.compile(
    r"Operational sync complete\.\s*inserted=(\d+)\s+updated=(\d+)",
    re.IGNORECASE,
)
CATALOG_SYNC_SOURCE_STATUS_PATTERN = re.compile(
    r"CATALOG_SYNC_SOURCE_STATUS\s+lasgo=(\S+)\s+moovies=(\S+)",
    re.IGNORECASE,
)
CATALOG_SYNC_SUMMARY_LINE_PATTERN = re.compile(
    r"^CATALOG_SYNC_SUMMARY:\s*(.+?)\s*$",
    re.IGNORECASE,
)

_START_CATALOG_SYNC = re.compile(r"Starting\s+CATALOG\s+SYNC", re.IGNORECASE)
_START_STOCK_SYNC = re.compile(r"Starting\s+STOCK\s+SYNC", re.IGNORECASE)


def _is_pipeline_run_start_line(line: str) -> bool:
    return bool(_START_CATALOG_SYNC.search(line) or _START_STOCK_SYNC.search(line))


def _chunk_lines_by_run_start(lines: List[str]) -> List[List[str]]:
    """Split log lines into chunks, each beginning at 'Starting CATALOG|STOCK SYNC'."""
    idxs = [i for i, line in enumerate(lines) if _is_pipeline_run_start_line(line)]
    if not idxs:
        return [lines] if lines else [[]]
    chunks: List[List[str]] = []
    for j, start in enumerate(idxs):
        end = idxs[j + 1] if j + 1 < len(idxs) else len(lines)
        chunks.append(lines[start:end])
    return chunks


def _append_supplier_file(run: PipelineRun, fname: str) -> None:
    fname = fname.strip().strip("'\"")
    if not fname or fname in run.supplier_files:
        return
    run.supplier_files.append(fname)


def _parse_line_for_supplier_files(run: PipelineRun, line: str) -> None:
    m = DOWNLOADED_BASENAME_PATTERN.search(line)
    if m:
        _append_supplier_file(run, m.group(1))
        return
    m2 = ENV_CATALOG_FILE_PATTERN.match(line.strip())
    if m2:
        raw = m2.group(1).strip().strip("'\"")
        _append_supplier_file(run, os.path.basename(raw))


def _parse_run_chunk(lines: List[str], filepath: str) -> PipelineRun:
    """Parse one pipeline invocation (lines already scoped to that run)."""
    run = PipelineRun(log_file=filepath)
    current_step: Optional[StepResult] = None
    last_bracket_ts: Optional[str] = None
    sync_complete_banner_at: Optional[str] = None

    for line in lines:
        line = line.rstrip()

        ts_match = TIMESTAMP_PATTERN.search(line)
        if ts_match:
            ts = ts_match.group(1)
            if run.started_at is None:
                run.started_at = ts
            last_bracket_ts = ts

        step_match = STEP_PATTERN.search(line)
        if step_match:
            if current_step:
                current_step.completed = True
            current_step = StepResult(
                name=f"Step {step_match.group(1)}: {step_match.group(2).strip()}"
            )
            if ts_match:
                current_step.started_at = ts_match.group(1)
            run.steps.append(current_step)

        if current_step:
            ins = INSERTED_PATTERN.search(line)
            if ins:
                current_step.rows_inserted += int(ins.group(1))
            upd = UPDATED_PATTERN.search(line)
            if upd:
                current_step.rows_updated += int(upd.group(1))
            rows = ROWS_PATTERN.search(line)
            if rows:
                current_step.rows_processed = max(
                    current_step.rows_processed, int(rows.group(1))
                )
            if RETRY_PATTERN.search(line):
                current_step.retries += 1
            if "ERROR" in line or "FAIL" in line:
                current_step.failures += 1
            if ts_match:
                current_step.ended_at = ts_match.group(1)

        _parse_line_for_supplier_files(run, line)

        if LOCK_PATTERN.search(line):
            run.lock_encountered = True

        op = OPERATIONAL_SYNC_PATTERN.search(line)
        if op:
            run.has_operational_totals = True
            run.operational_inserted = int(op.group(1))
            run.operational_updated = int(op.group(2))

        banner = SYNC_COMPLETE_BANNER_PATTERN.search(line)
        if banner:
            sync_complete_banner_at = banner.group(1)
            run.completed = True
            kind = banner.group(2).upper()
            if kind == "CATALOG":
                run.pipeline_type = "catalog_sync"
            elif kind == "STOCK":
                run.pipeline_type = "stock_sync"
        elif COMPLETE_PATTERN.search(line):
            # Legacy line without leading "[ts]" (unlikely); still mark complete.
            run.completed = True
            if "CATALOG" in line.upper():
                run.pipeline_type = "catalog_sync"
            elif "STOCK" in line.upper():
                run.pipeline_type = "stock_sync"

        if APPEND_HISTORY_OK_PATTERN.search(line):
            # Historic logs: step 08 ran before the shell wrote the completion banner.
            if not run.completed:
                run.completed = True

        css = CATALOG_SYNC_SOURCE_STATUS_PATTERN.search(line)
        if css:
            run.catalog_source_lasgo_status = css.group(1).strip().lower()
            run.catalog_source_moovies_status = css.group(2).strip().lower()

        csum = CATALOG_SYNC_SUMMARY_LINE_PATTERN.match(line.strip())
        if csum:
            run.catalog_sync_summary = csum.group(1).strip()

    if current_step:
        current_step.completed = True

    # Prefer "[ts] CATALOG|STOCK SYNC complete" so later lines (e.g. future banners) do not shift ended_at.
    if sync_complete_banner_at:
        run.ended_at = sync_complete_banner_at
    elif run.completed and last_bracket_ts:
        run.ended_at = last_bracket_ts
    else:
        run.ended_at = last_bracket_ts or run.started_at

    if run.started_at and run.ended_at:
        try:
            start = datetime.strptime(run.started_at, "%Y-%m-%d %H:%M:%S")
            end = datetime.strptime(run.ended_at, "%Y-%m-%d %H:%M:%S")
            run.duration_seconds = (end - start).total_seconds()
        except ValueError:
            pass

    return run


def parse_log_runs(filepath: str) -> List[PipelineRun]:
    """
    Parse every pipeline invocation in a log file (split on 'Starting CATALOG SYNC' /
    'Starting STOCK SYNC'). If the file has no such markers, the whole file is one run.
    """
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        lines = [ln.rstrip("\r\n") for ln in f]
    chunks = _chunk_lines_by_run_start(lines)
    return [_parse_run_chunk(ch, filepath) for ch in chunks if ch]


def parse_log_file(filepath: str) -> PipelineRun:
    """
    Parse the **last** pipeline invocation in the file (same file may contain many runs
    appended across the day). For all runs use parse_log_runs().
    """
    runs = parse_log_runs(filepath)
    if not runs:
        return PipelineRun(log_file=filepath)
    return runs[-1]


def extract_catalog_upsert_counts(run: PipelineRun) -> tuple[int, int]:
    """
    Prefer totals from 'Operational sync complete. inserted=X updated=Y' in this run chunk;
    else use parsed counts from the upsert-to-catalog step in the log.
    """
    if run.has_operational_totals:
        return run.operational_inserted, run.operational_updated

    for step in run.steps:
        if "Upsert" in step.name and "catalog" in step.name.lower():
            return step.rows_inserted, step.rows_updated

    # Legacy: last operational line in file (whole file) — only if single-run parse failed
    with open(run.log_file, "r", encoding="utf-8", errors="replace") as f:
        last: Optional[tuple[int, int]] = None
        for line in f:
            m = OPERATIONAL_SYNC_PATTERN.search(line)
            if m:
                last = (int(m.group(1)), int(m.group(2)))
        if last:
            return last
    return 0, 0


def total_failures(run: PipelineRun) -> int:
    return sum(s.failures for s in run.steps)
