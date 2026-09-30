"""
Bound staging_lasgo_raw growth by retaining only the newest N completed batches.

Keeps the batch-based INSERT model. Does not UPSERT per EAN.

Safety:
  - Call only after a Lasgo import has completed successfully with rows inserted.
  - A failed/partial import must never invoke prune.
  - Deletes are scoped by import_batch_id (index-friendly), never unscoped full-table deletes.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


DEFAULT_KEEP = 2
DEFAULT_TABLE = "staging_lasgo_raw"
DEFAULT_REGISTRY_REL = ".state/lasgo_raw_completed_batches.json"


def retention_enabled() -> bool:
    raw = (os.getenv("LASGO_RAW_RETENTION_ENABLED") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def retention_keep_count() -> int:
    raw = (os.getenv("LASGO_RAW_RETENTION_KEEP") or str(DEFAULT_KEEP)).strip()
    try:
        n = int(raw)
    except ValueError:
        return DEFAULT_KEEP
    return max(1, min(20, n))


def default_registry_path(project_root: Optional[str] = None) -> Path:
    override = (os.getenv("LASGO_RAW_BATCH_REGISTRY_PATH") or "").strip()
    if override:
        return Path(override)
    root = Path(project_root) if project_root else Path(__file__).resolve().parents[2]
    return root / DEFAULT_REGISTRY_REL


@dataclass(frozen=True)
class BatchRegistryEntry:
    import_batch_id: str
    completed_at: str
    row_count: int
    mode: str = ""
    source_filename: str = ""


@dataclass(frozen=True)
class RetentionResult:
    enabled: bool
    keep: list[str]
    deleted: list[str]
    skipped_reason: str = ""
    registry_path: str = ""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_registry(path: Path) -> list[BatchRegistryEntry]:
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    out: list[BatchRegistryEntry] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        bid = str(item.get("import_batch_id") or "").strip()
        if not bid:
            continue
        try:
            row_count = int(item.get("row_count") or 0)
        except (TypeError, ValueError):
            row_count = 0
        out.append(
            BatchRegistryEntry(
                import_batch_id=bid,
                completed_at=str(item.get("completed_at") or ""),
                row_count=row_count,
                mode=str(item.get("mode") or ""),
                source_filename=str(item.get("source_filename") or ""),
            )
        )
    return out


def save_registry(path: Path, entries: list[BatchRegistryEntry]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "import_batch_id": e.import_batch_id,
            "completed_at": e.completed_at,
            "row_count": e.row_count,
            "mode": e.mode,
            "source_filename": e.source_filename,
        }
        for e in entries
    ]
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def record_completed_batch(
    path: Path,
    *,
    import_batch_id: str,
    row_count: int,
    mode: str = "",
    source_filename: str = "",
    completed_at: Optional[str] = None,
) -> list[BatchRegistryEntry]:
    """Append/update a successfully completed batch; newest-last order preserved."""
    bid = (import_batch_id or "").strip()
    if not bid:
        return load_registry(path)
    entries = [e for e in load_registry(path) if e.import_batch_id != bid]
    entries.append(
        BatchRegistryEntry(
            import_batch_id=bid,
            completed_at=completed_at or _now_iso(),
            row_count=int(row_count),
            mode=mode or "",
            source_filename=source_filename or "",
        )
    )
    save_registry(path, entries)
    return entries


def plan_retention(
    entries: list[BatchRegistryEntry],
    *,
    keep_newest: int = DEFAULT_KEEP,
    completed_batch_id: str,
) -> tuple[list[str], list[str]]:
    """
    Return (keep_ids, delete_ids).

    Newest entries are at the end of the registry. The completed_batch_id must be
    present (caller records it first). Only batches older than the keep window
    are deleted candidates.
    """
    keep_n = max(1, keep_newest)
    # De-dupe preserving order (oldest → newest)
    ordered: list[str] = []
    seen: set[str] = set()
    for e in entries:
        bid = e.import_batch_id
        if bid in seen:
            continue
        seen.add(bid)
        ordered.append(bid)

    completed = (completed_batch_id or "").strip()
    if completed and completed not in seen:
        ordered.append(completed)

    if len(ordered) <= keep_n:
        return ordered[-keep_n:], []

    keep = ordered[-keep_n:]
    delete = [b for b in ordered[:-keep_n] if b not in keep]
    return keep, delete


def count_batch_rows(supabase: Any, table: str, import_batch_id: str) -> int:
    resp = (
        supabase.table(table)
        .select("id", count="exact")
        .eq("import_batch_id", import_batch_id)
        .limit(0)
        .execute()
    )
    return int(resp.count or 0)


def delete_batch_rows(supabase: Any, table: str, import_batch_id: str) -> None:
    """Delete all rows for one import_batch_id (uses batch index)."""
    supabase.table(table).delete().eq("import_batch_id", import_batch_id).execute()


def prune_lasgo_raw_batches(
    supabase: Any,
    *,
    completed_batch_id: str,
    row_count: int,
    mode: str = "",
    source_filename: str = "",
    table: str = DEFAULT_TABLE,
    keep_newest: Optional[int] = None,
    registry_path: Optional[Path] = None,
    project_root: Optional[str] = None,
    require_batch_rows: bool = True,
) -> RetentionResult:
    """
    After a successful Lasgo import: record the batch, keep newest N, delete older
    registry-known batches by import_batch_id.
    """
    path = registry_path or default_registry_path(project_root)
    keep_n = retention_keep_count() if keep_newest is None else max(1, keep_newest)
    bid = (completed_batch_id or "").strip()

    if not retention_enabled():
        return RetentionResult(
            enabled=False,
            keep=[],
            deleted=[],
            skipped_reason="retention_disabled",
            registry_path=str(path),
        )

    if not bid:
        return RetentionResult(
            enabled=True,
            keep=[],
            deleted=[],
            skipped_reason="missing_batch_id",
            registry_path=str(path),
        )

    if row_count <= 0:
        return RetentionResult(
            enabled=True,
            keep=[],
            deleted=[],
            skipped_reason="empty_import",
            registry_path=str(path),
        )

    if require_batch_rows:
        live = count_batch_rows(supabase, table, bid)
        if live <= 0:
            return RetentionResult(
                enabled=True,
                keep=[],
                deleted=[],
                skipped_reason="completed_batch_not_found",
                registry_path=str(path),
            )

    entries = record_completed_batch(
        path,
        import_batch_id=bid,
        row_count=row_count,
        mode=mode,
        source_filename=source_filename,
    )
    keep, to_delete = plan_retention(
        entries, keep_newest=keep_n, completed_batch_id=bid
    )

    deleted: list[str] = []
    for old_id in to_delete:
        delete_batch_rows(supabase, table, old_id)
        deleted.append(old_id)

    # Persist registry with only keep + any unknown kept for audit? Keep full history
    # of completed IDs but mark pruned ones removed from "active" by rewriting to keep
    # window only — avoids unbounded registry growth and makes retention idempotent.
    keep_set = set(keep)
    trimmed = [e for e in entries if e.import_batch_id in keep_set]
    # Preserve newest-last order of keep list
    by_id = {e.import_batch_id: e for e in trimmed}
    save_registry(path, [by_id[i] for i in keep if i in by_id])

    print(
        f"[lasgo-retention] keep={keep} deleted={deleted} table={table!r} "
        f"registry={path}",
        flush=True,
    )
    return RetentionResult(
        enabled=True,
        keep=keep,
        deleted=deleted,
        registry_path=str(path),
    )
