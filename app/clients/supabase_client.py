"""
Centralised Supabase client with shared retry and pagination logic.

All pipeline scripts should use get_client() instead of calling
create_client() directly. This ensures consistent auth loading
and provides retry-aware query helpers.
"""

import os
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional

from dotenv import load_dotenv
from supabase import Client, create_client

from app.helpers.retry_helpers import execute_with_retry
from app.helpers.text_helpers import chunked


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value


@lru_cache(maxsize=1)
def get_client(env_file: str = ".env") -> Client:
    """Create and cache a Supabase client from environment variables."""
    load_dotenv(env_file)
    return create_client(
        _require_env("SUPABASE_URL"),
        _require_env("SUPABASE_SERVICE_KEY"),
    )


def create_fresh_client(env_file: str = ".env") -> Client:
    """Create a new (non-cached) Supabase client. Use for isolated test contexts."""
    load_dotenv(env_file)
    return create_client(
        _require_env("SUPABASE_URL"),
        _require_env("SUPABASE_SERVICE_KEY"),
    )


def paginated_fetch(
    client: Client,
    table: str,
    select: str,
    *,
    page_size: int = 1000,
    filters: Optional[Callable] = None,
    label: str = "",
) -> List[Dict[str, Any]]:
    """
    Fetch all rows from a table with pagination and retry.

    filters is an optional callable that receives the query builder
    and returns the modified query builder (e.g. to add .eq() or .is_()).
    """
    out: List[Dict[str, Any]] = []
    offset = 0

    while True:
        off = offset

        def _page():
            q = client.table(table).select(select).range(off, off + page_size - 1)
            if filters:
                q = filters(q)
            return q

        resp = execute_with_retry(_page, label=label or f"{table} offset={off}")
        page = resp.data or []
        if not page:
            break
        out.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    return out


def batch_upsert(
    client: Client,
    table: str,
    rows: List[Dict[str, Any]],
    *,
    on_conflict: str,
    batch_size: int = 1000,
    label: str = "",
) -> int:
    """Upsert rows in batches with retry. Returns total rows upserted."""
    total = 0
    for i, batch in enumerate(chunked(rows, batch_size)):
        def _upsert():
            return client.table(table).upsert(batch, on_conflict=on_conflict)
        execute_with_retry(_upsert, label=label or f"upsert batch {i + 1}")
        total += len(batch)
    return total
