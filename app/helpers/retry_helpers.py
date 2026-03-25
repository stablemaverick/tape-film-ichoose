"""
Shared retry logic for Supabase and HTTP operations.

All pipeline scripts that interact with Supabase use this module
to handle transient HTTP/2 stream resets, gateway errors, and timeouts.
"""

import time
from typing import Any, Callable

import httpx

TRANSIENT_EXCEPTIONS = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)

GATEWAY_ERROR_CODES = {"502", "504"}


def execute_with_retry(
    build_query: Callable[[], Any],
    *,
    max_retries: int = 10,
    label: str = "",
    initial_delay: float = 1.0,
    max_delay: float = 90.0,
) -> Any:
    """
    Execute a Supabase query with automatic retry on transient errors.

    build_query must be a callable that returns a query chain (before .execute()).
    The chain is rebuilt on each attempt to avoid stale connection state.

    Retries on:
      - httpx transport errors (connection reset, timeout, pool exhaustion)
      - HTTP 502/504 gateway errors from Supabase/Cloudflare
    """
    delay = initial_delay
    last_err: Exception | None = None

    for attempt in range(max_retries):
        try:
            return build_query().execute()
        except TRANSIENT_EXCEPTIONS as exc:
            last_err = exc
            if attempt == max_retries - 1:
                raise
            tag = f" [{label}]" if label else ""
            print(
                f"WARN: transient HTTP error{tag} ({exc!r}); "
                f"retry {attempt + 1}/{max_retries} in {delay:.1f}s"
            )
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
        except Exception as exc:
            err_str = str(exc)
            if any(code in err_str for code in GATEWAY_ERROR_CODES) or "Bad gateway" in err_str:
                last_err = exc
                if attempt == max_retries - 1:
                    raise
                tag = f" [{label}]" if label else ""
                print(
                    f"WARN: gateway error{tag}; "
                    f"retry {attempt + 1}/{max_retries} in {delay:.1f}s"
                )
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
            else:
                raise

    raise RuntimeError("Retry loop exhausted") from last_err
