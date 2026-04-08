from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.services.moovies_import_service import (  # noqa: E402
    _resolve_raw_upsert_chunk_size,
    _upsert_raw_rows_in_chunks,
)


def test_moovies_raw_upsert_splits_into_chunks():
    batch_lengths: list[int] = []
    table_mock = MagicMock()

    def upsert_side_effect(batch, on_conflict=None):
        batch_lengths.append(len(batch))
        ret = MagicMock()
        ret.execute.return_value = None
        return ret

    table_mock.upsert.side_effect = upsert_side_effect
    sb = MagicMock()
    sb.table.return_value = table_mock

    rows = [{"supplier": "moovies", "upsert_key": f"k{i}"} for i in range(1200)]
    _upsert_raw_rows_in_chunks(
        sb,
        "staging_moovies_raw",
        rows,
        supplier="moovies",
        chunk_size=500,
    )

    assert batch_lengths == [500, 500, 200]
    assert sb.table.call_args[0][0] == "staging_moovies_raw"


@patch("app.services.moovies_import_service.time.sleep", lambda _s: None)
def test_moovies_raw_upsert_retries_timeout_once():
    attempts = {"n": 0}
    table_mock = MagicMock()

    def upsert_side_effect(batch, on_conflict=None):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise TimeoutError("canceling statement due to statement timeout (57014)")
        ret = MagicMock()
        ret.execute.return_value = None
        return ret

    table_mock.upsert.side_effect = upsert_side_effect
    sb = MagicMock()
    sb.table.return_value = table_mock

    _upsert_raw_rows_in_chunks(
        sb,
        "staging_moovies_raw",
        [{"supplier": "moovies", "upsert_key": "k1"}],
        supplier="moovies",
        chunk_size=250,
    )
    assert attempts["n"] == 2


@patch("app.services.moovies_import_service.time.sleep", lambda _s: None)
def test_moovies_raw_upsert_repeated_timeout_raises_clear_runtime_error():
    table_mock = MagicMock()

    def upsert_side_effect(batch, on_conflict=None):
        raise RuntimeError("APIError: canceling statement due to statement timeout (57014)")

    table_mock.upsert.side_effect = upsert_side_effect
    sb = MagicMock()
    sb.table.return_value = table_mock

    with pytest.raises(RuntimeError) as exc:
        _upsert_raw_rows_in_chunks(
            sb,
            "staging_moovies_raw",
            [{"supplier": "moovies", "upsert_key": "k1"}],
            supplier="moovies",
            chunk_size=250,
        )

    msg = str(exc.value)
    assert "supplier='moovies'" in msg
    assert "table='staging_moovies_raw'" in msg
    assert "chunk=1/1" in msg
    assert "rows_in_chunk=1" in msg
    assert "attempt=2" in msg
    assert "elapsed_ms=" in msg
    assert "MOOVIES_RAW_UPSERT_CHUNK_SIZE" in msg


def test_resolve_raw_upsert_chunk_size_prefers_moovies_specific_env(monkeypatch):
    monkeypatch.setenv("MOOVIES_RAW_UPSERT_CHUNK_SIZE", "250")
    monkeypatch.setenv("RAW_UPSERT_CHUNK_SIZE", "900")
    assert _resolve_raw_upsert_chunk_size(None) == 250

