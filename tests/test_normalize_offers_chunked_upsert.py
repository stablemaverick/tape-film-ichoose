"""Chunked upsert for staging_supplier_offers (Lasgo / Moovies normalization)."""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.services.normalize_offers_service import (  # noqa: E402
    _upsert_staging_offers_in_chunks,
)


def test_upsert_splits_into_expected_chunk_sizes():
    batch_lengths: list[int] = []
    table_mock = MagicMock()

    def upsert_side_effect(batch, on_conflict=None):
        batch_lengths.append(len(batch))
        b = MagicMock()
        b.execute.return_value = None
        return b

    table_mock.upsert.side_effect = upsert_side_effect
    sb = MagicMock()
    sb.table.return_value = table_mock

    rows = [{"supplier": "lasgo", "barcode": str(i)} for i in range(1200)]
    _upsert_staging_offers_in_chunks(
        sb,
        "staging_supplier_offers",
        rows,
        supplier="lasgo",
        log_prefix="[test]",
        chunk_size=500,
    )

    assert batch_lengths == [500, 500, 200]
    assert sb.table.call_args[0][0] == "staging_supplier_offers"


@patch("app.services.normalize_offers_service.time.sleep", lambda _s: None)
def test_upsert_retries_failed_chunk_once():
    attempts = {"n": 0}

    table_mock = MagicMock()

    class ExecOk:
        def execute(self):
            return None

    def upsert_side_effect(batch, on_conflict=None):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise TimeoutError("canceling statement due to statement timeout")
        ret = MagicMock()
        ret.execute = ExecOk().execute
        return ret

    table_mock.upsert.side_effect = upsert_side_effect

    sb = MagicMock()
    sb.table.return_value = table_mock

    rows = [{"supplier": "lasgo", "barcode": "1"}]
    _upsert_staging_offers_in_chunks(
        sb,
        "staging_supplier_offers",
        rows,
        supplier="lasgo",
        log_prefix="[test]",
        chunk_size=500,
    )

    assert attempts["n"] == 2
