"""Moovies file loading (extensionless FTP feeds)."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.services.moovies_import_service import load_moovies_file  # noqa: E402


def test_extensionless_pipe_delimited_feed(tmp_path):
    p = tmp_path / "Feed-22-03-2026"
    p.write_text("ColA|ColB|ColC\n1|2|3\n4|5|6\n", encoding="utf-8")
    df = load_moovies_file(str(p))
    assert df.shape == (2, 3)
    assert list(df.columns) == ["ColA", "ColB", "ColC"]


def test_txt_suffix_still_pipe(tmp_path):
    p = tmp_path / "f.txt"
    p.write_text("A|B\nx|y\n", encoding="utf-8")
    df = load_moovies_file(str(p))
    assert list(df.columns) == ["A", "B"]
