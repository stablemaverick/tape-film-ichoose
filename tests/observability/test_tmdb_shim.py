"""Root tmdb_match_helpers re-exports app.helpers.tmdb_match_helpers."""

import importlib
import os
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")


@pytest.fixture()
def fresh_modules():
    """Ensure clean import of root shim vs app helper."""
    for name in list(sys.modules):
        if name == "tmdb_match_helpers" or name.startswith("app.helpers.tmdb_match_helpers"):
            del sys.modules[name]
    yield
    for name in list(sys.modules):
        if name == "tmdb_match_helpers" or name.startswith("app.helpers.tmdb_match_helpers"):
            del sys.modules[name]


def test_root_shim_delegates_to_app_helper(fresh_modules):
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    root_mod = importlib.import_module("tmdb_match_helpers")
    app_mod = importlib.import_module("app.helpers.tmdb_match_helpers")
    assert root_mod.normalize_match_title is app_mod.normalize_match_title
    assert root_mod.search_tmdb_movie_safe is app_mod.search_tmdb_movie_safe
