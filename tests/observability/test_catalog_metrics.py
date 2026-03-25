"""
Pure calculation tests for catalog health metrics.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.observability.catalog_metrics import (  # noqa: E402
    EXIT_CRITICAL,
    EXIT_OK,
    EXIT_WARNING,
    compute_film_link_pct,
    compute_missing_price_pct,
    compute_tmdb_match_rate_pct,
    resolve_exit_code_from_alerts,
)


class TestComputeFilmLinkPct:
    def test_zero_total(self):
        assert compute_film_link_pct(0, 0) == 0.0

    def test_rounding(self):
        assert compute_film_link_pct(1, 3) == 33.3


class TestComputeTmdbMatchRate:
    def test_zero_denom(self):
        assert compute_tmdb_match_rate_pct(0, 0) == 0.0

    def test_basic(self):
        assert compute_tmdb_match_rate_pct(9, 1) == 90.0


class TestComputeMissingPricePct:
    def test_basic(self):
        assert compute_missing_price_pct(5, 100) == 5.0


class TestResolveExitCode:
    def test_empty(self):
        assert resolve_exit_code_from_alerts([]) == EXIT_OK

    def test_warning(self):
        alerts = [{"level": "WARNING", "message": "x"}]
        assert resolve_exit_code_from_alerts(alerts) == EXIT_WARNING

    def test_critical_wins(self):
        alerts = [
            {"level": "WARNING", "message": "w"},
            {"level": "CRITICAL", "message": "c"},
        ]
        assert resolve_exit_code_from_alerts(alerts) == EXIT_CRITICAL
