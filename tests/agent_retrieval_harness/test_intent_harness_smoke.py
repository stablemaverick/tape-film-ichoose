"""Smoke tests for isolated intent detection + intent-aware ranking (no production imports)."""

from __future__ import annotations

import pytest

from tests.agent_retrieval_harness.intent_catalog_fixtures import rows_as_dict_list
from tests.agent_retrieval_harness.intent_detection_harness import detect_intent_category
from tests.agent_retrieval_harness.intent_query_cases import INTENT_QUERY_CASES
from tests.agent_retrieval_harness.intent_ranking_harness import rank_intent_aware


def test_decade_query_maps_to_year_decade_intent() -> None:
    assert detect_intent_category("80s action movies") == "year_decade"


@pytest.mark.parametrize(
    "query,expected",
    [
        ("films by christopher nolan", "director"),
        ("movies starring tom hanks", "actor"),
        ("horror films from 1999", "year_decade"),
        ("criterion collection kurosawa", "distributor_label"),
        ("oscar winning best picture", "awards_discovery"),
        ("in stock 4k", "commerce"),
    ],
)
def test_detect_intent_category(query: str, expected: str) -> None:
    assert detect_intent_category(query) == expected


def test_expected_catalog_ids_exist_in_fixtures() -> None:
    rows = rows_as_dict_list()
    for case in INTENT_QUERY_CASES:
        q = case["query"]
        ranked = rank_intent_aware(q, rows)
        ids = [r["catalog_id"] for r in ranked]
        assert len(ids) == len(rows)
        for aid in case["expected_acceptable_ids"]:
            assert aid in ids
