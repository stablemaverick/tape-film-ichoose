"""
Intent-aware harness: queries, ground-truth intent label, acceptable top results.

``expected_intent`` is what the isolated detector should return for intent-detection pass.
``expected_acceptable_ids`` — pass if any id appears within ``pass_within_rank`` (default 3).
"""

from __future__ import annotations

from typing import Any, Dict, List, TypedDict


class IntentQueryCase(TypedDict, total=False):
    case_id: str
    query: str
    intent_group: str
    expected_intent: str
    expected_acceptable_ids: List[str]
    pass_within_rank: int
    notes: str


INTENT_QUERY_CASES: List[IntentQueryCase] = [
    {
        "case_id": "iq_director_01",
        "query": "films by christopher nolan",
        "intent_group": "director",
        "expected_intent": "director",
        "expected_acceptable_ids": ["nolan_inception_4k", "nolan_dunkirk_bd"],
        "pass_within_rank": 3,
        "notes": "Director corpus — either Nolan SKU acceptable",
    },
    {
        "case_id": "iq_actor_01",
        "query": "movies starring tom hanks",
        "intent_group": "actor",
        "expected_intent": "actor",
        "expected_acceptable_ids": ["hanks_forrest_gump", "hanks_cast_away"],
        "pass_within_rank": 3,
        "notes": "Lead actor metadata",
    },
    {
        "case_id": "iq_year_01",
        "query": "horror films from 1999",
        "intent_group": "year_decade",
        "expected_intent": "year_decade",
        "expected_acceptable_ids": ["horror_1999_sixth"],
        "pass_within_rank": 3,
        "notes": "Year + genre",
    },
    {
        "case_id": "iq_decade_01",
        "query": "80s action movies",
        "intent_group": "year_decade",
        "expected_intent": "year_decade",
        "expected_acceptable_ids": ["action_1986_top_gun"],
        "pass_within_rank": 3,
        "notes": "Decade bucket",
    },
    {
        "case_id": "iq_label_01",
        "query": "criterion collection kurosawa",
        "intent_group": "distributor_label",
        "expected_intent": "distributor_label",
        "expected_acceptable_ids": ["criterion_seven_samurai"],
        "pass_within_rank": 3,
        "notes": "Label + title token overlap",
    },
    {
        "case_id": "iq_label_02",
        "query": "arrow video horror",
        "intent_group": "distributor_label",
        "expected_intent": "distributor_label",
        "expected_acceptable_ids": ["arrow_dawn_dead"],
        "pass_within_rank": 3,
        "notes": "Arrow label",
    },
    {
        "case_id": "iq_label_03",
        "query": "indicator quatermass blu-ray",
        "intent_group": "distributor_label",
        "expected_intent": "distributor_label",
        "expected_acceptable_ids": ["indicator_quatermass"],
        "pass_within_rank": 3,
        "notes": "Indicator label + title token",
    },
    {
        "case_id": "iq_awards_01",
        "query": "oscar winning best picture drama",
        "intent_group": "awards_discovery",
        "expected_intent": "awards_discovery",
        "expected_acceptable_ids": ["oscar_parasite", "oscar_moonlight"],
        "pass_within_rank": 3,
        "notes": "Awards metadata",
    },
    {
        "case_id": "iq_commerce_01",
        "query": "blade runner 2049 4k ultra hd in stock",
        "intent_group": "commerce",
        "expected_intent": "commerce",
        "expected_acceptable_ids": ["commerce_blade_runner_bd_stock"],
        "pass_within_rank": 2,
        "notes": "In-stock BD should beat OOS 4k when query stresses stock + format",
    },
    {
        "case_id": "iq_commerce_02",
        "query": "matrix preorder blu-ray",
        "intent_group": "commerce",
        "expected_intent": "commerce",
        "expected_acceptable_ids": ["matrix_preorder_bd"],
        "pass_within_rank": 3,
        "notes": "Preorder SKU vs in-stock DVD sibling",
    },
    {
        "case_id": "iq_commerce_03",
        "query": "the matrix best edition 4k steelbook",
        "intent_group": "commerce",
        "expected_intent": "commerce",
        "expected_acceptable_ids": ["best_edition_matrix_4k"],
        "pass_within_rank": 2,
        "notes": "Premium / best edition",
    },
]
