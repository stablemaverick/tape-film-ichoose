"""Unit tests for Ordering Agent V1 intent, ranking, and public serializers."""

from __future__ import annotations

import pytest

from app.services.ordering_agent_intent import (
    dominant_candidate,
    extract_release_intent,
    rank_release_candidates,
)
from app.services.ordering_agent_public import (
    assert_ordering_public_safe,
    customer_status_phrase,
    public_offer_from_commerce,
)


def test_extract_intent_title_format_label():
    intent = extract_release_intent(
        "I'm looking for the Second Sight edition of The Hitcher on 4K"
    )
    assert intent.label == "Second Sight"
    assert intent.format == "4K UHD"
    assert intent.title and "hitcher" in intent.title.lower()
    assert "second sight" not in intent.title.lower()
    assert intent.intent in {"find_release", "check_availability"}


def test_extract_common_availability_queries():
    tr = extract_release_intent("Do you have True Romance on Blu-ray?")
    assert tr.title and tr.title.lower().startswith("true romance")
    assert tr.format == "Blu-ray"

    aw = extract_release_intent("Can I get American Werewolf in London 4k?")
    assert aw.title and "werewolf" in aw.title.lower() and "london" in aw.title.lower()
    assert aw.format == "4K UHD"

    cr = extract_release_intent("Can you order Creepozoids?")
    assert cr.title and "creepozoids" in cr.title.lower()

    adv = extract_release_intent("Which supplier has True Romance?")
    assert adv.title and adv.title.lower() == "true romance"


def test_extract_steelbook_and_limited():
    s = extract_release_intent("Do you have The Thing steelbook?")
    assert s.steelbook is True
    le = extract_release_intent("I'm after The Addiction limited edition")
    assert le.limited_edition is True


def test_extract_barcode():
    intent = extract_release_intent("lookup 5037899091425")
    assert intent.barcode == "5037899091425"


def test_rank_prefers_format_and_does_not_use_cost():
    cands = [
        {"release_variant_id": "a", "title": "The Thing Blu-Ray", "format": "BLU-RAY"},
        {"release_variant_id": "b", "title": "The Thing 4K Ultra HD", "format": "4K UHD"},
        {"release_variant_id": "c", "title": "The Thing Steelbook 4K", "format": "4K UHD"},
    ]
    intent = extract_release_intent("Do you have The Thing on 4K steelbook?")
    ranked = rank_release_candidates(cands, intent)
    assert ranked[0]["release_variant_id"] == "c"


def test_dominant_vs_ambiguous():
    strong = [
        {"release_variant_id": "1", "title": "Creepozoids Blu-Ray", "format": "BLU-RAY", "rank_score": 10},
        {"release_variant_id": "2", "title": "Creepozoids DVD", "format": "DVD", "rank_score": 2},
    ]
    assert dominant_candidate(strong)["release_variant_id"] == "1"

    weak = [
        {"release_variant_id": "1", "title": "Suspiria 4K", "format": "4K UHD", "rank_score": 4},
        {"release_variant_id": "2", "title": "Suspiria Blu-Ray", "format": "BLU-RAY", "rank_score": 3},
    ]
    assert dominant_candidate(weak) is None


def test_customer_status_phrases():
    assert "In Stock" in customer_status_phrase("in_stock", 28.99)
    assert "Available from Supplier" in customer_status_phrase("available_from_supplier", 42.99)
    assert "Available to Order" in customer_status_phrase("available_to_order", 43.99)
    assert customer_status_phrase("out_of_stock", 34.99) == "Out of Stock"


def test_public_offer_strips_and_blocks_supplier_leak():
    public = {
        "release_variant_id": "x",
        "title": "Creepozoids Blu-Ray",
        "listing_type": "agent_only",
        "sellable": True,
        "availability": "available_to_order",
        "price": 43.99,
        "currency": "AUD",
    }
    card = public_offer_from_commerce(public, format="BLU-RAY")
    assert card["shopify_listed"] is False
    assert "supplier" not in str(card).lower()
    assert_ordering_public_safe(card)

    with pytest.raises(AssertionError):
        assert_ordering_public_safe({"title": "x", "unit_cost": 10})


def test_malformed_empty_intent():
    intent = extract_release_intent("")
    assert intent.intent == "unknown"
    assert intent.title is None
