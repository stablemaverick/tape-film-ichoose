from __future__ import annotations

from app.services.shopify_catalog_hygiene_service import (
    classify_zero_stock_candidate,
    is_valid_future_preorder,
    parse_iso_date,
    remove_exact_new_tag,
)


def test_zero_stock_classification_examples():
    cases = [
        ([0], True),
        ([0, 0], True),
        ([1], False),
        ([1, 0], False),
        ([-1], False),
        ([-1, 0], False),
        ([-2, -1], False),
    ]
    for qtys, expected in cases:
        got, _reason = classify_zero_stock_candidate(
            product_status="ACTIVE",
            variant_available_quantities=qtys,
            has_relevant_variants=True,
            is_valid_preorder=False,
        )
        assert got is expected


def test_zero_stock_classification_protects_future_preorder():
    got, reason = classify_zero_stock_candidate(
        product_status="ACTIVE",
        variant_available_quantities=[0],
        has_relevant_variants=True,
        is_valid_preorder=True,
    )
    assert got is False
    assert reason == "valid_future_preorder_protected"


def test_preorder_date_semantics():
    today = parse_iso_date("2026-08-11")
    assert today is not None
    assert is_valid_future_preorder(True, parse_iso_date("2026-08-12"), today) is True
    assert is_valid_future_preorder(True, parse_iso_date("2026-08-11"), today) is False
    assert is_valid_future_preorder(True, parse_iso_date("2026-08-10"), today) is False
    assert is_valid_future_preorder(True, None, today) is False
    # future release without preorder flag is reported-only, not auto-enabled
    assert is_valid_future_preorder(False, parse_iso_date("2026-08-20"), today) is False


def test_remove_exact_new_tag_only():
    tags, removed = remove_exact_new_tag(["4K UHD", "Limited Edition", "New", "Second Sight"])
    assert removed is True
    assert tags == ["4K UHD", "Limited Edition", "Second Sight"]


def test_remove_exact_new_tag_idempotent_when_already_clean():
    tags, removed = remove_exact_new_tag(["4K UHD", "Limited Edition", "Second Sight"])
    assert removed is False
    assert tags == ["4K UHD", "Limited Edition", "Second Sight"]
