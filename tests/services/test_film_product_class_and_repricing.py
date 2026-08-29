"""Tests for film-only product classification and Region B repricing gates."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from app.services.film_product_class import (
    ACTION_OUT_OF_SCOPE_NON_FILM,
    ACTION_SKIP_AMBIGUOUS_PRODUCT,
    PRODUCT_CLASS_CD,
    PRODUCT_CLASS_FILM,
    PRODUCT_CLASS_GAME,
    PRODUCT_CLASS_VINYL,
    classify_product_class,
    product_class_to_action,
)
from app.services.region_b_film_repricing_service import (
    ACTION_PRICE_INCREASE_AUTO_ELIGIBLE,
    ACTION_REGION_A_BLOCKED,
    ACTION_REVIEW_COST_ANOMALY,
    ACTION_REVIEW_LARGE_INCREASE,
    ACTION_REVIEW_PRICE_INCREASE,
    HARD_REVIEW_BARCODES,
    RepriceRow,
    auto_apply_targets,
    build_candidate_rows,
    decide_action,
    effective_apply_price,
)
from app.services.supplier_margin_protection_service import ApplyAllowlist


def test_vinyl_format_excluded():
    cls, reason = classify_product_class(
        title="Some Album",
        product_id="gid://shopify/Product/1",
        format_value="Vinyl",
    )
    assert cls == PRODUCT_CLASS_VINYL
    action, _ = product_class_to_action(cls, reason)
    assert action == ACTION_OUT_OF_SCOPE_NON_FILM


def test_cd_format_excluded():
    cls, reason = classify_product_class(
        title="Soundtrack CD",
        product_id="gid://shopify/Product/2",
        format_value="CD",
        collection_handles={"soundtracks"},
    )
    assert cls == PRODUCT_CLASS_CD
    assert product_class_to_action(cls, reason)[0] == ACTION_OUT_OF_SCOPE_NON_FILM


def test_soundtracks_collection_excluded_even_without_format():
    cls, reason = classify_product_class(
        title="La La Land Soundtrack",
        product_id="gid://shopify/Product/3",
        collection_handles={"soundtracks"},
    )
    assert cls == PRODUCT_CLASS_VINYL
    assert "non_film" in reason


def test_film_bluray_eligible():
    cls, reason = classify_product_class(
        title="Some Film",
        product_id="gid://shopify/Product/4",
        format_value="4K, Blu-ray",
        collection_handles={"all-film"},
    )
    assert cls == PRODUCT_CLASS_FILM
    assert product_class_to_action(cls, reason) == ("", "")


def test_game_title_excluded():
    cls, reason = classify_product_class(
        title="Cinephile : A Card Game (Base Set)",
        product_id="gid://shopify/Product/5",
    )
    assert cls == PRODUCT_CLASS_GAME
    assert product_class_to_action(cls, reason)[0] == ACTION_OUT_OF_SCOPE_NON_FILM


def test_ambiguous_skips():
    cls, reason = classify_product_class(
        title="Mystery Merch",
        product_id="gid://shopify/Product/6",
    )
    assert cls != PRODUCT_CLASS_FILM
    assert product_class_to_action(cls, reason)[0] == ACTION_SKIP_AMBIGUOUS_PRODUCT


def test_decide_action_never_auto_decreases():
    action, reason, proposed, delta, _ = decide_action(current=80.99, floor=74.99)
    assert action == "KEEP_CURRENT_PRICE"
    assert proposed == 80.99
    assert delta < 0


def test_decide_action_tiered_increase_bands():
    auto, _, proposed, delta, _ = decide_action(current=50.99, floor=55.99)
    assert auto == ACTION_PRICE_INCREASE_AUTO_ELIGIBLE
    assert proposed == 55.99
    assert delta == 5.0

    commercial, _, _, delta6, _ = decide_action(current=50.99, floor=56.99)
    assert commercial == ACTION_REVIEW_PRICE_INCREASE
    assert delta6 == 6.0

    at_ten, _, _, delta10, _ = decide_action(current=50.99, floor=60.99)
    assert at_ten == ACTION_REVIEW_PRICE_INCREASE
    assert delta10 == 10.0

    large, _, proposed_l, delta15, _ = decide_action(current=40.99, floor=55.99)
    assert large == ACTION_REVIEW_LARGE_INCREASE
    assert proposed_l == 55.99
    assert delta15 == 15.0


def test_effective_apply_price_refuses_silent_below_floor_override():
    row = RepriceRow(
        title="t",
        variant_title="",
        barcode="1",
        sku="",
        product_id="p",
        variant_id="v",
        studio_raw="",
        studio_norm="Arrow",
        region_raw="B",
        normalized_region="B",
        product_type="",
        media_format="",
        product_class=PRODUCT_CLASS_FILM,
        class_reason="film",
        collection_handles="",
        preferred_supplier="moovies",
        source_currency="GBP",
        source_cost_gbp=10.0,
        gbp_aud_rate=2.0,
        landed_aud_cost=22.4,
        shopify_unit_cost_aud=None,
        current_retail=40.99,
        current_gp_pct=20.0,
        floor_retail=48.99,
        proposed_retail=48.99,
        proposed_gp_pct=28.0,
        dollar_change=8.0,
        pct_change=20.0,
        action=ACTION_REVIEW_PRICE_INCREASE,
        reason="increase_requires_commercial_review",
        calculated_floor_price=48.99,
        approved_retail_price=44.99,
        approved_gp_percent=22.0,
        pricing_exception_reason="",
    )
    assert effective_apply_price(row, 48.99) is None
    row.pricing_exception_reason = "MARKET_POSITIONING_EXCEPTION"
    assert effective_apply_price(row, 48.99) == 44.99


def _fresh_supplier_timestamp() -> str:
    """Supplier offers must look fresh relative to evaluation time."""
    return datetime.now(timezone.utc).isoformat()


class _SB:
    def __init__(self, tables: dict):
        self._tables = tables

    def table(self, name: str):
        return _Table(self._tables.get(name, []))


class _Table:
    def __init__(self, rows):
        self._rows = rows
        self._filters = []

    def select(self, *_a, **_k):
        return self

    def in_(self, col, values):
        self._filters.append((col, set(values)))
        return self

    def eq(self, col, val):
        self._filters.append((col, {val}))
        return self

    def execute(self):
        rows = self._rows
        for col, wanted in self._filters:
            if col == "active" and True in wanted:
                rows = [r for r in rows if r.get("active", True)]
            else:
                rows = [r for r in rows if r.get(col) in wanted]
        return MagicMock(data=rows)


def _vinyl_product_with_gbp_offer(*, barcode: str = "VINYL999", vid: str = "gid://shopify/ProductVariant/99"):
    """Vinyl product that ALSO has a usable Region B Lasgo/Moovies GBP offer."""
    product = {
        "id": "gid://shopify/Product/vinyl1",
        "title": "Fake Vinyl Album",
        "handle": "fake-vinyl",
        "status": "ACTIVE",
        "productType": "",
        "tags": [],
        "studio": {"value": "Criterion Collection"},
        "region": {"value": "Region B"},
        "formatMeta": {"value": "Vinyl"},
        "mediaFormat": None,
        "collections": {"nodes": [{"handle": "soundtracks"}]},
        "variants": {
            "nodes": [
                {
                    "id": vid,
                    "title": "Default Title",
                    "sku": "V1",
                    "barcode": barcode,
                    "price": "40.99",
                    "region": None,
                    "inventoryItem": {"unitCost": {"amount": "20.00", "currencyCode": "AUD"}},
                }
            ]
        },
    }
    now = "2026-08-26T00:00:00+00:00"
    sb = _SB(
        {
            "release_shopify_listings": [
                {"shopify_variant_id": vid, "release_variant_id": "rv-vinyl"}
            ],
            "supplier_offers": [
                {
                    "id": "off1",
                    "release_variant_id": "rv-vinyl",
                    "catalog_item_id": None,
                    "supplier_id": "lasgo",
                    "supplier_sku": "x",
                    "raw_barcode": barcode,
                    "availability_status": "in_stock",
                    "reported_quantity": 5,
                    "quantity_is_exact": True,
                    "unit_cost": 12.5,
                    "currency": "GBP",
                    "last_seen_at": now,
                    "source_feed_at": now,
                    "pipeline_completed_at": now,
                    "active": True,
                }
            ],
            "tape_inventory_levels": [],
            "suppliers": [{"id": "lasgo", "display_name": "Lasgo"}],
        }
    )
    return product, sb, vid, barcode


def test_non_film_exclusion_beats_region_b_gbp_and_allowlist():
    """Valid Region B + GBP supplier match must NOT produce PRICE_INCREASE for vinyl."""
    product, sb, vid, barcode = _vinyl_product_with_gbp_offer()
    rows = build_candidate_rows(
        [product],
        supabase=sb,
        soundtrack_product_ids={"gid://shopify/Product/vinyl1"},
        labels=["Criterion Collection"],
        now=datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row.action == ACTION_OUT_OF_SCOPE_NON_FILM
    assert row.product_class == PRODUCT_CLASS_VINYL
    assert row.action != ACTION_PRICE_INCREASE_AUTO_ELIGIBLE

    # Even an explicit allowlist must not select it for apply.
    allowlist = ApplyAllowlist(variant_ids=frozenset({vid}), barcodes=frozenset({barcode}))
    targets = auto_apply_targets(rows, allowlist)
    assert targets == []


def test_region_a_film_blocked_from_gbp_path():
    product = {
        "id": "gid://shopify/Product/us1",
        "title": "US Criterion 4K",
        "handle": "us-crit",
        "status": "ACTIVE",
        "productType": "",
        "tags": [],
        "studio": {"value": "Criterion Collection"},
        "region": {"value": "Region A"},
        "formatMeta": {"value": "4K"},
        "mediaFormat": None,
        "collections": {"nodes": [{"handle": "all-film"}, {"handle": "usa"}]},
        "variants": {
            "nodes": [
                {
                    "id": "gid://shopify/ProductVariant/us1",
                    "title": "Default Title",
                    "sku": "US1",
                    "barcode": "715515337816",
                    "price": "74.99",
                    "region": None,
                    "inventoryItem": {"unitCost": {"amount": "54.81", "currencyCode": "AUD"}},
                }
            ]
        },
    }
    sb = _SB(
        {
            "release_shopify_listings": [],
            "supplier_offers": [],
            "tape_inventory_levels": [],
            "suppliers": [],
        }
    )
    rows = build_candidate_rows([product], supabase=sb, labels=["Criterion Collection"])
    assert rows[0].action == ACTION_REGION_A_BLOCKED


def test_hard_review_barcodes_documented():
    assert "5050629184334" in HARD_REVIEW_BARCODES  # Easy Rider / Moonrise collision
    assert "5027035029276" in HARD_REVIEW_BARCODES  # The Mask LE cost drift
    assert "5056453207812" in HARD_REVIEW_BARCODES  # Gladiator II steelbook drift
    assert "5051892252850" in HARD_REVIEW_BARCODES  # Poltergeist Film Vault LE drift
    # Florida Project active barcode is correct — not permanent hard-review.
    assert "5028836042709" not in HARD_REVIEW_BARCODES


def test_inactive_sibling_same_barcode_cannot_be_allowlisted_by_active_variant_id():
    """Variant-ID allowlist must not select an inactive sibling sharing a barcode."""
    active_vid = "gid://shopify/ProductVariant/47676581478624"
    inactive_vid = "gid://shopify/ProductVariant/47676546154720"
    shared_bc = "5028836042709"
    al = ApplyAllowlist(
        variant_ids=frozenset({active_vid}),
        barcodes=frozenset({shared_bc}),
    )
    assert al.matches({"variant_id": active_vid, "barcode": shared_bc})
    assert not al.matches({"variant_id": inactive_vid, "barcode": shared_bc})

    active_row = RepriceRow(
        title="The Florida Project 4K Ultra HD",
        variant_title="Default Title",
        barcode=shared_bc,
        sku="2NDBR4270",
        product_id="gid://shopify/Product/8963095003360",
        variant_id=active_vid,
        studio_raw="Second Sight",
        studio_norm="Second Sight",
        region_raw="Region B",
        normalized_region="B",
        product_type="",
        media_format="4K",
        product_class=PRODUCT_CLASS_FILM,
        class_reason="film",
        collection_handles="",
        preferred_supplier="moovies",
        source_currency="GBP",
        source_cost_gbp=16.74,
        gbp_aud_rate=2.0,
        landed_aud_cost=37.5,
        shopify_unit_cost_aud=32.64,
        current_retail=54.99,
        current_gp_pct=24.0,
        floor_retail=57.99,
        proposed_retail=57.99,
        proposed_gp_pct=28.0,
        dollar_change=3.0,
        pct_change=5.0,
        action=ACTION_PRICE_INCREASE_AUTO_ELIGIBLE,
        reason="current_below_28pct_floor_auto_eligible",
    )
    inactive_row = RepriceRow(
        title="The Florida Project Limited Edition 4K Ultra HD + Blu-Ray",
        variant_title="Default Title",
        barcode=shared_bc,
        sku="2NDBR4270",
        product_id="gid://shopify/Product/8963085828320",
        variant_id=inactive_vid,
        studio_raw="Second Sight",
        studio_norm="Second Sight",
        region_raw="Region B",
        normalized_region="B",
        product_type="",
        media_format="4K",
        product_class=PRODUCT_CLASS_FILM,
        class_reason="film",
        collection_handles="",
        preferred_supplier="moovies",
        source_currency="GBP",
        source_cost_gbp=16.74,
        gbp_aud_rate=2.0,
        landed_aud_cost=37.5,
        shopify_unit_cost_aud=71.85,
        current_retail=103.99,
        current_gp_pct=40.0,
        floor_retail=57.99,
        proposed_retail=57.99,
        proposed_gp_pct=28.0,
        dollar_change=-46.0,
        pct_change=-44.0,
        action=ACTION_PRICE_INCREASE_AUTO_ELIGIBLE,
        reason="should_not_apply",
    )
    targets = auto_apply_targets([active_row, inactive_row], al)
    assert [t.variant_id for t in targets] == [active_vid]


def test_allowlist_prefers_variant_id_when_present():
    al = ApplyAllowlist(
        variant_ids=frozenset({"gid://shopify/ProductVariant/1"}),
        barcodes=frozenset({"111"}),
    )
    assert al.matches({"variant_id": "gid://shopify/ProductVariant/1", "barcode": "999"})
    assert not al.matches({"variant_id": "gid://shopify/ProductVariant/2", "barcode": "111"})


def test_fetch_active_products_query_scopes_active_only():
    import inspect
    from app.services import region_b_film_repricing_service as mod

    src = inspect.getsource(mod.fetch_active_products)
    assert "status:active" in src


def test_gbp_cost_increased_materially_and_exception_recognition():
    from app.services.region_b_film_repricing_service import (
        ACTION_APPROVED_PRICING_EXCEPTION,
        ACTION_REVIEW_PRICE_INCREASE,
        gbp_cost_increased_materially,
    )

    assert not gbp_cost_increased_materially(15.35, 15.35)
    assert not gbp_cost_increased_materially(15.40, 15.35)  # < £0.50 and <5%
    assert gbp_cost_increased_materially(16.20, 15.35)  # >£0.50
    # Percentage threshold with absolute movement below £0.50.
    assert gbp_cost_increased_materially(1.05, 1.00)  # +5%, abs £0.05
    assert not gbp_cost_increased_materially(1.04, 1.00)  # +4%, abs £0.04
    # Cost decreases never reopen.
    assert not gbp_cost_increased_materially(14.00, 15.35)

    # Build one film row with an active exception matching current retail.
    product = {
        "id": "gid://shopify/Product/1",
        "title": "Werewolf 4K",
        "productType": "Blu-ray",
        "tags": [],
        "studio": {"value": "Arrow"},
        "region": {"value": "Region B"},
        "formatMeta": {"value": "4K"},
        "mediaFormat": {"value": ""},
        "collections": {"nodes": []},
        "variants": {
            "nodes": [
                {
                    "id": "gid://shopify/ProductVariant/exc1",
                    "title": "Default Title",
                    "sku": "X",
                    "barcode": "5027035024776",
                    "price": "49.99",
                    "region": None,
                    "inventoryItem": {"unitCost": {"amount": "34.38", "currencyCode": "AUD"}},
                }
            ]
        },
    }
    now = _fresh_supplier_timestamp()
    sb = _SB(
        {
            "release_shopify_listings": [
                {"shopify_variant_id": "gid://shopify/ProductVariant/exc1", "release_variant_id": "rv1"}
            ],
            "supplier_offers": [
                {
                    "id": "o1",
                    "release_variant_id": "rv1",
                    "supplier_id": "moovies",
                    "supplier_sku": "x",
                    "raw_barcode": "5027035024776",
                    "availability_status": "in_stock",
                    "reported_quantity": 1,
                    "unit_cost": 15.35,
                    "currency": "GBP",
                    "last_seen_at": now,
                    "source_feed_at": now,
                    "active": True,
                }
            ],
            "tape_inventory_levels": [],
            "suppliers": [{"id": "moovies", "display_name": "Moovies"}],
        }
    )
    exceptions = {
        "gid://shopify/ProductVariant/exc1": {
            "approved_retail_price": 49.99,
            "source_cost_gbp_at_approval": 15.35,
            "calculated_floor_at_approval": 52.99,
            "pricing_exception_reason": "MARKET_POSITIONING_EXCEPTION",
        }
    }
    rows = build_candidate_rows(
        [product],
        supabase=sb,
        labels=["Arrow"],
        pricing_exceptions=exceptions,
    )
    assert rows[0].action == ACTION_APPROVED_PRICING_EXCEPTION
    assert rows[0].pricing_exception_reason == "MARKET_POSITIONING_EXCEPTION"
    assert rows[0].source_cost_gbp_at_approval == 15.35
    assert rows[0].gbp_movement_from_approval == 0.0

    # Small below-threshold increase keeps approved exception.
    sb_small = _SB(
        {
            "release_shopify_listings": [
                {"shopify_variant_id": "gid://shopify/ProductVariant/exc1", "release_variant_id": "rv1"}
            ],
            "supplier_offers": [
                {
                    "id": "o1",
                    "release_variant_id": "rv1",
                    "supplier_id": "moovies",
                    "supplier_sku": "x",
                    "raw_barcode": "5027035024776",
                    "availability_status": "in_stock",
                    "reported_quantity": 1,
                    "unit_cost": 15.40,
                    "currency": "GBP",
                    "last_seen_at": now,
                    "source_feed_at": now,
                    "active": True,
                }
            ],
            "tape_inventory_levels": [],
            "suppliers": [{"id": "moovies", "display_name": "Moovies"}],
        }
    )
    rows_small = build_candidate_rows(
        [product],
        supabase=sb_small,
        labels=["Arrow"],
        pricing_exceptions=exceptions,
    )
    assert rows_small[0].action == ACTION_APPROVED_PRICING_EXCEPTION

    # Cost decrease keeps approved exception (never auto-reduce retail).
    sb_down = _SB(
        {
            "release_shopify_listings": [
                {"shopify_variant_id": "gid://shopify/ProductVariant/exc1", "release_variant_id": "rv1"}
            ],
            "supplier_offers": [
                {
                    "id": "o1",
                    "release_variant_id": "rv1",
                    "supplier_id": "moovies",
                    "supplier_sku": "x",
                    "raw_barcode": "5027035024776",
                    "availability_status": "in_stock",
                    "reported_quantity": 1,
                    "unit_cost": 14.00,
                    "currency": "GBP",
                    "last_seen_at": now,
                    "source_feed_at": now,
                    "active": True,
                }
            ],
            "tape_inventory_levels": [],
            "suppliers": [{"id": "moovies", "display_name": "Moovies"}],
        }
    )
    rows_down = build_candidate_rows(
        [product],
        supabase=sb_down,
        labels=["Arrow"],
        pricing_exceptions=exceptions,
    )
    assert rows_down[0].action == ACTION_APPROVED_PRICING_EXCEPTION
    assert rows_down[0].gbp_movement_from_approval == -1.35

    exceptions["gid://shopify/ProductVariant/exc1"]["source_cost_gbp_at_approval"] = 14.00
    rows2 = build_candidate_rows(
        [product],
        supabase=sb,
        labels=["Arrow"],
        pricing_exceptions=exceptions,
    )
    assert rows2[0].action == ACTION_REVIEW_PRICE_INCREASE
    assert "exception_reopen_gbp_cost_increased" in (rows2[0].reason or "")


def test_exception_live_price_mismatch_forces_review_not_auto():
    from app.services.region_b_film_repricing_service import (
        ACTION_REVIEW_PRICE_INCREASE,
    )

    product = {
        "id": "gid://shopify/Product/1",
        "title": "Werewolf 4K",
        "productType": "Blu-ray",
        "tags": [],
        "studio": {"value": "Arrow"},
        "region": {"value": "Region B"},
        "formatMeta": {"value": "4K"},
        "mediaFormat": {"value": ""},
        "collections": {"nodes": []},
        "variants": {
            "nodes": [
                {
                    "id": "gid://shopify/ProductVariant/exc1",
                    "title": "Default Title",
                    "sku": "X",
                    "barcode": "5027035024776",
                    "price": "42.99",  # drifted from approved 49.99
                    "region": None,
                    "inventoryItem": {"unitCost": {"amount": "34.38", "currencyCode": "AUD"}},
                }
            ]
        },
    }
    now = _fresh_supplier_timestamp()
    sb = _SB(
        {
            "release_shopify_listings": [
                {"shopify_variant_id": "gid://shopify/ProductVariant/exc1", "release_variant_id": "rv1"}
            ],
            "supplier_offers": [
                {
                    "id": "o1",
                    "release_variant_id": "rv1",
                    "supplier_id": "moovies",
                    "supplier_sku": "x",
                    "raw_barcode": "5027035024776",
                    "availability_status": "in_stock",
                    "reported_quantity": 1,
                    "unit_cost": 15.35,
                    "currency": "GBP",
                    "last_seen_at": now,
                    "source_feed_at": now,
                    "active": True,
                }
            ],
            "tape_inventory_levels": [],
            "suppliers": [{"id": "moovies", "display_name": "Moovies"}],
        }
    )
    exceptions = {
        "gid://shopify/ProductVariant/exc1": {
            "approved_retail_price": 49.99,
            "source_cost_gbp_at_approval": 15.35,
            "calculated_floor_at_approval": 52.99,
            "pricing_exception_reason": "MARKET_POSITIONING_EXCEPTION",
        }
    }
    rows = build_candidate_rows(
        [product],
        supabase=sb,
        labels=["Arrow"],
        pricing_exceptions=exceptions,
    )
    assert rows[0].action == ACTION_REVIEW_PRICE_INCREASE
    assert rows[0].reason == "exception_live_price_mismatch"


def test_pricing_health_readonly_blocks_apply_mutations(monkeypatch):
    import os

    from app.services.region_b_film_repricing_service import apply_price_updates

    monkeypatch.setenv("REGION_B_PRICING_HEALTH_READONLY", "1")
    try:
        apply_price_updates(
            client=None,  # type: ignore[arg-type]
            rows=[],
            supabase=None,
            soundtrack_product_ids=set(),
            gbp_aud=2.0,
            landed_markup=1.12,
            margin_floor=0.28,
            max_auto_increase=5.0,
            anomaly_pct=0.25,
        )
        raise AssertionError("expected RuntimeError")
    except RuntimeError as exc:
        assert "REGION_B_PRICING_HEALTH_READONLY" in str(exc)
    finally:
        os.environ.pop("REGION_B_PRICING_HEALTH_READONLY", None)


def test_reviewed_override_is_apply_target():
    from app.services.region_b_film_repricing_service import ACTION_REVIEW_PRICE_INCREASE

    row = RepriceRow(
        title="t",
        variant_title="",
        barcode="1",
        sku="",
        product_id="p",
        variant_id="gid://shopify/ProductVariant/9",
        studio_raw="",
        studio_norm="Arrow",
        region_raw="B",
        normalized_region="B",
        product_type="",
        media_format="",
        product_class=PRODUCT_CLASS_FILM,
        class_reason="film",
        collection_handles="",
        preferred_supplier="moovies",
        source_currency="GBP",
        source_cost_gbp=15.0,
        gbp_aud_rate=2.0,
        landed_aud_cost=33.6,
        shopify_unit_cost_aud=None,
        current_retail=42.99,
        current_gp_pct=12.0,
        floor_retail=52.99,
        proposed_retail=52.99,
        proposed_gp_pct=28.0,
        dollar_change=10.0,
        pct_change=23.0,
        action=ACTION_REVIEW_PRICE_INCREASE,
        reason="increase_requires_commercial_review",
        calculated_floor_price=52.99,
        approved_retail_price=49.99,
        approved_gp_percent=24.0,
        pricing_exception_reason="MARKET_POSITIONING_EXCEPTION",
    )
    al = ApplyAllowlist(
        variant_ids=frozenset({row.variant_id}),
        barcodes=frozenset({row.barcode}),
    )
    assert auto_apply_targets([row], al) == [row]
    product = {
        "id": "gid://shopify/Product/1",
        "title": "Dup Film 4K",
        "productType": "Blu-ray",
        "tags": [],
        "studio": {"value": "Arrow"},
        "region": {"value": "Region B"},
        "formatMeta": {"value": "4K"},
        "mediaFormat": {"value": ""},
        "collections": {"nodes": []},
        "variants": {
            "nodes": [
                {
                    "id": "gid://shopify/ProductVariant/9",
                    "title": "Default Title",
                    "sku": "X",
                    "barcode": "9990001112223",
                    "price": "40.99",
                    "region": None,
                    "inventoryItem": {"unitCost": {"amount": "20", "currencyCode": "AUD"}},
                }
            ]
        },
    }
    sb = _SB(
        {
            "release_shopify_listings": [],
            "supplier_offers": [
                {
                    "id": "o1",
                    "release_variant_id": "rv-a",
                    "supplier_id": "lasgo",
                    "supplier_sku": "a",
                    "raw_barcode": "9990001112223",
                    "availability_status": "in_stock",
                    "reported_quantity": 1,
                    "unit_cost": 10.0,
                    "currency": "GBP",
                    "last_seen_at": "2026-08-26T00:00:00+00:00",
                    "source_feed_at": "2026-08-26T00:00:00+00:00",
                    "active": True,
                },
                {
                    "id": "o2",
                    "release_variant_id": "rv-b",
                    "supplier_id": "moovies",
                    "supplier_sku": "b",
                    "raw_barcode": "9990001112223",
                    "availability_status": "in_stock",
                    "reported_quantity": 1,
                    "unit_cost": 11.0,
                    "currency": "GBP",
                    "last_seen_at": "2026-08-26T00:00:00+00:00",
                    "source_feed_at": "2026-08-26T00:00:00+00:00",
                    "active": True,
                },
            ],
            "tape_inventory_levels": [],
            "suppliers": [
                {"id": "lasgo", "display_name": "Lasgo"},
                {"id": "moovies", "display_name": "Moovies"},
            ],
        }
    )
    rows = build_candidate_rows([product], supabase=sb, labels=["Arrow"])
    assert rows[0].action == ACTION_REVIEW_COST_ANOMALY
    assert "ambiguous_barcode" in (rows[0].reason or "")
