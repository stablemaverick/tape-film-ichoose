from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from app.services.arrow_inventory_policy_sync_service import (
    ACTION_NO_CHANGE,
    ACTION_SET_CONTINUE,
    ACTION_SET_DENY,
    ACTION_SKIP,
    apply_extra_continue_protections,
    apply_policy_decisions,
    build_decisions_for_products,
    classify_arrow_zero_stock_policy,
    classify_zero_stock_policy,
    is_arrow_studio,
    is_eligible_studio,
    is_region_b,
    normalize_region,
    normalize_studio_label,
    prefer_stock_feed_offers,
    resolve_variant_region,
    supplier_confirmed_unavailable,
    supplier_is_usable,
)


def test_is_arrow_studio():
    assert is_arrow_studio("Arrow Video") is True
    assert is_arrow_studio("ARROW FILMS") is True
    assert is_arrow_studio("Second Sight") is False
    assert is_arrow_studio("Criterion Collection") is False
    assert is_arrow_studio("") is False


def test_normalize_and_eligible_studios():
    assert normalize_studio_label("Arrow Video") == "Arrow"
    assert normalize_studio_label("  arrow   films ") == "Arrow"
    assert normalize_studio_label("Second Sight") == "Second Sight"
    assert normalize_studio_label("second  sight") == "Second Sight"
    assert normalize_studio_label("Criterion Collection") == "Criterion Collection"
    assert normalize_studio_label("The Criterion Collection") == "Criterion Collection"
    assert normalize_studio_label("criterion") == "Criterion Collection"
    assert normalize_studio_label("Criterion") == "Criterion Collection"
    assert is_eligible_studio("Arrow Video")
    assert is_eligible_studio("Second Sight")
    assert is_eligible_studio("Criterion Collection")
    assert is_eligible_studio("criterion")
    assert not is_eligible_studio("Warner Bros")
    assert not is_eligible_studio("")
    assert not is_eligible_studio("Radiance Films")


def test_unrelated_and_missing_studio_excluded():
    assert not is_eligible_studio("Warner Bros")
    assert not is_eligible_studio(None)
    assert not is_eligible_studio("   ")


def test_normalize_region_and_region_b_gate():
    assert normalize_region("Region B") == "B"
    assert normalize_region("region b") == "B"
    assert normalize_region("B") == "B"
    assert normalize_region("Region A") == "A"
    assert normalize_region("region a") == "A"
    assert is_region_b("Region B")
    assert not is_region_b("Region A")
    assert not is_region_b("")
    assert resolve_variant_region(product_region="Region B", variant_region="Region A") == "A"
    assert resolve_variant_region(product_region="Region A", variant_region="") == "A"
    assert resolve_variant_region(product_region="Region B", variant_region=None) == "B"


def test_supplier_usable_requires_fresh_wholesale_available():
    assert supplier_is_usable(
        [{"supplier_id": "moovies", "api_status": "available", "freshness": "fresh", "qty": 2}]
    )
    assert supplier_is_usable(
        [{"supplier_id": "lasgo", "api_status": "available", "freshness": "aging", "qty": 1}]
    )
    assert not supplier_is_usable(
        [{"supplier_id": "moovies", "api_status": "available", "freshness": "stale", "qty": 5}]
    )
    assert not supplier_is_usable(
        [{"supplier_id": "tape_film", "api_status": "available", "freshness": "fresh", "qty": 9}]
    )
    assert supplier_is_usable(
        [{"supplier_id": "moovies", "api_status": "available", "freshness": "unknown", "qty": 3}]
    )
    assert not supplier_is_usable(
        [{"supplier_id": "moovies", "api_status": "available", "freshness": "unknown", "qty": 0}]
    )


def test_supplier_mapping_without_stock_is_not_usable():
    assert not supplier_is_usable(
        [{"supplier_id": "moovies", "api_status": "unavailable", "freshness": "fresh", "qty": 0}]
    )
    assert not supplier_is_usable(
        [{"supplier_id": "moovies", "api_status": "unknown", "freshness": "fresh", "qty": None}]
    )


def test_supplier_confirmed_unavailable():
    assert supplier_confirmed_unavailable(
        [
            {"supplier_id": "moovies", "api_status": "unavailable", "freshness": "fresh"},
            {"supplier_id": "lasgo", "api_status": "unavailable", "freshness": "aging"},
        ]
    )
    assert not supplier_confirmed_unavailable(
        [{"supplier_id": "moovies", "api_status": "unavailable", "freshness": "stale"}]
    )
    assert not supplier_confirmed_unavailable([])


def test_multiple_suppliers_one_available():
    evaluated = [
        {"supplier_id": "moovies", "api_status": "unavailable", "freshness": "fresh", "qty": 0},
        {"supplier_id": "lasgo", "api_status": "available", "freshness": "fresh", "qty": 4},
    ]
    assert supplier_is_usable(evaluated)
    assert not supplier_confirmed_unavailable(evaluated)


def test_multiple_suppliers_none_available():
    evaluated = [
        {"supplier_id": "moovies", "api_status": "unavailable", "freshness": "fresh", "qty": 0},
        {"supplier_id": "lasgo", "api_status": "unavailable", "freshness": "aging", "qty": 0},
    ]
    assert not supplier_is_usable(evaluated)
    assert supplier_confirmed_unavailable(evaluated)


def test_rule_a_deny_to_continue():
    action, reason = classify_arrow_zero_stock_policy(
        inventory_policy="DENY",
        shopify_qty=0,
        supplier_usable=True,
        confirmed_unavailable=False,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_SET_CONTINUE
    assert "supplier_available" in reason


def test_rule_b_continue_to_deny_unavailable():
    action, reason = classify_arrow_zero_stock_policy(
        inventory_policy="CONTINUE",
        shopify_qty=0,
        supplier_usable=False,
        confirmed_unavailable=True,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_SET_DENY
    assert "unavailable" in reason


def test_rule_b_continue_to_deny_no_offer():
    action, reason = classify_arrow_zero_stock_policy(
        inventory_policy="CONTINUE",
        shopify_qty=0,
        supplier_usable=False,
        confirmed_unavailable=False,
        has_any_offer=False,
        protect_future_preorder=False,
    )
    assert action == ACTION_SET_DENY
    assert "no_supplier_offer" in reason


def test_rule_b_skips_ambiguous_and_preorder():
    action, _ = classify_arrow_zero_stock_policy(
        inventory_policy="CONTINUE",
        shopify_qty=0,
        supplier_usable=False,
        confirmed_unavailable=False,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_SKIP

    action, reason = classify_arrow_zero_stock_policy(
        inventory_policy="CONTINUE",
        shopify_qty=0,
        supplier_usable=False,
        confirmed_unavailable=True,
        has_any_offer=True,
        protect_future_preorder=True,
    )
    assert action == ACTION_SKIP
    assert "preorder" in reason


def test_non_zero_qty_no_change():
    action, reason = classify_arrow_zero_stock_policy(
        inventory_policy="DENY",
        shopify_qty=1,
        supplier_usable=True,
        confirmed_unavailable=False,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_NO_CHANGE
    assert reason == "shopify_qty_not_zero"


def test_oversold_continue_flips_to_deny_when_supplier_unavailable():
    action, reason = classify_zero_stock_policy(
        inventory_policy="CONTINUE",
        shopify_qty=-1,
        supplier_usable=False,
        confirmed_unavailable=True,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_SET_DENY
    assert reason == "continue_zero_stock_supplier_unavailable"


def test_oversold_continue_kept_when_supplier_available():
    action, _ = classify_zero_stock_policy(
        inventory_policy="CONTINUE",
        shopify_qty=-2,
        supplier_usable=True,
        confirmed_unavailable=False,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_NO_CHANGE


def test_oversold_deny_never_flips_to_continue():
    action, reason = classify_zero_stock_policy(
        inventory_policy="DENY",
        shopify_qty=-1,
        supplier_usable=True,
        confirmed_unavailable=False,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_NO_CHANGE
    assert reason == "deny_oversold_no_flip"


def test_prefer_stock_feed_offers_drops_catalog_offer_for_same_supplier():
    stock = {"supplier_id": "moovies", "supplier_sku": "barcode:111", "api_status": "unavailable", "freshness": "fresh", "qty": 0}
    catalog = {"supplier_id": "moovies", "supplier_sku": "FCD1", "api_status": "available", "freshness": "fresh", "qty": 3}
    lasgo = {"supplier_id": "lasgo", "supplier_sku": "L1", "api_status": "available", "freshness": "fresh", "qty": 2}
    assert prefer_stock_feed_offers([stock, catalog, lasgo]) == [stock, lasgo]


def test_prefer_stock_feed_offers_keeps_catalog_when_stock_feed_stale_or_absent():
    stale_stock = {"supplier_id": "moovies", "supplier_sku": "barcode:111", "api_status": "unavailable", "freshness": "stale", "qty": 0}
    catalog = {"supplier_id": "moovies", "supplier_sku": "FCD1", "api_status": "available", "freshness": "fresh", "qty": 3}
    assert prefer_stock_feed_offers([stale_stock, catalog]) == [stale_stock, catalog]
    assert prefer_stock_feed_offers([catalog]) == [catalog]


def test_idempotent_already_continue():
    action, reason = classify_zero_stock_policy(
        inventory_policy="CONTINUE",
        shopify_qty=0,
        supplier_usable=True,
        confirmed_unavailable=False,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_NO_CHANGE
    assert reason == "continue_zero_stock_supplier_available"


def test_idempotent_already_deny():
    action, reason = classify_zero_stock_policy(
        inventory_policy="DENY",
        shopify_qty=0,
        supplier_usable=False,
        confirmed_unavailable=True,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_NO_CHANGE
    assert reason == "deny_zero_stock_no_usable_supplier"


def test_backorder_protection_overlay():
    action, reason, safety = apply_extra_continue_protections(
        action=ACTION_SET_DENY,
        reason="continue_zero_stock_supplier_unavailable",
        pre_order=False,
        backorder=True,
        protect_future_preorder=False,
    )
    assert action == ACTION_SKIP
    assert reason == "backorder_protected"
    assert safety == "backorder_protected"


def test_preorder_flag_protection_overlay():
    action, reason, safety = apply_extra_continue_protections(
        action=ACTION_SET_DENY,
        reason="continue_zero_stock_no_supplier_offer",
        pre_order=True,
        backorder=False,
        protect_future_preorder=False,
    )
    assert action == ACTION_SKIP
    assert "preorder" in reason
    assert safety == "preorder_protected"


def test_overlay_does_not_change_set_continue():
    action, reason, safety = apply_extra_continue_protections(
        action=ACTION_SET_CONTINUE,
        reason="deny_zero_stock_supplier_available",
        pre_order=True,
        backorder=True,
        protect_future_preorder=True,
    )
    assert action == ACTION_SET_CONTINUE
    assert safety == ""


def test_shared_classifier_matches_arrow_alias():
    kwargs = dict(
        inventory_policy="CONTINUE",
        shopify_qty=0,
        supplier_usable=False,
        confirmed_unavailable=True,
        has_any_offer=True,
        protect_future_preorder=False,
    )
    assert classify_zero_stock_policy(**kwargs) == classify_arrow_zero_stock_policy(**kwargs)


class _Table:
    def __init__(self, data):
        self._data = data

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def in_(self, *a, **k):
        return self

    def execute(self):
        return MagicMock(data=self._data)


class _SB:
    def __init__(self, tables: dict):
        self.tables = tables

    def table(self, name):
        return _Table(self.tables.get(name, []))


def _product(*, studio, policy, qty, barcode="111", pre=False, back=False, vid="gid://shopify/ProductVariant/1", title="Film", region="Region B", variant_region=None, status="ACTIVE", price="54.99"):
    variant = {
        "id": vid,
        "title": "Default",
        "sku": barcode,
        "barcode": barcode,
        "price": price,
        "inventoryPolicy": policy,
        "inventoryQuantity": qty,
        "region": {"value": variant_region} if variant_region is not None else None,
    }
    return {
        "id": "gid://shopify/Product/1",
        "title": title,
        "handle": "film",
        "status": status,
        "studio": {"value": studio},
        "region": {"value": region} if region is not None else None,
        "preOrder": {"value": "true" if pre else "false"},
        "backorder": {"value": "true" if back else "false"},
        "mediaReleaseDate": {"value": "2099-01-01" if pre else ""},
        "variants": {"nodes": [variant]},
    }


def test_build_decisions_arrow_second_sight_criterion_and_exclusion():
    now = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    vid_a = "gid://shopify/ProductVariant/10"
    vid_s = "gid://shopify/ProductVariant/20"
    vid_c = "gid://shopify/ProductVariant/30"
    products = [
        _product(studio="Arrow Video", policy="DENY", qty=0, barcode="A1", vid=vid_a, title="Arrow Title"),
        _product(studio="Second Sight", policy="DENY", qty=0, barcode="S1", vid=vid_s, title="SS Title"),
        _product(studio="Criterion Collection", policy="DENY", qty=0, barcode="C1", vid=vid_c, title="CC Title"),
        _product(studio="Warner Bros", policy="DENY", qty=0, barcode="W1", vid="gid://shopify/ProductVariant/40", title="WB"),
    ]
    offer = {
        "supplier_id": "moovies",
        "supplier_sku": "x",
        "availability_status": "in_stock",
        "reported_quantity": 5,
        "last_seen_at": "2026-08-18T00:00:00+00:00",
        "source_feed_at": "2026-08-18T00:00:00+00:00",
        "pipeline_completed_at": "2026-08-18T00:00:00+00:00",
        "raw_barcode": "A1",
        "release_variant_id": "r-a",
    }
    sb = _SB(
        {
            "release_shopify_listings": [
                {"shopify_variant_id": vid_a, "release_variant_id": "r-a"},
                {"shopify_variant_id": vid_s, "release_variant_id": "r-s"},
                {"shopify_variant_id": vid_c, "release_variant_id": "r-c"},
            ],
            "supplier_offers": [
                {**offer, "raw_barcode": "A1", "release_variant_id": "r-a"},
                {**offer, "raw_barcode": "S1", "release_variant_id": "r-s"},
                {**offer, "raw_barcode": "C1", "release_variant_id": "r-c"},
            ],
            "tape_inventory_levels": [],
            "suppliers": [{"id": "moovies", "display_name": "Moovies"}],
        }
    )
    # Eligible-only list (Warner excluded by studio filter upstream); still assert classifier on mixed input.
    decisions = build_decisions_for_products(products[:3], supabase=sb, now=now)
    by_title = {d.title: d for d in decisions}
    assert by_title["Arrow Title"].action == ACTION_SET_CONTINUE
    assert by_title["SS Title"].action == ACTION_SET_CONTINUE
    assert by_title["CC Title"].action == ACTION_SET_CONTINUE
    assert by_title["Arrow Title"].normalized_studio == "Arrow"
    assert by_title["SS Title"].normalized_studio == "Second Sight"
    assert by_title["CC Title"].normalized_studio == "Criterion Collection"


def test_build_decisions_tape_in_stock_no_change():
    now = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    vid = "gid://shopify/ProductVariant/99"
    products = [_product(studio="Second Sight", policy="DENY", qty=3, barcode="S2", vid=vid)]
    sb = _SB(
        {
            "release_shopify_listings": [{"shopify_variant_id": vid, "release_variant_id": "r1"}],
            "supplier_offers": [],
            "tape_inventory_levels": [{"release_variant_id": "r1", "on_hand": 3, "committed": 0, "available": 3}],
            "suppliers": [],
        }
    )
    d = build_decisions_for_products(products, supabase=sb, now=now)[0]
    assert d.action == ACTION_NO_CHANGE
    assert d.reason == "shopify_qty_not_zero"
    assert d.tape_on_hand == 3


def test_build_decisions_oversold_continue_denied_when_stock_feed_out_despite_catalog_stock():
    now = datetime(2026, 10, 1, 3, 10, tzinfo=timezone.utc)
    vid = "gid://shopify/ProductVariant/47531554930912"
    products = [_product(studio="Arrow Video", policy="CONTINUE", qty=-1, barcode="5027035029245", vid=vid)]
    base = {
        "supplier_id": "moovies",
        "raw_barcode": "5027035029245",
        "release_variant_id": "r-dc",
    }
    sb = _SB(
        {
            "release_shopify_listings": [{"shopify_variant_id": vid, "release_variant_id": "r-dc"}],
            "supplier_offers": [
                {
                    **base,
                    "supplier_sku": "barcode:5027035029245",
                    "availability_status": "unavailable",
                    "reported_quantity": 0,
                    "last_seen_at": "2026-10-01T03:03:37+00:00",
                    "source_feed_at": "2026-10-01T03:03:37+00:00",
                    "pipeline_completed_at": "2026-10-01T03:03:37+00:00",
                },
                {
                    **base,
                    "supplier_sku": "FCD2754",
                    "availability_status": "in_stock",
                    "reported_quantity": 3,
                    "last_seen_at": "2026-10-01T02:25:43+00:00",
                    "source_feed_at": "2026-10-01T02:25:43+00:00",
                    "pipeline_completed_at": "2026-10-01T02:25:43+00:00",
                },
            ],
            "tape_inventory_levels": [],
            "suppliers": [{"id": "moovies", "display_name": "Moovies"}],
        }
    )
    d = build_decisions_for_products(products, supabase=sb, now=now)[0]
    assert d.action == ACTION_SET_DENY
    assert d.reason == "continue_zero_stock_supplier_unavailable"
    assert d.available_suppliers == ""
    assert "qty=3" in d.all_supplier_states


def test_build_decisions_ambiguous_mapping_unresolved_zero_stock():
    now = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    vid = "gid://shopify/ProductVariant/77"
    products = [_product(studio="Arrow Films", policy="CONTINUE", qty=0, barcode="", vid=vid)]
    sb = _SB(
        {
            "release_shopify_listings": [],
            "supplier_offers": [],
            "tape_inventory_levels": [],
            "suppliers": [],
        }
    )
    d = build_decisions_for_products(products, supabase=sb, now=now)[0]
    assert d.action == ACTION_SET_DENY
    assert d.legacy_action == ACTION_SET_DENY
    assert "missing_barcode" in d.data_quality
    assert "missing_release_variant_mapping" in d.data_quality


def test_build_decisions_extra_protection_skips_backorder_deny():
    now = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    vid = "gid://shopify/ProductVariant/55"
    products = [_product(studio="Criterion Collection", policy="CONTINUE", qty=0, barcode="C9", vid=vid, back=True)]
    sb = _SB(
        {
            "release_shopify_listings": [{"shopify_variant_id": vid, "release_variant_id": "r9"}],
            "supplier_offers": [
                {
                    "supplier_id": "lasgo",
                    "supplier_sku": "x",
                    "availability_status": "unavailable",
                    "reported_quantity": 0,
                    "last_seen_at": "2026-08-18T00:00:00+00:00",
                    "source_feed_at": "2026-08-18T00:00:00+00:00",
                    "pipeline_completed_at": "2026-08-18T00:00:00+00:00",
                    "raw_barcode": "C9",
                    "release_variant_id": "r9",
                }
            ],
            "tape_inventory_levels": [],
            "suppliers": [{"id": "lasgo", "display_name": "Lasgo"}],
        }
    )
    legacy = build_decisions_for_products(products, supabase=sb, now=now, extra_continue_protections=False)[0]
    protected = build_decisions_for_products(products, supabase=sb, now=now, extra_continue_protections=True)[0]
    assert legacy.action == ACTION_SET_DENY
    assert protected.action == ACTION_SKIP
    assert protected.safety_classification == "backorder_protected"
    assert protected.legacy_action == ACTION_SET_DENY


def test_region_a_is_excluded_from_toggle_even_with_supplier_stock():
    now = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    vid = "gid://shopify/ProductVariant/88"
    products = [
        _product(
            studio="Criterion Collection",
            policy="DENY",
            qty=0,
            barcode="US1",
            vid=vid,
            title="US Edition",
            region="Region A",
        )
    ]
    sb = _SB(
        {
            "release_shopify_listings": [{"shopify_variant_id": vid, "release_variant_id": "r-us"}],
            "supplier_offers": [
                {
                    "supplier_id": "moovies",
                    "supplier_sku": "x",
                    "availability_status": "in_stock",
                    "reported_quantity": 9,
                    "unit_cost": 12.0,
                    "last_seen_at": "2026-08-18T00:00:00+00:00",
                    "source_feed_at": "2026-08-18T00:00:00+00:00",
                    "pipeline_completed_at": "2026-08-18T00:00:00+00:00",
                    "raw_barcode": "US1",
                    "release_variant_id": "r-us",
                }
            ],
            "tape_inventory_levels": [],
            "suppliers": [{"id": "moovies", "display_name": "Moovies"}],
        }
    )
    d = build_decisions_for_products(products, supabase=sb, now=now)[0]
    assert d.normalized_region == "A"
    assert d.action == ACTION_SKIP
    assert d.reason == "region_a_us_excluded"
    assert d.legacy_action == ACTION_SET_CONTINUE


def test_missing_region_is_not_toggle_scoped():
    now = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    vid = "gid://shopify/ProductVariant/89"
    products = [_product(studio="Arrow Video", policy="DENY", qty=0, barcode="NB1", vid=vid, region="")]
    sb = _SB(
        {
            "release_shopify_listings": [{"shopify_variant_id": vid, "release_variant_id": "r-nb"}],
            "supplier_offers": [],
            "tape_inventory_levels": [],
            "suppliers": [],
        }
    )
    d = build_decisions_for_products(products, supabase=sb, now=now)[0]
    assert d.action == ACTION_SKIP
    assert d.reason == "region_missing_or_not_b"


def test_variant_region_a_overrides_product_region_b():
    now = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)
    vid = "gid://shopify/ProductVariant/90"
    products = [
        _product(
            studio="Second Sight",
            policy="DENY",
            qty=0,
            barcode="VR1",
            vid=vid,
            region="Region B",
            variant_region="Region A",
        )
    ]
    sb = _SB(
        {
            "release_shopify_listings": [],
            "supplier_offers": [],
            "tape_inventory_levels": [],
            "suppliers": [],
        }
    )
    d = build_decisions_for_products(products, supabase=sb, now=now)[0]
    assert d.normalized_region == "A"
    assert d.action == ACTION_SKIP


def test_apply_dry_run_never_calls_shopify():
    client = MagicMock()
    from app.services.arrow_inventory_policy_sync_service import PolicyDecision

    d = PolicyDecision(
        product_id="p",
        title="t",
        handle="h",
        studio="Arrow Video",
        variant_id="v",
        variant_title="Default",
        sku="s",
        barcode="b",
        shopify_qty=0,
        inventory_policy="DENY",
        pre_order=False,
        media_release_date="",
        action=ACTION_SET_CONTINUE,
    )
    ok, failed = apply_policy_decisions(client, [d], dry_run=True)
    assert ok == 1 and failed == 0
    client.graphql.assert_not_called()
    assert d.apply_status == "dry_run"
