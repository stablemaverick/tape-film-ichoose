"""Unit tests for supplier open-PO inbound parse / match / net helpers."""

from __future__ import annotations

from pathlib import Path

from app.services.supplier_orders_report_service import (
    _ShopifyNeedCandidate,
    apply_po_cover_to_candidates,
    classify_supplier_need,
    enrich_with_moovies_catalog_sku,
)
from app.services.supplier_po_inbound import (
    allocate_po_cover_for_variant,
    aggregate_po_lines,
    build_shopify_match_indexes,
    collect_unmatched_po_lines,
    find_latest_inbound_csv,
    find_latest_readable_inbound_csv,
    is_open_po_status,
    load_po_inbound_snapshot,
    normalize_match_key,
    parse_po_csv,
    remaining_qty_maps,
)


def test_normalize_match_key_collapses_whitespace_and_case():
    assert normalize_match_key("  AMSSB10006  ") == "amssb10006"
    assert normalize_match_key("Evil  Dead Burn") == "evil dead burn"


def test_open_po_statuses():
    assert is_open_po_status("Pre-Order")
    assert is_open_po_status("picking")
    assert is_open_po_status("Awaiting Stock")
    assert not is_open_po_status("Shipped")
    assert not is_open_po_status("Cancelled")


def test_parse_and_aggregate_sums_by_sku(tmp_path: Path):
    path = tmp_path / "open_pos.csv"
    path.write_text(
        "\n".join(
            [
                "order_id,sku,title,qty,unit_cost,line_total,status",
                "1,AMSSB10006,Project Hail Mary LE,2,33.42,66.84,Pre-Order",
                "2,AMSSB10006,Project Hail Mary LE,5,33.42,167.10,Pre-Order",
                "3,OTHER,Ignored Title,9,1,9,Shipped",
                "4,,Title Only Match,3,1,3,Picking",
            ]
        ),
        encoding="utf-8",
    )
    lines, errors, skipped = parse_po_csv(path)
    assert errors == []
    assert skipped == 1
    assert len(lines) == 3
    by_sku, by_title = aggregate_po_lines(lines)
    assert by_sku["amssb10006"].qty == 7
    assert by_sku["amssb10006"].order_ids == ["1", "2"]
    assert by_title["title only match"].qty == 3


def test_find_latest_inbound_csv(tmp_path: Path):
    older = tmp_path / "a.csv"
    newer = tmp_path / "b.csv"
    older.write_text("order_id,sku,title,qty,unit_cost,line_total,status\n", encoding="utf-8")
    newer.write_text("order_id,sku,title,qty,unit_cost,line_total,status\n", encoding="utf-8")
    # bump mtime on newer
    newer.touch()
    assert find_latest_inbound_csv(tmp_path) == newer
    assert load_po_inbound_snapshot(tmp_path / "missing").path is None


def test_find_latest_readable_inbound_csv_skips_unreadable(tmp_path: Path):
    older = tmp_path / "older.csv"
    newer = tmp_path / "newer.csv"
    older.write_text("order_id,sku,title,qty,unit_cost,line_total,status\n", encoding="utf-8")
    newer.write_text("order_id,sku,title,qty,unit_cost,line_total,status\n", encoding="utf-8")
    older.touch()
    newer.touch()

    # Simulate unreadable newest file by monkeypatching Path.open.
    original_open = Path.open

    def fake_open(self, *args, **kwargs):
        if self == newer:
            raise PermissionError("permission denied")
        return original_open(self, *args, **kwargs)

    from unittest.mock import patch

    with patch.object(Path, "open", fake_open):
        readable, unreadable, err = find_latest_readable_inbound_csv(tmp_path)

    assert readable == older
    assert unreadable == newer
    assert "permission denied" in (err or "")


def test_build_indexes_marks_ambiguous_titles():
    variants = [
        {"shopify_variant_id": "v1", "sku": "SKU1", "product_title": "Same Title"},
        {"shopify_variant_id": "v2", "sku": "SKU2", "product_title": "Same Title"},
        {"shopify_variant_id": "v3", "sku": "SKU3", "product_title": "Unique"},
    ]
    by_sku, by_title, ambiguous = build_shopify_match_indexes(variants)
    assert by_sku["sku1"] == "v1"
    assert "same title" in ambiguous
    assert "same title" not in by_title
    assert by_title["unique"] == "v3"


def test_allocate_prefers_sku_then_title():
    from app.services.supplier_po_inbound import PoBucket, SupplierPoLine

    line_sku = SupplierPoLine(
        order_id="10",
        sku="ABC",
        title="Film A",
        qty=4,
        unit_cost="",
        line_total="",
        status="Pre-Order",
        sku_key="abc",
        title_key="film a",
    )
    line_title = SupplierPoLine(
        order_id="11",
        sku="",
        title="Film B",
        qty=2,
        unit_cost="",
        line_total="",
        status="Picking",
        sku_key="",
        title_key="film b",
    )
    by_sku = {"abc": PoBucket()}
    by_sku["abc"].add(line_sku)
    by_title = {"film a": PoBucket(), "film b": PoBucket()}
    by_title["film a"].add(line_sku)
    by_title["film b"].add(line_title)
    remaining_sku = {"abc": 4}
    remaining_title = {"film a": 4, "film b": 2}

    applied, match, oids = allocate_po_cover_for_variant(
        sku="ABC",
        product_title="Film A",
        shopify_need=3,
        remaining_sku_qty=remaining_sku,
        remaining_title_qty=remaining_title,
        by_sku=by_sku,
        by_title=by_title,
        ambiguous_titles=set(),
    )
    assert (applied, match) == (3, "sku")
    assert "10" in oids
    assert remaining_sku["abc"] == 1

    applied2, match2, _ = allocate_po_cover_for_variant(
        sku="",
        product_title="Film B",
        shopify_need=5,
        remaining_sku_qty=remaining_sku,
        remaining_title_qty=remaining_title,
        by_sku=by_sku,
        by_title=by_title,
        ambiguous_titles=set(),
    )
    assert (applied2, match2) == (2, "title")
    assert remaining_title["film b"] == 0


def test_enrich_with_moovies_catalog_sku_replaces_shopify_sku():
    candidates = [
        _ShopifyNeedCandidate(
            product_title="Project Hail Mary",
            barcode="5051888281123",
            sku="5051888281123",  # Shopify often stores barcode as sku
            shopify_need=2,
            committed=2,
            available=-2,
            on_hand=0,
            incoming=0,
            inventory_policy="CONTINUE",
            is_preorder=True,
            pre_order_metafield=True,
            backorder_metafield=False,
            media_release_date="2026-12-01",
            product_status="ACTIVE",
            shopify_product_id="gid://shopify/Product/1",
            shopify_variant_id="gid://shopify/ProductVariant/1",
        ),
        _ShopifyNeedCandidate(
            product_title="Lasgo Only Title",
            barcode="999",
            sku="999",
            shopify_need=1,
            committed=1,
            available=-1,
            on_hand=0,
            incoming=0,
            inventory_policy="CONTINUE",
            is_preorder=False,
            pre_order_metafield=False,
            backorder_metafield=True,
            media_release_date="",
            product_status="ACTIVE",
            shopify_product_id="gid://shopify/Product/2",
            shopify_variant_id="gid://shopify/ProductVariant/2",
        ),
    ]
    variants = [
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/1",
            "sku": "5051888281123",
            "barcode": "5051888281123",
            "product_title": "Project Hail Mary",
        },
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/2",
            "sku": "999",
            "barcode": "999",
            "product_title": "Lasgo Only Title",
        },
    ]
    cand_n, var_n = enrich_with_moovies_catalog_sku(
        candidates,
        variants,
        {"5051888281123": "AMSSB10006"},
    )
    assert cand_n == 1
    assert var_n == 1
    assert candidates[0].sku == "AMSSB10006"
    assert variants[0]["sku"] == "AMSSB10006"
    # No Moovies map → leave Shopify sku (Lasgo has none in catalog)
    assert candidates[1].sku == "999"
    assert variants[1]["sku"] == "999"


def test_po_cover_uses_enriched_moovies_sku(tmp_path: Path):
    inbound = tmp_path / "inbound"
    inbound.mkdir()
    (inbound / "open_pos.csv").write_text(
        "order_id,sku,title,qty,unit_cost,line_total,status\n"
        "1,AMSSB10006,Project Hail Mary,5,1,5,Pre-Order\n",
        encoding="utf-8",
    )
    candidates = [
        _ShopifyNeedCandidate(
            product_title="Project Hail Mary",
            barcode="5051888281123",
            sku="5051888281123",
            shopify_need=8,
            committed=8,
            available=-8,
            on_hand=0,
            incoming=0,
            inventory_policy="CONTINUE",
            is_preorder=True,
            pre_order_metafield=True,
            backorder_metafield=False,
            media_release_date="2026-12-01",
            product_status="ACTIVE",
            shopify_product_id="gid://shopify/Product/1",
            shopify_variant_id="gid://shopify/ProductVariant/1",
        )
    ]
    variants = [
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/1",
            "sku": "5051888281123",
            "barcode": "5051888281123",
            "product_title": "Project Hail Mary",
        }
    ]
    enrich_with_moovies_catalog_sku(
        candidates, variants, {"5051888281123": "AMSSB10006"}
    )
    pre, other, meta = apply_po_cover_to_candidates(candidates, variants, inbound)
    assert other == []
    assert len(pre) == 1
    assert pre[0].sku == "AMSSB10006"
    assert pre[0].open_po_qty == 5
    assert pre[0].qty_to_order == 3
    assert pre[0].po_match == "sku"
    assert meta["unmatched_po_count"] == 0


def test_apply_po_cover_nets_still_needed(tmp_path: Path):
    inbound = tmp_path / "inbound"
    inbound.mkdir()
    (inbound / "open_pos.csv").write_text(
        "\n".join(
            [
                "order_id,sku,title,qty,unit_cost,line_total,status",
                "1,HAIL,Project Hail Mary,5,1,5,Pre-Order",
            ]
        ),
        encoding="utf-8",
    )
    candidates = [
        _ShopifyNeedCandidate(
            product_title="Project Hail Mary",
            barcode="111",
            sku="HAIL",
            shopify_need=8,
            committed=8,
            available=-8,
            on_hand=0,
            incoming=0,
            inventory_policy="CONTINUE",
            is_preorder=True,
            pre_order_metafield=True,
            backorder_metafield=False,
            media_release_date="2026-12-01",
            product_status="ACTIVE",
            shopify_product_id="gid://shopify/Product/1",
            shopify_variant_id="gid://shopify/ProductVariant/1",
        )
    ]
    variants = [
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/1",
            "sku": "HAIL",
            "product_title": "Project Hail Mary",
        }
    ]
    pre, other, meta = apply_po_cover_to_candidates(candidates, variants, inbound)
    assert other == []
    assert len(pre) == 1
    assert pre[0].shopify_need == 8
    assert pre[0].open_po_qty == 5
    assert pre[0].qty_to_order == 3
    assert pre[0].po_match == "sku"
    assert meta["po_units_applied"] == 5
    assert meta["unmatched_po_count"] == 0


def test_fully_covered_by_po_drops_from_buy_list(tmp_path: Path):
    inbound = tmp_path / "inbound"
    inbound.mkdir()
    (inbound / "open_pos.csv").write_text(
        "order_id,sku,title,qty,unit_cost,line_total,status\n"
        "1,HAIL,Project Hail Mary,10,1,10,Pre-Order\n",
        encoding="utf-8",
    )
    candidates = [
        _ShopifyNeedCandidate(
            product_title="Project Hail Mary",
            barcode="111",
            sku="HAIL",
            shopify_need=8,
            committed=8,
            available=-8,
            on_hand=0,
            incoming=0,
            inventory_policy="CONTINUE",
            is_preorder=True,
            pre_order_metafield=True,
            backorder_metafield=False,
            media_release_date="2026-12-01",
            product_status="ACTIVE",
            shopify_product_id="gid://shopify/Product/1",
            shopify_variant_id="gid://shopify/ProductVariant/1",
        )
    ]
    variants = [
        {
            "shopify_variant_id": "gid://shopify/ProductVariant/1",
            "sku": "HAIL",
            "product_title": "Project Hail Mary",
        }
    ]
    pre, other, meta = apply_po_cover_to_candidates(candidates, variants, inbound)
    assert pre == []
    assert other == []
    assert meta["po_units_applied"] == 8
    assert classify_supplier_need(
        is_preorder=True,
        policy="CONTINUE",
        backorder=False,
        available=-8,
        committed=8,
        on_hand=0,
        need=0,
    ) is None


def test_unmatched_po_when_sku_unknown(tmp_path: Path):
    inbound = tmp_path / "inbound"
    inbound.mkdir()
    (inbound / "open_pos.csv").write_text(
        "order_id,sku,title,qty,unit_cost,line_total,status\n"
        "9,UNKNOWN,Mystery Title,2,1,2,Pre-Order\n",
        encoding="utf-8",
    )
    snapshot = load_po_inbound_snapshot(inbound)
    remaining_sku, remaining_title = remaining_qty_maps(snapshot)
    assert remaining_sku["unknown"] == 2
    unmatched = collect_unmatched_po_lines(
        snapshot,
        matched_sku_keys=set(),
        matched_title_keys=set(),
        shopify_sku_keys={"hail"},
        shopify_title_keys={"project hail mary"},
        ambiguous_titles=set(),
    )
    assert len(unmatched) == 1
    assert unmatched[0]["reason"] == "no_match"
    assert remaining_title["mystery title"] == 2


def test_load_po_inbound_snapshot_unreadable_file_does_not_crash(tmp_path: Path):
    inbound = tmp_path / "inbound"
    inbound.mkdir()
    path = inbound / "orders.csv"
    path.write_text(
        "order_id,sku,title,qty,unit_cost,line_total,status\n"
        "1,SKU,Title,1,1,1,Pre-Order\n",
        encoding="utf-8",
    )
    original_open = Path.open

    def fake_open(self, *args, **kwargs):
        if self == path:
            raise PermissionError("permission denied")
        return original_open(self, *args, **kwargs)

    from unittest.mock import patch

    with patch.object(Path, "open", fake_open):
        snapshot = load_po_inbound_snapshot(inbound)

    assert snapshot.path is None
    assert snapshot.lines == []
    assert snapshot.parse_errors
