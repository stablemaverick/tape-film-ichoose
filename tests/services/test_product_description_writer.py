from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from app.services import product_description_writer_service as writer
from app.services.product_description_writer_service import apply_description, description_hash


class FakeShopify:
    def __init__(self, *, status: str = "DRAFT", barcode: str = "123", html: str = "",
                 stored_hash: Optional[str] = None, saved_html_suffix: str = ""):
        self.status = status
        self.barcode = barcode
        self.html = html
        self.stored_hash = stored_hash
        self.saved_html_suffix = saved_html_suffix
        self.calls: List[Dict[str, Any]] = []

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        self.calls.append({"query": query, "variables": variables})
        if "productUpdate" in query:
            assert "status" not in variables["input"]
            self.html = variables["input"]["descriptionHtml"] + self.saved_html_suffix
            return {"productUpdate": {"product": {"id": "p", "status": self.status, "descriptionHtml": self.html},
                                      "userErrors": []}}
        if "metafieldsSet" in query:
            for m in variables["metafields"]:
                if m["key"] == "description_hash":
                    self.stored_hash = m["value"]
            return {"metafieldsSet": {"metafields": [], "userErrors": []}}
        return {"product": {
            "id": "p", "title": "T", "status": self.status, "descriptionHtml": self.html,
            "variants": {"nodes": [{"barcode": self.barcode}]},
            "descriptionHash": {"value": self.stored_hash} if self.stored_hash else None,
        }}

    def writes(self) -> int:
        return sum(1 for c in self.calls if "productUpdate" in c["query"])


def _record(**overrides: Any) -> Dict[str, Any]:
    rec = {"barcode": "123", "synopsis": "A synopsis.", "special_features": ["Trailer"],
           "description_html": "<p>A synopsis.</p>", "source_type": "official_distributor",
           "synopsis_source": "distributor", "features_status": "announced"}
    rec.update(overrides)
    return rec


def test_applies_to_empty_draft_and_stores_hash_of_saved_html() -> None:
    shop = FakeShopify(saved_html_suffix="\n")
    result = apply_description(shop, product_id="p", record=_record())
    assert result["status"] == "applied"
    assert shop.stored_hash == description_hash("<p>A synopsis.</p>\n")
    assert shop.writes() == 1


def test_skips_non_draft_products() -> None:
    shop = FakeShopify(status="ACTIVE")
    assert apply_description(shop, product_id="p", record=_record())["status"] == "skipped_not_draft"
    assert shop.writes() == 0


def test_skips_barcode_mismatch() -> None:
    shop = FakeShopify(barcode="999")
    assert apply_description(shop, product_id="p", record=_record())["status"] == "skipped_barcode_mismatch"
    assert shop.writes() == 0


def test_skips_manual_edit_unless_forced() -> None:
    shop = FakeShopify(html="<p>Hand written</p>")
    assert apply_description(shop, product_id="p", record=_record())["status"] == "skipped_manual_edit"
    assert shop.writes() == 0
    assert apply_description(shop, product_id="p", record=_record(), force=True)["status"] == "applied"


def test_overwrites_own_previous_description_and_detects_unchanged() -> None:
    old = "<p>Old draft</p>"
    shop = FakeShopify(html=old, stored_hash=description_hash(old))
    assert apply_description(shop, product_id="p", record=_record())["status"] == "applied"
    assert apply_description(shop, product_id="p", record=_record())["status"] == "unchanged"
    assert shop.writes() == 1


def test_skips_empty_drafts() -> None:
    shop = FakeShopify()
    result = apply_description(shop, product_id="p", record=_record(synopsis="", special_features=[]))
    assert result["status"] == "skipped_empty"


def test_run_description_stage_dry_run_and_table_failure_is_non_fatal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(writer, "run_product_description_drafting", lambda **k: [
        _record(barcode="123"), {"barcode": "456", "error": "boom", "needs_review": True}
    ])

    class BrokenTable:
        def table(self, name: str) -> Any:
            raise RuntimeError("relation does not exist")

    shop = FakeShopify()
    dry = writer.run_description_stage(product_ids_by_barcode={"123": None, "456": None}, supabase=BrokenTable(),
                                       shopify=shop, apply=False, out_path=tmp_path / "d.json")
    assert dry["summary"]["outcomes"] == {"drafted": 1, "failed": 1}
    assert shop.writes() == 0

    live = writer.run_description_stage(product_ids_by_barcode={"123": "p", "456": "q"}, supabase=BrokenTable(),
                                        shopify=shop, apply=True, out_path=tmp_path / "a.json")
    assert live["summary"]["outcomes"] == {"applied": 1, "failed": 1}
    assert "relation does not exist" in live["summary"]["table_error"]
    assert (tmp_path / "a.json").exists()
