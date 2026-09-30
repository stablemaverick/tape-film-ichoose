from __future__ import annotations

from typing import Any, Dict, List

import pytest

from app.rules.au_classification import (
    AU_CLASSIFICATION_CHOICES,
    AU_CLASSIFICATION_IMAGES,
    most_restrictive,
    normalise_au_rating,
)
from app.services import au_classification_service as svc
from app.services.product_description_writer_service import apply_description


@pytest.mark.parametrize(
    "raw,expected",
    [("MA15+", "MA 15+"), ("ma 15+", "MA 15+"), ("R18+", "R 18+"), ("R 18+", "R 18+"), ("M", "M"), ("PG", "PG"),
     ("G", "G"), ("X 18+", "X 18+"), ("", None), ("CTC", None), ("E", None), (None, None)],
)
def test_normalise_au_rating(raw: Any, expected: Any) -> None:
    assert normalise_au_rating(raw) == expected


def test_most_restrictive() -> None:
    assert most_restrictive(["PG", "M", "G"]) == "M"
    assert most_restrictive(["MA 15+", "R 18+"]) == "R 18+"
    assert most_restrictive([]) is None


class FakeTmdb:
    def __init__(self, results: List[Dict[str, Any]]):
        self.results = results

    def get_release_dates(self, tmdb_id: int) -> Dict[str, Any]:
        return {"results": self.results}


def _au(*certs: str) -> List[Dict[str, Any]]:
    return [
        {"iso_3166_1": "US", "release_dates": [{"certification": "R", "type": 3}]},
        {"iso_3166_1": "AU", "release_dates": [{"certification": c, "type": 3, "release_date": "1990-01-01"}
                                               for c in certs]},
    ]


def test_resolve_from_tmdb_without_search(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "board_search", lambda *a, **k: pytest.fail("should not search"))
    result = svc.resolve_au_classification(tmdb=FakeTmdb(_au("", "MA15+")), client=object(), model="m",
                                           film_title="X", year="1990", tmdb_id=1)
    assert result["rating"] == "MA 15+"
    assert result["choice"] == AU_CLASSIFICATION_CHOICES["MA 15+"]
    assert result["image_gid"] == AU_CLASSIFICATION_IMAGES["MA 15+"]
    assert result["source"] == "tmdb"
    assert result["review_reasons"] == []


def test_tmdb_exact_match_ignores_undated_stub() -> None:
    from app.services.description_extraction_service import _tmdb_exact_match

    class SearchTmdb:
        def __init__(self, results: List[Dict[str, Any]]):
            self.results = results

        def search(self, title: str) -> List[Dict[str, Any]]:
            return self.results

    stub = {"id": 2, "title": "Logan's Run", "release_date": ""}
    film = {"id": 1, "title": "Logan's Run", "release_date": "1976-06-23"}
    assert _tmdb_exact_match(SearchTmdb([film, stub]), "Logans Run", None) == 1
    assert _tmdb_exact_match(SearchTmdb([film, {**film, "id": 3}]), "Logans Run", None) is None


def test_resolve_conflict_uses_most_restrictive() -> None:
    result = svc.resolve_au_classification(tmdb=FakeTmdb(_au("M", "MA 15+")), client=None, model="m",
                                           film_title="X", year=None, tmdb_id=1)
    assert result["rating"] == "MA 15+"
    assert "classification_conflict" in result["review_reasons"]


def test_resolve_falls_back_to_board_search(monkeypatch: pytest.MonkeyPatch) -> None:
    entries = [{"title": "ACROSS 110TH STREET", "rating": "R 18+", "url": "https://www.classification.gov.au/titles/x"}]
    monkeypatch.setattr(svc, "board_search", lambda *a, **k: (entries, (100, 10)))
    result = svc.resolve_au_classification(tmdb=FakeTmdb(_au()), client=object(), model="m",
                                           film_title="Across 110th Street", year="1972", tmdb_id=1)
    assert result["rating"] == "R 18+"
    assert result["source"] == "classification_board_search"
    assert "classification_from_search" in result["review_reasons"]
    assert result["search_calls"] == 1


def test_resolve_missing_and_unmappable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(svc, "board_search", lambda *a, **k: ([], (0, 0)))
    missing = svc.resolve_au_classification(tmdb=None, client=object(), model="m", film_title="X", year=None,
                                            tmdb_id=None)
    assert missing["rating"] is None and "classification_missing" in missing["review_reasons"]
    x = svc.resolve_au_classification(tmdb=FakeTmdb(_au("X 18+")), client=None, model="m", film_title="X",
                                      year=None, tmdb_id=1)
    assert x["rating"] == "X 18+" and x["choice"] is None
    assert "classification_unmappable" in x["review_reasons"]


def test_board_search_filters_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"entries": [
        {"title": "HOOK", "year": "1991", "category": "Film - Sale/Hire", "classification": "PG",
         "date_of_classification": "", "url": "https://www.classification.gov.au/titles/hook"},
        {"title": "HOOK", "year": "1991", "category": "Film (Advertisement)", "classification": "M",
         "date_of_classification": "", "url": "https://www.classification.gov.au/titles/hook-ad"},
        {"title": "HOOK", "year": "1991", "category": "Film - Sale/Hire", "classification": "M",
         "date_of_classification": "", "url": "https://www.zavvi.com/hook"},
        {"title": "CAPTAIN BLOOD", "year": "1935", "category": "Film - Sale/Hire", "classification": "M",
         "date_of_classification": "", "url": "https://www.classification.gov.au/titles/captain-blood"},
    ]}
    monkeypatch.setattr(svc, "output_json", lambda resp: payload)
    monkeypatch.setattr(svc, "usage_tokens", lambda resp: (1, 1))

    class Client:
        def responses(self, **kwargs: Any) -> Dict[str, Any]:
            assert kwargs["tools"][0]["filters"]["allowed_domains"] == ["classification.gov.au"]
            return {}

    entries, _ = svc.board_search(Client(), "m", film_title="Hook", year="1991")
    assert [e["rating"] for e in entries] == ["PG"]


class FakeShop:
    def __init__(self, classification: str | None = None):
        self.classification = classification
        self.metafield_sets: List[List[Dict[str, Any]]] = []

    def graphql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        if "metafieldsSet" in query:
            self.metafield_sets.append(variables["metafields"])
            return {"metafieldsSet": {"metafields": [], "userErrors": []}}
        if "productUpdate" in query:
            return {"productUpdate": {"product": {"descriptionHtml": variables["input"]["descriptionHtml"]},
                                      "userErrors": []}}
        return {"product": {
            "id": "p", "title": "T", "status": "DRAFT", "descriptionHtml": "<p>Hand written</p>",
            "variants": {"nodes": [{"barcode": "123"}]}, "descriptionHash": None,
            "classificationDescription": {"value": self.classification} if self.classification else None,
        }}


def _record(rating: str | None) -> Dict[str, Any]:
    return {
        "barcode": "123", "synopsis": "S", "special_features": [], "description_html": "<p>S</p>",
        "au_classification": {
            "rating": rating, "choice": AU_CLASSIFICATION_CHOICES.get(rating or ""),
            "image_gid": AU_CLASSIFICATION_IMAGES.get(rating or ""), "source": "tmdb", "evidence": [],
        },
    }


def test_writer_fills_empty_classification_even_when_description_is_protected() -> None:
    shop = FakeShop()
    result = apply_description(shop, product_id="p", record=_record("M"))
    assert result["status"] == "skipped_manual_edit"
    assert result["classification_status"] == "applied"
    keys = {m["key"]: m for m in shop.metafield_sets[-1]}
    assert keys["classification_description"]["value"] == AU_CLASSIFICATION_CHOICES["M"]
    assert keys["classification"] == {"ownerId": "p", "namespace": "custom", "key": "classification",
                                      "type": "file_reference", "value": AU_CLASSIFICATION_IMAGES["M"]}


def test_writer_never_overwrites_existing_classification() -> None:
    shop = FakeShop(classification=AU_CLASSIFICATION_CHOICES["PG"])
    result = apply_description(shop, product_id="p", record=_record("M"))
    assert result["classification_status"] == "skipped_existing"
    assert shop.metafield_sets == []
    same = FakeShop(classification=AU_CLASSIFICATION_CHOICES["M"])
    assert apply_description(same, product_id="p", record=_record("M"))["classification_status"] == "unchanged"


def test_writer_classification_not_found() -> None:
    shop = FakeShop()
    result = apply_description(shop, product_id="p", record=_record(None))
    assert result["classification_status"] == "not_found"
    assert shop.metafield_sets == []
