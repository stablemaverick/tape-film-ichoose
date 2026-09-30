from __future__ import annotations

from typing import Any, Dict, List

import pytest

from app.rules.distributor_sources import is_allowed_url, is_blocked_url, resolve_distributor
from app.services import product_description_drafting_service as drafting
from app.services.description_sources_service import (
    SourcePage,
    film_page_candidates,
    film_title_from_catalog,
    html_to_text,
    mirror_url,
)
from app.services.description_verification_service import (
    check_lines,
    copied_runs,
    disallowed_urls,
    unsupported_terms,
)


@pytest.mark.parametrize(
    "studio,key",
    [
        ("Transmission", "radiance"),
        ("Toy Robot Video", "toy_robot"),
        ("Arrow Films", "arrow"),
        ("Studio Canal", "studiocanal"),
        ("Walt Disney", "disney"),
        ("Criterion Collection", "criterion"),
        ("Second Sight", "second_sight"),
        ("Curzon Film World", "curzon"),
    ],
)
def test_resolve_distributor(studio: str, key: str) -> None:
    dist = resolve_distributor(studio)
    assert dist is not None and dist.key == key


def test_resolve_distributor_unknown() -> None:
    assert resolve_distributor("Some Unknown Label") is None
    assert resolve_distributor(None) is None


def test_url_allow_and_block() -> None:
    arrow = resolve_distributor("Arrow Films")
    assert is_allowed_url("https://www.arrowfilms.com/p/x/1/", arrow)
    assert not is_allowed_url("https://www.zavvi.com/p/x/1/", arrow)
    assert not is_allowed_url("https://www.radiancefilms.co.uk/products/x", arrow)
    assert is_blocked_url("https://www.blu-ray.com/movies/x")
    disney = resolve_distributor("Walt Disney")
    assert is_allowed_url("https://press.disney.co.uk/press-kit/x", disney)
    assert disallowed_urls(["https://justlovemovies.com/x", "https://www.disney.co.uk/movies/x"], disney) == [
        "https://justlovemovies.com/x"
    ]


@pytest.mark.parametrize(
    "title,expected",
    [
        ("Pressure (2026) Limited Edition Steelbook 4K Ultra HD", ("Pressure", "2026")),
        ("Passenger 57 4K Ultra HD + Blu-Ray", ("Passenger 57", None)),
        ("Moana (Live Action) Limited Edition Steelbook 4K Ultra HD + Blu-Ray", ("Moana", None)),
        ("The Man Who Fell To Earth Limited Collectors Edition 4K Ultra HD", ("The Man Who Fell To Earth", None)),
    ],
)
def test_film_title_from_catalog(title: str, expected: tuple) -> None:
    assert film_title_from_catalog(title) == expected


def test_html_to_text_includes_embedded_copy_for_focus_title_only() -> None:
    embedded_target = "Blue Velvet " + " ".join(["word"] * 40) + "\\nProduct Features\\nOuttakes"
    embedded_other = "Other Film " + " ".join(["word"] * 40)
    html = (
        "<html><head><title>x</title></head><body><nav>Menu</nav><h1>Blue Velvet</h1>"
        "<ul><li>Visible line</li></ul>"
        f'<script>var s = {{"a": "{embedded_target}", "b": "{embedded_other}"}}</script></body></html>'
    )
    text = html_to_text(html, focus="Blue Velvet")
    assert "Visible line" in text
    assert "Outtakes" in text
    assert "Other Film" not in text
    assert "Menu" not in text


def test_check_lines_keeps_verbatim_and_fuzzy_drops_invented() -> None:
    source = "SPECIAL FEATURES\n- New audio commentary with critic Mike Sargent\n- Trailer\n"
    result = check_lines(
        [
            "New audio commentary with critic Mike Sargent",
            "New audio commentary with critic Mike Sargent.",
            "Trailer",
            "Brand new making-of documentary",
        ],
        source,
    )
    assert result.kept == [
        "New audio commentary with critic Mike Sargent",
        "New audio commentary with critic Mike Sargent.",
        "Trailer",
    ]
    assert result.dropped == ["Brand new making-of documentary"]


def test_unsupported_terms_flags_new_names_and_numbers_but_not_possessives() -> None:
    reference = "Marianne is played by Noémie Merlant. Héloïse is played by Adèle Haenel. Set in 1770 Brittany."
    synopsis = "In Brittany, Marianne (Noémie Merlant) paints Adèle Haenel's Héloïse in 1770. A French tale from 1999."
    missing = unsupported_terms(synopsis, reference)
    assert "french" in missing
    assert "1999" in missing
    assert "haenel" not in missing
    assert "brittany" not in missing


def test_unsupported_terms_ignores_sentence_initial_words() -> None:
    assert unsupported_terms("Joined by Maui, Moana sails.", "Moana and Maui sail together.") == []


def test_mirror_url_only_for_configured_domains() -> None:
    assert mirror_url("https://www.criterion.com/films/35257-wild-at-heart") == (
        "https://criterion-v2.herokuapp.com/films/35257-wild-at-heart"
    )
    assert mirror_url("https://www.arrowfilms.com/p/x/1/") is None


def test_film_page_candidates_try_year_and_previous_year() -> None:
    urls = film_page_candidates(resolve_distributor("Studio Canal"), film_title="Pressure", year="2026")
    assert urls == [
        "https://www.studiocanal.co.uk/title/pressure-2026/",
        "https://www.studiocanal.co.uk/title/pressure-2025/",
        "https://www.studiocanal.com/title/pressure-2026/",
        "https://www.studiocanal.com/title/pressure-2025/",
    ]
    assert film_page_candidates(resolve_distributor("Studio Canal"), film_title="Pressure", year=None) == []
    assert film_page_candidates(resolve_distributor("Warner Bros"), film_title="Practical Magic 2", year=None) == [
        "https://www.warnerbros.com/movies/practical-magic-2",
        "https://www.warnerbros.co.uk/movies/practical-magic-2",
    ]


def test_split_piped_lines() -> None:
    assert drafting._split_piped(["A | B", "C"]) == ["A", "B", "C"]


def test_copied_runs_detects_reused_phrasing() -> None:
    reference = "A young painter is commissioned to paint the wedding portrait of a reluctant bride."
    assert copied_runs("She is commissioned to paint the wedding portrait of her.", reference)
    assert not copied_runs("An artist must secretly capture a reluctant subject on canvas.", reference)


def test_build_description_html_tbc_and_escaping() -> None:
    html = drafting.build_description_html(
        {"synopsis": "One & two.\n\nThree <b>.", "edition_contents": [], "technical_format": ["Region: B"],
         "special_features": []}
    )
    assert html.startswith("<p>One &amp; two.</p><p>Three &lt;b&gt;.</p>")
    assert "<h3>Format</h3><ul><li>Region: B</li></ul>" in html
    assert "Special features to be confirmed by the distributor." in html


class _FakeSearch:
    def __init__(self, pages: List[SourcePage]):
        self.pages = pages
        self.attempts = ["fake"]
        self.tokens_in = 100
        self.tokens_out = 10
        self.search_calls = 1


def _row(**overrides: Any) -> Dict[str, Any]:
    row = {"id": 1, "barcode": "5060974682973", "title": "Across 110th Street Limited Edition 4K Ultra HD + Blu-Ray",
           "studio": "Transmission", "format": "4K UHD", "film_released": "1972-12-19", "tmdb_id": None}
    row.update(overrides)
    return row


def test_draft_description_official_distributor_drops_invented_lines(monkeypatch: pytest.MonkeyPatch) -> None:
    page_text = (
        "Three crooks hold up a Mob bank in Harlem and escape with $300,000. Captain Mattelli (Anthony Quinn) "
        "and Lt. Pope (Yaphet Kotto) must work together.\nSPECIAL FEATURES\n- Trailer\n"
        "- New audio commentary with critic Mike Sargent\nRegion: B"
    )
    page = SourcePage(url="https://www.radiancefilms.co.uk/products/across-110th-street-uhd-bd-le", text=page_text,
                      page_kind="edition", text_origin="shopify_json", barcode_on_page=True)
    monkeypatch.setattr(drafting, "find_official_pages", lambda *a, **k: _FakeSearch([page]))
    synopsis = ("Three crooks hold up a Mob bank in Harlem and escape with $300,000. " * 3).strip() + "\n\n" + (
        "Captain Mattelli (Anthony Quinn) and Lt. Pope (Yaphet Kotto) must work together. " * 3
    ).strip()
    extraction = {
        "edition_source_id": "S1", "edition_match": "same_edition_likely", "edition_match_reason": "",
        "synopsis_available": True, "synopsis_source_ids": ["S1"], "synopsis": synopsis,
        "edition_contents": [], "technical_format": ["Region: B"],
        "special_features": ["Trailer", "New audio commentary with critic Mike Sargent", "Invented documentary"],
        "notes": "",
    }
    monkeypatch.setattr(drafting, "extract_from_sources", lambda *a, **k: (extraction, (1000, 200)))

    ctx = drafting.DraftingContext(client=object(), model="test", classify=False)
    record = drafting.draft_description(_row(), ctx)

    assert record["source_type"] == "official_distributor"
    assert record["edition_match"] == "barcode_confirmed"
    assert record["edition_match_confirmed"] is True
    assert record["special_features"] == ["Trailer", "New audio commentary with critic Mike Sargent"]
    assert record["dropped_lines"] == ["Invented documentary"]
    assert record["features_status"] == "announced"
    assert "dropped_unverified_lines" in record["review_reasons"]
    assert record["synopsis_source"] == "distributor"
    assert "<h3>Special Features</h3>" in record["description_html"]


def test_draft_description_film_page_only_is_fallback_with_tbc(monkeypatch: pytest.MonkeyPatch) -> None:
    page = SourcePage(url="https://www.sonypictures.co.uk/movies/hook", text="Hook synopsis text",
                      page_kind="film", text_origin="fetched")
    monkeypatch.setattr(drafting, "find_official_pages", lambda *a, **k: _FakeSearch([page]))
    extraction = {
        "edition_source_id": "", "edition_match": "no_edition_page", "edition_match_reason": "",
        "synopsis_available": True, "synopsis_source_ids": ["S1"], "synopsis": "Hook synopsis text",
        "edition_contents": [], "technical_format": [], "special_features": ["Should be ignored"], "notes": "",
    }
    monkeypatch.setattr(drafting, "extract_from_sources", lambda *a, **k: (extraction, (10, 1)))
    record = drafting.draft_description(_row(barcode="5050630318728", title="Hook 4K Ultra HD", studio="Sony Pictures"),
                                        drafting.DraftingContext(client=object(), model="test", classify=False))
    assert record["source_type"] == "official_fallback"
    assert record["special_features"] == []
    assert record["features_status"] == "tbc"
    assert record["source_urls"] == ["https://www.sonypictures.co.uk/movies/hook"]


def test_draft_description_unmapped_label_without_tmdb_has_no_synopsis() -> None:
    record = drafting.draft_description(_row(studio="Unknown Label"), drafting.DraftingContext(client=object(), model="t", classify=False))
    assert record["source_type"] == "none"
    assert record["synopsis_source"] == "none"
    assert {"distributor_unmapped", "no_synopsis"} <= set(record["review_reasons"])
