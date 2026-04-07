"""
Rich catalog fixtures for intent-aware retrieval harness (isolated).

Fields mirror agent-facing catalog metadata used for filtering/ranking experiments.
"""

from __future__ import annotations

from typing import Any, Dict, List, TypedDict


class IntentFixtureRow(TypedDict, total=False):
    id: str
    title: str
    edition_title: str
    director: str
    lead_actor: str
    studio: str
    film_year: int
    availability_status: str
    ranking_bucket: str
    genre_tags: str
    awards_note: str
    fixture_notes: str


# Isolated harness corpus: synthetic but structurally realistic SKUs.
INTENT_FIXTURE_ROWS: List[IntentFixtureRow] = [
    {
        "id": "nolan_inception_4k",
        "title": "Inception 4K Ultra HD",
        "director": "Christopher Nolan",
        "studio": "Warner Bros",
        "film_year": 2010,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "sci-fi thriller",
        "awards_note": "",
        "fixture_notes": "director + commerce 4k in stock",
    },
    {
        "id": "nolan_dunkirk_bd",
        "title": "Dunkirk Blu-Ray",
        "director": "Christopher Nolan",
        "studio": "Warner Bros",
        "film_year": 2017,
        "availability_status": "supplier_stock",
        "ranking_bucket": "supplier_in_stock",
        "genre_tags": "war drama",
        "awards_note": "bafta nominee",
        "fixture_notes": "same director alt",
    },
    {
        "id": "hanks_forrest_gump",
        "title": "Forrest Gump Blu-Ray",
        "director": "Robert Zemeckis",
        "lead_actor": "Tom Hanks",
        "studio": "Paramount",
        "film_year": 1994,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "drama",
        "awards_note": "oscar best picture",
        "fixture_notes": "actor + awards",
    },
    {
        "id": "hanks_cast_away",
        "title": "Cast Away Blu-Ray",
        "director": "Robert Zemeckis",
        "lead_actor": "Tom Hanks",
        "studio": "20th Century Fox",
        "film_year": 2000,
        "availability_status": "preorder",
        "ranking_bucket": "preorder",
        "genre_tags": "drama",
        "awards_note": "",
        "fixture_notes": "actor preorder",
    },
    {
        "id": "horror_1999_sixth",
        "title": "The Sixth Sense Blu-Ray",
        "director": "M. Night Shyamalan",
        "studio": "Hollywood Pictures",
        "film_year": 1999,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "horror thriller",
        "awards_note": "oscar nominee",
        "fixture_notes": "year 1999 horror",
    },
    {
        "id": "horror_2018_hereditary",
        "title": "Hereditary Blu-Ray",
        "director": "Ari Aster",
        "studio": "A24",
        "film_year": 2018,
        "availability_status": "supplier_stock",
        "ranking_bucket": "supplier_in_stock",
        "genre_tags": "horror",
        "awards_note": "",
        "fixture_notes": "modern horror",
    },
    {
        "id": "action_1986_top_gun",
        "title": "Top Gun Blu-Ray",
        "director": "Tony Scott",
        "studio": "Paramount",
        "film_year": 1986,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "action",
        "awards_note": "",
        "fixture_notes": "80s action",
    },
    {
        "id": "action_2022_bullet_train",
        "title": "Bullet Train 4K",
        "director": "David Leitch",
        "studio": "Sony",
        "film_year": 2022,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "action comedy",
        "awards_note": "",
        "fixture_notes": "2020s action",
    },
    {
        "id": "criterion_seven_samurai",
        "title": "Seven Samurai Criterion Collection Blu-Ray",
        "director": "Akira Kurosawa",
        "studio": "Criterion",
        "film_year": 1954,
        "availability_status": "supplier_stock",
        "ranking_bucket": "supplier_in_stock",
        "genre_tags": "classic drama",
        "awards_note": "",
        "fixture_notes": "criterion label",
    },
    {
        "id": "arrow_dawn_dead",
        "title": "Dawn of the Dead Arrow Video Limited Edition",
        "director": "George A. Romero",
        "studio": "Arrow Video",
        "film_year": 1978,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "horror",
        "awards_note": "",
        "fixture_notes": "arrow label",
    },
    {
        "id": "indicator_quatermass",
        "title": "Quatermass and the Pit Indicator Blu-Ray",
        "director": "Roy Ward Baker",
        "studio": "Indicator",
        "film_year": 1967,
        "availability_status": "supplier_out",
        "ranking_bucket": "supplier_out",
        "genre_tags": "sci-fi horror",
        "awards_note": "",
        "fixture_notes": "indicator label OOS",
    },
    {
        "id": "oscar_parasite",
        "title": "Parasite 4K",
        "director": "Bong Joon Ho",
        "studio": "Curzon",
        "film_year": 2019,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "thriller drama",
        "awards_note": "oscar best picture best director",
        "fixture_notes": "awards discovery",
    },
    {
        "id": "oscar_moonlight",
        "title": "Moonlight Blu-Ray",
        "director": "Barry Jenkins",
        "studio": "A24",
        "film_year": 2016,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "drama",
        "awards_note": "oscar best picture",
        "fixture_notes": "awards",
    },
    {
        "id": "commerce_blade_runner_4k_oos",
        "title": "Blade Runner 2049 4K Ultra HD",
        "director": "Denis Villeneuve",
        "studio": "Warner Bros",
        "film_year": 2017,
        "availability_status": "supplier_out",
        "ranking_bucket": "supplier_out",
        "genre_tags": "sci-fi",
        "awards_note": "",
        "fixture_notes": "4k OOS commerce",
    },
    {
        "id": "commerce_blade_runner_bd_stock",
        "title": "Blade Runner 2049 Blu-Ray",
        "director": "Denis Villeneuve",
        "studio": "Warner Bros",
        "film_year": 2017,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "sci-fi",
        "awards_note": "",
        "fixture_notes": "same film in stock BD",
    },
    {
        "id": "best_edition_matrix_4k",
        "title": "The Matrix 4K Ultra HD Best Buy Steelbook",
        "director": "Lana Wachowski",
        "studio": "Warner Bros",
        "film_year": 1999,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "sci-fi action",
        "awards_note": "oscar technical",
        "fixture_notes": "best edition premium",
    },
    {
        "id": "matrix_dvd",
        "title": "The Matrix DVD",
        "director": "Lana Wachowski",
        "studio": "Warner Bros",
        "film_year": 1999,
        "availability_status": "store_stock",
        "ranking_bucket": "store_in_stock",
        "genre_tags": "sci-fi action",
        "awards_note": "",
        "fixture_notes": "budget edition",
    },
    {
        "id": "matrix_preorder_bd",
        "title": "The Matrix Blu-Ray",
        "director": "Lana Wachowski",
        "studio": "Warner Bros",
        "film_year": 1999,
        "availability_status": "preorder",
        "ranking_bucket": "preorder",
        "genre_tags": "sci-fi action",
        "awards_note": "",
        "fixture_notes": "commerce preorder same franchise",
    },
]


def rows_as_dict_list() -> List[Dict[str, Any]]:
    return [dict(r) for r in INTENT_FIXTURE_ROWS]
