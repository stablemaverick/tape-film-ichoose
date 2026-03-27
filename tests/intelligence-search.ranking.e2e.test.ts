import "./setup-intelligence-search-e2e-env.js";

/**
 * A4 — Deterministic ranking (fixture-backed E2E against runIntelligenceSearch).
 *
 * Plan:
 * 1) Title search + equal popularity → film order follows scoreFilmMatch (exact title first).
 * 2) latest=true + two preorders → nearer media_release_date first (tie on bucket).
 * 3) latest=true + two in-stock releases → newer media_release_date first among same bucket.
 */

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import type { FilmFixture, OfferFixture, PopularityFixture } from "./fixtures/intelligence-search.shared.js";
import { createSequentialSupabaseMock } from "./helpers/supabase-intelligence-search-mock.js";

function filmPopularityOffersSequence(
  films: FilmFixture[],
  popularity: PopularityFixture[],
  offers: OfferFixture[],
) {
  return [
    { data: films, error: null },
    { data: popularity, error: null },
    { data: offers, error: null },
  ] as const;
}

describe("intelligence-search ranking (A4) E2E", () => {
  test("exact title ranks above weaker title match when popularity is tied", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-aliens",
        title: "Aliens",
        director: "James Cameron",
        film_released: "1986-07-18",
        tmdb_title: "Aliens",
        genres: "Action,Horror",
        top_cast: null,
      },
      {
        id: "f-alien",
        title: "Alien",
        director: "Ridley Scott",
        film_released: "1979-05-25",
        tmdb_title: "Alien",
        genres: "Horror",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-alien", popularity_score: 0 },
      { film_id: "f-aliens", popularity_score: 0 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o-alien",
        title: "Alien Blu-ray",
        format: "blu-ray",
        studio: "fox",
        film_id: "f-alien",
        active: true,
        supplier_stock_status: 0,
        availability_status: "supplier_out",
        barcode: "b1",
      },
      {
        id: "o-aliens",
        title: "Aliens Blu-ray",
        format: "blu-ray",
        studio: "fox",
        film_id: "f-aliens",
        active: true,
        supplier_stock_status: 0,
        availability_status: "supplier_out",
        barcode: "b2",
      },
    ];

    const db = createSequentialSupabaseMock([
      ...filmPopularityOffersSequence(films, popularity, offers),
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request(
      "http://test/api/intelligence-search?q=alien",
    );
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as {
      films: { film: { title: string }; score: number }[];
    };

    assert.equal(body.films.length, 2);
    assert.equal(body.films[0].film.title, "Alien");
    assert.equal(body.films[1].film.title, "Aliens");
    assert.ok(body.films[0].score >= body.films[1].score);
  });

  test("latest=true: nearer preorder (sooner media_release_date) before later preorder", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-late",
        title: "Late Street",
        director: "A",
        film_released: "2020-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
      {
        id: "f-soon",
        title: "Soon Street",
        director: "B",
        film_released: "2020-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-late", popularity_score: 0 },
      { film_id: "f-soon", popularity_score: 0 },
    ];
    const far = new Date();
    far.setFullYear(far.getFullYear() + 2);
    const near = new Date();
    near.setFullYear(near.getFullYear() + 1);
    const offers: OfferFixture[] = [
      {
        id: "o-late",
        title: "Late preorder",
        format: "4k",
        film_id: "f-late",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        media_release_date: far.toISOString().slice(0, 10),
        barcode: "bl",
      },
      {
        id: "o-soon",
        title: "Soon preorder",
        format: "4k",
        film_id: "f-soon",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        media_release_date: near.toISOString().slice(0, 10),
        barcode: "bs",
      },
    ];

    const db = createSequentialSupabaseMock([
      ...filmPopularityOffersSequence(films, popularity, offers),
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request(
      "http://test/api/intelligence-search?q=street&latest=true",
    );
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };

    assert.equal(body.films[0].film.title, "Soon Street");
    assert.equal(body.films[1].film.title, "Late Street");
  });

  test("latest=true: newer released media_release_date before older when buckets match", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-old",
        title: "Old Disc",
        director: "A",
        film_released: "2015-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
      {
        id: "f-new",
        title: "New Disc",
        director: "B",
        film_released: "2024-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-old", popularity_score: 0 },
      { film_id: "f-new", popularity_score: 0 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o-old",
        title: "Old Disc Blu",
        format: "blu-ray",
        film_id: "f-old",
        active: true,
        supplier_stock_status: 5,
        availability_status: "supplier_stock",
        media_release_date: "2015-03-01",
        barcode: "bo",
      },
      {
        id: "o-new",
        title: "New Disc Blu",
        format: "blu-ray",
        film_id: "f-new",
        active: true,
        supplier_stock_status: 5,
        availability_status: "supplier_stock",
        media_release_date: "2024-03-01",
        barcode: "bn",
      },
    ];

    const db = createSequentialSupabaseMock([
      ...filmPopularityOffersSequence(films, popularity, offers),
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request(
      "http://test/api/intelligence-search?q=disc&latest=true",
    );
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };

    assert.equal(body.films[0].film.title, "New Disc");
    assert.equal(body.films[1].film.title, "Old Disc");
  });
});
