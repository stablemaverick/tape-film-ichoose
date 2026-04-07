import "./setup-intelligence-search-e2e-env.js";

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import type {
  FilmFixture,
  OfferFixture,
  PopularityFixture,
} from "./fixtures/intelligence-search.shared.js";
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

describe("intelligence-search exact short title (film-level)", () => {
  test('query "the killer" drops "The Killer Inside Me" when exact "The Killer" exists', async () => {
    const films: FilmFixture[] = [
      {
        id: "f-inside",
        title: "The Killer Inside Me",
        director: "Michael Winterbottom",
        film_released: "2010-01-01",
        tmdb_title: "The Killer Inside Me",
        genres: "Crime",
        top_cast: null,
      },
      {
        id: "f-killer",
        title: "The Killer",
        director: "John Woo",
        film_released: "1989-01-01",
        tmdb_title: "Die xue shuang xiong",
        genres: "Action",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-inside", popularity_score: 500 },
      { film_id: "f-killer", popularity_score: 1 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o-inside",
        title: "The Killer Inside Me Blu-ray",
        format: "blu-ray",
        film_id: "f-inside",
        active: true,
        supplier_stock_status: 1,
        availability_status: "supplier_stock",
        barcode: "b-inside",
      },
      {
        id: "o-killer",
        title: "The Killer Blu-ray",
        format: "blu-ray",
        film_id: "f-killer",
        active: true,
        supplier_stock_status: 1,
        availability_status: "supplier_stock",
        barcode: "b-killer",
      },
    ];

    const db = createSequentialSupabaseMock([
      ...filmPopularityOffersSequence(films, popularity, offers),
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request("http://test/api/intelligence-search?q=the+killer");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as {
      films: { film: { title: string } }[];
    };

    const titles = body.films.map((x) => x.film.title);
    assert.ok(
      titles.includes("The Killer"),
      `expected The Killer in results, got ${titles.join(", ")}`,
    );
    assert.ok(
      !titles.includes("The Killer Inside Me"),
      `did not expect The Killer Inside Me when exact title exists; got ${titles.join(", ")}`,
    );
    assert.equal(body.films[0].film.title, "The Killer");
  });

  test("keeps superstring title when no exact primary-title match in candidate set", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-inside-only",
        title: "The Killer Inside Me",
        director: "Michael Winterbottom",
        film_released: "2010-01-01",
        tmdb_title: "The Killer Inside Me",
        genres: "Crime",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-inside-only", popularity_score: 0 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o1",
        title: "The Killer Inside Me Blu-ray",
        format: "blu-ray",
        film_id: "f-inside-only",
        active: true,
        supplier_stock_status: 1,
        availability_status: "supplier_stock",
        barcode: "b1",
      },
    ];

    const db = createSequentialSupabaseMock([
      ...filmPopularityOffersSequence(films, popularity, offers),
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request("http://test/api/intelligence-search?q=the+killer");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as {
      films: { film: { title: string } }[];
    };

    assert.equal(body.films.length, 1);
    assert.equal(body.films[0].film.title, "The Killer Inside Me");
  });
});
