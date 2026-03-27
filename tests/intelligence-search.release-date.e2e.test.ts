import "./setup-intelligence-search-e2e-env.js";

/**
 * A7 — Supplier-backed media_release_date on offers (fixture-backed E2E).
 *
 * Plan:
 * 1) title= + q= title lookup returns bestOffer.media_release_date from catalog row.
 * 2) Future media_release_date → rankingBucket preorder (aligned with ranking module).
 */

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import type { FilmFixture, OfferFixture, PopularityFixture } from "./fixtures/intelligence-search.shared.js";
import { createSequentialSupabaseMock } from "./helpers/supabase-intelligence-search-mock.js";

describe("intelligence-search release date (A7) E2E", () => {
  test("bestOffer carries media_release_date from fixture; future date → preorder bucket", async () => {
    const release = "2026-11-20";
    const films: FilmFixture[] = [
      {
        id: "f-pos",
        title: "Possession",
        director: "Andrzej Zulawski",
        film_released: "1981-05-27",
        tmdb_title: "Possession",
        genres: "Horror,Drama",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [{ film_id: "f-pos", popularity_score: 1 }];
    const offers: OfferFixture[] = [
      {
        id: "o-pos-4k",
        title: "Possession 4K UHD",
        format: "4k",
        studio: "radiance",
        film_id: "f-pos",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        media_release_date: release,
        barcode: "brp",
      },
    ];

    const db = createSequentialSupabaseMock([
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request(
      `http://test/api/intelligence-search?q=possession&title=possession`,
    );
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as {
      films: {
        bestOffer: {
          media_release_date: string | null;
          rankingBucket: string;
        } | null;
      }[];
    };

    assert.equal(body.films.length, 1);
    const bo = body.films[0].bestOffer;
    assert.ok(bo);
    assert.equal(bo.media_release_date, release);
    assert.equal(bo.rankingBucket, "preorder");
  });

  test("past media_release_date with store_stock is not preorder", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-old",
        title: "Old Release",
        director: "X",
        film_released: "2010-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [{ film_id: "f-old", popularity_score: 0 }];
    const offers: OfferFixture[] = [
      {
        id: "o-old",
        title: "Old Release Blu",
        format: "blu-ray",
        film_id: "f-old",
        active: true,
        supplier_stock_status: 2,
        availability_status: "store_stock",
        media_release_date: "2010-06-01",
        barcode: "bold",
      },
    ];

    const db = createSequentialSupabaseMock([
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request(
      "http://test/api/intelligence-search?q=old%20release&title=old%20release",
    );
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as {
      films: { bestOffer: { rankingBucket: string; media_release_date: string } | null }[];
    };

    assert.equal(body.films[0].bestOffer?.rankingBucket, "store_in_stock");
    assert.equal(body.films[0].bestOffer?.media_release_date, "2010-06-01");
  });
});
