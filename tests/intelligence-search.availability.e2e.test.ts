import "./setup-intelligence-search-e2e-env.js";

/**
 * A6 — Availability semantics on returned offers (fixture-backed E2E).
 *
 * Plan:
 * 1) Shopify variant id without availability_status=store_stock → rankingBucket out_of_stock.
 * 2) Explicit store_stock → store_in_stock and becomes bestOffer when competing with (1).
 * 3) supplier_stock status with qty → supplier_in_stock, ranks below store.
 */

import assert from "node:assert/strict";
import { describe, test } from "node:test";
import type { FilmFixture, OfferFixture, PopularityFixture } from "./fixtures/intelligence-search.shared.js";
import { createSequentialSupabaseMock } from "./helpers/supabase-intelligence-search-mock.js";

describe("intelligence-search availability (A6) E2E", () => {
  test("Shopify-linked offer without store_stock does not get store_in_stock; explicit store_stock wins bestOffer", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-one",
        title: "Test Film",
        director: "Director",
        film_released: "2020-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-one", popularity_score: 0 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o-shopify-only",
        title: "Listed on Shopify",
        format: "4k",
        film_id: "f-one",
        active: true,
        supplier_stock_status: 0,
        availability_status: null,
        shopify_product_id: "gid://shopify/Product/1",
        shopify_variant_id: "gid://shopify/ProductVariant/1",
        barcode: "bs",
      },
      {
        id: "o-store",
        title: "Store copy",
        format: "4k",
        film_id: "f-one",
        active: true,
        supplier_stock_status: 3,
        availability_status: "store_stock",
        shopify_product_id: null,
        shopify_variant_id: null,
        barcode: "bst",
      },
      {
        id: "o-supplier",
        title: "Supplier copy",
        format: "4k",
        film_id: "f-one",
        active: true,
        supplier_stock_status: 10,
        availability_status: "supplier_stock",
        barcode: "bsu",
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
    const req = new Request("http://test/api/intelligence-search?q=test%20film");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as {
      films: {
        bestOffer: { id: string; rankingBucket: string } | null;
        offers: { id: string; rankingBucket: string }[];
      }[];
    };

    assert.equal(body.films.length, 1);
    const row = body.films[0];
    assert.equal(row.bestOffer?.id, "o-store");
    assert.equal(row.bestOffer?.rankingBucket, "store_in_stock");

    const shopifyOnly = row.offers.find((o) => o.id === "o-shopify-only");
    assert.equal(shopifyOnly?.rankingBucket, "out_of_stock");

    const supplier = row.offers.find((o) => o.id === "o-supplier");
    assert.equal(supplier?.rankingBucket, "supplier_in_stock");
  });
});
