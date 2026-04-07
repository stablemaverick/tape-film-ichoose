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

describe("intelligence-search offer dedupe (Shopify vs supplier)", () => {
  test("keeps in-stock and preorder Shopify; drops supplier with matching barcode", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-neon",
        title: "Neon City",
        director: "A",
        film_released: "2020-01-01",
        tmdb_title: "Neon City",
        genres: "Sci-Fi",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-neon", popularity_score: 0 },
    ];
    const future = new Date();
    future.setFullYear(future.getFullYear() + 1);
    const offers: OfferFixture[] = [
      {
        id: "o-shop-stock",
        title: "Neon City Blu-ray In Stock",
        format: "blu-ray",
        film_id: "f-neon",
        active: true,
        supplier_stock_status: 0,
        availability_status: "store_stock",
        shopify_variant_id: "var-stock",
        shopify_product_id: "prod-stock",
        barcode: "111111",
        calculated_sale_price: 25,
      },
      {
        id: "o-shop-pre",
        title: "Neon City 4K Preorder",
        format: "4k",
        film_id: "f-neon",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        media_release_date: future.toISOString().slice(0, 10),
        shopify_variant_id: "var-pre",
        shopify_product_id: "prod-pre",
        barcode: "222222",
        calculated_sale_price: 40,
      },
      {
        id: "o-supp-dup",
        title: "Neon City 4K Preorder Supplier",
        format: "4k",
        film_id: "f-neon",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        media_release_date: future.toISOString().slice(0, 10),
        barcode: "222222",
        calculated_sale_price: 10,
      },
    ];

    const db = createSequentialSupabaseMock([
      ...filmPopularityOffersSequence(films, popularity, offers),
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request("http://test/api/intelligence-search?q=neon+city");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as {
      films: {
        offers: OfferFixture[];
      }[];
    };

    assert.equal(body.films.length, 1);
    const ids = body.films[0].offers.map((o) => o.id).sort();
    assert.deepEqual(ids, ["o-shop-pre", "o-shop-stock"]);
    const shopifyRows = body.films[0].offers.filter(
      (o) => o.shopify_variant_id,
    );
    assert.equal(shopifyRows.length, 2);
  });

  test("two Shopify variants with same barcode both remain visible", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-dupbc",
        title: "Barcode Twins",
        director: "B",
        film_released: "2019-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-dupbc", popularity_score: 0 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o-a",
        title: "Barcode Twins Standard",
        format: "blu-ray",
        film_id: "f-dupbc",
        active: true,
        supplier_stock_status: 0,
        availability_status: "store_stock",
        shopify_variant_id: "var-a",
        barcode: "SHARED",
        calculated_sale_price: 20,
      },
      {
        id: "o-b",
        title: "Barcode Twins Deluxe Preorder",
        format: "4k",
        film_id: "f-dupbc",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        shopify_variant_id: "var-b",
        barcode: "SHARED",
        calculated_sale_price: 35,
        media_release_date: "2099-01-01",
      },
    ];

    const db = createSequentialSupabaseMock([
      ...filmPopularityOffersSequence(films, popularity, offers),
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request("http://test/api/intelligence-search?q=barcode+twins");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as {
      films: { offers: { id: string }[] }[];
    };

    assert.equal(body.films[0].offers.length, 2);
    const ids = new Set(body.films[0].offers.map((o) => o.id));
    assert.ok(ids.has("o-a"));
    assert.ok(ids.has("o-b"));
  });

  test("out-of-stock Shopify listing does not suppress in-stock supplier duplicate", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-hardboiled",
        title: "Hard Boiled",
        director: "John Woo",
        film_released: "1992-04-16",
        tmdb_title: "Hard Boiled",
        genres: "Action",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-hardboiled", popularity_score: 0 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o-shop-dead",
        title: "Hard Boiled Blu-ray (Shopify old listing)",
        format: "blu-ray",
        film_id: "f-hardboiled",
        active: true,
        supplier_stock_status: 0,
        availability_status: "store_out",
        shopify_variant_id: "var-dead",
        shopify_product_id: "prod-dead",
        barcode: "HB111",
        calculated_sale_price: 28,
      },
      {
        id: "o-supp-live",
        title: "Hard Boiled Blu-ray Supplier",
        format: "blu-ray",
        film_id: "f-hardboiled",
        active: true,
        supplier_stock_status: 125,
        availability_status: "supplier_stock",
        barcode: "HB111",
        calculated_sale_price: 24,
      },
    ];

    const db = createSequentialSupabaseMock([
      ...filmPopularityOffersSequence(films, popularity, offers),
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request("http://test/api/intelligence-search?q=hard+boiled");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as {
      films: {
        bestOffer: { id: string; rankingBucket: string } | null;
        offers: { id: string; rankingBucket: string }[];
      }[];
    };

    assert.equal(body.films.length, 1);
    const ids = new Set(body.films[0].offers.map((o) => o.id));
    assert.ok(ids.has("o-shop-dead"));
    assert.ok(ids.has("o-supp-live"));
    assert.equal(body.films[0].bestOffer?.id, "o-supp-live");
    assert.equal(body.films[0].bestOffer?.rankingBucket, "supplier_in_stock");
  });
});
