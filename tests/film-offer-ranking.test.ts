import assert from "node:assert/strict";
import { describe, test } from "node:test";
import {
  getOfferRankingBucket,
  sortFilmsWithOffersFinal,
  type RankingFilmOfferItem,
} from "../app/lib/film-offer-ranking.server";

describe("getOfferRankingBucket", () => {
  test("Shopify ids alone do not imply store in-stock", () => {
    assert.equal(
      getOfferRankingBucket({
        shopify_variant_id: "x",
        availability_status: null,
        supplier_stock_status: 0,
      }),
      "out_of_stock",
    );
  });

  test("explicit store_stock wins over Shopify", () => {
    assert.equal(
      getOfferRankingBucket({
        shopify_variant_id: "x",
        availability_status: "store_stock",
        supplier_stock_status: 0,
      }),
      "store_in_stock",
    );
  });

  test("future media_release_date is preorder", () => {
    const far = new Date();
    far.setFullYear(far.getFullYear() + 1);
    assert.equal(
      getOfferRankingBucket({
        media_release_date: far.toISOString().slice(0, 10),
        availability_status: "supplier_stock",
        supplier_stock_status: 5,
      }),
      "preorder",
    );
  });

  test("supplier_stock status maps to supplier_in_stock", () => {
    assert.equal(
      getOfferRankingBucket({
        availability_status: "supplier_stock",
        supplier_stock_status: 0,
      }),
      "supplier_in_stock",
    );
  });

  test("future non-ISO media_release_date (dd/mm/yyyy) is preorder", () => {
    const far = new Date();
    far.setFullYear(far.getFullYear() + 1);
    const dd = String(far.getDate()).padStart(2, "0");
    const mm = String(far.getMonth() + 1).padStart(2, "0");
    const yyyy = String(far.getFullYear());
    const dmy = `${dd}/${mm}/${yyyy}`;

    assert.equal(
      getOfferRankingBucket({
        media_release_date: dmy,
        availability_status: "supplier_out",
        supplier_stock_status: 0,
      }),
      "preorder",
    );
  });

  test("past/current supplier availability remains available-to-order bucket", () => {
    assert.equal(
      getOfferRankingBucket({
        media_release_date: "2023-05-01",
        availability_status: "supplier_stock",
        supplier_stock_status: 4,
      }),
      "supplier_in_stock",
    );
  });

  test("no stock and no future release date stays out_of_stock", () => {
    assert.equal(
      getOfferRankingBucket({
        media_release_date: "2020-01-01",
        availability_status: "supplier_out",
        supplier_stock_status: 0,
      }),
      "out_of_stock",
    );
  });
});

describe("sortFilmsWithOffersFinal", () => {
  test("latest: nearer preorder before later preorder, then popularity", () => {
    const near = new Date();
    near.setDate(near.getDate() + 7);
    const far = new Date();
    far.setFullYear(far.getFullYear() + 1);

    const a: RankingFilmOfferItem = {
      film: { id: "a" },
      score: 10,
      popularity: { popularity_score: 1 },
      bestOffer: {
        rankingBucket: "preorder",
        media_release_date: far.toISOString().slice(0, 10),
      },
    };
    const b: RankingFilmOfferItem = {
      film: { id: "b" },
      score: 5,
      popularity: { popularity_score: 100 },
      bestOffer: {
        rankingBucket: "preorder",
        media_release_date: near.toISOString().slice(0, 10),
      },
    };

    const out = sortFilmsWithOffersFinal([a, b], true);
    assert.equal(out[0].film.id, "b");
    assert.equal(out[1].film.id, "a");
  });

  test("non-latest: higher popularity first", () => {
    const a: RankingFilmOfferItem = {
      film: { id: "a" },
      score: 10,
      popularity: { popularity_score: 1 },
      bestOffer: { rankingBucket: "out_of_stock" },
    };
    const b: RankingFilmOfferItem = {
      film: { id: "b" },
      score: 5,
      popularity: { popularity_score: 50 },
      bestOffer: { rankingBucket: "out_of_stock" },
    };
    const out = sortFilmsWithOffersFinal([a, b], false);
    assert.equal(out[0].film.id, "b");
  });
});
