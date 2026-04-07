import assert from "node:assert/strict";
import { describe, test } from "node:test";
import {
  buildOfferRareTokenDf,
  looseTokens,
  mediaRetrievalRankAdjustment,
} from "../app/lib/media-offer-ranking-tweaks.server";

describe("mediaRetrievalRankAdjustment", () => {
  test("Ghost in the Shell: SAC query demotes 1995 film SKU without SAC/2045 cues", () => {
    const q = "ghost in the shell sac 2045";
    const df = new Map<string, number>();
    const film1995 = {
      title: "Ghost in the Shell (1995) Blu-ray",
      edition_title: "",
    };
    const sac = {
      title: "Ghost in the Shell SAC_2045 Blu-ray",
      edition_title: "",
    };
    assert.ok(
      mediaRetrievalRankAdjustment(q, film1995, df) >
        mediaRetrievalRankAdjustment(q, sac, df),
      "1995 row should have higher (worse) adjustment than SAC row",
    );
    assert.ok(
      mediaRetrievalRankAdjustment(q, film1995, df) >= 0.25,
      "expect franchise + optional year demotion",
    );
  });

  test("parenthetical year in title demotes when query omits that year", () => {
    const q = "blade runner";
    const df = new Map<string, number>();
    const withYear = { title: "Blade Runner (1982) 4K", edition_title: "" };
    const plain = { title: "Blade Runner 4K Final Cut", edition_title: "" };
    assert.ok(
      mediaRetrievalRankAdjustment(q, withYear, df) >
        mediaRetrievalRankAdjustment(q, plain, df),
    );
  });

  test("query includes year — no parenthetical-year demotion for matching year", () => {
    const q = "blade runner 1982";
    const df = new Map<string, number>();
    const withYear = { title: "Blade Runner (1982) 4K", edition_title: "" };
    assert.ok(mediaRetrievalRankAdjustment(q, withYear, df) < 0.15);
  });

  test("season agreement promotes matching SKU", () => {
    const q = "breaking bad season 1";
    const df = new Map<string, number>();
    const s1 = { title: "Breaking Bad Season 1 Blu-ray", edition_title: "" };
    const plain = { title: "Breaking Bad Complete Series", edition_title: "" };
    assert.ok(
      mediaRetrievalRankAdjustment(q, s1, df) <
        mediaRetrievalRankAdjustment(q, plain, df),
    );
  });

  test("rare overlapping token promotes within corpus (df<=2)", () => {
    const q = "the wombles movie";
    const offers = [
      { title: "The Wombles Movie DVD", edition_title: "" },
      { title: "Generic Family DVD", edition_title: "" },
    ];
    const df = buildOfferRareTokenDf(offers);
    const w = mediaRetrievalRankAdjustment(q, offers[0], df);
    const g = mediaRetrievalRankAdjustment(q, offers[1], df);
    assert.ok(w < g, "wombles token should be rarer and match first row only");
  });
});

describe("looseTokens", () => {
  test("splits underscores", () => {
    assert.deepEqual(looseTokens("ghost sac_2045"), [
      "ghost",
      "sac",
      "2045",
    ]);
  });
});
