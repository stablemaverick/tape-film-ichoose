import assert from "node:assert/strict";
import { test } from "node:test";
import {
  getAvailabilityMode,
  parseTapeAgentQueryDeterministic,
} from "../app/lib/tape-agent-query-parser.server";

test("parser: is alien available is title-anchored (franchise cleared, title kept)", () => {
  const p = parseTapeAgentQueryDeterministic("is alien available");
  assert.equal(p.primaryIntent, "availability");
  assert.equal(p.facets.title, "alien");
  assert.equal(p.facets.franchise, undefined);
  assert.equal(p.facets.availabilityBrowse, undefined);
  assert.equal(getAvailabilityMode(p.primaryIntent, p.facets), "title_anchored");
});

test("parser: criterion studio browse + availability", () => {
  const p = parseTapeAgentQueryDeterministic(
    "what criterion films do you have in stock",
  );
  assert.equal(p.primaryIntent, "availability");
  assert.equal(p.facets.studio, "criterion");
  assert.equal(p.facets.availabilityBrowse, true);
  assert.equal(p.residualQuery, "");
  assert.equal(getAvailabilityMode(p.primaryIntent, p.facets), "browse");
});

test("parser: arrow titles browse + availability", () => {
  const p = parseTapeAgentQueryDeterministic("what arrow titles are available");
  assert.equal(p.primaryIntent, "availability");
  assert.equal(p.facets.studio, "arrow");
  assert.equal(p.facets.availabilityBrowse, true);
  assert.equal(getAvailabilityMode(p.primaryIntent, p.facets), "browse");
});

test("parser: horror genre browse + availability", () => {
  const p = parseTapeAgentQueryDeterministic(
    "what horror films are available now",
  );
  assert.equal(p.primaryIntent, "availability");
  assert.equal(p.facets.genre, "Horror");
  assert.equal(p.facets.availabilityBrowse, true);
  assert.equal(getAvailabilityMode(p.primaryIntent, p.facets), "browse");
});

test("parser: title-anchored vs browse use different narrowing (alien vs criterion browse)", () => {
  const title = parseTapeAgentQueryDeterministic("is alien available");
  const browse = parseTapeAgentQueryDeterministic("criterion in stock");
  assert.equal(getAvailabilityMode(title.primaryIntent, title.facets), "title_anchored");
  assert.equal(getAvailabilityMode(browse.primaryIntent, browse.facets), "browse");
  assert.ok(title.facets.title);
  assert.equal(browse.facets.title, undefined);
});

test("agent: browse availability uses empty q and does not force studio into q", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";

  const urls: string[] = [];
  const origFetch = globalThis.fetch;
  globalThis.fetch = async (input: RequestInfo | URL) => {
    urls.push(String(input));
    return Response.json({
      films: [
        {
          film: {
            id: "f1",
            title: "Test Film",
            director: null,
            filmReleased: null,
            genres: null,
            topCast: null,
          },
          offers: [
            {
              id: "o1",
              title: "Test",
              format: "blu-ray",
              studio: "criterion",
              availability_status: "store_stock",
              rankingBucket: "store_in_stock",
              supplier_stock_status: 0,
              shopify_variant_id: null,
              shopify_product_id: null,
              calculated_sale_price: 20,
              cost_price: 10,
              supplier_sku: "x",
              media_release_date: null,
            },
          ],
        },
      ],
    });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const req = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: "what criterion films do you have in stock",
      }),
    });
    const res = await action({ request: req });
    assert.equal(res.status, 200);
    const intelUrls = urls.filter((u) => u.includes("/api/intelligence-search"));
    assert.equal(intelUrls.length, 1);
    const u = new URL(intelUrls[0]!);
    assert.equal(u.searchParams.get("q"), "");
    assert.equal(u.searchParams.get("studio"), "criterion");
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});

test("agent: title-anchored availability passes title and omits franchise param", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";

  const urls: string[] = [];
  const origFetch = globalThis.fetch;
  globalThis.fetch = async (input: RequestInfo | URL) => {
    urls.push(String(input));
    return Response.json({
      films: [
        {
          film: {
            id: "f1",
            title: "Alien",
            director: "Ridley Scott",
            filmReleased: "1979-05-25",
            genres: "Science Fiction",
            topCast: null,
          },
          offers: [
            {
              id: "o1",
              title: "Alien 4K",
              format: "4k",
              studio: "disney",
              availability_status: "store_stock",
              rankingBucket: "store_in_stock",
              supplier_stock_status: 0,
              shopify_variant_id: null,
              shopify_product_id: null,
              calculated_sale_price: 25,
              cost_price: 10,
              supplier_sku: "a1",
              media_release_date: null,
            },
          ],
        },
        {
          film: {
            id: "f2",
            title: "Some Other Film",
            director: null,
            filmReleased: null,
            genres: null,
            topCast: null,
          },
          offers: [
            {
              id: "o2",
              title: "Other",
              format: "blu-ray",
              studio: "arrow",
              availability_status: "store_stock",
              rankingBucket: "store_in_stock",
              supplier_stock_status: 0,
              shopify_variant_id: null,
              shopify_product_id: null,
              calculated_sale_price: 15,
              cost_price: 8,
              supplier_sku: "b2",
              media_release_date: null,
            },
          ],
        },
      ],
    });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const req = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "is alien available" }),
    });
    const res = await action({ request: req });
    assert.equal(res.status, 200);
    const body = (await res.json()) as {
      structuredParse: { availabilityMode: string };
      recommendedOption: { filmTitle: string } | null;
    };
    assert.equal(body.structuredParse.availabilityMode, "title_anchored");
    assert.equal(body.recommendedOption?.filmTitle, "Alien");
    const intelUrls = urls.filter((u) => u.includes("/api/intelligence-search"));
    assert.equal(intelUrls.length, 1);
    const u = new URL(intelUrls[0]!);
    assert.equal(u.searchParams.get("title"), "alien");
    assert.equal(u.searchParams.get("franchise"), null);
    assert.equal(u.searchParams.get("q"), "alien");
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});
