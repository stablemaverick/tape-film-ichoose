import assert from "node:assert/strict";
import { afterEach, beforeEach, test } from "node:test";

const filmFixture = {
  film: {
    id: "f1",
    title: "Possession",
    director: "Andrzej Zulawski",
    filmReleased: "1981-05-27",
    genres: "Horror",
    topCast: null,
  },
  offers: [
    {
      id: "o1",
      title: "Possession 4K",
      edition_title: null,
      format: "4k",
      studio: "radiance",
      supplier: "test",
      supplier_sku: "x",
      barcode: null,
      cost_price: 10,
      calculated_sale_price: 25,
      supplier_stock_status: 0,
      supplier_priority: 1,
      availability_status: "preorder",
      shopify_product_id: null,
      shopify_variant_id: null,
      media_release_date: "2026-06-15",
      rankingBucket: "preorder",
      explanation: [],
    },
  ],
};

test("agent action: release_date query gets release-style reply and structuredParse", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";

  const origFetch = globalThis.fetch;
  globalThis.fetch = async (input: RequestInfo | URL) => {
    const u = String(input);
    if (u.includes("/api/intelligence-search")) {
      return Response.json({ films: [filmFixture] });
    }
    return Response.json({ films: [] });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const req = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "when is possession released" }),
    });
    const res = await action({ request: req });
    assert.equal(res.status, 200);
    const body = (await res.json()) as {
      structuredParse: { primaryIntent: string };
      reply: string;
    };
    assert.equal(body.structuredParse.primaryIntent, "release_date");
    assert.match(body.reply, /media release date/i);
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});
