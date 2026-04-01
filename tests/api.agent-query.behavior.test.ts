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

test("agent exposes third offer per film (Shopify preorder) — not truncated at 2", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";

  const killerFilm = {
    film: {
      id: "film-killer",
      title: "The Killer",
      director: "John Woo",
      filmReleased: "1989-01-01",
      genres: "Action",
      topCast: null,
    },
    offers: [
      {
        id: "o-shop-in-stock",
        title: "The Killer Blu-ray",
        format: "blu-ray",
        studio: "test",
        supplier_sku: "s1",
        barcode: "111",
        cost_price: 5,
        calculated_sale_price: 22,
        supplier_stock_status: 0,
        availability_status: "store_stock",
        shopify_variant_id: "var-stock",
        shopify_product_id: "prod-1",
        media_release_date: null,
        rankingBucket: "store_in_stock",
      },
      {
        id: "o-supplier-alt",
        title: "The Killer Blu-ray Supplier",
        format: "blu-ray",
        studio: "test",
        supplier_sku: "s2",
        barcode: "222",
        cost_price: 4,
        calculated_sale_price: 18,
        supplier_stock_status: 3,
        availability_status: "supplier_stock",
        shopify_variant_id: null,
        shopify_product_id: null,
        media_release_date: null,
        rankingBucket: "supplier_in_stock",
      },
      {
        id: "o-shop-preorder",
        title: "The Killer 4K Preorder",
        format: "4k",
        studio: "test",
        supplier_sku: "s3",
        barcode: "333",
        cost_price: 8,
        calculated_sale_price: 40,
        supplier_stock_status: 0,
        availability_status: "preorder",
        shopify_variant_id: "var-pre",
        shopify_product_id: "prod-2",
        media_release_date: "2099-06-01",
        rankingBucket: "preorder",
      },
    ],
  };

  const origFetch = globalThis.fetch;
  globalThis.fetch = async (input: RequestInfo | URL) => {
    const u = String(input);
    if (u.includes("/api/intelligence-search")) {
      return Response.json({ films: [killerFilm] });
    }
    return Response.json({ films: [] });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const req = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "the killer" }),
    });
    const res = await action({ request: req });
    assert.equal(res.status, 200);
    const body = (await res.json()) as { options: { catalogItemId: string }[] };
    const ids = body.options.map((o) => o.catalogItemId);
    assert.ok(
      ids.includes("o-shop-preorder"),
      `expected Shopify preorder in options, got: ${ids.join(",")}`,
    );
    assert.ok(ids.includes("o-shop-in-stock"));
    assert.ok(ids.includes("o-supplier-alt"));
    assert.equal(ids.length, 3);
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});

test("blank guided browse: new_releases vs preorders are differentiated", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";

  const mixedBrowseFilm = {
    film: {
      id: "film-browse",
      title: "Browse Mix",
      director: "X",
      filmReleased: "2000-01-01",
      genres: "Drama",
      topCast: null,
    },
    offers: [
      {
        id: "o-pre",
        title: "Browse Mix 4K Pre",
        format: "4k",
        studio: "arrow",
        supplier_sku: "bp1",
        barcode: "p1",
        cost_price: 10,
        calculated_sale_price: 30,
        supplier_stock_status: 0,
        availability_status: "supplier_out",
        media_release_date: "2099-05-25",
        rankingBucket: "preorder",
      },
      {
        id: "o-stock",
        title: "Browse Mix Blu Stock",
        format: "blu-ray",
        studio: "arrow",
        supplier_sku: "bs1",
        barcode: "s1",
        cost_price: 8,
        calculated_sale_price: 20,
        supplier_stock_status: 12,
        availability_status: "supplier_stock",
        media_release_date: "2024-05-25",
        rankingBucket: "supplier_in_stock",
      },
    ],
  };
  const stockBrowseFilm = {
    film: {
      id: "film-stock",
      title: "Browse Stock",
      director: "Y",
      filmReleased: "2001-01-01",
      genres: "Drama",
      topCast: null,
    },
    offers: [
      {
        id: "o-stock-2",
        title: "Browse Stock Blu",
        format: "blu-ray",
        studio: "arrow",
        supplier_sku: "bs2",
        barcode: "s2",
        cost_price: 7,
        calculated_sale_price: 19,
        supplier_stock_status: 9,
        availability_status: "supplier_stock",
        media_release_date: "2024-05-20",
        rankingBucket: "supplier_in_stock",
      },
    ],
  };

  const origFetch = globalThis.fetch;
  globalThis.fetch = async (input: RequestInfo | URL) => {
    const u = String(input);
    if (u.includes("/api/intelligence-search")) {
      return Response.json({ films: [mixedBrowseFilm, stockBrowseFilm] });
    }
    return Response.json({ films: [] });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");

    const newReq = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "", intentMode: "new_releases" }),
    });
    const newRes = await action({ request: newReq });
    assert.equal(newRes.status, 200);
    const newBody = (await newRes.json()) as { options: { id: string }[] };
    assert.ok(newBody.options.some((o) => o.id === "o-pre"));
    assert.ok(newBody.options.some((o) => o.id === "o-stock-2"));

    const preReq = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "", intentMode: "preorders" }),
    });
    const preRes = await action({ request: preReq });
    assert.equal(preRes.status, 200);
    const preBody = (await preRes.json()) as { options: { id: string }[] };
    assert.deepEqual(preBody.options.map((o) => o.id), ["o-pre"]);
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});

test("future-dated supplier/catalog row is exposed as preorder availability for UI", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";

  const futureFilm = {
    film: {
      id: "film-future",
      title: "The Devil's Candy",
      director: "Sean Byrne",
      filmReleased: "2015-01-01",
      genres: "Horror",
      topCast: null,
    },
    offers: [
      {
        id: "o-devils-candy",
        title: "The Devil's Candy 4K Ultra HD",
        format: "4k",
        studio: "arrow",
        supplier_sku: "dc4k",
        barcode: "dc1",
        cost_price: 12,
        calculated_sale_price: 34,
        supplier_stock_status: 0,
        availability_status: "supplier_out",
        media_release_date: "2026-05-25",
        rankingBucket: "preorder",
      },
    ],
  };

  const origFetch = globalThis.fetch;
  globalThis.fetch = async (input: RequestInfo | URL) => {
    const u = String(input);
    if (u.includes("/api/intelligence-search")) {
      return Response.json({ films: [futureFilm] });
    }
    return Response.json({ films: [] });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const req = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "devil's candy 4k" }),
    });
    const res = await action({ request: req });
    assert.equal(res.status, 200);
    const body = (await res.json()) as {
      options: { id: string; availability: string | null; rankingBucket: string | null }[];
    };
    const row = body.options.find((o) => o.id === "o-devils-candy");
    assert.ok(row);
    assert.equal(row?.rankingBucket, "preorder");
    assert.equal(row?.availability, "preorder");
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});

test("browse modes return multi-film list without single recommended collapse", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";

  const browseFilms = [
    {
      film: {
        id: "film-a",
        title: "Film A",
        director: "Dir A",
        filmReleased: "1990-01-01",
        genres: "Action",
        topCast: null,
      },
      offers: [
        {
          id: "o-a-1",
          title: "Film A 4K",
          format: "4k",
          studio: "arrow",
          supplier_sku: "a1",
          barcode: "a1",
          cost_price: 10,
          calculated_sale_price: 30,
          supplier_stock_status: 1,
          availability_status: "supplier_stock",
          media_release_date: "2025-01-01",
          rankingBucket: "supplier_in_stock",
        },
        {
          id: "o-a-2",
          title: "Film A Blu",
          format: "blu-ray",
          studio: "arrow",
          supplier_sku: "a2",
          barcode: "a2",
          cost_price: 8,
          calculated_sale_price: 24,
          supplier_stock_status: 2,
          availability_status: "supplier_stock",
          media_release_date: "2025-01-01",
          rankingBucket: "supplier_in_stock",
        },
      ],
      bestOffer: {
        id: "o-a-1",
        title: "Film A 4K",
        format: "4k",
        studio: "arrow",
        supplier_sku: "a1",
        barcode: "a1",
        cost_price: 10,
        calculated_sale_price: 30,
        supplier_stock_status: 1,
        availability_status: "supplier_stock",
        media_release_date: "2025-01-01",
        rankingBucket: "supplier_in_stock",
      },
    },
    {
      film: {
        id: "film-b",
        title: "Film B",
        director: "Dir B",
        filmReleased: "1991-01-01",
        genres: "Action",
        topCast: null,
      },
      offers: [
        {
          id: "o-b-1",
          title: "Film B 4K",
          format: "4k",
          studio: "arrow",
          supplier_sku: "b1",
          barcode: "b1",
          cost_price: 11,
          calculated_sale_price: 31,
          supplier_stock_status: 1,
          availability_status: "supplier_stock",
          media_release_date: "2025-01-02",
          rankingBucket: "supplier_in_stock",
        },
      ],
      bestOffer: {
        id: "o-b-1",
        title: "Film B 4K",
        format: "4k",
        studio: "arrow",
        supplier_sku: "b1",
        barcode: "b1",
        cost_price: 11,
        calculated_sale_price: 31,
        supplier_stock_status: 1,
        availability_status: "supplier_stock",
        media_release_date: "2025-01-02",
        rankingBucket: "supplier_in_stock",
      },
    },
  ];

  const origFetch = globalThis.fetch;
  globalThis.fetch = async (input: RequestInfo | URL) => {
    const u = String(input);
    if (u.includes("/api/intelligence-search")) {
      return Response.json({ films: browseFilms });
    }
    return Response.json({ films: [] });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const req = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "arrow", intentMode: "label_studio" }),
    });
    const res = await action({ request: req });
    assert.equal(res.status, 200);
    const body = (await res.json()) as {
      recommendedOption: unknown;
      options: { filmId: string; id: string }[];
    };
    assert.equal(body.recommendedOption, null);
    assert.equal(body.options.length, 2);
    assert.deepEqual(
      body.options.map((o) => o.filmId),
      ["film-a", "film-b"],
    );
    assert.deepEqual(
      body.options.map((o) => o.id),
      ["o-a-1", "o-b-1"],
    );
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});

test("director mode ranks exact director matches ahead of commercial-only noise", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";
  const origFetch = globalThis.fetch;

  const films = [
    {
      film: { id: "f1", title: "Witness", director: "Peter Weir", filmReleased: "1985-01-01", genres: "Drama", topCast: null },
      bestOffer: { rankingBucket: "supplier_in_stock", media_release_date: "2025-01-10" },
      offers: [
        {
          id: "o-witness",
          title: "Witness Blu",
          format: "blu-ray",
          studio: "paramount",
          supplier_sku: "w1",
          barcode: "w1",
          cost_price: 8,
          calculated_sale_price: 22,
          supplier_stock_status: 10,
          availability_status: "supplier_stock",
          media_release_date: "2025-01-10",
          rankingBucket: "supplier_in_stock",
        },
      ],
    },
    {
      film: { id: "f2", title: "Dead Poets Society", director: "Peter Weir", filmReleased: "1989-01-01", genres: "Drama", topCast: null },
      bestOffer: { rankingBucket: "preorder", media_release_date: "2099-05-01" },
      offers: [
        {
          id: "o-dps",
          title: "Dead Poets Society 4K",
          format: "4k",
          studio: "disney",
          supplier_sku: "d1",
          barcode: "d1",
          cost_price: 12,
          calculated_sale_price: 35,
          supplier_stock_status: 0,
          availability_status: "preorder",
          media_release_date: "2099-05-01",
          rankingBucket: "preorder",
        },
      ],
    },
  ];

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const u = String(input);
    if (u.includes("/api/intelligence-search")) return Response.json({ films });
    return Response.json({ films: [] });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const req = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "peter weir", intentMode: "director" }),
    });
    const res = await action({ request: req });
    const body = (await res.json()) as { options: { filmTitle: string }[]; recommendedOption: unknown };
    assert.equal(res.status, 200);
    assert.equal(body.recommendedOption, null);
    assert.equal(body.options[0]?.filmTitle, "Witness");
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});

test("in_stock mode supports person/genre-like queries (john woo / horror)", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";
  const origFetch = globalThis.fetch;
  const urls: string[] = [];

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const u = String(input);
    urls.push(u);
    return Response.json({
      films: [
        {
          film: { id: "fwoo", title: "Hard Boiled", director: "John Woo", filmReleased: "1992-01-01", genres: "Action", topCast: null },
          offers: [
            {
              id: "o-woo",
              title: "Hard Boiled Blu",
              format: "blu-ray",
              studio: "arrow",
              supplier_sku: "hw1",
              barcode: "hw1",
              cost_price: 9,
              calculated_sale_price: 26,
              supplier_stock_status: 120,
              availability_status: "supplier_stock",
              media_release_date: "2024-03-01",
              rankingBucket: "supplier_in_stock",
            },
          ],
        },
        {
          film: { id: "fhorror", title: "The Blob", director: "Chuck Russell", filmReleased: "1988-01-01", genres: "Horror", topCast: null },
          offers: [
            {
              id: "o-horror",
              title: "The Blob Blu",
              format: "blu-ray",
              studio: "scream factory",
              supplier_sku: "hb1",
              barcode: "hb1",
              cost_price: 6,
              calculated_sale_price: 18,
              supplier_stock_status: 5,
              availability_status: "supplier_stock",
              media_release_date: "2024-02-01",
              rankingBucket: "supplier_in_stock",
            },
          ],
        },
      ],
    });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const reqA = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "john woo", intentMode: "in_stock" }),
    });
    const resA = await action({ request: reqA });
    const bodyA = (await resA.json()) as { options: { filmTitle: string }[] };
    assert.equal(resA.status, 200);
    assert.ok(bodyA.options.length >= 1);
    assert.ok(bodyA.options.some((o) => o.filmTitle === "Hard Boiled"));
    const intelA = urls.find((u) => u.includes("/api/intelligence-search"));
    assert.ok(intelA);
    assert.equal(new URL(intelA!).searchParams.get("title"), null);

    urls.length = 0;
    const reqB = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "horror", intentMode: "in_stock" }),
    });
    const resB = await action({ request: reqB });
    const bodyB = (await resB.json()) as { options: unknown[] };
    assert.equal(resB.status, 200);
    assert.ok(bodyB.options.length >= 1);
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});

test("director mode: neil jordan prefers exact director title over unrelated high-popularity title", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";
  const origFetch = globalThis.fetch;

  const films = [
    {
      film: { id: "f-jordan", title: "The Crying Game", director: "Neil Jordan", filmReleased: "1992-01-01", genres: "Drama", topCast: null },
      bestOffer: { rankingBucket: "supplier_in_stock", media_release_date: "2024-10-10" },
      popularity: { popularity_score: 1 },
      offers: [
        {
          id: "o-jordan",
          title: "The Crying Game Blu",
          format: "blu-ray",
          studio: "criterion",
          supplier_sku: "cj1",
          barcode: "cj1",
          cost_price: 8,
          calculated_sale_price: 24,
          supplier_stock_status: 6,
          availability_status: "supplier_stock",
          media_release_date: "2024-10-10",
          rankingBucket: "supplier_in_stock",
        },
      ],
    },
    {
      film: { id: "f-high", title: "High Spirits", director: "Someone Else", filmReleased: "1988-01-01", genres: "Comedy", topCast: null },
      bestOffer: { rankingBucket: "store_in_stock", media_release_date: "2025-02-01" },
      popularity: { popularity_score: 999 },
      offers: [
        {
          id: "o-high",
          title: "High Spirits 4K",
          format: "4k",
          studio: "arrow",
          supplier_sku: "hs1",
          barcode: "hs1",
          cost_price: 12,
          calculated_sale_price: 34,
          supplier_stock_status: 0,
          availability_status: "store_stock",
          media_release_date: "2025-02-01",
          rankingBucket: "store_in_stock",
        },
      ],
    },
  ];

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const u = String(input);
    if (u.includes("/api/intelligence-search")) return Response.json({ films });
    return Response.json({ films: [] });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const req = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "neil jordan", intentMode: "director" }),
    });
    const res = await action({ request: req });
    const body = (await res.json()) as { options: { filmTitle: string }[] };
    assert.equal(res.status, 200);
    assert.equal(body.options[0]?.filmTitle, "The Crying Game");
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});

test("upcoming releases + second sight includes future supplier_stock/variant offers", async () => {
  const prevKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";
  const origFetch = globalThis.fetch;

  const films = [
    {
      film: { id: "f-ss", title: "Second Sight Future", director: "X", filmReleased: "2000-01-01", genres: "Horror", topCast: null },
      offers: [
        {
          id: "o-ss-current",
          title: "Second Sight Current",
          format: "blu-ray",
          studio: "Second Sight Films",
          supplier: "Second Sight Limited",
          supplier_sku: "ss1",
          barcode: "ss1",
          cost_price: 8,
          calculated_sale_price: 22,
          supplier_stock_status: 5,
          availability_status: "supplier_stock",
          media_release_date: "2024-01-01",
          rankingBucket: "supplier_in_stock",
        },
        {
          id: "o-ss-future",
          title: "Second Sight Future",
          format: "4k",
          studio: "Second Sight Films",
          supplier: "Second Sight Limited",
          supplier_sku: "ss2",
          barcode: "ss2",
          cost_price: 12,
          calculated_sale_price: 34,
          supplier_stock_status: 3,
          availability_status: "supplier_stock",
          media_release_date: "2099-05-11",
          rankingBucket: "preorder",
        },
      ],
    },
  ];

  globalThis.fetch = async (input: RequestInfo | URL) => {
    const u = String(input);
    if (u.includes("/api/intelligence-search")) return Response.json({ films });
    return Response.json({ films: [] });
  };

  try {
    const { action } = await import("../app/routes/api.agent-query.js");
    const req = new Request("http://local.test/api/agent-query", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: "second sight", intentMode: "preorders" }),
    });
    const res = await action({ request: req });
    const body = (await res.json()) as { options: { id: string; rankingBucket: string }[] };
    assert.equal(res.status, 200);
    assert.deepEqual(body.options.map((o) => o.id), ["o-ss-future"]);
    assert.equal(body.options[0]?.rankingBucket, "preorder");
  } finally {
    globalThis.fetch = origFetch;
    if (prevKey === undefined) delete process.env.OPENAI_API_KEY;
    else process.env.OPENAI_API_KEY = prevKey;
  }
});
