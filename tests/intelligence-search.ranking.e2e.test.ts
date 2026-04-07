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

  test("latest-only browse (no q) uses commercial release order, not original film year", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-classic",
        title: "Classic Reissue",
        director: "Legacy",
        film_released: "1978-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
      {
        id: "f-modern",
        title: "Modern Pressing",
        director: "Now",
        film_released: "2024-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-classic", popularity_score: 0 },
      { film_id: "f-modern", popularity_score: 100 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o-classic",
        title: "Classic Reissue 4K",
        format: "4k",
        studio: "arrow",
        film_id: "f-classic",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        media_release_date: "2027-01-15",
        barcode: "bc1",
      },
      {
        id: "o-modern",
        title: "Modern Pressing Blu",
        format: "blu-ray",
        studio: "arrow",
        film_id: "f-modern",
        active: true,
        supplier_stock_status: 10,
        availability_status: "supplier_stock",
        media_release_date: "2024-03-10",
        barcode: "bm1",
      },
    ];

    const db = createSequentialSupabaseMock([
      { data: offers.map((o) => ({ film_id: o.film_id, media_release_date: o.media_release_date, availability_status: o.availability_status, active: true })), error: null },
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request("http://test/api/intelligence-search?latest=true");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };

    assert.equal(body.films[0].film.title, "Classic Reissue");
    assert.equal(body.films[1].film.title, "Modern Pressing");
  });

  test("studio browse prefers available-now over preorder even with old film year noise", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-old-film",
        title: "Old Film New Arrow",
        director: "Dir A",
        film_released: "1965-01-01",
        tmdb_title: null,
        genres: "Crime",
        top_cast: null,
      },
      {
        id: "f-new-film",
        title: "New Film Old Arrow",
        director: "Dir B",
        film_released: "2023-01-01",
        tmdb_title: null,
        genres: "Crime",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-old-film", popularity_score: 0 },
      { film_id: "f-new-film", popularity_score: 500 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o-old-film-new-release",
        title: "Old Film New Arrow 4K",
        format: "4k",
        studio: "arrow",
        film_id: "f-old-film",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        media_release_date: "2027-02-01",
        barcode: "s1",
      },
      {
        id: "o-new-film-old-release",
        title: "New Film Old Arrow Blu",
        format: "blu-ray",
        studio: "arrow",
        film_id: "f-new-film",
        active: true,
        supplier_stock_status: 20,
        availability_status: "supplier_stock",
        media_release_date: "2020-06-01",
        barcode: "s2",
      },
    ];

    const db = createSequentialSupabaseMock([
      { data: offers.map((o) => ({ film_id: o.film_id, studio: "arrow", active: true })), error: null },
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import(
      "../app/routes/api.intelligence-search.js"
    );
    const req = new Request(
      "http://test/api/intelligence-search?q=arrow&studio=arrow",
    );
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };

    assert.equal(body.films[0].film.title, "New Film Old Arrow");
    assert.equal(body.films[1].film.title, "Old Film New Arrow");
  });

  test("blank new_releases browse excludes future titles and keeps last-28-day releases only", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-recent-a",
        title: "Recent A",
        director: "Dir A",
        film_released: "1990-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
      {
        id: "f-recent-b",
        title: "Recent B",
        director: "Dir B",
        film_released: "1995-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
      {
        id: "f-future",
        title: "Future C",
        director: "Dir C",
        film_released: "2000-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
      {
        id: "f-old",
        title: "Old D",
        director: "Dir D",
        film_released: "1980-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-recent-a", popularity_score: 10 },
      { film_id: "f-recent-b", popularity_score: 5 },
      { film_id: "f-future", popularity_score: 50 },
      { film_id: "f-old", popularity_score: 100 },
    ];
    const now = new Date();
    const d7 = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const d20 = new Date(now.getTime() - 20 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const d60 = new Date(now.getTime() - 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const future = new Date(now.getTime() + 12 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const offers: OfferFixture[] = [
      { id: "o-ra", title: "Recent A", format: "blu-ray", film_id: "f-recent-a", active: true, supplier_stock_status: 2, availability_status: "supplier_stock", media_release_date: d7, barcode: "ra" },
      { id: "o-rb", title: "Recent B", format: "blu-ray", film_id: "f-recent-b", active: true, supplier_stock_status: 1, availability_status: "supplier_stock", media_release_date: d20, barcode: "rb" },
      { id: "o-fu", title: "Future C", format: "4k", film_id: "f-future", active: true, supplier_stock_status: 0, availability_status: "supplier_out", media_release_date: future, barcode: "fu" },
      { id: "o-old", title: "Old D", format: "dvd", film_id: "f-old", active: true, supplier_stock_status: 3, availability_status: "supplier_stock", media_release_date: d60, barcode: "od" },
    ];

    const db = createSequentialSupabaseMock([
      { data: offers.map((o) => ({ film_id: o.film_id, media_release_date: o.media_release_date, availability_status: o.availability_status, active: true })), error: null },
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import("../app/routes/api.intelligence-search.js");
    const req = new Request("http://test/api/intelligence-search?latest=true&recentReleased=true");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };

    const titles = body.films.map((f) => f.film.title);
    assert.deepEqual(titles, ["Recent A", "Recent B"]);
  });

  test("blank preorders browse still returns future-dated titles", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-future-only",
        title: "Future Only",
        director: "Dir P",
        film_released: "2001-01-01",
        tmdb_title: null,
        genres: "Horror",
        top_cast: null,
      },
      {
        id: "f-current-only",
        title: "Current Only",
        director: "Dir C",
        film_released: "2002-01-01",
        tmdb_title: null,
        genres: "Horror",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-future-only", popularity_score: 0 },
      { film_id: "f-current-only", popularity_score: 0 },
    ];
    const future = new Date(Date.now() + 25 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const current = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const offers: OfferFixture[] = [
      { id: "o-future", title: "Future", format: "4k", film_id: "f-future-only", active: true, supplier_stock_status: 0, availability_status: "preorder", media_release_date: future, barcode: "pf" },
      { id: "o-current", title: "Current", format: "blu-ray", film_id: "f-current-only", active: true, supplier_stock_status: 2, availability_status: "supplier_stock", media_release_date: current, barcode: "pc" },
    ];

    const db = createSequentialSupabaseMock([
      { data: offers.map((o) => ({ film_id: o.film_id, media_release_date: o.media_release_date, availability_status: o.availability_status, active: true })), error: null },
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import("../app/routes/api.intelligence-search.js");
    const req = new Request("http://test/api/intelligence-search?latest=true");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };

    assert.equal(body.films[0].film.title, "Future Only");
  });

  test("new_releases + studio query excludes future titles (recentReleased=true)", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-future-arrow",
        title: "Future Arrow",
        director: "A",
        film_released: "1980-01-01",
        tmdb_title: null,
        genres: "Horror",
        top_cast: null,
      },
      {
        id: "f-recent-arrow",
        title: "Recent Arrow",
        director: "B",
        film_released: "1981-01-01",
        tmdb_title: null,
        genres: "Horror",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-future-arrow", popularity_score: 0 },
      { film_id: "f-recent-arrow", popularity_score: 0 },
    ];
    const future = new Date(Date.now() + 20 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const recent = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const offers: OfferFixture[] = [
      {
        id: "o-future-arrow",
        title: "Future Arrow 4K",
        format: "4k",
        studio: "arrow",
        film_id: "f-future-arrow",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        media_release_date: future,
        barcode: "fa",
      },
      {
        id: "o-recent-arrow",
        title: "Recent Arrow Blu",
        format: "blu-ray",
        studio: "arrow",
        film_id: "f-recent-arrow",
        active: true,
        supplier_stock_status: 3,
        availability_status: "supplier_stock",
        media_release_date: recent,
        barcode: "ra",
      },
    ];

    const db = createSequentialSupabaseMock([
      { data: offers.map((o) => ({ film_id: o.film_id, studio: "arrow", active: true })), error: null },
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import("../app/routes/api.intelligence-search.js");
    const req = new Request(
      "http://test/api/intelligence-search?q=arrow&studio=arrow&latest=true&recentReleased=true",
    );
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };

    assert.deepEqual(body.films.map((f) => f.film.title), ["Recent Arrow"]);
  });

  test("upcoming releases + studio query still includes future titles", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-future-upcoming",
        title: "Future Upcoming",
        director: "U",
        film_released: "1999-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
      {
        id: "f-current-upcoming",
        title: "Current Upcoming",
        director: "V",
        film_released: "1998-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-future-upcoming", popularity_score: 0 },
      { film_id: "f-current-upcoming", popularity_score: 0 },
    ];
    const future = new Date(Date.now() + 15 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const current = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const offers: OfferFixture[] = [
      {
        id: "o-future-upcoming",
        title: "Future Upcoming",
        format: "4k",
        studio: "arrow",
        film_id: "f-future-upcoming",
        active: true,
        supplier_stock_status: 0,
        availability_status: "preorder",
        media_release_date: future,
        barcode: "uf",
      },
      {
        id: "o-current-upcoming",
        title: "Current Upcoming",
        format: "blu-ray",
        studio: "arrow",
        film_id: "f-current-upcoming",
        active: true,
        supplier_stock_status: 2,
        availability_status: "supplier_stock",
        media_release_date: current,
        barcode: "uc",
      },
    ];

    const db = createSequentialSupabaseMock([
      { data: offers.map((o) => ({ film_id: o.film_id, studio: "arrow", active: true })), error: null },
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import("../app/routes/api.intelligence-search.js");
    const req = new Request("http://test/api/intelligence-search?q=arrow&studio=arrow&latest=true");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };

    assert.equal(body.films[0].film.title, "Future Upcoming");
  });

  test("studio=disney matches disney-family aliases (pixar / fox / marvel)", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-pixar",
        title: "Toy Story",
        director: "John Lasseter",
        film_released: "1995-01-01",
        tmdb_title: null,
        genres: "Animation",
        top_cast: null,
      },
      {
        id: "f-fox",
        title: "Alien",
        director: "Ridley Scott",
        film_released: "1979-01-01",
        tmdb_title: null,
        genres: "Science Fiction",
        top_cast: null,
      },
      {
        id: "f-marvel",
        title: "Iron Man",
        director: "Jon Favreau",
        film_released: "2008-01-01",
        tmdb_title: null,
        genres: "Action",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-pixar", popularity_score: 0 },
      { film_id: "f-fox", popularity_score: 0 },
      { film_id: "f-marvel", popularity_score: 0 },
    ];
    const offers: OfferFixture[] = [
      { id: "o-pixar", title: "Toy Story Blu", format: "blu-ray", studio: "Pixar", film_id: "f-pixar", active: true, supplier_stock_status: 3, availability_status: "supplier_stock", media_release_date: "2024-01-01", barcode: "dp1" },
      { id: "o-fox", title: "Alien 4K", format: "4k", studio: "20th Century Fox", film_id: "f-fox", active: true, supplier_stock_status: 4, availability_status: "supplier_stock", media_release_date: "2024-01-02", barcode: "df1" },
      { id: "o-marvel", title: "Iron Man 4K", format: "4k", studio: "Marvel", film_id: "f-marvel", active: true, supplier_stock_status: 5, availability_status: "supplier_stock", media_release_date: "2024-01-03", barcode: "dm1" },
    ];

    const db = createSequentialSupabaseMock([
      { data: offers.map((o) => ({ film_id: o.film_id, studio: o.studio, active: true })), error: null },
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import("../app/routes/api.intelligence-search.js");
    const req = new Request("http://test/api/intelligence-search?q=disney&studio=disney");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };
    const titles = new Set(body.films.map((f) => f.film.title));
    assert.ok(titles.has("Toy Story"));
    assert.ok(titles.has("Alien"));
    assert.ok(titles.has("Iron Man"));
  });

  test("studio browse matches supplier/vendor text when studio field varies", async () => {
    const films: FilmFixture[] = [
      {
        id: "f-ss-vendor",
        title: "Second Sight Vendor Match",
        director: "D",
        film_released: "2001-01-01",
        tmdb_title: null,
        genres: "Horror",
        top_cast: null,
      },
    ];
    const popularity: PopularityFixture[] = [
      { film_id: "f-ss-vendor", popularity_score: 0 },
    ];
    const offers: OfferFixture[] = [
      {
        id: "o-ss-vendor",
        title: "Second Sight Vendor Match",
        format: "blu-ray",
        studio: "SS",
        supplier: "Second Sight Limited",
        film_id: "f-ss-vendor",
        active: true,
        supplier_stock_status: 2,
        availability_status: "supplier_stock",
        media_release_date: "2024-02-01",
        barcode: "ssv1",
      },
    ];

    const db = createSequentialSupabaseMock([
      { data: offers.map((o) => ({ film_id: o.film_id, studio: o.studio, supplier: o.supplier, active: true })), error: null },
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import("../app/routes/api.intelligence-search.js");
    const req = new Request("http://test/api/intelligence-search?q=second+sight&studio=second%20sight&latest=true");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { title: string } }[] };
    assert.equal(body.films[0]?.film?.title, "Second Sight Vendor Match");
  });

  test("studio+latest trims after filtering/ranking, not before", async () => {
    const films: FilmFixture[] = [];
    const popularity: PopularityFixture[] = [];
    const offers: OfferFixture[] = [];
    const browseSeedRows: Array<{ film_id: string; studio: string; active: boolean }> = [];

    for (let i = 0; i < 120; i += 1) {
      const id = `f-trim-${i}`;
      films.push({
        id,
        title: `Trim Film ${i}`,
        director: "D",
        film_released: "2000-01-01",
        tmdb_title: null,
        genres: "Drama",
        top_cast: null,
      });
      popularity.push({ film_id: id, popularity_score: 0 });

      const isFutureTail = i >= 110;
      offers.push({
        id: `o-trim-${i}`,
        title: `Trim Offer ${i}`,
        format: "blu-ray",
        studio: "Criterion",
        supplier: "Criterion",
        film_id: id,
        active: true,
        supplier_stock_status: isFutureTail ? 1 : 3,
        availability_status: isFutureTail ? "preorder" : "supplier_stock",
        media_release_date: isFutureTail ? "2099-08-01" : "2024-01-01",
        barcode: `tb-${i}`,
      });
      browseSeedRows.push({ film_id: id, studio: "Criterion", active: true });
    }

    const db = createSequentialSupabaseMock([
      { data: browseSeedRows, error: null },
      { data: films, error: null },
      { data: popularity, error: null },
      { data: offers, error: null },
    ]);

    const { runIntelligenceSearch } = await import("../app/routes/api.intelligence-search.js");
    const req = new Request("http://test/api/intelligence-search?q=criterion&studio=criterion&latest=true");
    const res = await runIntelligenceSearch(req, db as never);
    const body = (await res.json()) as { films: { film: { id: string } }[] };
    const ids = new Set(body.films.map((f) => f.film.id));
    assert.ok(ids.has("f-trim-110"));
    assert.ok(ids.has("f-trim-119"));
  });
});
