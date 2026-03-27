import { supabase } from "../lib/supabase.server";

/** Supabase-like client for tests (sequential mock) */
export type IntelligenceSearchDb = typeof supabase;
import {
  detectFormat as detectFormatFacet,
  detectFranchise,
  detectGenre as detectGenreFacet,
  detectLatestIntent,
  detectStudio as detectStudioFacet,
  detectYearOrDecade as detectYearOrDecadeFacet,
  stripDetectedFacetsFromQuery,
} from "../lib/search-query-facets.server";
import {
  getOfferRankingBucket,
  isFutureRelease,
  rankOffer,
  rankOfferWithPreferences,
  sortFilmsWithOffersFinal,
} from "../lib/film-offer-ranking.server";

type FilmRow = {
  id: string;
  title: string;
  director?: string | null;
  film_released?: string | null;
  tmdb_title?: string | null;
  genres?: string | null;
  top_cast?: string | null;
};

type OfferRow = {
  id: string;
  title: string;
  edition_title?: string | null;
  format?: string | null;
  studio?: string | null;
  supplier?: string | null;
  supplier_sku?: string | null;
  barcode?: string | null;
  cost_price?: number | null;
  calculated_sale_price?: number | null;
  supplier_stock_status?: number | null;
  supplier_priority?: number | null;
  availability_status?: string | null;
  shopify_product_id?: string | null;
  shopify_variant_id?: string | null;
  media_release_date?: string | null;
  active?: boolean | null;
  film_id?: string | null;
};

function normalize(text: string | null | undefined) {
  return (text || "").trim().toLowerCase();
}


function scoreFilmMatch(film: FilmRow, query: string) {
  const q = normalize(query);
  const title = normalize(film.title);
  const tmdbTitle = normalize(film.tmdb_title);
  const director = normalize(film.director);
  const genres = normalize(film.genres);
  const cast = normalize(film.top_cast);

  let score = 0;

  if (title === q) score += 120;
  if (tmdbTitle === q) score += 115;
  if (director === q) score += 80;
  
  if (title.startsWith(q)) score += 50;
  if (tmdbTitle.startsWith(q)) score += 45;
  if (director.startsWith(q)) score += 25;
  
  const titleWords = ` ${title} `;
  const tmdbTitleWords = ` ${tmdbTitle} `;
  const qWords = ` ${q} `;
  
  if (titleWords.includes(qWords)) score += 45;
  if (tmdbTitleWords.includes(qWords)) score += 40;
  
  if (title.includes(q)) score += 15;
  if (tmdbTitle.includes(q)) score += 12;
  if (director.includes(q)) score += 20;
  if (genres.includes(q)) score += 10;
  if (cast.includes(q)) score += 8;
  
  // Penalise misleading substring matches like "Runner Runner" for "Blade Runner"
  if (!titleWords.includes(qWords) && title.includes(q)) score -= 20;
  if (!tmdbTitleWords.includes(qWords) && tmdbTitle.includes(q)) score -= 20;
  
  // Penalise sequel drift for "best edition" / exact-title style searches
  if (q && (title.includes(`${q} 2`) || title.includes(`${q} ii`) || title.includes(`${q} 2049`))) {
    score -= 15;
  }
  if (q && (tmdbTitle.includes(`${q} 2`) || tmdbTitle.includes(`${q} ii`) || tmdbTitle.includes(`${q} 2049`))) {
    score -= 15;
  }
  
  if (q.includes("hong kong") && (genres.includes("action") || genres.includes("crime"))) {
    score += 10;
  }
  
  if (q.includes("new wave") && genres.includes("drama")) {
    score += 8;
  }

  return score;
}

function dedupeOffersByBarcode(offers: OfferRow[]) {
  const bestByKey = new Map<string, OfferRow>();

  for (const offer of offers) {
    const key =
      (offer.barcode && offer.barcode.trim()) ||
      `no-barcode:${offer.id}`;

    const existing = bestByKey.get(key);

    if (!existing) {
      bestByKey.set(key, offer);
      continue;
    }

    const existingScore = rankOffer(existing);
    const newScore = rankOffer(offer);

    if (newScore < existingScore) {
      bestByKey.set(key, offer);
      continue;
    }

    if (newScore === existingScore) {
      const existingPrice = Number(existing.calculated_sale_price ?? 999999);
      const newPrice = Number(offer.calculated_sale_price ?? 999999);

      if (newPrice < existingPrice) {
        bestByKey.set(key, offer);
      }
    }
  }

  return Array.from(bestByKey.values());
}

function normalizeOffer(offer: OfferRow) {
  return {
    ...offer,
    cost_price: offer.cost_price != null ? Number(offer.cost_price) : null,
    calculated_sale_price:
      offer.calculated_sale_price != null ? Number(offer.calculated_sale_price) : null,
    supplier_stock_status:
      offer.supplier_stock_status != null ? Number(offer.supplier_stock_status) : 0,
    supplier_priority:
      offer.supplier_priority != null ? Number(offer.supplier_priority) : null,
  };
}

function extractReleaseYear(dateValue: string | null | undefined) {
  if (!dateValue) return null;

  const year = Number(String(dateValue).slice(0, 4));
  return Number.isFinite(year) ? year : null;
}

function filmMatchesGenre(film: FilmRow, requestedGenre: string | null) {
  if (!requestedGenre) return true;
  return normalize(film.genres).includes(normalize(requestedGenre));
}

function filmMatchesYearOrDecade(
  film: FilmRow,
  exactYear: number | null,
  decadeStart: number | null,
) {
  const releaseYear = extractReleaseYear(film.film_released);

  if (!releaseYear) return false;

  if (exactYear && releaseYear !== exactYear) {
    return false;
  }

  if (decadeStart && (releaseYear < decadeStart || releaseYear > decadeStart + 9)) {
    return false;
  }

  return true;
}

function explainFilmMatch(
  film: FilmRow,
  searchTerm: string,
  requestedStudio: string | null,
  latestQuery: boolean,
) {
  const reasons: string[] = [];

  const q = normalize(searchTerm);
  const title = normalize(film.title);
  const tmdbTitle = normalize(film.tmdb_title);
  const director = normalize(film.director);
  const genres = normalize(film.genres);
  const cast = normalize(film.top_cast);

  if (q) {
    if (title === q || tmdbTitle === q) {
      reasons.push(`Exact film title match for "${searchTerm}"`);
    } else if (title.includes(q) || tmdbTitle.includes(q)) {
      reasons.push(`Matched film title for "${searchTerm}"`);
    } else if (director.includes(q)) {
      reasons.push(`Matched director "${film.director}"`);
    } else if (genres.includes(q)) {
      reasons.push(`Matched genre "${searchTerm}"`);
    } else if (cast.includes(q)) {
      reasons.push(`Matched cast for "${searchTerm}"`);
    }
  }

  if (requestedStudio) {
    reasons.push(`Browsing studio "${requestedStudio}"`);
  }

  if (latestQuery) {
    reasons.push("Prioritised upcoming releases, then newest available titles");
  }

  return reasons;
}

function explainOffer(
  offer: OfferRow,
  requestedFormat: string | null,
  requestedStudio: string | null,
  latestQuery: boolean,
) {
  const reasons: string[] = [];

  const format = normalize(offer.format);
  const studio = normalize(offer.studio);
  const futureRelease = isFutureRelease(offer.media_release_date);
  const bucket = getOfferRankingBucket(offer);
  const listedShopify =
    !!offer.shopify_variant_id || !!offer.shopify_product_id;

  if (bucket === "store_in_stock") {
    reasons.push("Confirmed in stock (store inventory / availability_status)");
  } else if (bucket === "supplier_in_stock") {
    reasons.push("Available to order from supplier-side stock signals");
  } else if (bucket === "preorder") {
    reasons.push(`Pre-order / future release (${offer.media_release_date})`);
  } else if (listedShopify) {
    reasons.push(
      "Store listing present — stock not inferred without availability_status=store_stock",
    );
  } else {
    reasons.push("Not currently available from tracked inventory signals");
  }

  if (requestedFormat && format.includes(requestedFormat)) {
    reasons.push(`Matched format "${requestedFormat}"`);
  }

  if (requestedStudio && studio.includes(requestedStudio)) {
    reasons.push(`Matched studio "${offer.studio}"`);
  }

   if (latestQuery && offer.media_release_date) {
    if (futureRelease) {
      reasons.push(`Upcoming release date ${offer.media_release_date}`);
    } else {
      reasons.push(`Recent release date ${offer.media_release_date}`);
    }
  }

  return reasons;
}

async function fetchStudioBrowseFilms(
  db: IntelligenceSearchDb,
  requestedStudio: string,
) {
  const { data, error } = await db
    .from("catalog_items")
    .select(`
      film_id,
      studio,
      active
    `)
    .ilike("studio", `%${requestedStudio}%`)
    .eq("active", true)
    .not("film_id", "is", null)
    .limit(200);

  if (error) {
    throw new Error(error.message);
  }

  const filmIds = Array.from(
    new Set((data || []).map((row: any) => row.film_id).filter(Boolean)),
  );

  if (!filmIds.length) return [];

  const { data: filmData, error: filmError } = await db
    .from("films")
    .select(`
      id,
      title,
      director,
      film_released,
      tmdb_title,
      genres,
      top_cast
    `)
    .in("id", filmIds);

  if (filmError) {
    throw new Error(filmError.message);
  }

  return (filmData || []) as FilmRow[];
}


async function fetchGenreYearBrowseFilms(
  db: IntelligenceSearchDb,
  requestedGenre: string | null,
  exactYear: number | null,
  decadeStart: number | null,
) {
  let query = db
    .from("films")
    .select(`
      id,
      title,
      director,
      film_released,
      tmdb_title,
      genres,
      top_cast
    `);

  if (requestedGenre) {
    query = query.ilike("genres", `%${requestedGenre}%`);
  }

  if (exactYear) {
    query = query
      .gte("film_released", `${exactYear}-01-01`)
      .lte("film_released", `${exactYear}-12-31`);
  }

  if (decadeStart) {
    query = query
      .gte("film_released", `${decadeStart}-01-01`)
      .lte("film_released", `${decadeStart + 9}-12-31`);
  }

  const { data, error } = await query.limit(200);

  if (error) {
    throw new Error(error.message);
  }

  return (data || []) as FilmRow[];
}


export async function runIntelligenceSearch(
  request: Request,
  db: IntelligenceSearchDb = supabase,
) {
  try {
    const url = new URL(request.url);
    const q = url.searchParams.get("q")?.trim() || "";
    const titleParam = url.searchParams.get("title")?.trim() || "";
    const genreParam = url.searchParams.get("genre")?.trim() || null;
    const decadeParam = url.searchParams.get("decade")?.trim() || null;
    const yearParam = url.searchParams.get("year")?.trim() || null;
    const studioParam = url.searchParams.get("studio")?.trim() || null;
    const personParam = url.searchParams.get("person")?.trim() || null;
    const personRoleParam = (
      url.searchParams.get("personRole")?.trim() || "any"
    ).toLowerCase();
    const formatParam = url.searchParams.get("format")?.trim() || null;
    const franchiseParam = url.searchParams.get("franchise")?.trim() || null;
    const latestParam = url.searchParams.get("latest") === "true";

    const facetSource = [q, titleParam].filter(Boolean).join(" ").trim();

    const hasFocus =
      !!q ||
      !!titleParam ||
      !!studioParam ||
      !!genreParam ||
      !!decadeParam ||
      !!yearParam ||
      !!personParam ||
      !!franchiseParam;

    if (!hasFocus) {
      return Response.json({
        query: q,
        films: [],
      });
    }

    const requestedFormat =
      formatParam === "4k" || formatParam === "blu-ray" || formatParam === "dvd"
        ? formatParam
        : detectFormatFacet(facetSource || q || titleParam);

    const requestedStudio =
      studioParam || detectStudioFacet(facetSource || q || titleParam);
    const requestedGenre =
      genreParam || detectGenreFacet(facetSource || q || titleParam);

    const parsedYearDecade = detectYearOrDecadeFacet(facetSource || q || titleParam);
    const exactYear = yearParam
      ? Number(yearParam)
      : parsedYearDecade.exactYear;
    const decadeStart = decadeParam
      ? Number(decadeParam)
      : parsedYearDecade.decadeStart;

    const franchiseForStrip =
      franchiseParam?.toLowerCase() ||
      detectFranchise(facetSource || q || titleParam);

    const latestQuery =
      latestParam || detectLatestIntent(facetSource || q || titleParam);

    const strippedResidual = stripDetectedFacetsFromQuery(
      q || titleParam || "",
      {
        format: requestedFormat,
        studio: requestedStudio,
        genre: requestedGenre,
        decadeStart,
        exactYear,
        latest: latestQuery,
        franchise: franchiseForStrip,
      },
    ).trim();

    let searchTerm = (titleParam || strippedResidual || q).trim();

    if (
      !titleParam &&
      requestedStudio &&
      searchTerm &&
      normalize(searchTerm) === normalize(requestedStudio)
    ) {
      searchTerm = "";
    }
    if (
      !titleParam &&
      requestedGenre &&
      searchTerm &&
      normalize(searchTerm) === normalize(requestedGenre)
    ) {
      searchTerm = "";
    }

    const studioBrowseMode =
      !!requestedStudio && !searchTerm && !titleParam && !personParam;

    const genreYearBrowseMode =
      !searchTerm &&
      !titleParam &&
      !requestedStudio &&
      !personParam &&
      (!!requestedGenre || !!exactYear || !!decadeStart);

    let filmData: FilmRow[] = [];

    if (studioBrowseMode && requestedStudio) {
      filmData = await fetchStudioBrowseFilms(db, requestedStudio);
    } else if (genreYearBrowseMode) {
      filmData = await fetchGenreYearBrowseFilms(
        db,
        requestedGenre,
        exactYear,
        decadeStart,
      );
    } else {
      const effectiveSearchTerm = (
        personParam ||
        searchTerm ||
        q ||
        titleParam ||
        franchiseParam ||
        ""
      ).trim();

      if (!effectiveSearchTerm) {
        return Response.json({
          query: q || titleParam,
          format: requestedFormat,
          studio: requestedStudio,
          latest: latestQuery,
          films: [],
        });
      }

      const { data, error: filmError } = await db
        .from("films")
        .select(`
          id,
          title,
          director,
          film_released,
          tmdb_title,
          genres,
          top_cast
        `)
        .or(
          `title.ilike.%${effectiveSearchTerm}%,director.ilike.%${effectiveSearchTerm}%,tmdb_title.ilike.%${effectiveSearchTerm}%,genres.ilike.%${effectiveSearchTerm}%,top_cast.ilike.%${effectiveSearchTerm}%`,
        )
        .limit(50);

      if (filmError) {
        return Response.json(
          { error: "Film search failed", details: filmError.message },
          { status: 500 },
        );
      }

      filmData = (data || []) as FilmRow[];
    }

    let films: FilmRow[] = filmData || [];

    const debugIntel = process.env.TAPE_AGENT_DEBUG === "1";
    if (debugIntel) {
      console.log(
        "[intelligence-search]",
        JSON.stringify({
          q,
          titleParam,
          studioParam,
          genreParam,
          decadeParam,
          yearParam,
          personParam,
          franchiseParam,
          searchTerm,
          studioBrowseMode,
          genreYearBrowseMode,
          candidateFilmsBeforeFacetFilter: films.length,
        }),
      );
    }

    if (requestedGenre) {
      films = films.filter((film) => filmMatchesGenre(film, requestedGenre));
    }

    if (exactYear || decadeStart) {
      films = films.filter((film) =>
        filmMatchesYearOrDecade(film, exactYear, decadeStart),
      );
    }

    if (personParam) {
      const pn = normalize(personParam);
      if (personRoleParam === "director") {
        films = films.filter((film) => normalize(film.director).includes(pn));
      } else if (personRoleParam === "cast") {
        films = films.filter((film) => normalize(film.top_cast).includes(pn));
      }
    }

    if (!films.length) {
      return Response.json({
        query: q || titleParam,
        films: [],
      });
    }

    const scoreTerm = searchTerm;
    const sortedFilms = [...films]
      .map((film) => ({
        ...film,
        _score:
          (studioBrowseMode || genreYearBrowseMode) && !scoreTerm
            ? 50
            : scoreFilmMatch(film, scoreTerm),
      }))
      .sort((a, b) => b._score - a._score)
      .slice(0, studioBrowseMode || genreYearBrowseMode ? 10 : 5);

    const filmIds = sortedFilms.map((f) => f.id);

    const { data: popularityData } = await db
      .from("film_popularity")
      .select("film_id,popularity_score,orders_count,units_sold,last_sold_at")
      .in("film_id", filmIds);

    const popularityByFilmId = new Map(
      (popularityData || []).map((row: any) => [row.film_id, row]),
    );

    const { data: offerData, error: offerError } = await db
      .from("catalog_items")
      .select(`
    id,
    title,
    edition_title,
    format,
    studio,
    supplier,
    supplier_sku,
    barcode,
    cost_price,
    calculated_sale_price,
    supplier_stock_status,
    supplier_priority,
    availability_status,
    shopify_product_id,
    shopify_variant_id,
    media_release_date,
    active,
    film_id
  `)
      .in("film_id", filmIds)
      .eq("active", true)
      .eq("media_type", "film");

    if (offerError) {
      return Response.json(
        { error: "Offer fetch failed", details: offerError.message },
        { status: 500 },
      );
    }

    const allOffers: OfferRow[] = ((offerData || []) as OfferRow[]).map(
      normalizeOffer,
    );

    if (debugIntel) {
      console.log(
        "[intelligence-search]",
        JSON.stringify({
          candidateOffersAfterFetch: allOffers.length,
        }),
      );
    }

    let filmsWithOffers = sortedFilms
      .map((film) => {
        const offersForFilm = allOffers.filter(
          (offer) => offer.film_id === film.id,
        );
        const popularity = popularityByFilmId.get(film.id) || null;

        const shopifyOffers = offersForFilm.filter(
          (o) => !!o.shopify_variant_id || !!o.shopify_product_id,
        );

        const supplierOffers = offersForFilm.filter(
          (o) => !o.shopify_variant_id && !o.shopify_product_id,
        );

        const allCandidateOffers = [...shopifyOffers, ...supplierOffers];

        const dedupedCandidateOffers = dedupeOffersByBarcode(allCandidateOffers);

        const rankedOffers = [...dedupedCandidateOffers].sort(
          (a, b) =>
            rankOfferWithPreferences(
              a,
              requestedFormat,
              requestedStudio,
              latestQuery,
            ) -
            rankOfferWithPreferences(
              b,
              requestedFormat,
              requestedStudio,
              latestQuery,
            ),
        );

        const bestOffer = rankedOffers[0] || null;

        return {
          film: {
            id: film.id,
            title: film.title,
            director: film.director,
            filmReleased: film.film_released,
            genres: film.genres,
            topCast: film.top_cast,
          },
          popularity,
          explanation: explainFilmMatch(
            film,
            scoreTerm,
            requestedStudio,
            latestQuery,
          ),
          bestOffer: bestOffer
            ? {
                ...bestOffer,
                rankingBucket: getOfferRankingBucket(bestOffer),
                explanation: explainOffer(
                  bestOffer,
                  requestedFormat,
                  requestedStudio,
                  latestQuery,
                ),
              }
            : null,
          offers: rankedOffers.map((offer) => ({
            ...offer,
            rankingBucket: getOfferRankingBucket(offer),
            explanation: explainOffer(
              offer,
              requestedFormat,
              requestedStudio,
              latestQuery,
            ),
          })),
          score: film._score,
        };
      })
      .filter((item) => item.offers.length > 0);

    filmsWithOffers = sortFilmsWithOffersFinal(filmsWithOffers, latestQuery);

    return Response.json({
      query: q || titleParam,
      format: requestedFormat,
      studio: requestedStudio,
      latest: latestQuery,
      films: filmsWithOffers,
    });

  } catch (error) {
    return Response.json(
      {
        error: "Server exception",
        details: error instanceof Error ? error.message : String(error),
      },
      { status: 500 },
    );
  }
}

export async function loader({ request }: { request: Request }) {
  return runIntelligenceSearch(request);
}