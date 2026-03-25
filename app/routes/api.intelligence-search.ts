import { supabase } from "../lib/supabase.server";

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

function detectFormat(query: string) {
  const q = normalize(query);

  if (q.includes("4k") || q.includes("uhd")) {
    return "4k";
  }

  if (q.includes("blu ray") || q.includes("bluray") || q.includes("blu-ray")) {
    return "blu-ray";
  }

  if (q.includes("dvd")) {
    return "dvd";
  }

  return null;
}

function detectStudio(query: string) {
  const q = normalize(query);

  const studios = [
    { key: "arrow", value: "arrow" },
    { key: "criterion", value: "criterion" },
    { key: "second sight", value: "second sight" },
    { key: "radiance", value: "radiance" },
    { key: "eureka", value: "eureka" },
    { key: "88 films", value: "88 films" },
    { key: "vinegar syndrome", value: "vinegar syndrome" },
    { key: "severin", value: "severin" },
    { key: "imprint", value: "imprint" },
    { key: "studio canal", value: "studio canal" },
    { key: "studiocanal", value: "studiocanal" },
  ];

  for (const studio of studios) {
    if (q.includes(studio.key)) {
      return studio.value;
    }
  }

  return null;
}


function detectGenre(query: string) {
  const q = normalize(query);

  const genres = [
    { pattern: /\bgiallo\b/i, value: "Thriller" },
    { pattern: /\bhorror\b/i, value: "Horror" },
    { pattern: /\bthriller\b/i, value: "Thriller" },
    { pattern: /\bcrime\b/i, value: "Crime" },
    { pattern: /\bdrama\b/i, value: "Drama" },
    { pattern: /\baction\b/i, value: "Action" },
    { pattern: /\bcomedy\b/i, value: "Comedy" },
    { pattern: /\bromance\b/i, value: "Romance" },
    { pattern: /\bwar\b/i, value: "War" },
    { pattern: /\bwestern\b/i, value: "Western" },
    { pattern: /\banimation\b/i, value: "Animation" },
    { pattern: /\bfantasy\b/i, value: "Fantasy" },
    { pattern: /\bmystery\b/i, value: "Mystery" },
    { pattern: /\bsci fi\b/i, value: "Science Fiction" },
    { pattern: /\bsci-fi\b/i, value: "Science Fiction" },
    { pattern: /\bscience fiction\b/i, value: "Science Fiction" },
    { pattern: /\bdocumentary\b/i, value: "Documentary" },
    { pattern: /\bmusic\b/i, value: "Music" },
  ];

  for (const genre of genres) {
    if (genre.pattern.test(q)) {
      return genre.value;
    }
  }

  return null;
}
function detectYearOrDecade(query: string) {
  const q = normalize(query);

  const yearMatch = q.match(/\b(19|20)\d{2}\b/);
  if (yearMatch) {
    return {
      exactYear: Number(yearMatch[0]),
      decadeStart: null as number | null,
    };
  }

  const decadeMatch = q.match(/\b(19|20)\d0'?s\b/);
  if (decadeMatch) {
    const decade = decadeMatch[0].replace(/'s|s/gi, "");
    return {
      exactYear: null as number | null,
      decadeStart: Number(decade),
    };
  }

  const shortDecadeMatch = q.match(/\b\d0s\b/);
  if (shortDecadeMatch) {
    const short = shortDecadeMatch[0].replace("s", "");
    const decadeNum = Number(short);

    if (decadeNum >= 20 && decadeNum <= 90) {
      const century = decadeNum <= 20 ? 2000 : 1900;
      return {
        exactYear: null as number | null,
        decadeStart: century + decadeNum,
      };
    }
  }

  return {
    exactYear: null as number | null,
    decadeStart: null as number | null,
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

function isStudioBrowseQuery(
  query: string,
  requestedStudio: string | null,
  requestedFormat: string | null,
) {
  const q = normalize(query);

  if (!requestedStudio) return false;

  let cleaned = q
    .replace(/arrow/gi, "")
    .replace(/criterion/gi, "")
    .replace(/second sight/gi, "")
    .replace(/radiance/gi, "")
    .replace(/eureka/gi, "")
    .replace(/88 films/gi, "")
    .replace(/vinegar syndrome/gi, "")
    .replace(/severin/gi, "")
    .replace(/imprint/gi, "")
    .replace(/studio canal/gi, "")
    .replace(/studiocanal/gi, "")
    .replace(/collection/gi, "")
    .replace(/films/gi, "")
    .replace(/latest/gi, "")
    .replace(/\bnew\b/gi, "")
    .replace(/recent/gi, "")
    .replace(/releases/gi, "")
    .replace(/titles/gi, "");

  if (requestedFormat === "4k") {
    cleaned = cleaned.replace(/4k|uhd/gi, "");
  }

  if (requestedFormat === "blu-ray") {
    cleaned = cleaned.replace(/blu[\s-]?ray/gi, "");
  }
  
  if (requestedFormat === "blu ray") {
    cleaned = cleaned.replace(/blu[\s-]?ray/gi, "");
  }

  if (requestedFormat === "dvd") {
    cleaned = cleaned.replace(/dvd/gi, "");
  }

  cleaned = cleaned.trim();

  return cleaned === "";
}

function detectLatestQuery(query: string) {
  const q = normalize(query);

  return (
    q.includes("latest") ||
    q.includes("new") ||
    q.includes("recent")
  );
}

function isFutureRelease(value: string | null | undefined) {
  if (!value) return false;

  const ts = new Date(value).getTime();
  if (Number.isNaN(ts)) return false;

  return ts > Date.now();
}

function getOfferRankingBucket(offer: OfferRow) {
  const isShopify = !!offer.shopify_variant_id || !!offer.shopify_product_id;
  const supplierStock = Number(offer.supplier_stock_status || 0);
  const futureRelease = isFutureRelease(offer.media_release_date);

  if (futureRelease) return "preorder";
  if (isShopify) return "store_in_stock";
  if (supplierStock > 0) return "supplier_in_stock";
  return "out_of_stock";
}

function rankOffer(offer: OfferRow) {
  const bucket = getOfferRankingBucket(offer);

  if (bucket === "store_in_stock") return 1;
  if (bucket === "supplier_in_stock") return 2;
  if (bucket === "preorder") return 3;

  return 5;
}

function releaseDateValue(value: string | null | undefined) {
    if (!value) return 0;
  
    const ts = new Date(value).getTime();
    return Number.isNaN(ts) ? 0 : ts;
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

  const isShopify = !!offer.shopify_variant_id || !!offer.shopify_product_id;
  const format = normalize(offer.format);
  const studio = normalize(offer.studio);
  const supplierStock = Number(offer.supplier_stock_status || 0);
  const futureRelease = isFutureRelease(offer.media_release_date);

  if (isShopify && !futureRelease) {
    reasons.push("Shopify/store offer available now");
  } else if (supplierStock > 0 && !futureRelease) {
    reasons.push("Supplier offer in stock");
  } else if (futureRelease) {
    reasons.push(`Pre-order / future release (${offer.media_release_date})`);
  } else {
    reasons.push("Supplier offer currently out of stock");
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

function rankOfferWithPreferences(
  offer: OfferRow,
  requestedFormat: string | null,
  requestedStudio: string | null,
  latestQuery: boolean,
) {
  let score = rankOffer(offer);

  const format = normalize(offer.format);
  const studio = normalize(offer.studio);

  if (requestedFormat && format.includes(requestedFormat)) {
    score -= 0.5;
  }

  if (requestedStudio && studio.includes(requestedStudio)) {
    score -= 0.75;
  }

  if (latestQuery && offer.media_release_date) {
    const releaseTs = releaseDateValue(offer.media_release_date);
    const nowTs = Date.now();
    const daysOld = (nowTs - releaseTs) / (1000 * 60 * 60 * 24);

    if (daysOld <= 30) score -= 1.0;
    else if (daysOld <= 90) score -= 0.6;
    else if (daysOld <= 180) score -= 0.3;
  }

  return score;
}

async function fetchStudioBrowseFilms(requestedStudio: string) {
  const { data, error } = await supabase
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

  const { data: filmData, error: filmError } = await supabase
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
  requestedGenre: string | null,
  exactYear: number | null,
  decadeStart: number | null,
) {
  let query = supabase
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


export async function loader({ request }: { request: Request }) {
  try {
    const url = new URL(request.url);
    const q = url.searchParams.get("q")?.trim() || "";
    const genreParam = url.searchParams.get("genre")?.trim() || null;
    const decadeParam = url.searchParams.get("decade")?.trim() || null;
    const studioParam = url.searchParams.get("studio")?.trim() || null;
    const personParam = url.searchParams.get("person")?.trim() || null;
    
    if (!q) {
          return Response.json({
            query: q,
            films: [],
          });
        }
    
    const requestedFormat = detectFormat(q);
    
    const requestedStudio = studioParam || detectStudio(q);
    const requestedGenre = genreParam || detectGenre(q);
    
    const parsedYearDecade = detectYearOrDecade(q);
    const exactYear = parsedYearDecade.exactYear;
    const decadeStart = decadeParam ? Number(decadeParam) : parsedYearDecade.decadeStart;
    
    const latestQuery = detectLatestQuery(q);

    
    let filmQuery = q;
    
    if (requestedFormat === "4k") {
      filmQuery = filmQuery.replace(/4k|uhd/gi, "").trim();
    }
    
    if (requestedFormat === "blu-ray") {
      filmQuery = filmQuery.replace(/blu[\s-]?ray/gi, "").trim();
    }
    
    if (requestedFormat === "dvd") {
      filmQuery = filmQuery.replace(/dvd/gi, "").trim();
    }
    
    if (requestedStudio) {
      filmQuery = filmQuery
        .replace(/arrow/gi, "")
        .replace(/criterion/gi, "")
        .replace(/second sight/gi, "")
        .replace(/radiance/gi, "")
        .replace(/eureka/gi, "")
        .replace(/88 films/gi, "")
        .replace(/vinegar syndrome/gi, "")
        .replace(/severin/gi, "")
        .replace(/imprint/gi, "")
        .replace(/studio canal/gi, "")
        .replace(/studiocanal/gi, "")
        .trim();
    }
    
    if (requestedGenre) {
  filmQuery = filmQuery
    .replace(/\bscience fiction\b/gi, "")
    .replace(/\bsci-fi\b/gi, "")
    .replace(/\bsci fi\b/gi, "")
    .replace(/\bhorror\b/gi, "")
    .replace(/\bthriller\b/gi, "")
    .replace(/\bcrime\b/gi, "")
    .replace(/\bdrama\b/gi, "")
    .replace(/\baction\b/gi, "")
    .replace(/\bcomedy\b/gi, "")
    .replace(/\bromance\b/gi, "")
    .replace(/\bwar\b/gi, "")
    .replace(/\bwestern\b/gi, "")
    .replace(/\banimation\b/gi, "")
    .replace(/\bfantasy\b/gi, "")
    .replace(/\bmystery\b/gi, "")
    .replace(/\bdocumentary\b/gi, "")
    .replace(/\bmusic\b/gi, "")
    .trim();
}
    
    filmQuery = filmQuery
      .replace(/\b(19|20)\d{2}\b/g, "")
      .replace(/\b(19|20)\d0'?s\b/gi, "")
      .replace(/\b\d0s\b/gi, "")
      .replace(/\bfrom\b/gi, "")
      .replace(/latest/gi, "")
      .replace(/\bnew\b/gi, "")
      .replace(/recent/gi, "")
      .replace(/releases/gi, "")
      .replace(/titles/gi, "")
      .replace(/\s+/g, " ")
      .trim();
    
    const searchTerm = filmQuery.trim();
    
    const studioBrowseMode =
      !!requestedStudio && !searchTerm;
    
    const genreYearBrowseMode =
      !searchTerm && !requestedStudio && (!!requestedGenre || !!exactYear || !!decadeStart);
    
    // 1. Search films
    let filmData: FilmRow[] = [];
    
    if (studioBrowseMode && requestedStudio) {
      filmData = await fetchStudioBrowseFilms(requestedStudio);
    } else if (genreYearBrowseMode) {
      filmData = await fetchGenreYearBrowseFilms(
        requestedGenre,
        exactYear,
        decadeStart,
      );
    } else {
      const effectiveSearchTerm = personParam || searchTerm || q;
    
      const { data, error: filmError } = await supabase
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
    
    if (requestedGenre) {
      films = films.filter((film) =>
        filmMatchesGenre(film, requestedGenre)
      );
    }
    
    if (exactYear || decadeStart) {
      films = films.filter((film) =>
        filmMatchesYearOrDecade(film, exactYear, decadeStart)
      );
    }

if (!films.length) {
  return Response.json({
    query: q,
    films: [],
  });
}

const sortedFilms = [...films]
  .map((film) => ({
    ...film,
    _score:
      (studioBrowseMode || genreYearBrowseMode) && !searchTerm
        ? 50
        : scoreFilmMatch(film, searchTerm),
  }))
  .sort((a, b) => b._score - a._score)
  .slice(0, (studioBrowseMode || genreYearBrowseMode) ? 10 : 5);

const filmIds = sortedFilms.map((f) => f.id);

const { data: popularityData } = await supabase
  .from("film_popularity")
  .select("film_id,popularity_score,orders_count,units_sold,last_sold_at")
  .in("film_id", filmIds);

const popularityByFilmId = new Map(
  (popularityData || []).map((row: any) => [row.film_id, row]),
);

// 2. Fetch linked offers for all top films
const { data: offerData, error: offerError } = await supabase
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

const allOffers: OfferRow[] = ((offerData || []) as OfferRow[]).map(normalizeOffer);

let filmsWithOffers = sortedFilms
  .map((film) => {
    const offersForFilm = allOffers.filter((offer) => offer.film_id === film.id);
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
        rankOfferWithPreferences(a, requestedFormat, requestedStudio, latestQuery) -
        rankOfferWithPreferences(b, requestedFormat, requestedStudio, latestQuery),
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
        searchTerm,
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

if (latestQuery) {
  const latestBucket = (item: any) => {
    const offer = item.bestOffer;
    const supplierStock = Number(offer?.supplier_stock_status || 0);
    const isShopify = !!offer?.shopify_variant_id || !!offer?.shopify_product_id;
    const futureRelease = isFutureRelease(offer?.media_release_date);

    if (futureRelease) return 1;          // pre-orders first
    if (isShopify) return 2;              // store stock now
    if (supplierStock > 0) return 3;      // supplier stock now
    return 4;                             // out of stock last
  };

  filmsWithOffers = [...filmsWithOffers].sort((a, b) => {
    const bucketA = latestBucket(a);
    const bucketB = latestBucket(b);

    if (bucketA !== bucketB) {
      return bucketA - bucketB;
    }

    const aDate = releaseDateValue(a.bestOffer?.media_release_date);
    const bDate = releaseDateValue(b.bestOffer?.media_release_date);

    // For future releases, nearer upcoming dates first
    if (bucketA === 1) {
      return aDate - bDate;
    }

    // For released titles, newest first
    if (aDate !== bDate) {
      return bDate - aDate;
    }

    return b.score - a.score;
  });
}

   filmsWithOffers = [...filmsWithOffers].sort((a, b) => {
     const popularityA = Number(a.popularity?.popularity_score ?? 0);
     const popularityB = Number(b.popularity?.popularity_score ?? 0);
   
     if (latestQuery) {
       const aBucket = a.bestOffer?.rankingBucket;
       const bBucket = b.bestOffer?.rankingBucket;
   
       if (aBucket !== bBucket) {
         const bucketOrder = {
           preorder: 1,
           store_in_stock: 2,
           supplier_in_stock: 3,
           out_of_stock: 4,
         } as Record<string, number>;
   
         return (bucketOrder[aBucket || "out_of_stock"] ?? 99) -
                (bucketOrder[bBucket || "out_of_stock"] ?? 99);
       }
   
       const aDate = releaseDateValue(a.bestOffer?.media_release_date);
       const bDate = releaseDateValue(b.bestOffer?.media_release_date);
   
       if (aBucket === "preorder" && aDate !== bDate) {
         return aDate - bDate;
       }
   
       if (aDate !== bDate) {
         return bDate - aDate;
       }
     }
   
     if (popularityA !== popularityB) {
       return popularityB - popularityA;
     }
   
     return b.score - a.score;
   });

return Response.json({
  query: q,
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