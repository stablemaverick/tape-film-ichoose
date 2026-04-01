import { parseQueryWithLLM } from "../lib/query-parser.server";
import {
  getAvailabilityMode,
  mergeLlmTapeAgentParse,
  parseTapeAgentQueryDeterministic,
  type StructuredTapeAgentParse,
} from "../lib/tape-agent-query-parser.server";

function normalizeText(text: string) {
  return text.trim().toLowerCase();
}

type IntentMode =
  | "all"
  | "new_releases"
  | "film_title"
  | "director"
  | "label_studio"
  | "in_stock"
  | "preorders"
  | "best_edition";

function normalizeIntentMode(raw: unknown): IntentMode {
  const mode = String(raw || "").trim().toLowerCase();
  switch (mode) {
    case "new_releases":
    case "film_title":
    case "director":
    case "label_studio":
    case "in_stock":
    case "preorders":
    case "best_edition":
      return mode;
    default:
      return "all";
  }
}

function intentModeAllowsBlankQuery(mode: IntentMode): boolean {
  return mode === "new_releases" || mode === "in_stock" || mode === "preorders";
}

function applyIntentModePrior(
  structured: StructuredTapeAgentParse,
  mode: IntentMode,
  message: string,
): StructuredTapeAgentParse {
  if (mode === "all") return structured;

  const base = {
    ...structured,
    facets: { ...structured.facets },
    secondaryIntents: [...structured.secondaryIntents],
  };

  if (mode === "film_title") {
    const title = cleanupExtractedQuery(message).trim();
    if (title) base.facets.title = title;
    base.primaryIntent = "title_lookup";
    base.residualQuery = title || message.trim();
    return base;
  }

  if (mode === "director") {
    const person = extractPersonQuery(message).trim() || cleanupExtractedQuery(message).trim();
    if (person) base.facets.person = person;
    base.facets.personRole = "director";
    base.primaryIntent = "person";
    base.residualQuery = person || message.trim();
    return base;
  }

  if (mode === "label_studio") {
    const studio = String(base.facets.studio || "").trim() || cleanupExtractedQuery(message).trim();
    if (studio) base.facets.studio = studio;
    base.primaryIntent = "studio_browse";
    base.residualQuery = studio || message.trim();
    return base;
  }

  if (mode === "in_stock") {
    const q = extractAvailabilityQuery(message).trim();
    base.primaryIntent = "availability";
    base.facets.availabilityOnly = true;
    base.facets.availabilityIncludePreorder = false;
    delete base.facets.title;
    base.facets.availabilityBrowse = true;
    base.residualQuery = q;
    return base;
  }

  if (mode === "preorders") {
    const q = extractPreorderQuery(message).trim();
    base.primaryIntent = "preorder";
    base.facets.preorderOnly = true;
    base.facets.latest = true;
    base.residualQuery = q || message.trim();
    return base;
  }

  if (mode === "best_edition") {
    const q = extractBestEditionQuery(message).trim() || cleanupExtractedQuery(message).trim();
    base.primaryIntent = "best_edition";
    base.facets.bestEdition = true;
    if (q) {
      base.facets.title = q;
      base.residualQuery = q;
    }
    return base;
  }

  if (mode === "new_releases") {
    base.facets.latest = true;
    if (!base.facets.title && !base.facets.person && !base.facets.studio) {
      base.residualQuery = "";
    }
    if (base.primaryIntent === "search") {
      base.primaryIntent = "discovery";
    }
    return base;
  }

  return base;
}

function parseCustomerQuery(message: string) {
  const q = normalizeText(message);

  const availability =
    /\b(in stock|available|availability|do you have|can i get|have you got)\b/.test(q);

  const preorder =
    /\b(preorder|pre-order|coming soon|upcoming|coming out|release date)\b/.test(q);

  const bestEdition =
    /\b(best edition|best version|which edition|which version|should i buy|best 4k)\b/.test(q);

  const person =
    /\b(films by|movies by|directed by|starring|with)\b/.test(q);

  let intent: "availability" | "preorder" | "person" | "best_edition" | "search" = "search";

  if (bestEdition) intent = "best_edition";
  else if (preorder) intent = "preorder";
  else if (availability) intent = "availability";
  else if (person) intent = "person";

  return {
    intent,
    availability,
    preorder,
    bestEdition,
    person,
  };
}

function wishlistTargetFromOption(opt: any) {
  if (!opt) return null;
  return {
    catalogItemId: opt.catalogItemId ?? opt.id ?? null,
    filmId: opt.filmId ?? null,
    shopifyVariantId: opt.shopifyVariantId ?? null,
    filmTitle: opt.filmTitle ?? null,
    title: opt.title ?? null,
  };
}

function extractPersonQuery(message: string) {
  return message
    .replace(/films with/gi, "")
    .replace(/movies with/gi, "")
    .replace(/films by/gi, "")
    .replace(/movies by/gi, "")
    .replace(/directed by/gi, "")
    .replace(/starring/gi, "")
    .replace(/\bwith\b/gi, "")
    .replace(/\bfilms\b/gi, "")
    .replace(/\bmovies\b/gi, "")
    .trim();
}

function buildSalesReply({
  message,
  intent,
  recommendedOption,
  alternativeOptions,
}: {
  message: string;
  intent: string;
  recommendedOption: any | null;
  alternativeOptions: any[];
}) {
  if (!recommendedOption) {
    return {
      reply: `I couldn’t find a strong match for "${message}".`,
      upsell: null,
      wishlistPrompt: `If you'd like, I can help save this to a wishlist so you can come back to it later.`,
    };
  }

  const availability = recommendedOption.availabilityLabel || "Out of stock";
  const title = recommendedOption.filmTitle || recommendedOption.title;

  let reply = `I found a strong match for "${message}": ${title}.`;
  let upsell: string | null = null;
  let wishlistPrompt: string | null = null;

  if (intent === "best_edition") {
    reply = `The best edition I’d recommend for "${message}" is ${recommendedOption.title}.`;
  }

  if (intent === "person") {
    reply = `Here are the best matches I found for ${message}.`;
  }

  if (intent === "availability") {
    reply = `${title} is currently ${availability.toLowerCase()}.`;
  }

  if (intent === "release_date") {
    const when = recommendedOption.mediaReleaseDate;
    reply = when
      ? `For "${message}", our catalog shows a media release date of ${when} for ${title}.`
      : `I found ${title} for "${message}", but there isn’t a clear media release date in our catalog yet.`;
  }

  if (recommendedOption.availability === "store_stock") {
    upsell = `This one is in stock now, so it’s ready to buy straight away.`;
  } else if (recommendedOption.availability === "supplier_stock") {
    upsell = `This one is available to order, so I can help you add it to a draft order.`;
  } else if (
    recommendedOption.rankingBucket === "preorder" ||
    recommendedOption.availability === "preorder"
  ) {
    upsell = `This is currently available for pre-order ahead of release.`;
  } else {
    wishlistPrompt = `This one isn’t currently available, so a wishlist would be the best next step if you want to track it.`;
  }

  if (!wishlistPrompt && alternativeOptions.length === 0) {
    wishlistPrompt = `If this isn’t quite the one, I can also help build out a wishlist around similar titles.`;
  }

  return {
    reply,
    upsell,
    wishlistPrompt,
  };
}

function extractAvailabilityQuery(message: string) {
  return message
    .replace(/\bwhat\b/gi, "")
    .replace(/do you have/gi, "")
    .replace(/have you got/gi, "")
    .replace(/can i get/gi, "")
    .replace(/\bis\b/gi, "")
    .replace(/in stock/gi, "")
    .replace(/available/gi, "")
    .replace(/availability/gi, "")
    .replace(/[?]/g, "")
    .trim();
}

function detectFranchiseQuery(message: string) {
  const q = normalizeText(message);

  const franchisePatterns = [
    /star wars/i,
    /james bond/i,
    /mission impossible/i,
    /lord of the rings/i,
    /harry potter/i,
    /indiana jones/i,
    /rocky/i,
    /creed/i,
    /alien/i,
    /terminator/i,
    /mad max/i,
  ];

  return franchisePatterns.some((pattern) => pattern.test(q));
}

function expandFranchiseQuery(query: string, franchise?: string | null) {
  const q = normalizeText(franchise || query);

  if (q === "star wars") {
    return [
      "star wars",
      "a new hope",
      "empire strikes back",
      "return of the jedi",
      "phantom menace",
      "attack of the clones",
      "revenge of the sith",
      "force awakens",
      "last jedi",
      "rise of skywalker",
      "rogue one",
      "solo",
    ];
  }

  if (q === "james bond" || q === "007" || q === "bond") {
    return [
      "james bond",
      "007",
      "dr no",
      "from russia with love",
      "goldfinger",
      "thunderball",
      "you only live twice",
      "on her majesty's secret service",
      "diamonds are forever",
      "live and let die",
      "the man with the golden gun",
      "the spy who loved me",
      "moonraker",
      "for your eyes only",
      "octopussy",
      "a view to a kill",
      "the living daylights",
      "licence to kill",
      "goldeneye",
      "tomorrow never dies",
      "the world is not enough",
      "die another day",
      "casino royale",
      "quantum of solace",
      "skyfall",
      "spectre",
      "no time to die",
    ];
  }

  return [query];
}

function scoreBestEditionOption(opt: any) {
  let score = 0;

  const format = String(opt.format || "").toLowerCase();
  const studio = String(opt.studio || "").toLowerCase();
  const title = String(opt.title || "").toLowerCase();
  const popularity = Number(opt.popularityScore || 0);

  // Format
  if (format.includes("4k")) score += 40;
  else if (format.includes("blu")) score += 20;
  else if (format.includes("dvd")) score += 5;

  // Premium distributors / labels
  if (studio.includes("arrow")) score += 30;
  if (studio.includes("criterion")) score += 30;
  if (studio.includes("second sight")) score += 28;
  if (studio.includes("radiance")) score += 26;
  if (studio.includes("eureka")) score += 24;
  if (studio.includes("88 films")) score += 24;
  if (studio.includes("vinegar syndrome")) score += 24;
  if (studio.includes("severin")) score += 24;
  if (studio.includes("imprint")) score += 22;
  if (studio.includes("disney")) score += 18;

  // Collector packaging cues
  if (title.includes("limited edition")) score += 18;
  if (title.includes("steelbook")) score += 18;
  if (title.includes("collector")) score += 14;
  if (title.includes("box set")) score += 16;
  if (title.includes("deluxe")) score += 10;

  // Popularity
  score += Math.min(popularity * 5, 40);

  // Availability as tie-break support, not main driver
  if (opt.availability === "store_stock") score += 8;
  else if (opt.availability === "supplier_stock") score += 5;
  else if (opt.rankingBucket === "preorder" || opt.availability === "preorder") score += 3;

  return score;
}

function normalizeCollectionQuery(query: string) {
  let q = normalizeText(query);

  q = q
    .replace(/\bhong kong\b/gi, "hong kong")
    .replace(/\bitalian\b/gi, "italian")
    .replace(/\bfrench\b/gi, "french")
    .replace(/\bjapanese\b/gi, "japanese")
    .replace(/\bkorean\b/gi, "korean")
    .replace(/\bgiallo\b/gi, "giallo")
    .replace(/\bnew wave\b/gi, "new wave")
    .replace(/\bvideo\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();

  return q;
}

function expandCollectionHints(query: string) {
  const q = normalizeText(query);

  if (q.includes("giallo")) {
    return ["giallo", "italian horror", "italian thriller"];
  }

  if (q.includes("hong kong action")) {
    return ["hong kong action", "action", "john woo", "tsui hark"];
  }

  if (q.includes("french new wave")) {
    return ["french new wave", "france", "godard", "truffaut"];
  }

  return [query];
}  

function extractPreorderQuery(message: string) {
  return message
    .replace(/pre-?orders?/gi, "")
    .replace(/coming soon/gi, "")
    .replace(/upcoming/gi, "")
    .replace(/coming out/gi, "")
    .replace(/release date/gi, "")
    .replace(/latest/gi, "")
    .replace(/newest/gi, "")
    .replace(/new/gi, "")
    .replace(/\bfrom\b/gi, "")
    .replace(/what('?s| is)/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}
  

function extractBestEditionQuery(message: string) {
  return message
    .replace(/best edition of/gi, "")
    .replace(/best version of/gi, "")
    .replace(/which edition of/gi, "")
    .replace(/which version of/gi, "")
    .replace(/best edition/gi, "")
    .replace(/best version/gi, "")
    .replace(/which edition/gi, "")
    .replace(/which version/gi, "")
    .replace(/should i buy/gi, "")
    .replace(/best 4k of/gi, "")
    .replace(/best 4k/gi, "")
    .trim();
}

function cleanupExtractedQuery(text: string) {
  return text
    .replace(/\bwhat\b/gi, "")
    .replace(/\bfrom\b/gi, "")
    .replace(/\bthat\b/gi, "")
    .replace(/\bmovie\b/gi, "")
    .replace(/\bfilm\b/gi, "")
    .replace(/\bmovies\b/gi, "")
    .replace(/\bfilms\b/gi, "")
    .replace(/\bdo you have\b/gi, "")
    .replace(/[?]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeLooseFilmQuery(message: string) {
  let q = message;

  // Common descriptive phrasing cleanup
  q = q
    .replace(/\bthat\b/gi, "")
    .replace(/\bthe\b/gi, "")
    .replace(/\bmovie\b/gi, "")
    .replace(/\bfilm\b/gi, "")
    .replace(/\babout\b/gi, "")
    .replace(/\bwith\b/gi, "")
    .replace(/\s+/g, " ")
    .trim();

  // Hand-tuned synonym hints for well-known collector queries
  if (
    /\bcronenberg\b/i.test(message) &&
    /\btv\b/i.test(message) &&
    /\bsignal\b/i.test(message)
  ) {
    return "videodrome";
  }

  return q;
}

function formatAvailability(opt: any) {
  if (opt.availability === "store_stock") return "In stock";
  if (opt.availability === "supplier_stock") {
    if (opt.supplierStock && opt.supplierStock > 0) {
      return `Available to order (${opt.supplierStock} at supplier)`;
    }
    return "Available to order";
  }
  if (opt.rankingBucket === "preorder" || opt.availability === "preorder") {
    return "Pre-order";
  }
  return "Out of stock";
}

function availabilityForUi(offer: any): string | null {
  const bucket = String(offer?.rankingBucket || "").trim();
  if (bucket === "preorder") return "preorder";
  if (bucket === "store_in_stock") return "store_stock";
  if (bucket === "supplier_in_stock") return "supplier_stock";
  return offer?.availability_status || null;
}

function studioMatchTerms(studio: string | null | undefined): string[] {
  const s = String(studio || "").trim().toLowerCase();
  if (!s) return [];
  if (s !== "disney") return [s];
  return [
    "disney",
    "walt disney",
    "walt disney pictures",
    "buena vista",
    "touchstone",
    "pixar",
    "marvel",
    "lucasfilm",
    "20th century studios",
    "20th century fox",
    "fox",
    "searchlight",
  ];
}

function offerMatchesStudioTerms(offer: any, terms: string[]): boolean {
  if (!terms.length) return false;
  const studio = String(offer?.studio || "").toLowerCase();
  const supplier = String(offer?.supplier || "").toLowerCase();
  return terms.some((t) => studio.includes(t) || supplier.includes(t));
}

function directorMatchStrength(filmResult: any, person: string): number {
  const wanted = String(person || "").trim().toLowerCase();
  if (!wanted) return 0;
  const d = String(filmResult?.film?.director || "").trim().toLowerCase();
  if (!d) return 0;
  if (d === wanted) return 3;
  if (d.startsWith(wanted)) return 2;
  if (d.includes(wanted)) return 1;
  return 0;
}

function bucketPriorityForBrowse(bucket: string | null | undefined): number {
  const b = String(bucket || "");
  if (b === "store_in_stock") return 1;
  if (b === "supplier_in_stock") return 2;
  if (b === "preorder") return 3;
  return 4;
}

function releaseTs(value: string | null | undefined): number {
  const ts = new Date(String(value || "")).getTime();
  return Number.isNaN(ts) ? 0 : ts;
}

function mapOfferToAgentOption(filmResult: any, offer: any) {
  return {
    id: offer.id,
    catalogItemId: offer.id,
    filmId: filmResult.film.id,
    filmTitle: filmResult.film.title,
    director: filmResult.film.director,
    filmReleased: filmResult.film.filmReleased,
    title: offer.title,
    format: offer.format,
    studio: offer.studio,
    barcode: offer.barcode,
    mediaReleaseDate: offer.media_release_date,
    price: offer.calculated_sale_price,
    costGbp: offer.cost_price,
    availability: availabilityForUi(offer),
    supplierStock: offer.supplier_stock_status || 0,
    rankingBucket: offer.rankingBucket || null,
    productCode: offer.supplier_sku || null,
    sourceType: offer.source_type || null,
    shopifyVariantId: offer.shopify_variant_id || null,
  };
}

function buildRecommendationReason(opt: any) {
  if (opt.availability === "store_stock") {
    return "Best option because it is in stock now at TAPE Film.";
  }

  if (opt.availability === "supplier_stock") {
    return "Best option because it is available to order now.";
  }

  if (opt.rankingBucket === "preorder") {
    return "Best option because it is the strongest upcoming edition match.";
  }

  return "Best option based on current ranking.";
}

function commercialIntent(sp: StructuredTapeAgentParse) {
  switch (sp.primaryIntent) {
    case "release_date":
    case "availability":
    case "preorder":
    case "best_edition":
    case "person":
      return sp.primaryIntent;
    default:
      return "search";
  }
}

function applyStructuredFacetsToSearchUrl(
  url: URL,
  structured: StructuredTapeAgentParse,
  opts?: { omitFranchise?: boolean },
) {
  const f = structured.facets;
  if (f.studio) url.searchParams.set("studio", f.studio);
  if (f.genre) url.searchParams.set("genre", f.genre);
  if (f.decade != null) url.searchParams.set("decade", String(f.decade));
  if (f.exactYear != null) url.searchParams.set("year", String(f.exactYear));
  if (f.format) url.searchParams.set("format", f.format);
  if (f.person) url.searchParams.set("person", f.person);
  if (f.personRole && f.personRole !== "any") {
    url.searchParams.set("personRole", f.personRole);
  }
  if (f.franchise && !opts?.omitFranchise) {
    url.searchParams.set("franchise", f.franchise);
  }
  if (f.title) url.searchParams.set("title", f.title);
  if (f.latest) url.searchParams.set("latest", "true");
}

export async function action({ request }: { request: Request }) {
  try {
    const body = await request.json();
    const message = String(body.message || "").trim();
    const intentMode = normalizeIntentMode(body.intentMode);

    if (!message && !intentModeAllowsBlankQuery(intentMode)) {
      return Response.json({ error: "No message provided" }, { status: 400 });
    }

    let structured = parseTapeAgentQueryDeterministic(message);
    structured = applyIntentModePrior(structured, intentMode, message);

    try {
      if (message) {
        const llmParsed = await parseQueryWithLLM(message);
        structured = mergeLlmTapeAgentParse(structured, {
          cleaned_query: llmParsed.cleaned_query,
          franchise: llmParsed.franchise,
          person: llmParsed.person,
          studio: llmParsed.studio,
          genre: llmParsed.genre,
          decade: llmParsed.decade,
          year: llmParsed.year,
          format: llmParsed.format,
        });
      }
    } catch {
      if (intentMode === "all") {
        const fallback = parseCustomerQuery(message);
        if (fallback.intent === "person") {
          structured = {
            ...structured,
            residualQuery: extractPersonQuery(message),
          };
        } else if (fallback.intent === "availability") {
          structured = {
            ...structured,
            residualQuery: extractAvailabilityQuery(message),
          };
        } else if (fallback.intent === "preorder") {
          structured = {
            ...structured,
            residualQuery: extractPreorderQuery(message),
          };
        } else if (fallback.intent === "best_edition") {
          structured = {
            ...structured,
            residualQuery: extractBestEditionQuery(message),
          };
        }
      }
    }

    const parsed = {
      intent: commercialIntent(structured),
      availability: !!structured.facets.availabilityOnly,
      preorder: !!structured.facets.preorderOnly,
      bestEdition: !!structured.facets.bestEdition,
      person: structured.primaryIntent === "person",
    };

    const availabilityMode = getAvailabilityMode(
      structured.primaryIntent,
      structured.facets,
    );

    let searchQuery = cleanupExtractedQuery(structured.residualQuery);
    if (!structured.facets.title) {
      searchQuery = normalizeLooseFilmQuery(searchQuery);
    }
    searchQuery = searchQuery.trim().toLowerCase();

    const browseAvailability = availabilityMode === "browse";

    if (!searchQuery) {
      if (!browseAvailability) {
        if (intentMode !== "new_releases") {
          searchQuery = message.trim().toLowerCase();
        }
      }
    }

    const llmFranchise = structured.facets.franchise || null;
    const llmPerson = structured.facets.person || null;
    const llmStudio = structured.facets.studio || null;
    const llmGenre = structured.facets.genre || null;
    const llmDecade = structured.facets.decade ?? null;
    const personRole = structured.facets.personRole || "any";

    if (llmPerson) {
      searchQuery = llmPerson.trim().toLowerCase();
    }

    if (llmStudio && !llmPerson && !llmFranchise && !browseAvailability) {
      searchQuery = llmStudio.trim().toLowerCase();
    }

    let franchiseQueries: string[];
    if (availabilityMode === "title_anchored") {
      const t = (
        structured.facets.title ||
        searchQuery ||
        ""
      )
        .trim()
        .toLowerCase();
      franchiseQueries = t ? [t] : searchQuery ? [searchQuery] : [""];
    } else if (browseAvailability) {
      franchiseQueries = [""];
    } else {
      franchiseQueries = expandFranchiseQuery(
        searchQuery,
        structured.facets.franchise,
      );
    }

    const expandedQueries = franchiseQueries.flatMap((q) =>
      expandCollectionHints(normalizeCollectionQuery(q)),
    );

    const uniqueExpandedQueries = Array.from(new Set(expandedQueries)).slice(
      0,
      5,
    );

    const debugAgent = process.env.TAPE_AGENT_DEBUG === "1";
    if (debugAgent) {
      console.log(
        "[tape-agent] parse",
        JSON.stringify({
          structured,
          availabilityMode,
          searchQuery,
          uniqueExpandedQueries,
        }),
      );
    }

    let films: any[] = [];

    for (const queryPart of uniqueExpandedQueries) {
      const url = new URL("/api/intelligence-search", request.url);
      url.searchParams.set("q", queryPart.trim());
      applyStructuredFacetsToSearchUrl(url, structured, {
        omitFranchise: availabilityMode === "title_anchored",
      });

      if (intentMode === "new_releases") {
        url.searchParams.set("recentReleased", "true");
      }

      if (structured.primaryIntent === "preorder") {
        url.searchParams.set("latest", "true");
      }

      if (debugAgent) {
        console.log("[tape-agent] intelligence-search", url.toString());
      }

      const response = await fetch(url.toString());
      const result = await response.json();

      if (debugAgent) {
        console.log(
          "[tape-agent] intelligence-search films",
          result.films?.length ?? 0,
        );
      }

      if (result.films?.length) {
        films.push(...result.films);
      }
    }
    
    films = Array.from(
      new Map(films.map((film: any) => [film.film.id, film])).values()
    );
    
    // --- STRICT FILTERING (v1 deterministic rules) ---
    
    if (llmStudio) {
      const studioTerms = studioMatchTerms(llmStudio);
    
      films = films.filter((filmResult: any) => {
        const offers = Array.isArray(filmResult.offers) ? filmResult.offers : [];
    
        return offers.some((offer: any) => offerMatchesStudioTerms(offer, studioTerms));
      });
    }
    
    if (parsed.intent === "best_edition") {
      const wanted = (
        structured.facets.title ||
        searchQuery ||
        ""
      )
        .trim()
        .toLowerCase();
      if (wanted) {
        const exactish = films.filter((filmResult: any) => {
          const filmTitle = String(filmResult.film?.title || "").toLowerCase();
          return filmTitle === wanted || filmTitle.includes(wanted);
        });

        if (exactish.length > 0) {
          films = exactish.filter((filmResult: any) => {
            const filmTitle = String(filmResult.film?.title || "").toLowerCase();
            return (
              !filmTitle.includes(`${wanted} 2049`) || filmTitle === wanted
            );
          });
        }
      }
    }

    if (llmGenre) {
      const wantedGenre = llmGenre.trim().toLowerCase();
    
      films = films.filter((filmResult: any) => {
        const genres = String(
          filmResult.film?.genres || ""
        ).toLowerCase();
    
        return genres.includes(wantedGenre);
      });
    }

    if (
      parsed.intent === "availability" &&
      availabilityMode === "title_anchored" &&
      structured.facets.title?.trim()
    ) {
      const wanted = structured.facets.title.trim().toLowerCase();
      films = films.filter((filmResult: any) => {
        const t = String(filmResult.film?.title || "").toLowerCase();
        const tm = String(filmResult.film?.tmdb_title || "").toLowerCase();
        return (
          t === wanted ||
          tm === wanted ||
          t.includes(wanted) ||
          tm.includes(wanted)
        );
      });
    }
    
    if (parsed.intent === "person" && llmPerson) {
      const wantedPerson = llmPerson.trim().toLowerCase();

      if (personRole === "director") {
        films = films.filter((filmResult: any) => {
          const d = String(filmResult.film?.director || "").toLowerCase();
          return d.includes(wantedPerson);
        });
      } else if (personRole === "cast") {
        films = films.filter((filmResult: any) => {
          const topCast = String(
            filmResult.film?.topCast ||
              filmResult.film?.top_cast ||
              "",
          ).toLowerCase();
          return topCast.includes(wantedPerson);
        });
      }
    }
    
    if (!films.length) {
      const reply =
        parsed.intent === "availability" && availabilityMode === "title_anchored"
          ? `I couldn’t find a confident match for that title in our catalog.`
          : `I couldn't find anything matching "${message}".`;
      return Response.json({
        reply,
        intent: parsed.intent,
        wishlistSuggested: false,
        wishlistPrompt: null,
        structuredParse: {
          primaryIntent: structured.primaryIntent,
          secondaryIntents: structured.secondaryIntents,
          facets: structured.facets,
          residualQuery: structured.residualQuery,
          availabilityMode,
        },
        options: [],
      });
    }

    const browsePresentationMode =
      intentMode === "new_releases" ||
      intentMode === "preorders" ||
      intentMode === "label_studio" ||
      intentMode === "in_stock" ||
      intentMode === "director";

    if (intentMode === "director") {
      const wantedDirector = String(structured.facets.person || searchQuery || "").trim();
      films = [...films].sort((a: any, b: any) => {
        const as = directorMatchStrength(a, wantedDirector);
        const bs = directorMatchStrength(b, wantedDirector);
        if (as !== bs) return bs - as;
        const ab = bucketPriorityForBrowse(a?.bestOffer?.rankingBucket);
        const bb = bucketPriorityForBrowse(b?.bestOffer?.rankingBucket);
        if (ab !== bb) return ab - bb;
        const ad = releaseTs(a?.bestOffer?.media_release_date);
        const bd = releaseTs(b?.bestOffer?.media_release_date);
        if (ad !== bd) return bd - ad;
        return Number(b?.popularity?.popularity_score ?? 0) - Number(a?.popularity?.popularity_score ?? 0);
      });
    }

    /** Was 2; too few when intelligence-search returns in-stock Shopify + supplier + preorder Shopify. */
    const MAX_OFFERS_PER_FILM_FOR_AGENT = 4;

    const options = browsePresentationMode
      ? films.slice(0, 12).flatMap((filmResult: any) => {
          let offers = filmResult.offers || [];

          if (llmStudio) {
            const terms = studioMatchTerms(llmStudio);
            const studioMatchedOffers = offers.filter((offer: any) =>
              offerMatchesStudioTerms(offer, terms),
            );
            if (studioMatchedOffers.length > 0) {
              offers = studioMatchedOffers;
            }
          }

          const bestOffer =
            intentMode === "preorders"
              ? offers.find((offer: any) => {
                  const bucket = String(offer?.rankingBucket || "");
                  return bucket === "preorder";
                }) || offers[0]
              : offers[0];
          if (!bestOffer) return [];
          return [mapOfferToAgentOption(filmResult, bestOffer)];
        })
      : films.slice(0, 5).flatMap((filmResult: any) => {
          let offers = filmResult.offers || [];

          if (debugAgent && offers.length > MAX_OFFERS_PER_FILM_FOR_AGENT) {
            console.log(
              "[tape-agent] truncating offers for film",
              filmResult.film?.id,
              "from",
              offers.length,
              "to",
              MAX_OFFERS_PER_FILM_FOR_AGENT,
            );
          }

          // --- enforce studio at OFFER level ---
          if (llmStudio) {
            const terms = studioMatchTerms(llmStudio);

            const studioMatchedOffers = offers.filter((offer: any) =>
              offerMatchesStudioTerms(offer, terms),
            );

            if (studioMatchedOffers.length > 0) {
              offers = studioMatchedOffers;
            }
          }

          return offers
            .slice(0, MAX_OFFERS_PER_FILM_FOR_AGENT)
            .map((offer: any) => mapOfferToAgentOption(filmResult, offer));
        });
    
    let filteredOptions = options;

    if (debugAgent) {
      console.log(
        "[tape-agent] candidate offers before availability filter",
        options.length,
      );
    }
    
    if (parsed.intent === "preorder") {
      const preorderOnly = options.filter((opt: any) => opt.rankingBucket === "preorder");
      filteredOptions = preorderOnly;
    }
    
    if (parsed.intent === "availability") {
      const allowPreorder =
        structured.facets.availabilityIncludePreorder === true;
      const availableOnly = options.filter((opt: any) => {
        const inStockish =
          opt.availability === "store_stock" ||
          opt.availability === "supplier_stock" ||
          opt.rankingBucket === "store_in_stock" ||
          opt.rankingBucket === "supplier_in_stock";
        const preorderish =
          opt.rankingBucket === "preorder" || opt.availability === "preorder";
        if (allowPreorder) return inStockish || preorderish;
        return inStockish;
      });
      if (availableOnly.length > 0) {
        filteredOptions = availableOnly;
      }
    }

    if (debugAgent) {
      console.log(
        "[tape-agent] candidate offers after availability filter",
        filteredOptions.length,
      );
    }
    
    if (parsed.intent === "best_edition") {
      filteredOptions = [...filteredOptions].sort((a: any, b: any) => {
        const aScore = scoreBestEditionOption(a);
        const bScore = scoreBestEditionOption(b);
    
        if (aScore !== bScore) {
          return bScore - aScore;
        }
    
        const aPrice = Number(a.price || 999999);
        const bPrice = Number(b.price || 999999);
    
        return aPrice - bPrice;
      });
    }
    	
    
    const recommendedOption = browsePresentationMode ? null : filteredOptions[0] || null;
    const alternativeOptions = browsePresentationMode
      ? []
      : parsed.intent === "best_edition"
        ? filteredOptions.slice(1, 3)
        : filteredOptions.slice(1, 5);

    const salesCopy = browsePresentationMode
      ? {
          reply:
            intentMode === "preorders"
              ? "Here are upcoming release options across different films."
              : intentMode === "new_releases"
                ? "Here are recently released options across different films."
                : intentMode === "label_studio"
                  ? "Here are label/studio matches across different films."
                : intentMode === "director"
                  ? "Here are director matches across different films."
                  : "Here are in-stock options across different films.",
          upsell: null,
          wishlistPrompt: null,
        }
      : buildSalesReply({
          message,
          intent: parsed.intent,
          recommendedOption: recommendedOption
            ? {
                ...recommendedOption,
                availabilityLabel: formatAvailability(recommendedOption),
              }
            : null,
          alternativeOptions,
        });
    
    const wishlistSuggested = Boolean(salesCopy.wishlistPrompt);

    return Response.json({
      reply: salesCopy.reply,
      upsell: salesCopy.upsell,
      wishlistPrompt: salesCopy.wishlistPrompt,
      wishlistSuggested,
      intent: parsed.intent,
        structuredParse: {
          primaryIntent: structured.primaryIntent,
          secondaryIntents: structured.secondaryIntents,
          facets: structured.facets,
          residualQuery: structured.residualQuery,
          availabilityMode,
        },
        searchQuery,
      recommendedOption: recommendedOption
        ? {
            ...recommendedOption,
            availabilityLabel: formatAvailability(recommendedOption),
            recommendationReason: buildRecommendationReason(recommendedOption),
            wishlistTarget: wishlistTargetFromOption(recommendedOption),
          }
        : null,
      alternativeOptions: alternativeOptions.map((opt: any) => ({
        ...opt,
        availabilityLabel: formatAvailability(opt),
        wishlistTarget: wishlistTargetFromOption(opt),
      })),
      options: filteredOptions.map((opt: any) => ({
        ...opt,
        availabilityLabel: formatAvailability(opt),
        wishlistTarget: wishlistTargetFromOption(opt),
      })),
    });
  } catch (err) {
    return Response.json(
      {
        error: "Agent error",
        details: err instanceof Error ? err.message : String(err),
      },
      { status: 500 },
    );
  }
}