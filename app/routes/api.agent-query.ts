import { parseQueryWithLLM } from "../lib/query-parser.server";

function normalizeText(text: string) {
  return text.trim().toLowerCase();
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

export async function action({ request }: { request: Request }) {
  try {
    const body = await request.json();
    const message = String(body.message || "").trim();

    if (!message) {
      return Response.json({ error: "No message provided" }, { status: 400 });
    }

    let parsed = parseCustomerQuery(message);
    let searchQuery = message;
    let llmFranchise: string | null = null;
    let llmPerson: string | null = null;
    let llmStudio: string | null = null;
    let llmGenre: string | null = null;
    let llmDecade: number | null = null;
    
    
    
    try {
      const llmParsed = await parseQueryWithLLM(message);
     
      // const debugParse = {
//         intent: llmParsed.intent || null,
//         cleaned_query: llmParsed.cleaned_query || null,
//         person: llmParsed.person || null,
//         franchise: llmParsed.franchise || null,
//         studio: llmParsed.studio || null,
//         genre: llmParsed.genre || null,
//         decade: llmParsed.decade || null,
//       };
//       
//       console.log("PARSE DEBUG:", JSON.stringify(debugParse, null, 2));
//       
//       return Response.json(debugParse);
//       
//       console.log("RAW MESSAGE:", message);
//       console.log("LLM PARSED:", JSON.stringify(llmParsed, null, 2));
      
      
      
    
      parsed = {
        intent: llmParsed.intent,
        availability: llmParsed.availability_only,
        preorder: llmParsed.preorder_only,
        bestEdition: llmParsed.best_edition,
        person: llmParsed.intent === "person",
      };
    
      searchQuery = llmParsed.cleaned_query?.trim() || message;
      llmFranchise = llmParsed.franchise?.trim() || null;
      llmPerson = llmParsed.person?.trim() || null;
      llmStudio = llmParsed.studio?.trim() || null;
      llmGenre = llmParsed.genre?.trim() || null;
      llmDecade = llmParsed.decade || null;
    
      if (llmFranchise) {
        searchQuery = llmFranchise;
      }
    } catch (error) {
      if (parsed.intent === "person") {
        searchQuery = extractPersonQuery(message);
      } else if (parsed.intent === "availability") {
        searchQuery = extractAvailabilityQuery(message);
      } else if (parsed.intent === "preorder") {
        searchQuery = extractPreorderQuery(message);
      } else if (parsed.intent === "best_edition") {
        searchQuery = extractBestEditionQuery(message);
      }
    }
    
    searchQuery = cleanupExtractedQuery(searchQuery);
    searchQuery = normalizeLooseFilmQuery(searchQuery);
    searchQuery = searchQuery.trim().toLowerCase();
    
    if (!searchQuery) {
      searchQuery = message;
    }
    
    if (llmPerson) {
      searchQuery = llmPerson;
    }
    
    if (llmStudio && !llmPerson && !llmFranchise) {
      searchQuery = llmStudio;
    }
    
    const franchiseQueries = expandFranchiseQuery(searchQuery, llmFranchise);
    
    const expandedQueries = franchiseQueries.flatMap((q) =>
      expandCollectionHints(normalizeCollectionQuery(q)),
    );
    
    const uniqueExpandedQueries = Array.from(new Set(expandedQueries)).slice(0, 5);
    
    let films: any[] = [];
    
    for (const queryPart of uniqueExpandedQueries) {
      const url = new URL("/api/intelligence-search", request.url);
    
      if (parsed.intent === "preorder") {
        url.searchParams.set("q", `latest ${queryPart}`.trim());
      } else {
        url.searchParams.set("q", queryPart);
      }
    
      if (llmGenre) {
        url.searchParams.set("genre", llmGenre);
      }
    
      if (llmDecade) {
        url.searchParams.set("decade", String(llmDecade));
      }
    
      if (llmStudio) {
        url.searchParams.set("studio", llmStudio);
      }
    
      if (llmPerson) {
        url.searchParams.set("person", llmPerson);
      }
    
      const response = await fetch(url.toString());
      const result = await response.json();
    
      if (result.films?.length) {
        films.push(...result.films);
      }
    }
    
    films = Array.from(
      new Map(films.map((film: any) => [film.film.id, film])).values()
    );
    
    // --- STRICT FILTERING (v1 deterministic rules) ---
    
    if (llmStudio) {
      const wantedStudio = llmStudio.trim().toLowerCase();
    
      films = films.filter((filmResult: any) => {
        const offers = Array.isArray(filmResult.offers) ? filmResult.offers : [];
    
        return offers.some((offer: any) =>
          String(offer?.studio || "").toLowerCase().includes(wantedStudio)
        );
      });
    }
    
    if (parsed.intent === "best_edition" && searchQuery) {
      const wanted = searchQuery.trim().toLowerCase();
    
      const exactish = films.filter((filmResult: any) => {
        const filmTitle = String(filmResult.film?.title || "").toLowerCase();
        return filmTitle === wanted || filmTitle.includes(wanted);
      });
    
      if (exactish.length > 0) {
        films = exactish.filter((filmResult: any) => {
          const filmTitle = String(filmResult.film?.title || "").toLowerCase();
          return !filmTitle.includes(`${wanted} 2049`) || filmTitle === wanted;
        });
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
    
    if (parsed.intent === "person" && llmPerson) {
      const wantedPerson = llmPerson.trim().toLowerCase();
    
      films = films.filter((filmResult: any) => {
        const topCast = String(
          filmResult.film?.topCast ||
          filmResult.film?.top_cast ||
          ""
        ).toLowerCase();
    
        return topCast.includes(wantedPerson);
      });
    }
    
    if (!films.length) {
      return Response.json({
        reply: `I couldn't find anything matching "${message}".`,
        intent: parsed.intent,
        options: [],
      });
    }

    const options = films.slice(0, 5).flatMap((filmResult: any) => {
      let offers = filmResult.offers || [];
    
      // --- enforce studio at OFFER level ---
      if (llmStudio) {
        const wantedStudio = llmStudio.trim().toLowerCase();
    
        const studioMatchedOffers = offers.filter((offer: any) =>
          String(offer?.studio || "").toLowerCase().includes(wantedStudio)
        );
    
        if (studioMatchedOffers.length > 0) {
          offers = studioMatchedOffers;
        }
      }
    
      return offers.slice(0, 2).map((offer: any) => ({
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
        availability: offer.availability_status || null,
        supplierStock: offer.supplier_stock_status || 0,
        rankingBucket: offer.rankingBucket || null,
        productCode: offer.supplier_sku || null,
        sourceType: offer.source_type || null,
        shopifyVariantId: offer.shopify_variant_id || null,
      }));
    });
    
    let filteredOptions = options;
    
    if (parsed.intent === "preorder") {
      const preorderOnly = options.filter((opt: any) => opt.rankingBucket === "preorder");
      if (preorderOnly.length > 0) {
        filteredOptions = preorderOnly;
      }
    }
    
    if (parsed.intent === "availability") {
      const availableOnly = options.filter(
        (opt: any) =>
          opt.availability === "store_stock" ||
          opt.availability === "supplier_stock" ||
          opt.rankingBucket === "preorder",
      );
      if (availableOnly.length > 0) {
        filteredOptions = availableOnly;
      }
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
    	
    
    const recommendedOption = filteredOptions[0] || null;
    const alternativeOptions =
      parsed.intent === "best_edition"
        ? filteredOptions.slice(1, 3)
        : filteredOptions.slice(1, 5);
    
    const salesCopy = buildSalesReply({
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
    
    return Response.json({
      reply: salesCopy.reply,
      upsell: salesCopy.upsell,
      wishlistPrompt: salesCopy.wishlistPrompt,
      intent: parsed.intent,
      searchQuery,
      recommendedOption: recommendedOption
        ? {
            ...recommendedOption,
            availabilityLabel: formatAvailability(recommendedOption),
            recommendationReason: buildRecommendationReason(recommendedOption),
          }
        : null,
      alternativeOptions: alternativeOptions.map((opt: any) => ({
        ...opt,
        availabilityLabel: formatAvailability(opt),
      })),
      options: filteredOptions.map((opt: any) => ({
        ...opt,
        availabilityLabel: formatAvailability(opt),
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