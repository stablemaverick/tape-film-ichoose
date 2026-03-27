/**
 * Structured deterministic query parse for Tape Agent v1.
 * Deterministic parse is the source of truth for intent, flags, personRole, and title.
 * LLM enriches only via mergeLlmTapeAgentParse (missing facets + safe normalization).
 */

import {
  detectFormat,
  detectFranchise,
  detectGenre,
  detectLatestIntent,
  detectStudio,
  detectYearOrDecade,
  normalizeSearchText,
  stripDetectedFacetsFromQuery,
} from "./search-query-facets.server";

export type TapeAgentPrimaryIntent =
  | "title_lookup"
  | "person"
  | "availability"
  | "preorder"
  | "release_date"
  | "best_edition"
  | "discovery"
  | "studio_browse"
  | "franchise"
  | "search";

export type PersonRole = "director" | "cast" | "any";

export type TapeAgentFacets = {
  title?: string;
  person?: string;
  personRole?: PersonRole;
  franchise?: string;
  studio?: string;
  genre?: string;
  format?: "4k" | "blu-ray" | "dvd";
  decade?: number;
  exactYear?: number;
  latest?: boolean;
  availabilityOnly?: boolean;
  /** Studio/genre/etc. browse + availability (no single-title anchor). */
  availabilityBrowse?: boolean;
  /** Include preorder buckets in browse availability (only when query mentions preorder/upcoming). */
  availabilityIncludePreorder?: boolean;
  preorderOnly?: boolean;
  releaseDateOnly?: boolean;
  bestEdition?: boolean;
};

export type StructuredTapeAgentParse = {
  primaryIntent: TapeAgentPrimaryIntent;
  secondaryIntents: string[];
  facets: TapeAgentFacets;
  /** Search string after facet stripping; safe for retrieval */
  residualQuery: string;
  /** Original user message */
  rawQuery: string;
};

export function detectAvailabilityIntent(query: string): boolean {
  const q = normalizeSearchText(query);
  return (
    /\bin stock\b/.test(q) ||
    /\bavailable now\b/.test(q) ||
    /\bavailability\b/.test(q) ||
    /\bdo you have\b/.test(q) ||
    /\bcan i get\b/.test(q) ||
    /\bhave you got\b/.test(q) ||
    /\bavailable\b/.test(q)
  );
}

/** Preorder / upcoming OK for browse availability filters when user asks for them. */
export function detectAvailabilityBrowseIncludesPreorder(query: string): boolean {
  return /\b(pre-?orders?|pre-?order|coming soon|upcoming)\b/i.test(query);
}

function clearFranchiseWhenSameAsTitle(facets: TapeAgentFacets) {
  if (!facets.title?.trim() || !facets.franchise) return;
  const t = normalizeSearchText(facets.title).replace(/\s+/g, " ").trim();
  if (t === facets.franchise) delete facets.franchise;
}

export function getAvailabilityMode(
  primaryIntent: TapeAgentPrimaryIntent,
  facets: TapeAgentFacets,
): "title_anchored" | "browse" | null {
  if (primaryIntent !== "availability" || !facets.availabilityOnly) return null;
  if (facets.availabilityBrowse) return "browse";
  if (facets.title?.trim()) return "title_anchored";
  return "browse";
}

/** True when user asks for preorders as a listing, not a single-title release date. */
export function detectPreorderIntent(query: string): boolean {
  const q = query;
  const qn = normalizeSearchText(query);
  if (detectReleaseDateIntent(query)) return false;
  return (
    /\bwhat\s+pre-?orders?\b/i.test(q) ||
    /\bpre-?orders?\s+(do you have|from|available)\b/i.test(q) ||
    /\b(any|what)\s+pre-?orders?\b/i.test(q) ||
    (/\bpre-?order\b/i.test(qn) && /\b(from|arrow|criterion|radiance|studio)\b/i.test(qn))
  );
}

export function detectReleaseDateIntent(query: string): boolean {
  const q = query;
  return (
    /\bwhen\s+is\b.*\b(released|out)\b/i.test(q) ||
    /\bwhen\s+was\b.*\b(released|out)\b/i.test(q) ||
    /\brelease date\b/i.test(q) ||
    /\bwhen\s+does\b.*\bcome out\b/i.test(q) ||
    /\bwhen\s+is\b.*\bout on\b/i.test(q) ||
    /\bwhen\s+can\b.*\b(get|buy)\b/i.test(q)
  );
}

export function detectBestEditionIntent(query: string): boolean {
  const q = normalizeSearchText(query);
  return (
    /\bbest edition\b/.test(q) ||
    /\bbest version\b/.test(q) ||
    /\bwhich edition\b/.test(q) ||
    /\bwhich version\b/.test(q) ||
    /\bshould i buy\b/.test(q) ||
    /\bbest\s+4k\b/.test(q)
  );
}

const DIRECTOR_PHRASES =
  /\b(directed by|films directed by|movies directed by|film director)\b/i;
const CAST_PHRASES = /\b(films with|movies with|starring|co-starring)\b/i;
const BY_PERSON_FILMS = /\b(films|movies)\s+by\b/i;

export function detectPersonIntentAndRole(query: string): {
  isPerson: boolean;
  role: PersonRole;
  extractedName: string;
} {
  const raw = query.trim();
  let role: PersonRole = "any";
  let work = raw;

  if (DIRECTOR_PHRASES.test(raw)) {
    role = "director";
    work = raw.replace(DIRECTOR_PHRASES, " ");
  } else if (CAST_PHRASES.test(raw)) {
    role = "cast";
    work = raw.replace(CAST_PHRASES, " ");
  } else if (BY_PERSON_FILMS.test(raw)) {
    role = "director";
    work = raw.replace(BY_PERSON_FILMS, " ");
  } else if (/\bby\s+[a-z]/i.test(raw) && /\b(criterion|arrow|radiance|eureka|88 films)\b/i.test(raw)) {
    const m = raw.match(/\bby\s+([a-z][a-z\s'.-]+)$/i);
    if (m) {
      return { isPerson: true, role: "director", extractedName: m[1].trim() };
    }
  }

  work = work
    .replace(/\b(films|movies|film|movie)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  const isPerson =
    role !== "any" || /\b(actor|actress|director)\b/i.test(raw);

  if (role === "any" && /\b\w+\s+films\b/i.test(raw) && !detectFranchise(raw)) {
    const m2 = raw.match(/^([a-z][a-z\s'.-]+)\s+films\b/i);
    if (m2 && !detectStudio(raw)) {
      return { isPerson: true, role: "director", extractedName: m2[1].trim() };
    }
  }

  return {
    isPerson: role !== "any" || isPerson,
    role,
    extractedName: work,
  };
}

export function extractResidualTitleQuery(
  message: string,
  facets: TapeAgentFacets,
  options?: {
    stripPersonPhrases?: boolean;
    /** Keep franchise tokens in the string (e.g. "is alien available" → title alien, not empty). */
    omitFranchiseStrip?: boolean;
  },
): string {
  let t = message;
  if (options?.stripPersonPhrases) {
    t = t
      .replace(DIRECTOR_PHRASES, " ")
      .replace(CAST_PHRASES, " ")
      .replace(BY_PERSON_FILMS, " ");
    t = t.replace(/\b(films|movies)\s+by\s+/gi, " ");
  }
  t = stripDetectedFacetsFromQuery(t, {
    format: facets.format ?? null,
    studio: facets.studio ?? null,
    genre: facets.genre ?? null,
    decadeStart: facets.decade ?? null,
    exactYear: facets.exactYear ?? null,
    latest: facets.latest ?? false,
    franchise: options?.omitFranchiseStrip ? null : facets.franchise ?? null,
  });

  t = t
    .replace(/\b(best edition of|best version of|which edition of|which version of)\b/gi, " ")
    .replace(/\b(best edition|best version|which edition|which version|should i buy)\b/gi, " ")
    .replace(/\bbest\s+4k\s+of\b/gi, " ")
    .replace(/\bbest\s+4k\b/gi, " ")
    .replace(/\bbest\s+of\b/gi, " ")
    .replace(/\b(when is|when was|when does|release date for|release date)\b/gi, " ")
    .replace(/\b(released|come out|out on)\b/gi, " ")
    .replace(/\b(in stock|available|availability|do you have|can i get)\b/gi, " ")
    .replace(/\b(pre-?order|preorders?|coming soon|upcoming)\b/gi, " ")
    .replace(/\b(what|which|do you have|from)\b/gi, " ")
    .replace(/[?.,!]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  t = t.replace(/^\s*is\s+/gi, "").trim();

  t = t
    .replace(/\b(films|movies|film|movie|titles)\b/gi, " ")
    .replace(/\b(are|is|was|now|currently)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  return t;
}

function extractReleaseTitle(message: string): string {
  let t = message
    .replace(/\bwhen\s+is\b/gi, "")
    .replace(/\bwhen\s+was\b/gi, "")
    .replace(/\bwhen\s+does\b/gi, "")
    .replace(/\brelease date\s+for\b/gi, "")
    .replace(/\brelease date\b/gi, "")
    .replace(/\bcome out\b/gi, "")
    .replace(/\breleased\b/gi, "")
    .replace(/\bout on\b/gi, "")
    .replace(/\b(4k|blu[\s-]?ray|dvd|uhd)\b/gi, "")
    .replace(/[?]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  return t;
}

export function parseTapeAgentQueryDeterministic(message: string): StructuredTapeAgentParse {
  const rawQuery = message.trim();
  const secondary: string[] = [];
  const facets: TapeAgentFacets = {};

  const fmt = detectFormat(rawQuery);
  if (fmt) facets.format = fmt;

  const studio = detectStudio(rawQuery);
  if (studio) facets.studio = studio;

  const genre = detectGenre(rawQuery);
  if (genre) facets.genre = genre;

  const yd = detectYearOrDecade(rawQuery);
  if (yd.exactYear != null) facets.exactYear = yd.exactYear;
  if (yd.decadeStart != null) facets.decade = yd.decadeStart;

  const latest = detectLatestIntent(rawQuery);
  if (latest) facets.latest = true;

  const franchiseDetected = detectFranchise(rawQuery);
  if (franchiseDetected) facets.franchise = franchiseDetected;

  if (
    detectBestEditionIntent(rawQuery) &&
    facets.franchise === "alien" &&
    /\b(of|for)\s+alien\b/i.test(rawQuery)
  ) {
    delete facets.franchise;
  }

  const releaseDate = detectReleaseDateIntent(rawQuery);
  const preorder = detectPreorderIntent(rawQuery);
  const bestEd = detectBestEditionIntent(rawQuery);
  const avail = detectAvailabilityIntent(rawQuery);
  const personInfo = detectPersonIntentAndRole(rawQuery);

  if (releaseDate) {
    facets.releaseDateOnly = true;
    const title = extractReleaseTitle(rawQuery);
    if (title) facets.title = title;
    return {
      primaryIntent: "release_date",
      secondaryIntents: secondary,
      facets,
      residualQuery:
        title ||
        stripDetectedFacetsFromQuery(rawQuery, {
          format: facets.format ?? null,
          studio: facets.studio ?? null,
          genre: facets.genre ?? null,
          decadeStart: facets.decade ?? null,
          exactYear: facets.exactYear ?? null,
          latest: facets.latest ?? false,
          franchise: facets.franchise ?? null,
        }),
      rawQuery,
    };
  }

  if (preorder) {
    facets.preorderOnly = true;
    const residual = extractResidualTitleQuery(rawQuery, facets, { stripPersonPhrases: false });
    return {
      primaryIntent: "preorder",
      secondaryIntents: secondary,
      facets,
      residualQuery: residual || studio || "",
      rawQuery,
    };
  }

  if (bestEd) {
    facets.bestEdition = true;
    if (avail) {
      facets.availabilityOnly = true;
      secondary.push("availability");
    }
    const residual = extractResidualTitleQuery(rawQuery, facets);
    if (residual) facets.title = residual;
    return {
      primaryIntent: "best_edition",
      secondaryIntents: secondary,
      facets,
      residualQuery: residual,
      rawQuery,
    };
  }

  if (
    studio &&
    /\bfilms\s+by\b/i.test(rawQuery) &&
    /\bby\s+[a-z]/i.test(rawQuery)
  ) {
    const m = rawQuery.match(/\bby\s+([a-z][a-z\s'.-]+?)(?:\s*$|\s+films?\b)/i);
    if (m) {
      facets.person = m[1].trim();
      facets.personRole = "director";
      secondary.push("discovery");
      const residual = extractResidualTitleQuery(rawQuery, facets, { stripPersonPhrases: true });
      return {
        primaryIntent: "person",
        secondaryIntents: secondary,
        facets,
        residualQuery: facets.person,
        rawQuery,
      };
    }
  }

  if (
    avail &&
    (studio || genre || facets.franchise) &&
    /\b(films|movies|titles)\b/i.test(rawQuery)
  ) {
    facets.availabilityOnly = true;
    facets.availabilityBrowse = true;
    if (detectAvailabilityBrowseIncludesPreorder(rawQuery)) {
      facets.availabilityIncludePreorder = true;
    }
    secondary.push("discovery");
    return {
      primaryIntent: "availability",
      secondaryIntents: secondary,
      facets,
      residualQuery: "",
      rawQuery,
    };
  }

  if (avail) {
    facets.availabilityOnly = true;
    if (detectAvailabilityBrowseIncludesPreorder(rawQuery)) {
      facets.availabilityIncludePreorder = true;
    }

    const titleCheckPhrase =
      /\b(is|do you have|can i get|have you got)\b/i.test(rawQuery);

    const residual = extractResidualTitleQuery(rawQuery, facets, {
      omitFranchiseStrip: titleCheckPhrase,
    });

    const resNorm = normalizeSearchText(residual);
    const studioNorm = studio ? normalizeSearchText(studio) : "";
    const genreNorm = genre ? normalizeSearchText(genre) : "";
    const franchiseNorm = facets.franchise
      ? normalizeSearchText(facets.franchise)
      : "";

    const hasBrowseFacet =
      !!studio ||
      !!genre ||
      !!facets.franchise ||
      facets.decade != null ||
      facets.exactYear != null;

    const residualEmptyOrFacetOnly =
      !resNorm ||
      (studio && resNorm === studioNorm) ||
      (genre && resNorm === genreNorm) ||
      (!!facets.franchise && resNorm === franchiseNorm);

    if (hasBrowseFacet && residualEmptyOrFacetOnly && !titleCheckPhrase) {
      facets.availabilityBrowse = true;
      secondary.push("discovery");
      return {
        primaryIntent: "availability",
        secondaryIntents: secondary,
        facets,
        residualQuery: "",
        rawQuery,
      };
    }

    if (residual) {
      facets.title = residual;
      clearFranchiseWhenSameAsTitle(facets);
    }
    return {
      primaryIntent: "availability",
      secondaryIntents: secondary,
      facets,
      residualQuery: residual,
      rawQuery,
    };
  }

  if (
    personInfo.isPerson &&
    (personInfo.role !== "any" || personInfo.extractedName.length > 1) &&
    /\b(films|movies|directed|starring|with|by)\b/i.test(rawQuery)
  ) {
    facets.person = personInfo.extractedName.replace(/\b(by|with)\b/gi, " ").trim();
    facets.personRole = personInfo.role === "any" ? "any" : personInfo.role;
    if (studio) secondary.push("discovery");
    const residual = extractResidualTitleQuery(rawQuery, facets, { stripPersonPhrases: true });
    return {
      primaryIntent: "person",
      secondaryIntents: secondary,
      facets,
      residualQuery: facets.person || residual,
      rawQuery,
    };
  }

  if (studio && latest && !facets.franchise) {
    const residual = extractResidualTitleQuery(rawQuery, facets);
    return {
      primaryIntent: "studio_browse",
      secondaryIntents: secondary,
      facets,
      residualQuery: residual || studio,
      rawQuery,
    };
  }

  if (facets.franchise && (fmt || !personInfo.isPerson)) {
    const residual = extractResidualTitleQuery(rawQuery, facets);
    return {
      primaryIntent: "franchise",
      secondaryIntents: secondary,
      facets,
      residualQuery: facets.franchise,
      rawQuery,
    };
  }

  if ((genre || facets.decade || facets.exactYear) && fmt) {
    const residual = extractResidualTitleQuery(rawQuery, facets);
    return {
      primaryIntent: "discovery",
      secondaryIntents: secondary,
      facets,
      residualQuery: residual,
      rawQuery,
    };
  }

  if (genre || facets.decade || facets.exactYear) {
    const residual = extractResidualTitleQuery(rawQuery, facets);
    return {
      primaryIntent: "discovery",
      secondaryIntents: secondary,
      facets,
      residualQuery: residual,
      rawQuery,
    };
  }

  const residual = extractResidualTitleQuery(rawQuery, facets);
  if (residual && residual.length >= 2) {
    facets.title = residual;
    return {
      primaryIntent: "title_lookup",
      secondaryIntents: secondary,
      facets,
      residualQuery: residual,
      rawQuery,
    };
  }

  return {
    primaryIntent: "search",
    secondaryIntents: secondary,
    facets,
    residualQuery: rawQuery,
    rawQuery,
  };
}

/** LLM payload: enrichment only. Intent, flags, personRole, and title must never be driven by the model. */
export type LlmTapeAgentEnrichment = {
  cleaned_query?: string;
  franchise?: string | null;
  person?: string | null;
  studio?: string | null;
  genre?: string | null;
  year?: number | null;
  decade?: number | null;
  format?: string | null;
};

function normKey(s: string) {
  return s.trim().toLowerCase().replace(/\s+/g, " ");
}

/**
 * Merge LLM enrichment into deterministic parse.
 * Preserves: primaryIntent, secondaryIntents, personRole, availabilityOnly, preorderOnly,
 * releaseDateOnly, bestEdition, latest, facets.title (deterministic title detection).
 * Fills missing facets only; person may be expanded/normalized when clearly the same entity.
 */
export function mergeLlmTapeAgentParse(
  det: StructuredTapeAgentParse,
  llm: LlmTapeAgentEnrichment | null,
): StructuredTapeAgentParse {
  if (!llm) return det;

  const out: StructuredTapeAgentParse = {
    ...det,
    secondaryIntents: [...det.secondaryIntents],
    facets: { ...det.facets },
  };

  const mergePerson = (detPerson: string | undefined, llmPerson: string | null | undefined) => {
    const l = llmPerson?.trim();
    if (!l) return detPerson;
    const d = detPerson?.trim();
    if (!d) return l;
    const nd = normKey(d);
    const nl = normKey(l);
    if (nd === nl) return l;
    if (nl.includes(nd) || nd.includes(nl)) return l;
    return detPerson;
  };

  if (llm.franchise?.trim() && !out.facets.franchise) {
    out.facets.franchise = llm.franchise.trim().toLowerCase();
  }
  if (llm.person != null) {
    const merged = mergePerson(out.facets.person, llm.person);
    if (merged) out.facets.person = merged;
  }
  if (llm.studio?.trim() && !out.facets.studio) {
    out.facets.studio = llm.studio.trim().toLowerCase();
  }
  if (llm.genre?.trim() && !out.facets.genre) {
    out.facets.genre = llm.genre.trim();
  }
  if (llm.decade != null && out.facets.decade == null) {
    out.facets.decade = llm.decade;
  }
  if (llm.year != null && out.facets.exactYear == null) {
    out.facets.exactYear = llm.year;
  }
  if (llm.format === "4k" || llm.format === "blu-ray" || llm.format === "dvd") {
    if (!out.facets.format) out.facets.format = llm.format;
  }

  const cq = llm.cleaned_query?.trim();
  if (cq && out.primaryIntent !== "release_date") {
    const res = out.residualQuery?.trim();
    const skipFillForBrowseAvail =
      out.primaryIntent === "availability" &&
      (out.facets.availabilityBrowse ||
        (!out.facets.title?.trim() &&
          !!(out.facets.studio || out.facets.genre)));
    if (!res && !skipFillForBrowseAvail) {
      out.residualQuery = cq;
    }
  }

  return out;
}
