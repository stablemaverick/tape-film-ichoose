/**
 * Shared deterministic facet detection for Tape Agent + /api/intelligence-search.
 * Single source of truth for format / studio / genre / decade / franchise / latest.
 */

export function normalizeSearchText(text: string | null | undefined): string {
  return (text || "").trim().toLowerCase();
}

export function detectFormat(query: string): "4k" | "blu-ray" | "dvd" | null {
  const q = normalizeSearchText(query);
  if (/\b4k\b/.test(q) || /\buhd\b/.test(q)) return "4k";
  if (/\bblu[\s-]?ray\b/.test(q) || /\bbluray\b/.test(q)) return "blu-ray";
  if (/\bdvd\b/.test(q)) return "dvd";
  return null;
}

export function detectStudio(query: string): string | null {
  const q = normalizeSearchText(query);
  const studios: { key: RegExp; value: string }[] = [
    { key: /\barrow\b/i, value: "arrow" },
    { key: /\bcriterion\b/i, value: "criterion" },
    { key: /\bsecond sight\b/i, value: "second sight" },
    { key: /\bradiance\b/i, value: "radiance" },
    { key: /\beureka\b/i, value: "eureka" },
    { key: /\b88 films\b/i, value: "88 films" },
    { key: /\bvinegar syndrome\b/i, value: "vinegar syndrome" },
    { key: /\bseverin\b/i, value: "severin" },
    { key: /\bimprint\b/i, value: "imprint" },
    { key: /\bstudio canal\b/i, value: "studio canal" },
    { key: /\bstudiocanal\b/i, value: "studiocanal" },
  ];
  for (const s of studios) {
    if (s.key.test(q)) return s.value;
  }
  return null;
}

export function detectGenre(query: string): string | null {
  const q = query;
  const genres: { pattern: RegExp; value: string }[] = [
    { pattern: /\bgiallo\b/i, value: "Thriller" },
    { pattern: /\bhorror\b/i, value: "Horror" },
    { pattern: /\bthrillers?\b/i, value: "Thriller" },
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
  for (const g of genres) {
    if (g.pattern.test(q)) return g.value;
  }
  return null;
}

export type YearDecadeResult = {
  exactYear: number | null;
  decadeStart: number | null;
};

export function detectYearOrDecade(query: string): YearDecadeResult {
  const q = normalizeSearchText(query);
  const yearMatch = q.match(/\b(19|20)\d{2}\b/);
  if (yearMatch) {
    return { exactYear: Number(yearMatch[0]), decadeStart: null };
  }
  const decadeMatch = q.match(/\b(19|20)\d0'?s\b/);
  if (decadeMatch) {
    const decade = decadeMatch[0].replace(/'s|s/gi, "");
    return { exactYear: null, decadeStart: Number(decade) };
  }
  const shortDecadeMatch = q.match(/\b([2-9]0)s\b/);
  if (shortDecadeMatch) {
    const short = shortDecadeMatch[1];
    const decadeNum = Number(short);
    if (decadeNum >= 20 && decadeNum <= 90) {
      const century = decadeNum <= 20 ? 2000 : 1900;
      return { exactYear: null, decadeStart: century + decadeNum };
    }
  }
  return { exactYear: null, decadeStart: null };
}

/** Canonical franchise slug → display name */
export const FRANCHISE_CANONICAL: { test: RegExp; franchise: string }[] = [
  { test: /\bstar wars\b/i, franchise: "star wars" },
  { test: /\bjames bond\b|\b007\b/i, franchise: "james bond" },
  { test: /\bmission impossible\b/i, franchise: "mission impossible" },
  { test: /\blord of the rings\b/i, franchise: "lord of the rings" },
  { test: /\bharry potter\b/i, franchise: "harry potter" },
  { test: /\bindiana jones\b/i, franchise: "indiana jones" },
  { test: /\brocky\b|\bcreed\b/i, franchise: "rocky" },
  { test: /\balien\b/i, franchise: "alien" },
  { test: /\bterminator\b/i, franchise: "terminator" },
  { test: /\bmad max\b/i, franchise: "mad max" },
];

export function detectFranchise(query: string): string | null {
  const q = query;
  for (const f of FRANCHISE_CANONICAL) {
    if (f.test.test(q)) return f.franchise;
  }
  return null;
}

/**
 * "Latest / recent / newest" browse — word-boundary safe (avoids matching "new" inside unrelated words).
 */
export function detectLatestIntent(query: string): boolean {
  const q = query;
  return (
    /\blatest\b/i.test(q) ||
    /\brecent\b/i.test(q) ||
    /\bnewest\b/i.test(q) ||
    /\bnew releases\b/i.test(q) ||
    /\bwhat'?s new\b/i.test(q)
  );
}

export type ConsumedFacetToken =
  | { kind: "format"; token: string }
  | { kind: "studio"; token: string }
  | { kind: "genre"; token: string }
  | { kind: "decade"; token: string }
  | { kind: "year"; token: string }
  | { kind: "latest"; token: string }
  | { kind: "franchise"; token: string };

/**
 * Remove only substrings that correspond to already-detected facets (case-insensitive).
 * Preserves title integrity (e.g. "The Thing") when those words were not facet tokens.
 */
export function stripDetectedFacetsFromQuery(
  query: string,
  facets: {
    format?: string | null;
    studio?: string | null;
    genre?: string | null;
    decadeStart?: number | null;
    exactYear?: number | null;
    latest?: boolean;
    franchise?: string | null;
  },
): string {
  let out = query;
  const qn = normalizeSearchText(query);

  const removePhrase = (pattern: RegExp) => {
    out = out.replace(pattern, " ");
  };

  if (facets.format === "4k") {
    removePhrase(/\b4k\b/gi);
    removePhrase(/\buhd\b/gi);
  } else if (facets.format === "blu-ray") {
    removePhrase(/\bblu[\s-]?ray\b/gi);
    removePhrase(/\bbluray\b/gi);
  } else if (facets.format === "dvd") {
    removePhrase(/\bdvd\b/gi);
  }

  if (facets.studio) {
    const map: Record<string, RegExp> = {
      arrow: /\barrow\b/gi,
      criterion: /\bcriterion\b/gi,
      "second sight": /\bsecond sight\b/gi,
      radiance: /\bradiance\b/gi,
      eureka: /\beureka\b/gi,
      "88 films": /\b88 films\b/gi,
      "vinegar syndrome": /\bvinegar syndrome\b/gi,
      severin: /\bseverin\b/gi,
      imprint: /\bimprint\b/gi,
      "studio canal": /\bstudio canal\b/gi,
      studiocanal: /\bstudiocanal\b/gi,
    };
    const re = map[facets.studio];
    if (re) out = out.replace(re, " ");
  }

  if (facets.genre) {
    const genrePatterns: Record<string, RegExp> = {
      Horror: /\bhorror\b/gi,
      Thriller: /\b(thrillers?|giallo)\b/gi,
      Crime: /\bcrime\b/gi,
      Drama: /\bdrama\b/gi,
      Action: /\baction\b/gi,
      Comedy: /\bcomedy\b/gi,
      Romance: /\bromance\b/gi,
      War: /\bwar\b/gi,
      Western: /\bwestern\b/gi,
      Animation: /\banimation\b/gi,
      Fantasy: /\bfantasy\b/gi,
      Mystery: /\bmystery\b/gi,
      "Science Fiction": /\b(sci fi|sci-fi|science fiction)\b/gi,
      Documentary: /\bdocumentary\b/gi,
      Music: /\bmusic\b/gi,
    };
    const gre = genrePatterns[facets.genre];
    if (gre) out = out.replace(gre, " ");
  }

  if (facets.exactYear != null) {
    out = out.replace(new RegExp(`\\b${facets.exactYear}\\b`, "g"), " ");
  }
  if (facets.decadeStart != null) {
    const d = facets.decadeStart;
    out = out.replace(new RegExp(`\\b${d}'?s\\b`, "gi"), " ");
    out = out.replace(new RegExp(`\\b${String(d).slice(-2)}s\\b`, "gi"), " ");
  }

  if (facets.latest) {
    removePhrase(/\blatest\b/gi);
    removePhrase(/\brecent\b/gi);
    removePhrase(/\bnewest\b/gi);
    removePhrase(/\bnew releases\b/gi);
    removePhrase(/\bwhat'?s new\b/gi);
    removePhrase(/\btitles\b/gi);
    removePhrase(/\breleases\b/gi);
  }

  if (facets.franchise) {
    for (const f of FRANCHISE_CANONICAL) {
      if (f.franchise === facets.franchise) {
        out = out.replace(f.test, " ");
        break;
      }
    }
  }

  out = out
    .replace(/\b(on|in)\s+(blu[\s-]?ray|4k|dvd|uhd)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  return out;
}
