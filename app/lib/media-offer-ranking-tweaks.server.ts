/**
 * Deterministic media/SKU disambiguation for catalog offer ranking (Tape Agent v1).
 *
 * Applied on top of `rankOfferWithPreferences` when sorting offers within a film.
 * Mirrors the proven isolated harness rules (no TMDB / no embeddings).
 *
 * Positive adjustment = demote (later in sort). Negative = promote.
 */

export type OfferTitleFields = {
  title?: string | null;
  edition_title?: string | null;
};

function catalogBlob(offer: OfferTitleFields): string {
  return `${offer.title || ""} ${offer.edition_title || ""}`.trim();
}

function normalizeMediaText(s: string): string {
  return s.trim().toLowerCase().replace(/\s+/g, " ");
}

/** Tokens with underscore split (e.g. sac_2045 → sac, 2045). */
export function looseTokens(norm: string): string[] {
  const out: string[] = [];
  for (const w of norm.split(/\s+/).filter(Boolean)) {
    if (w.includes("_")) {
      out.push(...w.replace(/_/g, " ").split(/\s+/).filter(Boolean));
    } else {
      out.push(w);
    }
  }
  return out.filter(Boolean);
}

/** Document frequency per token across offers in one film (for rare-token boost). */
export function buildOfferRareTokenDf(offers: OfferTitleFields[]): Map<string, number> {
  const df = new Map<string, number>();
  for (const o of offers) {
    const seen = new Set<string>();
    const n = normalizeMediaText(catalogBlob(o));
    for (const tok of looseTokens(n)) {
      if (tok.length < 3) continue;
      if (seen.has(tok)) continue;
      seen.add(tok);
      df.set(tok, (df.get(tok) ?? 0) + 1);
    }
  }
  return df;
}

function isRareToken(tok: string, df: Map<string, number>): boolean {
  return tok.length >= 4 && (df.get(tok) ?? 999) <= 2;
}

function querySubtypeCues(qn: string) {
  const t = qn.toLowerCase();
  return {
    hasSac: /\bsac\b|sac_2045|sac2045/i.test(t),
    has2045: t.includes("2045"),
    hasPart: /\bpart\s*2\b|\bpart\s+2\b|\bii\b/i.test(t),
    hasSeason: /\bseason\b/i.test(t),
    hasSeries: /\bseries\b/i.test(t),
    hasCompleteSeries: t.includes("complete") && t.includes("series"),
  };
}

function rowCues(catalogNorm: string, rawTitle: string) {
  const tl = catalogNorm.toLowerCase();
  const raw = rawTitle.toLowerCase();
  const rawUs = raw.replace(/\s+/g, "_");
  return {
    hasSac: tl.includes("sac") || rawUs.includes("sac_2045"),
    has2045: tl.includes("2045") || raw.includes("2045"),
    hasPart2: /\bpart\s*2\b|\bpart\s+2\b/i.test(raw),
    hasSeason: tl.includes("season"),
    hasSeries: tl.includes("series"),
    hasCompleteSeries: tl.includes("complete") && tl.includes("series"),
  };
}

/** Years appearing in parentheses in the catalog title, e.g. (1995). */
function parentheticalYearsInTitle(title: string): number[] {
  const out: number[] = [];
  const re = /\((19|20)\d{2}\)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(title)) !== null) {
    const y = Number(m[0].slice(1, 5));
    if (Number.isFinite(y)) out.push(y);
  }
  return out;
}

function queryContainsYear(qn: string, year: number): boolean {
  return new RegExp(`\\b${year}\\b`).test(qn);
}

/**
 * Extra sort key added to `rankOfferWithPreferences` (lower total = earlier in list).
 * Positive values demote; negative promote.
 */
export function mediaRetrievalRankAdjustment(
  queryText: string,
  offer: OfferTitleFields,
  df: Map<string, number>,
): number {
  const qn = normalizeMediaText(queryText);
  if (!qn) return 0;

  const rawTitle = String(offer.title || "");
  const blob = catalogBlob(offer);
  const cn = normalizeMediaText(blob);
  if (!cn) return 0;

  let adj = 0;

  const qc = querySubtypeCues(qn);
  const rc = rowCues(cn, rawTitle);

  const gitsFranchiseRow = cn.includes("ghost") && cn.includes("shell");
  if (gitsFranchiseRow && (qc.hasSac || qc.has2045)) {
    if (!(rc.hasSac || rc.has2045)) {
      adj += 0.28;
    }
  }

  const parenYears = parentheticalYearsInTitle(rawTitle);
  for (const y of parenYears) {
    if (!queryContainsYear(qn, y)) {
      adj += 0.2;
      break;
    }
  }

  if (qc.hasSeason && rc.hasSeason) adj -= 0.08;
  if (qc.hasSeries && rc.hasSeries) adj -= 0.05;
  if (qc.hasCompleteSeries && rc.hasCompleteSeries) adj -= 0.1;
  if (qc.hasPart && rc.hasPart2) adj -= 0.12;

  const qtoks = looseTokens(qn);
  let rareBoost = 0;
  for (const t of qtoks) {
    if (!isRareToken(t, df)) continue;
    if (looseTokens(cn).includes(t)) {
      rareBoost += 0.055;
    }
  }
  adj -= Math.min(rareBoost, 0.12);

  return adj;
}
