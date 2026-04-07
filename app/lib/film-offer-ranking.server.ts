/**
 * Deterministic offer buckets + single final film/offer ordering for intelligence search.
 * Do not treat Shopify linkage alone as confirmed store stock — use availability_status when present.
 */

export type RankingOfferLike = {
  format?: string | null;
  studio?: string | null;
  supplier_stock_status?: number | null;
  availability_status?: string | null;
  shopify_product_id?: string | null;
  shopify_variant_id?: string | null;
  media_release_date?: string | null;
};

export type RankingFilmOfferItem = {
  film: { id: string };
  popularity?: { popularity_score?: number | null } | null;
  score: number;
  bestOffer?: (RankingOfferLike & { rankingBucket?: string }) | null;
};

function normalize(text: string | null | undefined) {
  return (text || "").trim().toLowerCase();
}

function parseReleaseDateMs(value: string | null | undefined): number {
  if (!value) return 0;
  const raw = String(value).trim();
  if (!raw) return 0;

  const direct = new Date(raw).getTime();
  if (!Number.isNaN(direct)) return direct;

  // Supplier feeds can contain dd/mm/yyyy or dd-mm-yyyy.
  const dmy = raw.match(/^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$/);
  if (dmy) {
    const day = Number(dmy[1]);
    const month = Number(dmy[2]);
    const year = Number(dmy[3]);
    if (day >= 1 && day <= 31 && month >= 1 && month <= 12) {
      return Date.UTC(year, month - 1, day);
    }
  }

  return 0;
}

export function isFutureRelease(value: string | null | undefined) {
  const ts = parseReleaseDateMs(value);
  if (!ts) return false;
  return ts > Date.now();
}

export function releaseDateValue(value: string | null | undefined) {
  return parseReleaseDateMs(value);
}

/**
 * Buckets for ordering and UI. `store_in_stock` only when DB explicitly says store_stock.
 */
export function getOfferRankingBucket(offer: RankingOfferLike): string {
  const status = normalize(offer.availability_status);
  const supplierStock = Number(offer.supplier_stock_status || 0);
  const futureRelease = isFutureRelease(offer.media_release_date);
  const hasShopify = !!offer.shopify_variant_id || !!offer.shopify_product_id;

  if (futureRelease) return "preorder";
  if (status === "preorder") return "preorder";
  if (status === "store_stock") return "store_in_stock";
  if (status === "supplier_stock" || supplierStock > 0) return "supplier_in_stock";
  // Shopify listing without explicit store_stock / qty: do not claim in-stock (sync may lag).
  if (hasShopify) return "out_of_stock";
  return "out_of_stock";
}

export function rankOffer(offer: RankingOfferLike) {
  const bucket = getOfferRankingBucket(offer);
  if (bucket === "store_in_stock") return 1;
  if (bucket === "supplier_in_stock") return 2;
  if (bucket === "preorder") return 3;
  return 5;
}

export function rankOfferWithPreferences(
  offer: RankingOfferLike,
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

const BUCKET_ORDER: Record<string, number> = {
  preorder: 1,
  store_in_stock: 2,
  supplier_in_stock: 3,
  out_of_stock: 4,
};

/**
 * One deterministic sort: latest browse rules when applicable, else popularity, else film score.
 */
export function sortFilmsWithOffersFinal<T extends RankingFilmOfferItem>(
  items: T[],
  latestQuery: boolean,
  options?: { commercialRecencyFirst?: boolean; preferAvailableNow?: boolean },
): T[] {
  const commercialRecencyFirst = latestQuery || options?.commercialRecencyFirst === true;
  const bucketOrder = options?.preferAvailableNow
    ? {
        store_in_stock: 1,
        supplier_in_stock: 2,
        preorder: 3,
        out_of_stock: 4,
      }
    : BUCKET_ORDER;
  return [...items].sort((a, b) => {
    if (commercialRecencyFirst) {
      const aBucket = a.bestOffer?.rankingBucket;
      const bBucket = b.bestOffer?.rankingBucket;
      if (aBucket !== bBucket) {
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

    const popularityA = Number(a.popularity?.popularity_score ?? 0);
    const popularityB = Number(b.popularity?.popularity_score ?? 0);
    if (popularityA !== popularityB) {
      return popularityB - popularityA;
    }
    return b.score - a.score;
  });
}
