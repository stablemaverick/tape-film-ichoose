import { getOfferRankingBucket } from "./film-offer-ranking.server";
import { supabase } from "./supabase.server";

type AdminApi = {
  graphql: (
    query: string,
    options?: { variables?: Record<string, unknown> },
  ) => Promise<Response>;
};

/**
 * Resolve Shopify Customer GID by email (customer must exist on the shop).
 */
export async function resolveShopifyCustomerId(
  admin: AdminApi,
  email: string,
): Promise<string | null> {
  const trimmed = String(email || "").trim();
  if (!trimmed) return null;

  const queryString = `email:${trimmed.replace(/"/g, '\\"')}`;

  const response = await admin.graphql(
    `#graphql
    query WishlistResolveCustomer($q: String!) {
      customers(first: 1, query: $q) {
        edges {
          node {
            id
            email
          }
        }
      }
    }`,
    { variables: { q: queryString } },
  );

  const json = (await response.json()) as {
    data?: { customers?: { edges?: { node: { id: string } }[] } };
    errors?: { message?: string }[];
  };

  if (json.errors?.length) {
    return null;
  }

  const edges = json.data?.customers?.edges;
  if (!edges?.length) return null;
  return edges[0].node.id;
}

export type AddWishlistItemInput = {
  shopDomain: string;
  shopifyCustomerId: string;
  catalogItemId: string;
  filmId?: string | null;
  shopifyVariantId?: string | null;
  titleSnapshot: string;
  source?: string | null;
};

export async function addWishlistItem(input: AddWishlistItemInput) {
  const row = {
    shop_domain: input.shopDomain,
    shopify_customer_id: input.shopifyCustomerId,
    catalog_item_id: input.catalogItemId,
    film_id: input.filmId ?? null,
    shopify_variant_id: input.shopifyVariantId ?? null,
    title_snapshot: input.titleSnapshot,
    source: input.source ?? "agent_ui",
  };

  const { data, error } = await supabase
    .from("wishlist_items")
    .insert(row)
    .select("id, catalog_item_id, title_snapshot, created_at, notify_requested_at")
    .single();

  if (error) {
    if (error.code === "23505") {
      const { data: existing } = await supabase
        .from("wishlist_items")
        .select("id, catalog_item_id, title_snapshot, created_at, notify_requested_at")
        .eq("shop_domain", input.shopDomain)
        .eq("shopify_customer_id", input.shopifyCustomerId)
        .eq("catalog_item_id", input.catalogItemId)
        .maybeSingle();
      if (existing) {
        return { ok: true as const, item: existing, duplicate: true as const };
      }
    }
    return { ok: false as const, error: error.message };
  }

  return { ok: true as const, item: data, duplicate: false as const };
}

export async function listWishlistItems(shopDomain: string, shopifyCustomerId: string) {
  const { data, error } = await supabase
    .from("wishlist_items")
    .select(
      "id, catalog_item_id, film_id, shopify_variant_id, title_snapshot, created_at, notify_requested_at, source",
    )
    .eq("shop_domain", shopDomain)
    .eq("shopify_customer_id", shopifyCustomerId)
    .order("created_at", { ascending: false });

  if (error) {
    return { ok: false as const, error: error.message, items: [] as WishlistRow[] };
  }

  return { ok: true as const, items: (data ?? []) as WishlistRow[] };
}

export type WishlistRow = {
  id: string;
  catalog_item_id: string | null;
  film_id: string | null;
  shopify_variant_id: string | null;
  title_snapshot: string;
  created_at: string;
  notify_requested_at: string | null;
  source: string | null;
};

/** Current commercial snapshot from `catalog_items` (same signals as intelligence search). */
export type WishlistCommercialSnapshot = {
  catalogFound: boolean;
  /** Row exists but `active` is false — treat as stale / delisted. */
  catalogActive: boolean | null;
  displayTitle: string;
  price: number | null;
  currency: string | null;
  priceLabel: string | null;
  availabilityStatus: string | null;
  rankingBucket: string;
  availabilityLabel: string;
  /** High-level channel: store, supplier, preorder, etc. */
  sourceChannel: string;
  mediaReleaseDate: string | null;
  filmReleased: string | null;
  format: string | null;
  shopifyLinked: boolean;
  sourceType: string | null;
};

export type WishlistRowEnriched = WishlistRow & {
  commercial: WishlistCommercialSnapshot;
};

type CatalogOfferLike = {
  title: string;
  edition_title?: string | null;
  format?: string | null;
  calculated_sale_price?: number | null;
  supplier_currency?: string | null;
  availability_status?: string | null;
  supplier_stock_status?: number | null;
  shopify_product_id?: string | null;
  shopify_variant_id?: string | null;
  media_release_date?: string | null;
  film_released?: string | null;
  active?: boolean | null;
  source_type?: string | null;
};

function displayCatalogTitle(row: CatalogOfferLike) {
  const t = String(row.title || "").trim();
  const e = String(row.edition_title || "").trim();
  return e ? `${t} ${e}`.trim() : t;
}

function formatMoney(amount: number | null, currency: string | null) {
  if (amount == null || Number.isNaN(Number(amount))) return null;
  const n = Number(amount);
  const c = (currency || "GBP").toUpperCase();
  if (c === "GBP") return `£${n.toFixed(2)}`;
  try {
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: c,
    }).format(n);
  } catch {
    return `${n.toFixed(2)} ${c}`;
  }
}

function buildCommercialSnapshot(
  row: CatalogOfferLike | null,
  titleFallback: string,
): WishlistCommercialSnapshot {
  if (!row) {
    return {
      catalogFound: false,
      catalogActive: null,
      displayTitle: titleFallback,
      price: null,
      currency: null,
      priceLabel: null,
      availabilityStatus: null,
      rankingBucket: "out_of_stock",
      availabilityLabel: "Catalog row not found (it may have been removed)",
      sourceChannel: "Unknown",
      mediaReleaseDate: null,
      filmReleased: null,
      format: null,
      shopifyLinked: false,
      sourceType: null,
    };
  }

  const offerLike = {
    format: row.format,
    availability_status: row.availability_status,
    supplier_stock_status:
      row.supplier_stock_status != null ? Number(row.supplier_stock_status) : 0,
    shopify_product_id: row.shopify_product_id,
    shopify_variant_id: row.shopify_variant_id,
    media_release_date: row.media_release_date,
  };

  const rankingBucket = getOfferRankingBucket(offerLike);
  const hasShopify = !!(row.shopify_variant_id || row.shopify_product_id);
  const price =
    row.calculated_sale_price != null ? Number(row.calculated_sale_price) : null;

  let availabilityLabel: string;
  switch (rankingBucket) {
    case "store_in_stock":
      availabilityLabel = "In stock (store)";
      break;
    case "supplier_in_stock":
      availabilityLabel = "Available (supplier inventory)";
      break;
    case "preorder":
      availabilityLabel = "Pre-order / upcoming release";
      break;
    default:
      availabilityLabel = hasShopify
        ? "Store listing — stock not confirmed from inventory signals"
        : "Not currently available from tracked stock";
      break;
  }

  if (row.active === false) {
    availabilityLabel = "No longer active in catalog";
  }

  let sourceChannel: string;
  if (rankingBucket === "preorder") {
    sourceChannel = "Pre-order";
  } else if (rankingBucket === "store_in_stock") {
    sourceChannel = "Store";
  } else if (rankingBucket === "supplier_in_stock") {
    sourceChannel = "Supplier";
  } else if (hasShopify) {
    sourceChannel = "Store listing";
  } else {
    sourceChannel = "Supplier / catalog";
  }

  const st = String(row.source_type || "").toLowerCase();
  if (st === "shopify" && sourceChannel === "Supplier / catalog") {
    sourceChannel = "Store listing";
  }

  return {
    catalogFound: true,
    catalogActive: row.active !== false,
    displayTitle: displayCatalogTitle(row) || titleFallback,
    price,
    currency: row.supplier_currency ?? "GBP",
    priceLabel: formatMoney(price, row.supplier_currency ?? "GBP"),
    availabilityStatus: row.availability_status ?? null,
    rankingBucket,
    availabilityLabel,
    sourceChannel,
    mediaReleaseDate: row.media_release_date ?? null,
    filmReleased: row.film_released ?? null,
    format: row.format ?? null,
    shopifyLinked: hasShopify,
    sourceType: row.source_type ?? null,
  };
}

/**
 * Enrich wishlist rows with live `catalog_items` pricing and availability (batch).
 * Does not change storage; uses the same catalog fields as intelligence search / lookup.
 */
export async function enrichWishlistItemsWithCatalog(
  rows: WishlistRow[],
): Promise<WishlistRowEnriched[]> {
  const ids = Array.from(
    new Set(
      rows
        .map((r) => r.catalog_item_id)
        .filter((id): id is string => Boolean(id && String(id).trim())),
    ),
  );

  if (!ids.length) {
    return rows.map((r) => ({
      ...r,
      commercial: buildCommercialSnapshot(null, r.title_snapshot),
    }));
  }

  const { data: catalogRows, error } = await supabase
    .from("catalog_items")
    .select(
      `
      id,
      title,
      edition_title,
      format,
      calculated_sale_price,
      supplier_currency,
      availability_status,
      supplier_stock_status,
      source_type,
      shopify_product_id,
      shopify_variant_id,
      media_release_date,
      film_released,
      active
    `,
    )
    .in("id", ids);

  if (error) {
    return rows.map((r) => ({
      ...r,
      commercial: buildCommercialSnapshot(null, r.title_snapshot),
    }));
  }

  const byId = new Map<string, CatalogOfferLike>(
    (catalogRows || []).map((row: CatalogOfferLike & { id: string }) => [
      row.id,
      row,
    ]),
  );

  return rows.map((r) => {
    const cid = r.catalog_item_id ? String(r.catalog_item_id).trim() : "";
    const cat = cid ? byId.get(cid) : undefined;
    return {
      ...r,
      commercial: buildCommercialSnapshot(cat ?? null, r.title_snapshot),
    };
  });
}

export async function removeWishlistItem(
  shopDomain: string,
  shopifyCustomerId: string,
  wishlistItemId: string,
) {
  const { data: existing, error: selErr } = await supabase
    .from("wishlist_items")
    .select("id")
    .eq("id", wishlistItemId)
    .eq("shop_domain", shopDomain)
    .eq("shopify_customer_id", shopifyCustomerId)
    .maybeSingle();

  if (selErr) {
    return { ok: false as const, error: selErr.message };
  }
  if (!existing) {
    return { ok: false as const, error: "Not found" };
  }

  const { error: delErr } = await supabase
    .from("wishlist_items")
    .delete()
    .eq("id", wishlistItemId)
    .eq("shop_domain", shopDomain)
    .eq("shopify_customer_id", shopifyCustomerId);

  if (delErr) {
    return { ok: false as const, error: delErr.message };
  }

  return { ok: true as const };
}

/**
 * Future notify-me (no notifications in v1). Sets notify_requested_at.
 */
export async function setNotifyRequested(
  shopDomain: string,
  shopifyCustomerId: string,
  wishlistItemId: string,
) {
  const { data, error } = await supabase
    .from("wishlist_items")
    .update({ notify_requested_at: new Date().toISOString() })
    .eq("id", wishlistItemId)
    .eq("shop_domain", shopDomain)
    .eq("shopify_customer_id", shopifyCustomerId)
    .select("id, notify_requested_at")
    .maybeSingle();

  if (error) {
    return { ok: false as const, error: error.message };
  }
  if (!data) {
    return { ok: false as const, error: "Not found" };
  }
  return { ok: true as const, item: data };
}
