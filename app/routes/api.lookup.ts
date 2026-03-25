import { authenticate } from "../shopify.server";
import { supabase } from "../lib/supabase.server";

type ShopifyResult = {
  id: string;
  source: "shopify";
  title: string;
  handle: string;
  vendor?: string | null;
  price?: string | null;
  variantId?: string | null;
  sku?: string | null;
  barcode?: string | null;
  variantTitle?: string | null;
  inventory?: number | null;
  status?: string | null;
  inventoryPolicy?: string | null;
  director?: string | null;
  studio?: string | null;
  filmReleased?: string | null;
  mediaReleaseDate?: string | null;
  preorder?: boolean;
  backorder?: boolean;
  displaySource: "TAPE! Store";
};

type CatalogResult = {
  id: string;
  source: "catalog";
  title: string;
  baseTitle?: string | null;
  editionTitle?: string | null;
  format?: string | null;
  director?: string | null;
  studio?: string | null;
  filmReleased?: string | null;
  mediaReleaseDate?: string | null;
  barcode?: string | null;
  sku?: string | null;
  supplier?: string | null;
  supplierSku?: string | null;
  supplierPriority?: number | null;
  supplierCurrency?: string | null;
  costPrice?: number | null;
  pricingSource?: string | null;
  calculatedSalePrice?: number | null;
  availabilityStatus?: string | null;
  supplierStockStatus?: string | null;
  shopifyProductId?: string | null;
  shopifyVariantId?: string | null;
  displaySource: "Stock with Supplier";
};

type LookupResult = ShopifyResult | CatalogResult;

function normalizeKey(value: string | null | undefined) {
  return (value || "").trim().toLowerCase();
}

function fillMissing<T>(primary: T | null | undefined, fallback: T | null | undefined) {
  if (primary === null || primary === undefined || primary === "") {
    return fallback ?? primary;
  }
  return primary;
}

function enrichShopifyFromCatalog(shopify: ShopifyResult, catalog: CatalogResult): ShopifyResult {
  return {
    ...shopify,
    director: fillMissing(shopify.director, catalog.director),
    studio: fillMissing(shopify.studio, catalog.studio),
    filmReleased: fillMissing(shopify.filmReleased, catalog.filmReleased),
    mediaReleaseDate: fillMissing(shopify.mediaReleaseDate, catalog.mediaReleaseDate),
  };
}

function getCatalogDedupKey(item: CatalogResult) {
  const barcodeKey = normalizeKey(item.barcode);
  if (barcodeKey) return `barcode:${barcodeKey}`;

  const skuKey = normalizeKey(item.supplierSku || item.sku);
  if (skuKey) return `sku:${skuKey}`;

  return `title:${(item.title || "").trim().toLowerCase()}`;
}

function pickBetterCatalogResult(a: CatalogResult, b: CatalogResult) {
  const stockA = Number(a.supplierStockStatus || 0);
  const stockB = Number(b.supplierStockStatus || 0);

  const aHasStock = stockA > 0;
  const bHasStock = stockB > 0;

  // If only one supplier has stock, always prefer the one with stock
  if (aHasStock !== bHasStock) {
    return aHasStock ? a : b;
  }

  // If both suppliers have at least some stock, only let price win
  // when at least one supplier has 3+ copies available
  const priceCanDecide = stockA >= 3 || stockB >= 3;

  if (priceCanDecide) {
    const costA = Number(a.costPrice ?? 999999);
    const costB = Number(b.costPrice ?? 999999);

    if (costA !== costB) {
      return costA < costB ? a : b;
    }
  }

  // Otherwise, or if price is the same, prefer the supplier with more stock
  if (stockA !== stockB) {
    return stockA > stockB ? a : b;
  }

  // Final tie-breaker: supplier priority
  const priorityA = Number(a.supplierPriority ?? 999);
  const priorityB = Number(b.supplierPriority ?? 999);

  if (priorityA !== priorityB) {
    return priorityA < priorityB ? a : b;
  }

  return a;
}

export async function loader({ request }: { request: Request }) {
  try {
    const url = new URL(request.url);
    const q = url.searchParams.get("q")?.trim() || "";

    if (!q) {
      return Response.json({ results: [] });
    }

    const { admin } = await authenticate.admin(request);

    // 1. Shopify search
    const shopifyResponse = await admin.graphql(
      `#graphql
      query SearchProducts($query: String!) {
        products(first: 50, query: $query) {
          edges {
            node {
              id
              title
              handle
              vendor
              totalInventory

              filmReleased: metafield(namespace: "custom", key: "film_released") {
                value
              }

              studio: metafield(namespace: "custom", key: "studio") {
                value
              }

              director: metafield(namespace: "custom", key: "director") {
                value
              }

              preorder: metafield(namespace: "custom", key: "preorder") {
                value
              }

              backorder: metafield(namespace: "custom", key: "backorder") {
                value
              }

              mediaReleaseDate: metafield(namespace: "custom", key: "media_release_date") {
                value
              }

              variants(first: 1) {
                edges {
                  node {
                    id
                    title
                    price
                    sku
                    barcode
                    inventoryQuantity
                    inventoryPolicy
                  }
                }
              }
            }
          }
        }
      }
      `,
      {
        variables: {
          query: `title:*${q}*`,
        },
      },
    );

    const shopifyJson = await shopifyResponse.json();

    const shopifyResults: ShopifyResult[] =
      shopifyJson.data?.products?.edges?.map((edge: any) => {
        const node = edge.node;
        const variant = node.variants.edges[0]?.node;

        const preorder = node.preorder?.value === "true";
        const backorder = node.backorder?.value === "true";
        const inventoryPolicy = variant?.inventoryPolicy || "DENY";
        
        let status = "out_of_stock";
        
        if (node.totalInventory > 0) {
          status = "in_stock";
        } else if (preorder) {
          status = "preorder";
        } else if (backorder) {
          status = "backorder";
        } else if (inventoryPolicy === "CONTINUE") {
          status = "continue_selling";
        }

        return {
          id: node.id,
          source: "shopify",
          title: node.title,
          handle: node.handle,
          vendor: node.vendor,
          price: variant?.price,
          variantId: variant?.id,
          sku: variant?.sku,
          barcode: variant?.barcode,
          variantTitle: variant?.title,
          inventory: node.totalInventory,
          status,
          inventoryPolicy,
          director: node.director?.value || null,
          studio: node.studio?.value || null,
          filmReleased: node.filmReleased?.value || null,
          mediaReleaseDate: node.mediaReleaseDate?.value || null,
          preorder,
          backorder,
          displaySource: "TAPE! Store",
        };
      }) || [];

    // 2. Supabase search
    const { data: catalogData, error: catalogError } = await supabase
      .from("catalog_items")
      .select(`
        id,
        title,
        edition_title,
        format,
        director,
        studio,
        film_released,
        media_release_date,
        barcode,
        sku,
        supplier,
        supplier_sku,
        supplier_currency,
        cost_price,
        pricing_source,
        calculated_sale_price,
        availability_status,
        supplier_stock_status,
        supplier_priority,
        source_type,
        shopify_product_id,
        shopify_variant_id,
        active
      `)
      .eq("active", true)
      .or(
        `title.ilike.%${q}%,edition_title.ilike.%${q}%,director.ilike.%${q}%,studio.ilike.%${q}%,barcode.ilike.%${q}%,sku.ilike.%${q}%`,
      )
      .limit(100);

    if (catalogError) {
      return Response.json(
        {
          error: "Supabase query failed",
          details: catalogError.message,
        },
        { status: 500 },
      );
    }

    const catalogResults: CatalogResult[] = (catalogData || []).map((item) => ({
      id: item.id,
      source: "catalog",
      title: item.edition_title ? `${item.title} ${item.edition_title}` : item.title,
      baseTitle: item.title,
      editionTitle: item.edition_title,
      format: item.format,
      director: item.director,
      studio: item.studio,
      filmReleased: item.film_released,
      mediaReleaseDate: item.media_release_date,
      barcode: item.barcode,
      sku: item.sku,
      supplier: item.supplier,
      supplierSku: item.supplier_sku,
      supplierPriority: item.supplier_priority,
      supplierCurrency: item.supplier_currency,
      costPrice: item.cost_price,
      pricingSource: item.pricing_source,
      calculatedSalePrice: item.calculated_sale_price,
      availabilityStatus: item.availability_status,
      supplierStockStatus: item.supplier_stock_status,
      shopifyProductId: item.shopify_product_id,
      shopifyVariantId: item.shopify_variant_id,
      displaySource: "Stock with Supplier",
    }));
    
    const bestCatalogByKey = new Map<string, CatalogResult>();
    
    for (const item of catalogResults) {
      const key = getCatalogDedupKey(item);
      const existing = bestCatalogByKey.get(key);
    
      if (!existing) {
        bestCatalogByKey.set(key, item);
      } else {
        bestCatalogByKey.set(key, pickBetterCatalogResult(existing, item));
      }
    }
    
    const dedupedSupplierCatalogResults = Array.from(bestCatalogByKey.values());
    
    const bestCatalogByBarcode = new Map(
      dedupedSupplierCatalogResults
        .filter((item) => normalizeKey(item.barcode))
        .map((item) => [normalizeKey(item.barcode), item]),
    );
    
    const enrichedShopifyResults = shopifyResults.map((item) => {
      const barcodeKey = normalizeKey(item.barcode);
      const matchingCatalog = barcodeKey ? bestCatalogByBarcode.get(barcodeKey) : undefined;
    
      if (!matchingCatalog) {
        return item;
      }
    
      return enrichShopifyFromCatalog(item, matchingCatalog);
    });

    // 3. Dedupe: Shopify wins if barcode or SKU matches
    const shopifyBarcodeSet = new Set(
      shopifyResults.map((item) => normalizeKey(item.barcode)).filter(Boolean),
    );

    const shopifySkuSet = new Set(
      shopifyResults.map((item) => normalizeKey(item.sku)).filter(Boolean),
    );

    const shopifyByBarcode = new Map(
      enrichedShopifyResults
        .filter((item) => normalizeKey(item.barcode))
        .map((item) => [normalizeKey(item.barcode), item]),
    );
    
    const shopifyBySku = new Map(
      enrichedShopifyResults
        .filter((item) => normalizeKey(item.sku))
        .map((item) => [normalizeKey(item.sku), item]),
    );
    
    function isShopifySellable(item: ShopifyResult | undefined) {
      if (!item) return false;
    
      return (
        item.status === "in_stock" ||
        item.status === "preorder" ||
        item.status === "backorder" ||
        item.status === "continue_selling"
      );
    }
    
    const dedupedCatalogResults = dedupedSupplierCatalogResults.filter((item) => {
      const barcodeKey = normalizeKey(item.barcode);
      const skuKey = normalizeKey(item.sku);
    
      const matchedShopify =
        (barcodeKey ? shopifyByBarcode.get(barcodeKey) : undefined) ||
        (skuKey ? shopifyBySku.get(skuKey) : undefined);
    
      const supplierQty = Number(item.supplierStockStatus || 0);
      const explicitShopifyLink = !!item.shopifyVariantId || !!item.shopifyProductId;
    
      // If Shopify match exists and is sellable, suppress supplier duplicate.
      if (matchedShopify && isShopifySellable(matchedShopify)) {
        return false;
      }
    
      // If Shopify is truly out of stock but supplier has stock, keep supplier item.
      if (matchedShopify && matchedShopify.status === "out_of_stock" && supplierQty > 0) {
        return true;
      }
    
      // If there is an explicit Shopify link but no sellable Shopify state, suppress by default.
      if (explicitShopifyLink && supplierQty <= 0) {
        return false;
      }
    
      // No usable Shopify match, keep supplier result.
      return true;
    });

    // 4. Combined results
    const results: LookupResult[] = [...enrichedShopifyResults, ...dedupedCatalogResults];
    
    function getPriority(item: any) {
      if (item.source === "shopify") {
        if (item.status === "in_stock") return 1;
        if (item.status === "preorder" || item.status === "backorder" || item.status === "continue_selling") return 2;
        if (item.status === "out_of_stock") return 5;
        return 5;
      }
    
      if (item.source === "catalog") {
        const qty = Number(item.supplierStockStatus || 0);
        if (qty > 0) return 3;
        return 6;
      }
    
      return 99;
    }
    
    results.sort((a, b) => {
      const pa = getPriority(a);
      const pb = getPriority(b);
    
      if (pa !== pb) {
        return pa - pb;
      }
    
      const titleA = (a.title || "").toLowerCase();
      const titleB = (b.title || "").toLowerCase();
    
      return titleA.localeCompare(titleB);
    });

    const bestResult = results.length > 0 ? results[0] : null;
    
    
    const summary = {
      tapeStock: enrichedShopifyResults.filter(r => r.status === "in_stock").length,
      supplierStock: dedupedCatalogResults.filter(
        r => Number(r.supplierStockStatus || 0) > 0
      ).length
    };
    
    return Response.json({
      bestResult,
      results,
      summary,
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