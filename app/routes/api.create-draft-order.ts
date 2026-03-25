import { authenticate } from "../shopify.server";

type ShopifyLineItemInput = {
  source: "shopify";
  variantId: string;
  quantity: number;
};

type CustomCatalogLineItemInput = {
  source: "catalog";
  title: string;
  quantity: number;
  costGbp: number;
  productCode?: string;
  supplier?: string;
  format?: string;
  editionTitle?: string;
  sku?: string;
  barcode?: string;
  director?: string;
  studio?: string;
  filmReleased?: string;
  mediaReleaseDate?: string;
  catalogItemId?: string;
  filmId?: string;
  taxable?: boolean;
  requiresShipping?: boolean;
};

type IncomingLineItem = ShopifyLineItemInput | CustomCatalogLineItemInput;

function roundUpTo99(value: number): number {
  const roundedTo2 = Number(value.toFixed(2));
  const floorWhole = Math.floor(roundedTo2);
  const target = floorWhole + 0.99;

  if (roundedTo2 <= target) {
    return Number(target.toFixed(2));
  }

  return Number((floorWhole + 1 + 0.99).toFixed(2));
}

function calculateCustomCataloguePriceFromGBP(costGbp: number) {
  const audBase = costGbp * 2;
  const totalCost = audBase * 1.12;      // +12% shipping
  const preGstSale = totalCost * 1.32;   // +32% margin
  const finalSale = preGstSale * 1.10;   // +10% GST
  const roundedPrice = roundUpTo99(finalSale);

  return {
    costGbp: Number(costGbp.toFixed(2)),
    audBase: Number(audBase.toFixed(2)),
    totalCost: Number(totalCost.toFixed(2)),
    preGstSale: Number(preGstSale.toFixed(2)),
    finalSale: Number(finalSale.toFixed(2)),
    roundedPrice,
  };
}

function buildDraftLineTitle(item: CustomCatalogLineItemInput) {
  const baseTitle = String(item.title || "").trim();
  const edition = String(item.editionTitle || "").trim();
  const format = String(item.format || "").trim();

  const extras = [edition, format].filter(Boolean);

  if (!extras.length) {
    return baseTitle;
  }

  return `${baseTitle} — ${extras.join(" — ")}`;
}

function buildCustomAttributes(item: CustomCatalogLineItemInput) {
  return [
    { key: "source", value: "catalog" },

    ...(item.productCode ? [{ key: "Product code", value: item.productCode }] : []),
    ...(item.barcode ? [{ key: "Barcode", value: item.barcode }] : []),
    ...(item.director ? [{ key: "Director", value: item.director }] : []),
    ...(item.studio ? [{ key: "Studio", value: item.studio }] : []),
    ...(item.filmReleased ? [{ key: "Original release", value: item.filmReleased }] : []),
    ...(item.mediaReleaseDate ? [{ key: "Media release", value: item.mediaReleaseDate }] : []),
    ...(item.catalogItemId ? [{ key: "catalog_item_id", value: item.catalogItemId }] : []),
    ...(item.filmId ? [{ key: "film_id", value: item.filmId }] : []),
  ];
}
export async function action({ request }: { request: Request }) {
  try {
    const { admin } = await authenticate.admin(request);

    let body: any;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    const email = String(body.email || "").trim();
    const shippingTitle = String(body.shippingTitle || "Shipping").trim();
    const shippingAmountRaw = String(body.shippingAmount || "").trim();
    const incomingLineItems: IncomingLineItem[] = Array.isArray(body.lineItems)
      ? body.lineItems
      : [];

    if (!email) {
      return Response.json({ error: "Missing email" }, { status: 400 });
    }

    if (!incomingLineItems.length) {
      return Response.json({ error: "No line items supplied" }, { status: 400 });
    }

    const lineItems: any[] = [];
    const pricingBreakdown: any[] = [];

    for (const item of incomingLineItems) {
      if (item.source === "shopify") {
        if (!item.variantId || !Number.isFinite(item.quantity) || item.quantity < 1) {
          continue;
        }

        lineItems.push({
          variantId: item.variantId,
          quantity: item.quantity,
        });
      }

      if (item.source === "catalog") {
        if (!item.title || !Number.isFinite(item.quantity) || item.quantity < 1) {
          continue;
        }

        const costGbp = Number(item.costGbp);
        if (!Number.isFinite(costGbp) || costGbp <= 0) {
          continue;
        }

        const pricing = calculateCustomCataloguePriceFromGBP(costGbp);
        const lineTitle = buildDraftLineTitle(item);
        const customAttributes = buildCustomAttributes(item);
        
        lineItems.push({
          title: lineTitle,
          quantity: item.quantity,
          sku: item.productCode || undefined,
          taxable: item.taxable ?? true,
          requiresShipping: item.requiresShipping ?? true,
          originalUnitPriceWithCurrency: {
            amount: pricing.roundedPrice.toFixed(2),
            currencyCode: "AUD",
          },
          customAttributes,
        });
        
        
        pricingBreakdown.push({
          title: lineTitle,
          quantity: item.quantity,
          supplier: item.supplier || null,
          productCode: item.productCode || null,
          barcode: item.barcode || null,
          filmReleased: item.filmReleased || null,
          mediaReleaseDate: item.mediaReleaseDate || null,
          ...pricing,
        });
      }
    }

    if (!lineItems.length) {
      return Response.json(
        { error: "No valid line items after validation" },
        { status: 400 },
      );
    }

    const shippingAmount =
      shippingAmountRaw === "" ? null : Number.parseFloat(shippingAmountRaw);

    if (shippingAmountRaw !== "" && (!Number.isFinite(shippingAmount) || shippingAmount < 0)) {
      return Response.json(
        { error: "Shipping amount must be a valid number of 0 or more" },
        { status: 400 },
      );
    }

    const draftInput: any = {
      email,
      lineItems,
      presentmentCurrencyCode: "AUD",
    };

    if (shippingAmount !== null) {
      draftInput.shippingLine = {
        title: shippingTitle || "Shipping",
        priceWithCurrency: {
          amount: shippingAmount.toFixed(2),
          currencyCode: "AUD",
        },
      };
    }

    const response = await admin.graphql(
      `#graphql
      mutation CreateDraftOrder($input: DraftOrderInput!) {
        draftOrderCreate(input: $input) {
          draftOrder {
            id
            name
            invoiceUrl
            status
            subtotalPriceSet {
              shopMoney {
                amount
                currencyCode
              }
            }
            totalShippingPriceSet {
              shopMoney {
                amount
                currencyCode
              }
            }
            totalPriceSet {
              shopMoney {
                amount
                currencyCode
              }
            }
            lineItems(first: 50) {
              edges {
                node {
                  title
                  quantity
                  custom
                  sku
                  originalUnitPriceWithCurrency {
                    amount
                    currencyCode
                  }
                }
              }
            }
          }
          userErrors {
            field
            message
          }
        }
      }
      `,
      { variables: { input: draftInput } },
    );

    const raw = await response.text();

    let json: any;
    try {
      json = JSON.parse(raw);
    } catch {
      return Response.json(
        {
          error: "Shopify response was not JSON",
          raw,
        },
        { status: 500 },
      );
    }

    if (json.errors) {
      return Response.json(
        {
          error: "Shopify GraphQL errors",
          details: json.errors,
        },
        { status: 400 },
      );
    }

    const result = json?.data?.draftOrderCreate;

    if (result?.userErrors?.length) {
      return Response.json(
        {
          error: "Draft order userErrors",
          details: result.userErrors,
        },
        { status: 400 },
      );
    }

    return Response.json({
      draftOrder: result?.draftOrder || null,
      pricingBreakdown,
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