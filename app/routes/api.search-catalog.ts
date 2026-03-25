import { supabase } from "../lib/supabase.server";

export async function loader({ request }: { request: Request }) {
  try {
    const url = new URL(request.url);
    const q = url.searchParams.get("q")?.trim() || "";

    if (!q) {
      return Response.json({ results: [] });
    }

    const { data, error } = await supabase
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
        source_type,
        shopify_product_id,
        shopify_variant_id,
        active
      `)
      .eq("active", true)
      .or(`title.ilike.%${q}%,edition_title.ilike.%${q}%,director.ilike.%${q}%,studio.ilike.%${q}%,barcode.ilike.%${q}%,sku.ilike.%${q}%`)
      .limit(20);

    if (error) {
      return Response.json(
        {
          error: "Supabase query failed",
          details: error.message,
        },
        { status: 500 },
      );
    }

    const results = (data || []).map((item) => ({
      id: item.id,
      source: "catalog",
      title: item.edition_title
        ? `${item.title} ${item.edition_title}`
        : item.title,
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
      supplierCurrency: item.supplier_currency,
      costPrice: item.cost_price,
      pricingSource: item.pricing_source,
      calculatedSalePrice: item.calculated_sale_price,
      availabilityStatus: item.availability_status,
      shopifyProductId: item.shopify_product_id,
      shopifyVariantId: item.shopify_variant_id,
      displaySource: "Stock with Supplier",
    }));

    return Response.json({ results });
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