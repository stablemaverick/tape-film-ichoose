/** Shared row shapes for intelligence-search E2E mocks */

export type FilmFixture = {
  id: string;
  title: string;
  director: string | null;
  film_released: string | null;
  tmdb_title: string | null;
  genres: string | null;
  top_cast: string | null;
};

export type OfferFixture = {
  id: string;
  title: string;
  edition_title?: string | null;
  format?: string | null;
  studio?: string | null;
  supplier?: string | null;
  supplier_sku?: string | null;
  barcode?: string | null;
  cost_price?: number | null;
  calculated_sale_price?: number | null;
  supplier_stock_status?: number | null;
  supplier_priority?: number | null;
  availability_status?: string | null;
  shopify_product_id?: string | null;
  shopify_variant_id?: string | null;
  media_release_date?: string | null;
  active?: boolean | null;
  film_id?: string | null;
};

export type PopularityFixture = {
  film_id: string;
  popularity_score: number;
  orders_count?: number;
  units_sold?: number;
  last_sold_at?: string | null;
};
