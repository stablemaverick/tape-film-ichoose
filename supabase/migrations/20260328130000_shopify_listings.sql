-- Shopify listing mirror: one row per active variant with Admin API snapshot fields + catalog link.
-- Populated by jobs.shopify_store_sync; inventory job uses shopify_inventory_item_id + location.
-- Snapshot columns are Shopify-sourced only; they do not overwrite canonical supplier/catalog truth.
-- shopify_product_id, shopify_variant_id, shopify_inventory_item_id: raw Shopify GID strings.

create table if not exists public.shopify_listings (
  id uuid primary key default gen_random_uuid(),
  shop text not null,

  -- Identity (Shopify GIDs, verbatim from API)
  shopify_product_id text not null,
  shopify_variant_id text not null,

  -- Product-level snapshot (all nullable — Shopify may omit metafields)
  product_title text,
  vendor text,
  product_status text,
  director_text text,
  studio_text text,
  film_released_raw text,
  film_released_date date,
  media_release_raw text,
  media_release_date date,
  published_to_online_store boolean,
  product_type text,

  -- Variant-level snapshot
  variant_title text,
  sku text,
  barcode text,
  price_amount numeric(18, 4),
  price_currency_code text,
  inventory_quantity integer,
  inventory_policy text,
  tracks_inventory boolean,

  -- InventoryItem-level snapshot (shopify_inventory_item_id is GID string)
  shopify_inventory_item_id text,
  unit_cost_amount numeric(18, 4),
  unit_cost_currency_code text,

  -- Link to existing catalog row (no catalog_items inserts from this pipeline)
  catalog_item_id uuid references public.catalog_items (id) on delete set null,
  match_method text,
  match_status text,
  match_value text,

  last_store_sync_at timestamptz not null default now(),
  last_store_sync_error text,
  last_inventory_compare_at timestamptz,
  last_inventory_apply_at timestamptz,
  last_inventory_apply_error text
);

create unique index if not exists shopify_listings_shop_variant_uidx
  on public.shopify_listings (shop, shopify_variant_id);

create index if not exists shopify_listings_catalog_item_id_idx
  on public.shopify_listings (catalog_item_id) where catalog_item_id is not null;

create index if not exists shopify_listings_barcode_idx
  on public.shopify_listings (barcode) where barcode is not null;

create index if not exists shopify_listings_shopify_product_id_idx
  on public.shopify_listings (shopify_product_id);

create index if not exists shopify_listings_film_released_date_idx
  on public.shopify_listings (film_released_date)
  where film_released_date is not null;

create index if not exists shopify_listings_media_release_date_idx
  on public.shopify_listings (media_release_date)
  where media_release_date is not null;

create index if not exists shopify_listings_match_status_idx
  on public.shopify_listings (match_status)
  where match_status is not null;

comment on table public.shopify_listings is 'Shopify product/variant/inventory snapshot + optional catalog_items link; no supplier import';
comment on column public.shopify_listings.match_method is 'shopify_variant_id | barcode | title | unmatched | ignored (SKU not used)';
comment on column public.shopify_listings.match_status is 'Operational: matched | unmatched | ambiguous | ignored';
comment on column public.shopify_listings.match_value is 'Opaque ops detail (e.g. catalog id, barcode:key, candidate counts)';
comment on column public.shopify_listings.director_text is 'Shopify metafield custom.director value snapshot';
comment on column public.shopify_listings.studio_text is 'Shopify metafield custom.studio value snapshot';
comment on column public.shopify_listings.film_released_raw is 'Verbatim custom.film_released metafield (fallback when date parse fails or for audit)';
comment on column public.shopify_listings.film_released_date is 'Parsed date from custom.film_released when unambiguous; null if absent or unparseable';
comment on column public.shopify_listings.media_release_raw is 'Verbatim custom.media_release_date metafield';
comment on column public.shopify_listings.media_release_date is 'Parsed date from custom.media_release_date when unambiguous; null if absent or unparseable';
comment on column public.shopify_listings.published_to_online_store is 'True when product.publishedAt is set (Online Store publication snapshot)';
comment on column public.shopify_listings.tracks_inventory is 'inventoryItem.tracked from Shopify';
