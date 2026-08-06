-- Bootstrap operational replay source tables for controlled Phase 3b validation.
-- Use only in temporary/local test projects.
--
-- Needed so real service entry points can run with synthetic replay data:
--   - shopify_store_sync_service -> shopify_listings
--   - normalize_offers_service   -> staging_moovies_raw / staging_lasgo_raw / staging_supplier_offers

begin;

create extension if not exists pgcrypto;

-- ---------------------------------------------------------------------------
-- Shopify listing mirror (subset compatible with service writes)
-- ---------------------------------------------------------------------------
create table if not exists public.shopify_listings (
  id uuid primary key default gen_random_uuid(),
  shop text not null,
  shopify_product_id text not null,
  shopify_variant_id text not null,
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
  variant_title text,
  sku text,
  barcode text,
  price_amount numeric(18, 4),
  price_currency_code text,
  inventory_quantity integer,
  inventory_policy text,
  tracks_inventory boolean,
  shopify_inventory_item_id text,
  unit_cost_amount numeric(18, 4),
  unit_cost_currency_code text,
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

-- ---------------------------------------------------------------------------
-- Moovies raw staging
-- ---------------------------------------------------------------------------
create table if not exists public.staging_moovies_raw (
  id uuid primary key default gen_random_uuid(),
  import_batch_id uuid not null,
  imported_at timestamptz not null default now(),
  source_filename text,
  row_number integer,
  raw_title text,
  raw_barcode text,
  raw_format text,
  raw_price text,
  raw_qty text,
  raw_release text,
  raw_studio text,
  raw_director text,
  raw_sku text,
  raw_payload jsonb not null default '{}'::jsonb,
  supplier text not null default 'moovies',
  upsert_key text not null,
  source_file_hash text,
  raw_status text,
  raw_category text,
  raw_country_of_origin text
);

create unique index if not exists staging_moovies_raw_supplier_upsert_uidx
  on public.staging_moovies_raw (supplier, upsert_key);

create index if not exists staging_moovies_raw_batch_idx
  on public.staging_moovies_raw (import_batch_id);

-- ---------------------------------------------------------------------------
-- Lasgo raw staging
-- ---------------------------------------------------------------------------
create table if not exists public.staging_lasgo_raw (
  id uuid primary key default gen_random_uuid(),
  import_batch_id uuid not null,
  imported_at timestamptz not null default now(),
  source_filename text,
  row_number integer,
  raw_artist text,
  raw_title text,
  raw_ean text,
  raw_format_l2 text,
  raw_free_stock text,
  raw_selling_price_sterling text,
  raw_label text,
  raw_release_date text,
  raw_payload jsonb not null default '{}'::jsonb
);

create index if not exists staging_lasgo_raw_batch_idx
  on public.staging_lasgo_raw (import_batch_id);

-- ---------------------------------------------------------------------------
-- Normalized supplier offers staging
-- ---------------------------------------------------------------------------
create table if not exists public.staging_supplier_offers (
  id uuid primary key default gen_random_uuid(),
  import_batch_id uuid not null,
  imported_at timestamptz not null default now(),
  supplier text not null,
  source_filename text,
  source_row_number integer,
  supplier_sku text,
  barcode text,
  title text not null,
  normalized_title text,
  edition_title text,
  format text,
  media_type text default 'film',
  director text,
  studio text,
  label text,
  media_release_date date,
  supplier_stock_status integer,
  availability_status text,
  supplier_currency text,
  cost_price numeric(18, 4),
  calculated_sale_price numeric(18, 4),
  source_priority integer,
  source_type text default 'catalog',
  active boolean not null default true,
  harmonized_title text,
  harmonized_format text,
  harmonized_director text,
  harmonized_studio text,
  harmonized_from_supplier text,
  harmonized_at timestamptz,
  published_to_catalog boolean not null default false,
  published_catalog_item_id uuid,
  raw_source_id uuid,
  raw_source_table text not null,
  shopify_product_id text,
  shopify_variant_id text
);

create unique index if not exists staging_supplier_offers_supplier_barcode_key
  on public.staging_supplier_offers (supplier, barcode);

create index if not exists staging_supplier_offers_batch_idx
  on public.staging_supplier_offers (import_batch_id);

-- ---------------------------------------------------------------------------
-- Bring bootstrap catalog_items closer to operational shape for store-sync writes
-- ---------------------------------------------------------------------------
alter table public.catalog_items add column if not exists edition_title text;
alter table public.catalog_items add column if not exists format text;
alter table public.catalog_items add column if not exists availability_status text default 'supplier_stock';
alter table public.catalog_items add column if not exists supplier_stock_status text;
alter table public.catalog_items add column if not exists source_type text default 'catalog';
alter table public.catalog_items add column if not exists supplier_sku text;
alter table public.catalog_items add column if not exists supplier_currency text default 'GBP';
alter table public.catalog_items add column if not exists cost_price numeric(18, 4);
alter table public.catalog_items add column if not exists calculated_sale_price numeric(18, 4);
alter table public.catalog_items add column if not exists media_release_date date;
alter table public.catalog_items add column if not exists shopify_product_id text;
alter table public.catalog_items add column if not exists shopify_variant_id text;
alter table public.catalog_items add column if not exists supplier_last_seen_at timestamptz;

commit;

