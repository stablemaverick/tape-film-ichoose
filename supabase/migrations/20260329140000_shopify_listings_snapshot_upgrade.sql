-- Upgrade path if an older revision created minimal shopify_listings or text-only metafield columns.
-- Safe on fresh DBs (IF NOT EXISTS / guarded renames).

alter table public.shopify_listings add column if not exists vendor text;
alter table public.shopify_listings add column if not exists product_status text;
alter table public.shopify_listings add column if not exists director_text text;
alter table public.shopify_listings add column if not exists studio_text text;
alter table public.shopify_listings add column if not exists film_released_raw text;
alter table public.shopify_listings add column if not exists media_release_raw text;
alter table public.shopify_listings add column if not exists price_amount numeric(18, 4);
alter table public.shopify_listings add column if not exists price_currency_code text;
alter table public.shopify_listings add column if not exists inventory_policy text;
alter table public.shopify_listings add column if not exists unit_cost_amount numeric(18, 4);
alter table public.shopify_listings add column if not exists unit_cost_currency_code text;

-- Prefer rename legacy inventory_item_id → shopify_inventory_item_id (do not add the new name first).
do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'shopify_listings' and column_name = 'inventory_item_id'
  )
  and not exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'shopify_listings' and column_name = 'shopify_inventory_item_id'
  ) then
    alter table public.shopify_listings rename column inventory_item_id to shopify_inventory_item_id;
  end if;
end $$;

alter table public.shopify_listings add column if not exists shopify_inventory_item_id text;

-- Legacy text columns named film_released_date / media_release_date → copy to *_raw, drop text, then typed date.
do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'shopify_listings'
      and column_name = 'film_released_date' and data_type = 'text'
  ) then
    update public.shopify_listings
    set film_released_raw = coalesce(film_released_raw, film_released_date)
    where film_released_date is not null;
    alter table public.shopify_listings drop column film_released_date;
  end if;
end $$;

do $$
begin
  if exists (
    select 1 from information_schema.columns
    where table_schema = 'public' and table_name = 'shopify_listings'
      and column_name = 'media_release_date' and data_type = 'text'
  ) then
    update public.shopify_listings
    set media_release_raw = coalesce(media_release_raw, media_release_date)
    where media_release_date is not null;
    alter table public.shopify_listings drop column media_release_date;
  end if;
end $$;

alter table public.shopify_listings add column if not exists film_released_date date;
alter table public.shopify_listings add column if not exists media_release_date date;

-- Migrate legacy single "price" text into price_amount when present and price_amount is still null
update public.shopify_listings
set price_amount = nullif(trim(price), '')::numeric
where exists (
  select 1 from information_schema.columns
  where table_schema = 'public' and table_name = 'shopify_listings' and column_name = 'price'
)
and price is not null
and price ~ '^[0-9]+(\.[0-9]+)?$'
and price_amount is null;

alter table public.shopify_listings drop column if exists price;

create index if not exists shopify_listings_shopify_product_id_idx
  on public.shopify_listings (shopify_product_id);

create index if not exists shopify_listings_film_released_date_idx
  on public.shopify_listings (film_released_date)
  where film_released_date is not null;

create index if not exists shopify_listings_media_release_date_idx
  on public.shopify_listings (media_release_date)
  where media_release_date is not null;

-- Operational + snapshot refinements (match status, errors, Online Store / inventory flags)
alter table public.shopify_listings add column if not exists published_to_online_store boolean;
alter table public.shopify_listings add column if not exists product_type text;
alter table public.shopify_listings add column if not exists tracks_inventory boolean;
alter table public.shopify_listings add column if not exists match_status text;
alter table public.shopify_listings add column if not exists match_value text;
alter table public.shopify_listings add column if not exists last_store_sync_error text;
alter table public.shopify_listings add column if not exists last_inventory_apply_error text;

create index if not exists shopify_listings_match_status_idx
  on public.shopify_listings (match_status)
  where match_status is not null;
