-- Tape Agent v1: minimal wishlist (one row per catalog item per customer per shop).
-- notify_requested_at reserved for future notify-me; no jobs in v1.

create table if not exists public.wishlist_items (
  id uuid primary key default gen_random_uuid(),
  shop_domain text not null,
  shopify_customer_id text not null,
  catalog_item_id uuid references public.catalog_items (id) on delete cascade,
  film_id uuid,
  shopify_variant_id text,
  title_snapshot text not null,
  created_at timestamptz not null default now(),
  notify_requested_at timestamptz,
  source text
);

comment on table public.wishlist_items is 'Customer wishlist lines; keyed by Shopify customer GID + shop.';
comment on column public.wishlist_items.notify_requested_at is 'Future: back-in-stock intent; no automation in v1.';

create unique index if not exists wishlist_items_shop_customer_catalog_unique
  on public.wishlist_items (shop_domain, shopify_customer_id, catalog_item_id)
  where catalog_item_id is not null;

create index if not exists wishlist_items_shop_customer_idx
  on public.wishlist_items (shop_domain, shopify_customer_id);

create index if not exists wishlist_items_catalog_item_idx
  on public.wishlist_items (catalog_item_id);
