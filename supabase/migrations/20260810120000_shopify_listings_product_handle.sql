-- Additive: persist canonical Shopify product handle for Ordering Agent product_url.
alter table public.shopify_listings
  add column if not exists product_handle text;

comment on column public.shopify_listings.product_handle is
  'Canonical Shopify product handle from Admin API (storefront path /products/{handle})';

create index if not exists shopify_listings_product_handle_idx
  on public.shopify_listings (product_handle)
  where product_handle is not null;
