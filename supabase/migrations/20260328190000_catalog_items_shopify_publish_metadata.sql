-- Ad hoc catalog → Shopify publish flow metadata (not store sync / inventory sync).
alter table public.catalog_items
  add column if not exists published_to_shopify boolean;

alter table public.catalog_items
  add column if not exists shopify_published_at timestamptz;

comment on column public.catalog_items.published_to_shopify is
  'True when this row was the source record for an ad hoc Shopify product create (catalog publish job).';

comment on column public.catalog_items.shopify_published_at is
  'UTC timestamp when this row was published to Shopify via the ad hoc catalog publish flow.';
