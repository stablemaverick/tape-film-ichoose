-- Additive Shopify listing snapshots for film vs music/vinyl domain gating.
-- Does not mutate Shopify; store sync populates these from Admin API reads.

alter table public.shopify_listings
  add column if not exists media_format text;

alter table public.shopify_listings
  add column if not exists collection_handles text;

comment on column public.shopify_listings.media_format is
  'Shopify metafield custom.format snapshot (e.g. Vinyl, Blu-ray)';

comment on column public.shopify_listings.collection_handles is
  'Comma-separated Shopify collection handles for the product (e.g. soundtracks)';

-- Optional lightweight domain on releases for film II vs future music II.
alter table public.release_variants
  add column if not exists product_domain text;

comment on column public.release_variants.product_domain is
  'Inventory domain: film (default/null) | music_vinyl | future domains. Null treated as film.';
