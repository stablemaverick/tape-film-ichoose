-- Additive: audit log of drafted product descriptions
-- (publish_catalog_to_shopify --with-descriptions / --descriptions-only). One row per barcode, latest draft.
begin;

create table if not exists public.product_description_drafts (
  id bigint generated always as identity primary key,
  barcode text not null unique,
  catalog_item_id text,
  shopify_product_id text,
  product_title text,
  distributor text,
  source_type text not null,
  source_urls jsonb not null default '[]'::jsonb,
  edition_match text,
  edition_match_confirmed boolean not null default false,
  synopsis_source text,
  synopsis text,
  edition_contents jsonb not null default '[]'::jsonb,
  technical_format jsonb not null default '[]'::jsonb,
  special_features jsonb not null default '[]'::jsonb,
  features_status text not null,
  notes text,
  review_reasons jsonb not null default '[]'::jsonb,
  description_html text,
  description_hash text,
  status text not null,
  error text,
  model text,
  est_cost_usd numeric(10, 4),
  drafted_at timestamptz not null default now(),
  applied_at timestamptz,
  updated_at timestamptz not null default now()
);

comment on table public.product_description_drafts is
  'Latest drafted product description per barcode, with official source provenance and Shopify apply status';
comment on column public.product_description_drafts.status is
  'drafted | applied | unchanged | skipped_not_draft | skipped_barcode_mismatch | skipped_manual_edit | skipped_empty | failed';
comment on column public.product_description_drafts.description_hash is
  'sha256 prefix of descriptionHtml as saved by Shopify; also stored in product metafield custom.description_hash';

create index if not exists product_description_drafts_tbc_idx
  on public.product_description_drafts (drafted_at)
  where features_status = 'tbc';

create index if not exists product_description_drafts_shopify_product_idx
  on public.product_description_drafts (shopify_product_id)
  where shopify_product_id is not null;

-- Service role only (bypasses RLS); no anon/authenticated policies.
alter table public.product_description_drafts enable row level security;

commit;
