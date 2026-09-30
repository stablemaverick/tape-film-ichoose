-- Additive: Australian classification resolved alongside drafted product descriptions.
alter table public.product_description_drafts
  add column if not exists au_classification text,
  add column if not exists au_classification_source text,
  add column if not exists au_classification_status text;

comment on column public.product_description_drafts.au_classification is
  'Australian classification (G, PG, M, MA 15+, R 18+) written to custom.classification_description';
comment on column public.product_description_drafts.au_classification_source is
  'tmdb | classification_board_search';
comment on column public.product_description_drafts.au_classification_status is
  'applied | unchanged | skipped_existing | not_found | failed | found (dry run)';
