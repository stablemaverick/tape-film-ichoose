-- Pipeline observability for dashboard (Render / Supabase).
-- RLS: enable as needed; service role bypasses RLS for worker + app server.

create table if not exists public.pipeline_runs (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  pipeline_type text,
  log_file text,
  started_at timestamptz,
  ended_at timestamptz,
  duration_seconds double precision,
  completed boolean,
  inserts integer,
  updates integer,
  failures integer,
  lock_encountered boolean,
  health_exit_code integer,
  tmdb_matched_pct double precision,
  film_linked_pct double precision,
  catalog_rows_active integer,
  missing_sale_price_pct double precision,
  null_barcode_rows integer,
  duplicate_films integer,
  recorded_at timestamptz
);

create index if not exists pipeline_runs_created_at_idx
  on public.pipeline_runs (created_at desc);

create table if not exists public.catalog_health_snapshots (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  generated_at timestamptz,
  exit_code integer,
  metrics jsonb not null default '{}'::jsonb,
  alerts jsonb not null default '[]'::jsonb
);

create index if not exists catalog_health_snapshots_created_at_idx
  on public.catalog_health_snapshots (created_at desc);

comment on table public.pipeline_runs is 'Append-only pipeline sync runs (catalog/stock) for ops dashboard';
comment on table public.catalog_health_snapshots is 'Point-in-time catalog health metrics + alerts from gather_metrics()';
