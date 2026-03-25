-- Link catalog_health_snapshots to the pipeline_runs row produced in the same persist batch.

alter table public.catalog_health_snapshots
  add column if not exists pipeline_run_id uuid references public.pipeline_runs (id) on delete set null;

create index if not exists catalog_health_snapshots_pipeline_run_id_idx
  on public.catalog_health_snapshots (pipeline_run_id);

comment on column public.catalog_health_snapshots.pipeline_run_id is
  'FK to pipeline_runs.id for the sync run that produced this health snapshot';
