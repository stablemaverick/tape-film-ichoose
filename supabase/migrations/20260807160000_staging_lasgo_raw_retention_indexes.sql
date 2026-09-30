-- Index to support imported_at-ordered probes and Part-2 cleanup discovery.
-- Not used by catalog batch handoff (importer stdout is authoritative).
-- Safe / concurrent where supported; IF NOT EXISTS for re-runs.

create index if not exists staging_lasgo_raw_imported_at_idx
  on public.staging_lasgo_raw (imported_at desc);

-- Existing batch filter used by normalize + retention deletes:
--   staging_lasgo_raw_batch_idx on (import_batch_id)
-- Confirm present in production; create if missing.
create index if not exists staging_lasgo_raw_batch_idx
  on public.staging_lasgo_raw (import_batch_id);
