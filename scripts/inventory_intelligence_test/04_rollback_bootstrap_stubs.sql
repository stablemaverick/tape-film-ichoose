-- Optional: drop bootstrap stubs on a disposable temporary project only.
-- NEVER run against production.

begin;

drop table if exists public.pipeline_runs cascade;
drop table if exists public.catalog_items cascade;
drop table if exists public.films cascade;

commit;
