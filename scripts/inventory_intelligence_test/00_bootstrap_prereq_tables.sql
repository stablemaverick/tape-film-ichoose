-- Bootstrap minimal prerequisite tables for inventory-intelligence temporary validation.
-- Safe for empty temporary Supabase projects / local Supabase.
-- Does NOT clone production. Synthetic validation only.
--
-- Apply BEFORE:
--   supabase/migrations/20260806120000_inventory_intelligence_foundation.sql
--
-- These stubs satisfy foreign keys used by the foundation migration.
-- They are intentionally minimal and are not a catalogue schema clone.

begin;

create extension if not exists pgcrypto;

create table if not exists public.films (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  created_at timestamptz default now()
);

create table if not exists public.catalog_items (
  id uuid primary key default gen_random_uuid(),
  title text not null default 'synthetic',
  barcode text null,
  supplier text null,
  active boolean default true,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- Subset of columns required by foundation FKs / observability compatibility.
create table if not exists public.pipeline_runs (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  pipeline_type text not null default 'inventory_test',
  completed boolean not null default false,
  failures integer not null default 0,
  health_exit_code integer not null default 0,
  lock_encountered boolean not null default false
);

comment on table public.films is 'Temporary-test stub for inventory intelligence FK only';
comment on table public.catalog_items is 'Temporary-test stub for inventory intelligence FK only';
comment on table public.pipeline_runs is 'Temporary-test stub for inventory intelligence FK only';

commit;
