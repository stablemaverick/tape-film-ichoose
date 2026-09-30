# Production Schema-Only Deployment Worksheet

**Change type:** Schema-only deployment (no dual-write enablement)  
**Migration file:** `supabase/migrations/20260806120000_inventory_intelligence_foundation.sql`  
**Scope:** Additive inventory-intelligence schema objects only  
**Out of scope:** Backfill, dual-write enablement, consumer switch (agent/admin/storefront), Phase 4

> **Hard rule:** Do not execute this worksheet against non-approved environments.  
> **Hard rule:** Do not include secrets in this document or evidence fields.

---

## 1) Deployment metadata

Fill before and after execution.

| Field | Value |
|---|---|
| Deployment date (UTC) | |
| Deployment start timestamp (UTC) | |
| Deployment completion timestamp (UTC) | |
| Operator | |
| Reviewer | |
| Production Supabase project reference | |
| Approved application commit SHA | |
| Current deployed application version | |
| Migration filename | `supabase/migrations/20260806120000_inventory_intelligence_foundation.sql` |
| Backup / recovery-point reference | |
| Pre-deployment migration state reference | |
| Post-deployment migration state reference | |
| Final outcome (GO / HOLD / ROLLBACK) | |

---

## 2) Pre-deployment stop/go checks

Mark each item. If any required item is not confirmed, **STOP**.

- [ ] Approved commit is deployed or ready for deployment.
- [ ] Temporary-project validation passed (controlled + focused Shopify validation approved).
- [ ] Current production backup / recovery point exists and reference recorded.
- [ ] `INVENTORY_DUAL_WRITE_ENABLED=0` in production runtime.
- [ ] `INVENTORY_DUAL_WRITE_SHOPIFY=0` in production runtime.
- [ ] `INVENTORY_DUAL_WRITE_SUPPLIER=0` in production runtime.
- [ ] `INVENTORY_DUAL_WRITE_PO=0` in production runtime.
- [ ] No Phase 4 backfill will run in this change window.
- [ ] No agent/admin/storefront consumer switch will occur.
- [ ] No Shopify or supplier mutation job will be manually invoked.
- [ ] Rollback artifacts are available (`docs/inventory-intelligence/phase-3a-rollback.md` and rollback SQL scripts).
- [ ] Migration reviewed: additive-only, no destructive statements.
- [ ] Existing lint/typecheck failures are documented as pre-existing (not introduced by this release).

**STOP condition:** If any checkbox is unchecked, deployment is **HOLD**.

---

## 3) Environment confirmation (safe, no secrets)

### 3.1 Confirm target project ref

Record approved production project ref: `____________________`

If using Supabase CLI:

```bash
supabase projects list
supabase status
```

Evidence:
- Active target ref: `____________________`
- Matches approved production ref: [ ] Yes [ ] No (**STOP if No**)

### 3.2 Confirm temporary project is not targeted

Known temporary validation project ref: `vwbuwgfzksrzfmbqhqtn`  
Known current production project ref from ops docs: `zdvjokkslhpoftimvdis` (update if changed)

Evidence:
- Target ref is not temp ref: [ ] Yes [ ] No (**STOP if No**)

### 3.3 Confirm migration tool points to intended environment

Use your deployment platform’s environment view / runtime config and confirm:
- DB URL host corresponds to approved production project.
- No local `.env.inventory-test` values are used.

Evidence notes: `__________________________________________________________`

---

## 4) Pre-deployment schema capture (read-only SQL)

Run each query in Supabase SQL Editor (production) and save outputs with timestamp.

### 4.1 Existing public tables

```sql
select n.nspname as schema_name, c.relname as table_name
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public' and c.relkind = 'r'
order by c.relname;
```

### 4.2 Existing enums

```sql
select n.nspname as schema_name, t.typname as enum_name, e.enumlabel as enum_value, e.enumsortorder
from pg_type t
join pg_enum e on e.enumtypid = t.oid
join pg_namespace n on n.oid = t.typnamespace
where n.nspname = 'public'
order by t.typname, e.enumsortorder;
```

### 4.3 Existing indexes

```sql
select schemaname, tablename, indexname, indexdef
from pg_indexes
where schemaname = 'public'
order by tablename, indexname;
```

### 4.4 Existing constraints

```sql
select n.nspname as schema_name,
       c.relname as table_name,
       con.conname as constraint_name,
       con.contype as constraint_type,
       pg_get_constraintdef(con.oid) as definition
from pg_constraint con
join pg_class c on c.oid = con.conrelid
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public'
order by c.relname, con.conname;
```

### 4.5 Existing triggers

```sql
select n.nspname as schema_name,
       c.relname as table_name,
       t.tgname as trigger_name,
       pg_get_triggerdef(t.oid) as trigger_def
from pg_trigger t
join pg_class c on c.oid = t.tgrelid
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public'
  and not t.tgisinternal
order by c.relname, t.tgname;
```

### 4.6 Trigger functions referenced by triggers

```sql
select distinct p.oid::regprocedure as function_signature,
       n.nspname as schema_name,
       p.proname as function_name
from pg_trigger t
join pg_proc p on p.oid = t.tgfoid
join pg_namespace n on n.oid = p.pronamespace
where not t.tgisinternal
order by function_signature::text;
```

### 4.7 Migration history (where available)

```sql
select *
from supabase_migrations.schema_migrations
order by version;
```

If that table is unavailable, record your platform migration history source and snapshot reference.

### 4.8 Baseline operational row counts

```sql
select
  (select count(*) from public.catalog_items) as catalog_items_count,
  (select count(*) from public.films) as films_count,
  (select count(*) from public.shopify_listings) as shopify_listings_count,
  (select count(*) from public.pipeline_runs) as pipeline_runs_count,
  (select count(*) from public.staging_supplier_offers) as staging_supplier_offers_count,
  (select count(*) from public.supplier_orders) as supplier_orders_count;
```

Evidence file/link: `_______________________________________________`

---

## 5) Collision check (read-only, hard-stop gate)

Confirm none of these objects already exist unexpectedly **before** migration.

### 5.1 Table existence

```sql
with expected(name) as (
  values
    ('suppliers'),
    ('release_variants'),
    ('release_shopify_listings'),
    ('variant_identifiers'),
    ('tape_inventory_levels'),
    ('supplier_offers'),
    ('supplier_offer_observations'),
    ('supplier_sku_resolutions'),
    ('purchase_orders'),
    ('purchase_order_lines'),
    ('inventory_events')
)
select e.name as object_name,
       exists (
         select 1
         from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
         where n.nspname = 'public'
           and c.relkind = 'r'
           and c.relname = e.name
       ) as exists_pre_migration
from expected e
order by e.name;
```

### 5.2 Migration-defined index names

```sql
with expected(name) as (
  values
    ('release_variants_primary_barcode_idx'),
    ('release_variants_film_id_idx'),
    ('release_variants_catalog_item_id_idx'),
    ('release_variants_publication_status_idx'),
    ('release_shopify_listings_shop_variant_uidx'),
    ('release_shopify_listings_release_idx'),
    ('release_shopify_listings_inventory_item_idx'),
    ('variant_identifiers_release_type_value_uidx'),
    ('variant_identifiers_type_value_idx'),
    ('variant_identifiers_conflict_idx'),
    ('tape_inventory_levels_release_location_uidx'),
    ('tape_inventory_levels_location_idx'),
    ('tape_inventory_levels_last_synced_idx'),
    ('supplier_offers_supplier_sku_uidx'),
    ('supplier_offers_release_variant_id_idx'),
    ('supplier_offers_supplier_id_idx'),
    ('supplier_offers_raw_barcode_idx'),
    ('supplier_offers_last_seen_idx'),
    ('supplier_offers_status_idx'),
    ('supplier_offer_observations_dedupe_uidx'),
    ('supplier_offer_observations_offer_observed_idx'),
    ('supplier_offer_observations_pipeline_run_idx'),
    ('supplier_sku_resolutions_active_sku_uidx'),
    ('supplier_sku_resolutions_review_status_idx'),
    ('supplier_sku_resolutions_release_idx'),
    ('supplier_sku_resolutions_barcode_idx'),
    ('purchase_orders_supplier_number_uidx'),
    ('purchase_orders_status_idx'),
    ('purchase_order_lines_po_id_idx'),
    ('purchase_order_lines_release_variant_id_idx'),
    ('purchase_order_lines_barcode_idx'),
    ('purchase_order_lines_supplier_sku_idx'),
    ('inventory_events_dedupe_uidx'),
    ('inventory_events_release_observed_idx'),
    ('inventory_events_type_observed_idx'),
    ('inventory_events_observation_idx'),
    ('inventory_events_supplier_offer_idx'),
    ('inventory_events_pipeline_run_idx')
)
select e.name as index_name,
       exists (
         select 1
         from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
         where n.nspname = 'public'
           and c.relkind = 'i'
           and c.relname = e.name
       ) as exists_pre_migration
from expected e
order by e.name;
```

### 5.3 Migration-defined constraint names

```sql
with expected(name) as (
  values
    ('suppliers_priority_nonneg'),
    ('release_variants_publication_status_check'),
    ('variant_identifiers_type_check'),
    ('variant_identifiers_value_nonempty'),
    ('supplier_offers_availability_status_check'),
    ('supplier_offers_reported_quantity_nonneg'),
    ('supplier_offers_confidence_range'),
    ('supplier_offers_confidence_version_pair'),
    ('supplier_offers_sku_nonempty'),
    ('supplier_offer_observations_status_check'),
    ('supplier_sku_resolutions_review_status_check'),
    ('supplier_sku_resolutions_confidence_range'),
    ('supplier_sku_resolutions_sku_nonempty'),
    ('purchase_orders_status_check'),
    ('purchase_order_lines_qty_nonneg'),
    ('purchase_order_lines_received_vs_ordered'),
    ('inventory_events_type_check')
)
select e.name as constraint_name,
       exists (
         select 1
         from pg_constraint con
         where con.conname = e.name
       ) as exists_pre_migration
from expected e
order by e.name;
```

### 5.4 Migration-defined sequences (expected: none explicit)

```sql
select sequence_schema, sequence_name
from information_schema.sequences
where sequence_schema = 'public'
  and sequence_name like 'release_%'
order by sequence_name;
```

**STOP condition:** Any unexpected pre-existing object that collides with migration intent requires review before proceed.

---

## 6) Dual-write flag verification (runtime)

### 6.1 Automated safe check (prints only targeted keys)

Run on the production runtime host/container (adjust command runner to your platform):

```bash
python - <<'PY'
import os
keys = [
  "INVENTORY_DUAL_WRITE_ENABLED",
  "INVENTORY_DUAL_WRITE_SHOPIFY",
  "INVENTORY_DUAL_WRITE_SUPPLIER",
  "INVENTORY_DUAL_WRITE_PO",
]
for k in keys:
    print(f"{k}={os.getenv(k, '<unset>')}")
PY
```

Expected:
- each key equals `0` or is unset with application default OFF behavior documented.

### 6.2 Manual fallback (if runtime shell access unavailable)

Record evidence from deployment platform env UI:
- screenshot/reference id for each of the four flags showing OFF.

Evidence:
- `INVENTORY_DUAL_WRITE_ENABLED`: __________
- `INVENTORY_DUAL_WRITE_SHOPIFY`: __________
- `INVENTORY_DUAL_WRITE_SUPPLIER`: __________
- `INVENTORY_DUAL_WRITE_PO`: __________

**STOP condition:** If any flag is ON, stop deployment.

---

## 7) Migration command (apply only foundation migration)

> **Do not run all pending unrelated migrations.**  
> **Do not run seed/backfill scripts.**  
> **Do not invoke dual-write services.**  
> **Do not enable flags.**

### Approved procedure (single-file apply)

Use one of the following **single migration only** procedures:

1) Supabase SQL Editor: paste the contents of  
`supabase/migrations/20260806120000_inventory_intelligence_foundation.sql` and run once.

2) If using SQL client:

```bash
psql "$PRODUCTION_DATABASE_URL" \
  -v ON_ERROR_STOP=1 \
  -f supabase/migrations/20260806120000_inventory_intelligence_foundation.sql
```

Record:
- Command/procedure used: __________________________
- Start time: __________________________
- End time: __________________________
- Exit status: __________________________
- Output log reference: __________________________

**STOP condition:** Non-zero exit or partial apply requires hold + reviewer decision.

---

## 8) Immediate post-deployment verification (read-only SQL)

### 8.1 Confirm expected tables exist

```sql
select c.relname as table_name
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public'
  and c.relkind = 'r'
  and c.relname in (
    'suppliers',
    'release_variants',
    'release_shopify_listings',
    'variant_identifiers',
    'tape_inventory_levels',
    'supplier_offers',
    'supplier_offer_observations',
    'supplier_sku_resolutions',
    'purchase_orders',
    'purchase_order_lines',
    'inventory_events'
  )
order by c.relname;
```

### 8.2 Verify columns and types

```sql
select table_name, column_name, data_type, is_nullable
from information_schema.columns
where table_schema = 'public'
  and table_name in (
    'suppliers','release_variants','release_shopify_listings','variant_identifiers',
    'tape_inventory_levels','supplier_offers','supplier_offer_observations',
    'supplier_sku_resolutions','purchase_orders','purchase_order_lines','inventory_events'
  )
order by table_name, ordinal_position;
```

### 8.3 PK / unique / FK / CHECK constraints

```sql
select c.relname as table_name,
       con.conname as constraint_name,
       con.contype as constraint_type,
       pg_get_constraintdef(con.oid) as definition
from pg_constraint con
join pg_class c on c.oid = con.conrelid
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public'
  and c.relname in (
    'suppliers','release_variants','release_shopify_listings','variant_identifiers',
    'tape_inventory_levels','supplier_offers','supplier_offer_observations',
    'supplier_sku_resolutions','purchase_orders','purchase_order_lines','inventory_events'
  )
order by c.relname, con.conname;
```

### 8.4 Indexes

```sql
select tablename, indexname, indexdef
from pg_indexes
where schemaname = 'public'
  and tablename in (
    'release_variants','release_shopify_listings','variant_identifiers',
    'tape_inventory_levels','supplier_offers','supplier_offer_observations',
    'supplier_sku_resolutions','purchase_orders','purchase_order_lines','inventory_events'
  )
order by tablename, indexname;
```

### 8.5 Enum values (expected: none added by this migration)

```sql
select t.typname as enum_name, e.enumlabel, e.enumsortorder
from pg_type t
join pg_enum e on e.enumtypid = t.oid
join pg_namespace n on n.oid = t.typnamespace
where n.nspname = 'public'
order by t.typname, e.enumsortorder;
```

### 8.6 No unexpected triggers on target/operational tables

```sql
select c.relname as table_name, t.tgname as trigger_name, pg_get_triggerdef(t.oid) as trigger_def
from pg_trigger t
join pg_class c on c.oid = t.tgrelid
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public'
  and not t.tgisinternal
  and c.relname in (
    'catalog_items','shopify_listings','staging_supplier_offers',
    'suppliers','release_variants','release_shopify_listings','variant_identifiers',
    'tape_inventory_levels','supplier_offers','supplier_offer_observations',
    'supplier_sku_resolutions','purchase_orders','purchase_order_lines','inventory_events'
  )
order by c.relname, t.tgname;
```

---

## 9) Post-deployment row validation (read-only SQL)

### 9.1 New table row expectations

Expected immediately after schema-only deploy:
- `public.suppliers`: only intended seed rows (`moovies`, `lasgo`, `tape_film`)
- all other new inventory-intelligence tables: `0` rows

```sql
select 'suppliers' as table_name, count(*) as row_count from public.suppliers
union all select 'release_variants', count(*) from public.release_variants
union all select 'release_shopify_listings', count(*) from public.release_shopify_listings
union all select 'variant_identifiers', count(*) from public.variant_identifiers
union all select 'tape_inventory_levels', count(*) from public.tape_inventory_levels
union all select 'supplier_offers', count(*) from public.supplier_offers
union all select 'supplier_offer_observations', count(*) from public.supplier_offer_observations
union all select 'supplier_sku_resolutions', count(*) from public.supplier_sku_resolutions
union all select 'purchase_orders', count(*) from public.purchase_orders
union all select 'purchase_order_lines', count(*) from public.purchase_order_lines
union all select 'inventory_events', count(*) from public.inventory_events;
```

### 9.2 Suppliers seed exactness

```sql
select id, display_name, priority, active
from public.suppliers
order by id;
```

Expected rows:
- `lasgo`, `Lasgo`, `2`, `true`
- `moovies`, `Moovies`, `1`, `true`
- `tape_film`, `Tape Film`, `0`, `true`

### 9.3 Operational table count comparison (allow documented concurrent activity)

```sql
select
  (select count(*) from public.catalog_items) as catalog_items_count,
  (select count(*) from public.films) as films_count,
  (select count(*) from public.shopify_listings) as shopify_listings_count,
  (select count(*) from public.pipeline_runs) as pipeline_runs_count,
  (select count(*) from public.staging_supplier_offers) as staging_supplier_offers_count,
  (select count(*) from public.supplier_orders) as supplier_orders_count;
```

Record deltas vs pre-capture and annotate legitimate concurrent activity:
- `____________________________________________________________`

**STOP condition:** Unexpected non-seed rows in new tables (except `suppliers` seeds) require investigation.

---

## 10) Ownership and safety checks

Run/inspect the following to confirm safety characteristics.

### 10.1 No new triggers on operational tables

Use query from section 8.6 and confirm no new trigger attached to:
- `public.catalog_items`
- `public.shopify_listings`
- `public.staging_supplier_offers`

### 10.2 Migration has no Shopify mutation logic

Confirm migration file contains only DDL + supplier seed insert, no functions/triggers calling external APIs.

### 10.3 Data-model properties (schema inspection)

```sql
select table_name, column_name
from information_schema.columns
where table_schema='public'
  and table_name='tape_inventory_levels'
order by ordinal_position;
```

Confirm:
- retains `available` as signed integer (negative values allowed)
- has separate `po_incoming_confirmed` and `shopify_incoming_reported`
- no supplier quantity column exists in `tape_inventory_levels`

```sql
select conname, pg_get_constraintdef(oid)
from pg_constraint
where conname in (
  'supplier_offers_sku_nonempty',
  'supplier_offers_availability_status_check',
  'tape_inventory_levels_release_location_uidx'
);
```

Confirm:
- `release_shopify_listings` allows 0..N channels per release (no mandatory one-to-one constraint)
- `supplier_offers.release_variant_id` is nullable (unresolved linkage allowed)
- canonical key is `release_variants.id`, not barcode

---

## 11) Operational smoke tests (non-mutating / standard safe mode)

Use your existing production-safe checks only. Do not force dual-write or backfill.

### 11.1 Application health

- command/procedure: `________________________________________`
- expected: app healthy, no new errors attributable to schema migration
- evidence: `________________________________________`

### 11.2 Supplier pipeline health

- command/procedure: existing standard health dashboard/report
- expected: normal status, no dual-write activity while flags OFF
- evidence: `________________________________________`

### 11.3 Shopify sync health (non-mutating / standard mode)

- command/procedure: existing standard store/inventory health checks
- expected: unchanged behavior
- evidence: `________________________________________`

### 11.4 Agent regression smoke

- command/procedure: approved lightweight regression command
- expected: unchanged output behavior
- evidence: `________________________________________`

### 11.5 Storefront availability comparison

- procedure: existing non-invasive comparison/dashboard
- expected: unchanged
- evidence: `________________________________________`

---

## 12) Log review (no dual-write activity expected while flags OFF)

Search terms:
- `inventory dual write`
- `supplier dual-write`
- `shopify dual-write`
- `po dual-write`
- `release_variant`
- `tape_inventory_levels`
- `supplier_offers`
- `failed migration`
- `relation does not exist`
- `permission denied`

Evidence fields:
- Log source(s): `________________________________________`
- Time window: `________________________________________`
- Findings summary: `________________________________________`

Expected while flags OFF:
- No active dual-write service logs performing writes.
- No new error spikes from schema introduction.

---

## 13) Rollback decision

Rollback/HOLD is required if any of the following occur:
- migration failure or partial apply
- unexpected alteration of existing operational tables
- unexpected trigger/function behavior
- application/pipeline regression
- Shopify inventory change attributable to this deployment
- new inventory-intelligence tables populated unexpectedly (beyond `suppliers` seeds)
- permission/RLS issues affecting existing application behavior

### Approved rollback procedure reference

- Primary doc: `docs/inventory-intelligence/phase-3a-rollback.md`
- SQL artifacts:
  - `scripts/inventory_intelligence_test/03_rollback_inventory_foundation.sql`
  - (temp/disposable only) `scripts/inventory_intelligence_test/04_rollback_bootstrap_stubs.sql`

> ⚠️ **Destructive action warning:** Execute rollback SQL only with explicit reviewer approval and change ticket authorization.

Reviewer rollback approval:
- [ ] Approved
- Approver: `____________________`
- Timestamp: `____________________`

---

## 14) Final sign-off

- [ ] Schema verification passed
- [ ] `suppliers` seeds verified exactly
- [ ] All other new inventory-intelligence tables empty
- [ ] Existing operations healthy
- [ ] All four dual-write flags remain OFF
- [ ] No backfill run
- [ ] No consumer switch performed
- [ ] No Shopify inventory mutation observed
- [ ] Reviewer approval complete

Final decision:
- [ ] GO
- [ ] HOLD
- [ ] ROLLBACK

Sign-off:
- Operator: `____________________`
- Reviewer: `____________________`
- Date/time (UTC): `____________________`

---

## Manual substitutions / assumptions

Environment-specific values that must be substituted at runtime:
- Production Supabase project ref
- Production migration execution mechanism (SQL editor vs `psql` vs platform runner)
- Production runtime env inspection command path
- Operational smoke-test command names per hosting platform
- Backup/recovery reference ID

