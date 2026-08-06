# Temporary environment setup — inventory intelligence validation

**Purpose:** Validate Phase 3a schema and Phase 3b dual-write *rules/data model* without a production database clone and **without applying anything to production**.

**Hard rules for this kit**

- Do **not** point these scripts at production Supabase.
- Do **not** enable `INVENTORY_DUAL_WRITE_*` flags as part of this validation kit.
- Do **not** begin Phase 4 backfill.
- Do **not** switch agent / admin / storefront consumers.
- Seed data is **synthetic only** (titles, barcodes, SKUs, Shopify GIDs are fake).

This machine’s workspace currently has **no Docker** and **no Supabase CLI** installed. Local Supabase commands below are provided for environments where Docker Desktop + CLI are available.

---

## 1. Preferred: temporary Supabase cloud project

### 1.1 Create the project

1. Open [https://supabase.com/dashboard](https://supabase.com/dashboard).
2. **New project** → name e.g. `tape-inventory-intel-temp`.
3. Choose a region; set a strong DB password; wait until the project is healthy.
4. Project Settings → API:
   - copy **Project URL** → `SUPABASE_URL`
   - copy **service_role** key → `SUPABASE_SERVICE_KEY`
5. Project Settings → Database → Connection string (URI) (optional, for `psql` / `seed_synthetic.py`):
   - copy URI → `DATABASE_URL` / `SUPABASE_DB_URL`

### 1.2 Local env file (never commit)

Create `.env.inventory-test` in the repo root (gitignored if you add it; do not commit secrets):

```bash
SUPABASE_URL=https://YOUR_TEMP_PROJECT.supabase.co
SUPABASE_SERVICE_KEY=YOUR_TEMP_SERVICE_ROLE_KEY
# Optional for psycopg2 seed loader:
DATABASE_URL=postgresql://postgres.YOUR_REF:YOUR_PASSWORD@aws-0-REGION.pooler.supabase.com:6543/postgres

# Keep dual-write OFF
INVENTORY_DUAL_WRITE_ENABLED=0
INVENTORY_DUAL_WRITE_SHOPIFY=0
INVENTORY_DUAL_WRITE_SUPPLIER=0
INVENTORY_DUAL_WRITE_PO=0

# Optional marker so tools refuse “prod” profiles
INVENTORY_TEST_PROFILE=temporary
SHOPIFY_SHOP=synthetic-test.myshopify.com
```

Confirm this URL is **not** the production project (`zdvjokkslhpoftimvdis` in current prod config).

### 1.3 Exact migration application commands

**Option A — Supabase SQL editor (simplest)**

In the temp project SQL editor, run in order:

1. `scripts/inventory_intelligence_test/00_bootstrap_prereq_tables.sql`
2. `supabase/migrations/20260806120000_inventory_intelligence_foundation.sql`
3. `scripts/inventory_intelligence_test/02_seed_synthetic.sql`

**Option B — Supabase CLI linked to the temp project**

```bash
# Install CLI once: https://supabase.com/docs/guides/cli
npx supabase login
npx supabase link --project-ref YOUR_TEMP_PROJECT_REF

# Bootstrap stubs (not in numbered migrations — apply explicitly)
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/00_bootstrap_prereq_tables.sql

# Apply repo migrations (includes inventory foundation and earlier additive migrations).
# On an empty temp project, earlier migrations that assume live catalogue DDL may fail.
# Prefer Option A for inventory-only validation, OR apply only the foundation file:
psql "$DATABASE_URL" -f supabase/migrations/20260806120000_inventory_intelligence_foundation.sql

# Seed
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/02_seed_synthetic.sql
```

**Option C — Python seed helper (requires `psycopg2-binary` + `DATABASE_URL`)**

```bash
venv/bin/pip install psycopg2-binary
DATABASE_URL='postgresql://…' \
  venv/bin/python scripts/inventory_intelligence_test/seed_synthetic.py \
  --env-file .env.inventory-test \
  --bootstrap \
  --apply-foundation
```

### 1.4 Run verification

```bash
venv/bin/python scripts/inventory_intelligence_test/verify_invariants.py \
  --env-file .env.inventory-test \
  --format text
```

Exit code `0` = all scenarios passed. Use `--format json` for CI artefacts.

Also safe to run the read-only dual-write presence checker (flags must remain OFF):

```bash
venv/bin/python scripts/observability/inventory_dual_write_verify.py \
  --env-file .env.inventory-test \
  --format text
```

### 1.5 Rollback and reapplication

```bash
# Remove inventory foundation (+ seed data)
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/03_rollback_inventory_foundation.sql

# Optional: remove bootstrap stubs (disposable temp project ONLY)
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/04_rollback_bootstrap_stubs.sql

# Reapply
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/00_bootstrap_prereq_tables.sql
psql "$DATABASE_URL" -f supabase/migrations/20260806120000_inventory_intelligence_foundation.sql
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/02_seed_synthetic.sql
venv/bin/python scripts/inventory_intelligence_test/verify_invariants.py --env-file .env.inventory-test
```

Or delete the entire temporary Supabase project from the dashboard.

---

## 2. Alternative: local Supabase with Docker

### Feasibility note

Local Supabase requires:

- Docker Desktop (or compatible engine) running
- Supabase CLI (`npm i -g supabase` or `npx supabase`)

**On this workspace host (2026-08-06): Docker was not installed (`docker not found`), so local Supabase could not be started here.** Commands below are for a machine that has both.

### Commands

```bash
# From repo root
npx supabase init   # only if supabase/config.toml missing — do not overwrite carelessly

# Start local stack (Postgres on 54322 by default)
npx supabase start

# Capture local API URL + service role from CLI output, then:
export SUPABASE_URL=http://127.0.0.1:54321
export SUPABASE_SERVICE_KEY=…   # from `npx supabase status`
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:54322/postgres
export INVENTORY_DUAL_WRITE_ENABLED=0
export INVENTORY_DUAL_WRITE_SHOPIFY=0
export INVENTORY_DUAL_WRITE_SUPPLIER=0
export INVENTORY_DUAL_WRITE_PO=0

# Apply bootstrap + foundation + seed
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/00_bootstrap_prereq_tables.sql
psql "$DATABASE_URL" -f supabase/migrations/20260806120000_inventory_intelligence_foundation.sql
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/02_seed_synthetic.sql

# Verify
venv/bin/python scripts/inventory_intelligence_test/verify_invariants.py --format text

# Full unit/agent suite (does not need the temp DB)
venv/bin/python -m pytest tests/rules/test_availability_rules.py \
  tests/rules/test_inventory_invariant_rules.py \
  tests/services/test_inventory_dual_write.py \
  tests/services/test_inventory_dual_write_behaviour.py -q
npm run test:agent

# Rollback / reapply
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/03_rollback_inventory_foundation.sql
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/00_bootstrap_prereq_tables.sql
psql "$DATABASE_URL" -f supabase/migrations/20260806120000_inventory_intelligence_foundation.sql
psql "$DATABASE_URL" -f scripts/inventory_intelligence_test/02_seed_synthetic.sql

# Stop local stack
npx supabase stop
```

---

## 3. Expected results (by scenario)

| # | Scenario | Expected |
|---|----------|----------|
| 1 | Shopify-linked positive on-hand | Release `…aaa1`; tape `on_hand=5`, `available=5`, `committed=0`; Shopify channel exists |
| 2 | Shopify-linked committed | Release `…aaa2`; tape `on_hand=3`, `committed=1`, `available=2` |
| 3 | Oversold / preorder negative available | Release `…aaa3`; tape `on_hand=0`, `committed=2`, `available=-2` stored as-is; validator warns `committed_exceeds_on_hand`, no identity mismatch |
| 4 | Both Moovies and Lasgo | Release `…aaa4` has offers `SYN-MOOV-DELTA` + `SYN-LASGO-DELTA` |
| 5 | Supplier-only, no Shopify | Release `…aaa5`, `publication_status=supplier_only`; **0** `release_shopify_listings`; **0** `tape_inventory_levels`; offer `SYN-MOOV-EPS` present |
| 6 | Missing barcode | Offer `SYN-MOOV-NOBARCODE` with `raw_barcode` null; identity via `supplier_sku` |
| 7 | Two offers share barcode | Two offers with `raw_barcode=9900000000006` |
| 8 | Ambiguous identity | Resolution `SYN-MOOV-AMBIG` → `needs_review` / `barcode_ambiguous`; releases `…aaa6`+`…aaa7` share barcode `9900000000099` with `conflict_flag=true` |
| 9 | Unresolved supplier SKU | Offer + resolution `SYN-MOOV-UNRESOLVED`; `release_variant_id` null; `needs_review` |
| 10 | Exact numeric qty | Moovies Delta offer `reported_quantity=14`, `quantity_is_exact=true` |
| 11 | Non-numeric status | `SYN-LASGO-TEXTSTATUS`: `raw_status_text='In stock'`, `reported_quantity` null, `quantity_is_exact=false` (never invent 999) |
| 12 | Open PO | `SYN-PO-OPEN-001` status `confirmed`; line ordered=10, confirmed=10, received=0 |
| 13 | Partially received PO | `SYN-PO-PARTIAL-002` status `partially_received`; ordered=6, received=2 |
| 14 | Unchanged observation dedupe | One observation for dedupe key; second insert attempt fails unique constraint; count remains 1 |

Cross-cutting expected:

- Dual-write flags remain OFF.
- Supplier→tape mutation rule still fires in pure tests.
- Inventory event for Delta offer retains `observation_id`.

---

## 4. File map

| File | Role |
|------|------|
| `scripts/inventory_intelligence_test/00_bootstrap_prereq_tables.sql` | Minimal `films` / `catalog_items` / `pipeline_runs` stubs |
| `supabase/migrations/20260806120000_inventory_intelligence_foundation.sql` | Phase 3a/3b foundation |
| `scripts/inventory_intelligence_test/02_seed_synthetic.sql` | Deterministic synthetic dataset |
| `scripts/inventory_intelligence_test/03_rollback_inventory_foundation.sql` | Drop inventory tables |
| `scripts/inventory_intelligence_test/04_rollback_bootstrap_stubs.sql` | Drop stubs (temp only) |
| `scripts/inventory_intelligence_test/seed_synthetic.py` | Optional SQL applier via `DATABASE_URL` |
| `scripts/inventory_intelligence_test/verify_invariants.py` | Scenario + invariant verifier |
| `docs/inventory-intelligence/production-deployment-checklist.md` | Later production steps |

---

## 5. What this kit does *not* do

- Does not exercise live Moovies/Lasgo FTP or production Shopify GraphQL.
- Does not enable dual-write pipeline hooks end-to-end (that needs a non-prod DB **and** explicit flag enablement in a later controlled cycle).
- Does not replace unit tests — still run pytest for rules/dual-write behaviour.
