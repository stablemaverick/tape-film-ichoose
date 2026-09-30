# Staging Lasgo raw — production historical cleanup plan (dry-run first)

**Status:** Plan only. **Do not execute against production without explicit approval.**  
**Date prepared:** 2026-08-07  
**Table:** `public.staging_lasgo_raw` (~14.5M rows)  
**Part 1 (write-path retention) is separate** and does not delete these historical rows by itself until a registry of completed batches accumulates; this plan removes pre-registry history.

## Goals

- Retain the **newest two successfully completed** Lasgo `import_batch_id` values.
- Delete all older raw batches by `import_batch_id` (index-friendly).
- Leave untouched: `catalog_items`, `staging_supplier_offers`, inventory-intelligence tables (`supplier_offers`, `supplier_offer_observations`, `inventory_events`, resolutions/variants, etc.), Shopify/PO dual-write data.

## Preconditions

1. Part 1 code deployed (catalog batch handoff + retention registry).
2. Index present (migration `20260807160000_staging_lasgo_raw_retention_indexes.sql`):
   - `staging_lasgo_raw_batch_idx` on `(import_batch_id)`
   - `staging_lasgo_raw_imported_at_idx` on `(imported_at DESC)`
3. Maintenance window with pipelines paused / lock held so no concurrent Lasgo INSERT races the delete list.
4. Confirm production inventory flags unchanged (supplier ON; Shopify/PO OFF).

## Step 0 — Identify keep set (dry-run)

Prefer **importer/log-authoritative** IDs over a blind table scan.

### 0a. From recent successful pipeline logs (preferred)

```bash
# On the production VM
cd /opt/tape-film-ichoose
grep -h "Lasgo raw import complete" logs/stock_sync_*.log logs/catalog_sync_*.log \
  | sed -n 's/.*Batch: \([^ ]*\).*Total imported: \([0-9]*\).*/\1 \2/p' \
  | tail -20
```

Pick the **two newest successful** batch IDs (highest `imported_at` / latest log timestamps) with non-zero imported counts. Example placeholders:

```text
KEEP_A=<newest_batch_uuid>
KEEP_B=<second_newest_batch_uuid>
```

### 0b. Optional SQL verify (after `imported_at` index exists)

```sql
-- Sample recent rows via index-friendly ordering (not a full distinct scan)
WITH recent AS (
  SELECT import_batch_id, imported_at
  FROM public.staging_lasgo_raw
  ORDER BY imported_at DESC
  LIMIT 25000
)
SELECT import_batch_id,
       count(*) AS rows_in_sample,
       max(imported_at) AS max_imported_at
FROM recent
GROUP BY import_batch_id
ORDER BY max_imported_at DESC
LIMIT 5;
```

Cross-check that the top two match `KEEP_A` / `KEEP_B`.

## Step 1 — Validation queries (before)

```sql
-- Keep-batch row counts (should be ~8.5k–9.2k each)
SELECT import_batch_id, count(*) AS n
FROM public.staging_lasgo_raw
WHERE import_batch_id IN ('KEEP_A', 'KEEP_B')
GROUP BY import_batch_id;

-- Estimate delete volume (may be slow; prefer pg_class estimate if count times out)
SELECT reltuples::bigint AS approx_rows
FROM pg_class
WHERE oid = 'public.staging_lasgo_raw'::regclass;

-- Untouched baselines
SELECT count(*) FROM public.catalog_items;
SELECT count(*) FROM public.staging_supplier_offers;
-- Inventory intelligence (names as deployed):
-- SELECT count(*) FROM public.supplier_offers;
-- SELECT count(*) FROM public.supplier_offer_observations;
-- SELECT count(*) FROM public.inventory_events;
```

Record: `keep_a_n`, `keep_b_n`, `approx_total`, `catalog_n`, `offers_n`, II counts.

**Estimated deletes:** `approx_total - keep_a_n - keep_b_n` (expect ~14.5M − ~18k ≈ **14.48M**).

## Step 2 — Build obsolete batch ID list

Do **not** run a single unscoped `DELETE` of 14.5M rows.

### Recommended: list distinct batch IDs via SQL, exclude keep set

```sql
-- May take time; run with statement_timeout raised in a maintenance session.
SELECT import_batch_id, count(*) AS n, max(imported_at) AS max_ts
FROM public.staging_lasgo_raw
GROUP BY import_batch_id
ORDER BY max_ts DESC;
```

Export IDs where `import_batch_id NOT IN (KEEP_A, KEEP_B)` to a file `obsolete_lasgo_batches.txt`.

Alternative if `GROUP BY` is too heavy: derive obsolete IDs from retained pipeline logs (`524+` known batches) union any extras found by sampling, then verify each with:

```sql
SELECT count(*) FROM public.staging_lasgo_raw WHERE import_batch_id = '<uuid>';
```

## Step 3 — Delete by import_batch_id (chunked)

```sql
-- Template: one obsolete batch at a time (uses import_batch_id index)
BEGIN;
DELETE FROM public.staging_lasgo_raw
WHERE import_batch_id = '<obsolete_uuid>';
-- Expect ~8k–10k rows; COMMIT per batch or every N batches
COMMIT;
```

Or scripted:

```bash
# Pseudocode — run only after approval
while read -r BID; do
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM public.staging_lasgo_raw WHERE import_batch_id = '$BID';"
done < obsolete_lasgo_batches.txt
```

**Guards:**

- Never delete `KEEP_A` / `KEEP_B`.
- Abort if a delete returns 0 for a batch that Step 2 counted as non-zero (unexpected).
- Do not touch other schemas/tables in the same script.

## Step 4 — Validation queries (after)

```sql
SELECT import_batch_id, count(*) AS n
FROM public.staging_lasgo_raw
GROUP BY import_batch_id
ORDER BY max(imported_at) DESC;

-- Expect exactly 2 batch IDs (KEEP_A, KEEP_B) and ~18–20k rows total
SELECT count(*) FROM public.staging_lasgo_raw;

-- Confirm commercial / II baselines unchanged
SELECT count(*) FROM public.catalog_items;              -- == catalog_n
SELECT count(*) FROM public.staging_supplier_offers;  -- == offers_n
-- II counts unchanged
```

Update local registry `.state/lasgo_raw_completed_batches.json` on the VM to contain only `KEEP_A` and `KEEP_B` so Part 1 retention stays consistent.

## Step 5 — Maintenance after large deletes

**Yes — run afterward:**

```sql
VACUUM (ANALYZE) public.staging_lasgo_raw;
```

On Supabase, prefer the SQL editor / maintenance window; `VACUUM FULL` is **not** required unless disk reclaim is urgent (takes an exclusive lock).

## Approach comparison: DELETE-by-batch vs table-copy swap

| Approach | Pros | Cons |
|----------|------|------|
| **DELETE by `import_batch_id` (recommended)** | Index-friendly; no rename cutover; easy to pause/resume; keep set verified first; lower operational risk | Many round-trips (~1.5k batches); table bloat until VACUUM; longer wall clock |
| **Copy keep rows → new table → rename swap** | Faster reclaim; clean file; single cutover | Needs exclusive rename window; risk if pipelines write mid-copy; more moving parts; still must verify keep set |

**Recommendation:** Prefer **DELETE by `import_batch_id`** at this scale for safety and pause/resume, then `VACUUM (ANALYZE)`. Use copy+swap only if VACUUM/disk pressure forces a rebuild and a pipeline freeze is scheduled.

## Explicit non-goals

- No changes to `catalog_items` / offers / II tables.
- No Shopify or PO dual-write enablement.
- No conversion of Lasgo raw to per-EAN UPSERT in this cleanup.
- No execution of this plan in the Part 1 engineering task.
