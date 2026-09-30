# Production retry plan — Option A (bulk catalog + post-catalog projection)

**Do not enable production supplier dual-write until temp validation is READY TO RETEST.**

## Prerequisites

1. Temp validation report: `scripts/inventory_intelligence_test/option_a_perf_report.json` shows `READY TO RETEST`.
2. All production flags remain OFF until the controlled retest window:
   - `INVENTORY_DUAL_WRITE_ENABLED=0`
   - `INVENTORY_DUAL_WRITE_SUPPLIER=0`
   - `INVENTORY_DUAL_WRITE_SHOPIFY=0`
   - `INVENTORY_DUAL_WRITE_PO=0`

## Deploy order

1. **Apply RPC migration on production** (SQL editor / psql), file:
   `supabase/migrations/20260807120000_bulk_update_catalog_stock_fields.sql`
2. **Deploy application code** to the VM (catalog bulk path, post-catalog projection, history OSError harden).
3. Confirm `CATALOG_STOCK_UPDATE_MODE` unset or `bulk` (default).
4. Confirm history file ownership: `logs/pipeline_run_history.json` writable by the pipeline user.
5. Keep all four dual-write flags **0** for a smoke stock cycle (operational-only) if desired.
6. Only then run the controlled supplier dual-write retest (supplier flag ON, Shopify/PO OFF).

## Rollback

- Set `CATALOG_STOCK_UPDATE_MODE=per_row` to restore one-PATCH-per-row updates.
- Set all `INVENTORY_DUAL_WRITE_*=0`.
- Do **not** delete intelligence rows without separate approval.

## Controlled retest stop conditions

Stop and restore flags to 0 if:

- stock wall clock exceeds ~10 minutes materially;
- Lasgo skipped/timeout;
- identity same-release rate regresses materially;
- duplicate releases/offers/resolutions appear;
- unchanged replay creates observations/events;
- Shopify/TAPE/PO tables gain rows;
- dual-write failure rate is material;
- permission/RLS errors.

## Checks

- Pre/post counts: `release_variants`, `supplier_offers`, observations, resolutions, events, Shopify/TAPE/PO tables.
- Paginated identity report with `--allow-production-ref zdvjokkslhpoftimvdis`.
- Idempotency second cycle after first PASS.
- Separate log lines: `OPERATIONAL_STOCK_SYNC_STATUS` vs `INVENTORY_INTELLIGENCE_PROJECTION_STATUS`.
