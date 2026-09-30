# Option A temp validation report

**Generated:** 2026-08-07T01:29:29Z  
**Temp project:** `vwbuwgfzksrzfmbqhqtn`  
**Recommendation: READY TO RETEST**

Production dual-write flags were **not** enabled during this work.

## Root cause (recap)

Catalog stock upsert was one HTTP PATCH per changed row (~220ms each).  
4,685 updates ≈ 1,037s. Dual-write added ~6 min inline before catalog.

## Before / after catalog write profile

| Case | Mode | Rows | Elapsed | ms/row |
|------|------|------|---------|--------|
| Quiet | bulk RPC | 400 | 0.77s | 1.91 |
| Large | bulk RPC | 4,800 | **3.52s** | **0.73** |
| Sample | per_row PATCH | 50 | 11.3s | **226** |
| Extrapolated old path | per_row × 4,685 | — | **~1,059s** | 226 |

Bulk path: 10 RPC batches × ~500, avg batch latency **352ms**, 0 retries.  
No longer scales at ~220ms × N.

## Supplier projection (temp, flags ON in-process only)

| Check | Result |
|-------|--------|
| Flags OFF → skipped | pass |
| First write 200 offers | upserted=200, obs=200, events=0, errors=0, 4.0s |
| Unchanged replay | obs=0, events=0, offers_inserted=0 |
| Shopify/TAPE/PO tables | all **0** |

## Failure isolation

| Case | Result |
|------|--------|
| Malformed UUID in bulk payload | attempted=1, updated=0, no collateral |
| Projection exception after catalog | status=`failed`, operational path unaffected |
| Local history PermissionError | unit-tested: exit 0 when DB persist OK |

## Architecture delivered

1. `bulk_update_catalog_stock_fields` RPC (id join, stock fields only, service_role)
2. Stock upsert default `CATALOG_STOCK_UPDATE_MODE=bulk` (`per_row` rollback)
3. Supplier intelligence **after** catalog upsert (not in normalize)
4. Separate `OPERATIONAL_*_STATUS` / `INVENTORY_INTELLIGENCE_PROJECTION_STATUS`
5. History OSError non-fatal; VM history file ownership fixed

## Production flags

Remain **OFF** (`ENABLED/SUPPLIER/SHOPIFY/PO=0`). No Shopify/PO dual-write, no backfill, no consumer switch.

## Next: production retest

See [`docs/inventory-intelligence/option-a-production-retry-plan.md`](option-a-production-retry-plan.md).

1. Apply RPC migration on production  
2. Deploy app  
3. Smoke stock with flags OFF (optional)  
4. Controlled supplier-only dual-write retest  
5. Rollback: `CATALOG_STOCK_UPDATE_MODE=per_row` + all flags 0  

Raw JSON: `scripts/inventory_intelligence_test/option_a_perf_report.json`
