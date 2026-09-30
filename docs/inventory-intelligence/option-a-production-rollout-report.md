# Option A production rollout report

**Date:** 2026-08-07  
**Project:** `zdvjokkslhpoftimvdis`  
**Recommendation: KEEP SUPPLIER DUAL-WRITE ON**

Shopify/PO dual-write were never enabled. No backfill. No consumer switches.

---

## 1. RPC production deployment

Applied `bulk_update_catalog_stock_fields` on production.

- Empty payload → `{attempted:0, updated:0}`
- Malformed UUID → `{attempted:1, updated:0}` (no collateral)
- Service-role execute only; stock commercial fields only

## 2. Phase 2 — flags OFF bulk-path validation

| Metric | Result |
|--------|--------|
| Flags | all 0 |
| Start/end | 01:36:25 → 01:40:06 (~3.7 min) |
| Write mode | **bulk** |
| Catalog updates | 0 (quiet) |
| Catalog step duration | 47s |
| Intelligence deltas | **0** (all tables unchanged) |
| Shopify/TAPE/PO | **0** |
| Cron 03:00 also | bulk, 7 rows in **291ms** |

Note: first projection exit was `ModuleNotFoundError` (03b missing `sys.path`); fixed before Phase 3. Operational catalog path was unaffected.

## 3. Phase 3 — supplier-enabled stock

Flags: `ENABLED=1`, `SUPPLIER=1`, `SHOPIFY=0`, `PO=0`

| Metric | Value |
|--------|-------|
| Start/end | 04:23:42 → 04:32:01 |
| **Total runtime** | **499s (~8.3 min)** |
| Moovies / Lasgo staging | 13,546 / 8,555 |
| Catalog bulk | 2 rows, **361ms**, 1 RPC batch |
| Catalog step duration | 56s |
| Projection | **success**, offers=22,101 |
| Projection timing | **251s** total (preload ~213s) |
| Projection DB requests | 761 |
| Dual-write errors | **0** / failed_batches **0** |
| Pipeline run | `dec10adb-cc8a-47ae-8ae2-99b86b182a5f` |

Sequence confirmed: normalize → bulk catalog → operational success → post-catalog projection.

## 4–5. Catalog vs projection timing

| Stage | Time |
|-------|------|
| Catalog bulk (2 changed) | 0.36s |
| Full catalog step4 | ~56s (read+lookup dominate) |
| Supplier projection | ~251s |
| Full stock cycle | **~8.3 min** |

Vs prior HOLD: 4,685 per-row PATCHes ≈ 1,037s catalog alone.

## 6. Identity metrics (production, paginated)

| Metric | Value |
|--------|-------|
| Offers / resolutions | 22,104 / 22,104 complete |
| Moovies / Lasgo | 13,549 / 8,555 |
| Shared barcodes | 7,583 |
| Same-release rate | **1.0** |
| Suspected duplicates | **0** |
| Suspected false merges | **0** |
| needs_review | **0** |

## 7. Idempotency second cycle

| Metric | Value |
|--------|-------|
| Start/end | 04:52:58 → 05:01:36 |
| Duration | **518s (~8.6 min)** |
| Projection obs inserted | **0** |
| Projection events inserted | **0** |
| Offers inserted | **0** |
| Table deltas (variants/offers/obs/resolutions/events) | **all 0** |

## 8. Separation / safety

| Table | Count |
|-------|-------|
| `release_shopify_listings` | 0 |
| `tape_inventory_levels` | 0 |
| `purchase_orders` | 0 |
| `purchase_order_lines` | 0 |

No Shopify inventory mutations. History JSON append succeeded (PermissionError fixed).

## 9. Errors / warnings

- Fixed mid-rollout: `03b` `ModuleNotFoundError` (sys.path) and `load_dotenv(override=True)` so file flags win over stale shell exports.
- `health_exit_code=2` on pipeline_runs is existing catalog-health thresholds (film link ~79%, null barcodes) — not dual-write.

## 10. Final flag state

```
INVENTORY_DUAL_WRITE_ENABLED=1
INVENTORY_DUAL_WRITE_SUPPLIER=1
INVENTORY_DUAL_WRITE_SHOPIFY=0
INVENTORY_DUAL_WRITE_PO=0
```

## 11. Recommendation

**KEEP SUPPLIER DUAL-WRITE ON**

Do not enable Shopify or PO dual-write without a separate controlled plan.
