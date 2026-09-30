# Supplier dual-write — cross-supplier identity validation

**Date:** 2026-08-07  
**Temp project:** `vwbuwgfzksrzfmbqhqtn`  
**Production flags:** all `INVENTORY_DUAL_WRITE_*=0` (unchanged)  
**Recommendation:** **READY TO RETEST**

## 1) Root cause of the previous ~1,000-row report

PostgREST/Supabase enforces a **server `max-rows` of 1,000**.  
The prior perf report used `.limit(100000)` / a single `.execute()` page, which cannot exceed that cap. The Moovies-first synthetic feed meant the first page was almost entirely Moovies → `offers_by_supplier ≈ {moovies: 1000}` and `shared_barcode_same_release = 0` was a **reporting artefact**, not proof of resolver failure.

## 2) Corrected reporting

- New: `scripts/inventory_intelligence_test/run_cross_supplier_identity_report.py`
- Uses `.range(start, end)` pagination until a short page
- Coverage checked against `count=exact`
- Perf validation identity section updated to use the same helper

## 3–7) Full identity results (clean temp re-run after resolver fix)

| Metric | Value |
|---|---|
| Coverage `supplier_offers` | 24864 / 24864 complete |
| Coverage `supplier_sku_resolutions` | 24864 / 24864 complete |
| Coverage `release_variants` | 23745 / 23745 complete |
| Moovies offers | 17404 |
| Lasgo offers | 7460 |
| Shared barcodes | 1119 |
| Same `release_variant_id` | **1119** |
| Different `release_variant_id` | **0** |
| Unresolved shared | 0 |
| Attr-conflict excluded | 0 |
| **same_release_rate** | **1.0** |
| Suspected duplicates | **0** |
| Suspected false merges | **0** |
| `needs_review` | 0 |
| Resolutions | `created_supplier_only=23745`, `barcode_exact=1119` |

## 8) Resolver changes

Format-aware barcode matching added:

- Filter barcode candidates by format family (4K / Blu-ray / DVD)
- Compatible single hit → `barcode_exact`
- Multiple compatible → `barcode_ambiguous` / `needs_review`
- Incompatible barcode hits → keep separate (`created_supplier_only` with conflict note)
- Within-batch pending creates keyed by `(barcode, format_family)`

Files: `app/services/supplier_resolution_service.py`, `app/services/supplier_offer_dual_write_service.py`

## 9) Tests

`tests/services/test_inventory_dual_write_behaviour.py` now covers:

- Moovies→Lasgo same barcode → one release
- Lasgo→Moovies same barcode → one release
- Same barcode + conflicting format → two releases
- Baseline events / idempotency / batching (existing)

**20 passed**

## Perf re-check (clean temp)

- Full feed: **391.5s** (~6.5 min) for 24864 offers — under 10 min
- First-insert events: **0** (baseline fix held)
- Idempotent replay: **green**

## 10) Production retry plan (flags OFF by default)

1. Keep production `INVENTORY_DUAL_WRITE_*=0` until an explicit enablement window.
2. Deploy code (batched dual-write + Lasgo batch handoff + format-aware resolve).
3. Controlled enable: `ENABLED=1` + `SUPPLIER=1` only; Shopify/PO remain 0.
4. Existing prod partial rows (~428 releases / 427 offers) are upserted by `(supplier_id, supplier_sku)`; no delete required.
5. Historical baseline `supplier_price_changed` events from the aborted enablement remain as test artefacts until a separately reviewed cleanup.
6. Run one stock cycle; verify identity report + dual-write verify; then idempotent second cycle.
7. STOP/disable if separation, mass-unavailable, or material dual-write failure occurs.

**Do not enable Shopify/PO dual-write, backfill, or consumer switch in this step.**
