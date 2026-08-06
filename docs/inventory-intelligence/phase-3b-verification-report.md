# Phase 3b verification & reconciliation report

**Date:** 2026-08-06  
**Scope:** Dual-write implementation with `release_variants` identity model.  
**Consumer impact:** None (flags default OFF; agent/admin/storefront unchanged).

## 1. Dual-write verification

| Check | Result |
|-------|--------|
| Flags default OFF | Pass — `tests/services/test_inventory_dual_write.py` |
| Master flag required | Pass — stream flags ignored when master off |
| Supplier dual-write does not touch `tape_inventory_levels` | Pass — behaviour test |
| Shopify dual-write no-op when flag off | Pass |
| Normalize / store sync / PO report still succeed with flags off | Pass — hooks wrap dual-write in try/except; early return when disabled |
| Existing store sync + normalize unit tests | Pass (73 related tests green) |
| Idempotent events/observations | Pass — dedupe_key + skip when fingerprint unchanged |
| Text “In stock” never invents qty | Pass — availability rules tests |
| Stale feed mass-unavailable blocked | Pass — invariant rules |
| PO received ≤ ordered without adjustment | Pass — invariant rules |

### How to re-verify against a database

```bash
# After applying supabase/migrations/20260806120000_inventory_intelligence_foundation.sql
venv/bin/python scripts/observability/inventory_dual_write_verify.py --env-file .env.prod
```

With flags still off, expect foundation tables present (after migrate) and dual-write counts ≈ 0 until flags are enabled in a controlled environment.

## 2. Reconciliation report (rules + design)

| Invariant | Enforcement |
|-----------|-------------|
| TAPE vs supplier separation | Supplier dual-write never calls tape/Shopify qty APIs; behaviour test asserts no `tape_inventory_levels` table touch |
| `available` vs `on_hand` | Warning-only validation (Shopify negative available is valid) |
| Incoming not auto-summed | Separate columns; `validate_incoming_not_auto_summed`; reconcile helper `prefer_po_when_both` |
| Preorder ≠ positive on_hand from supplier | `validate_preorder_no_positive_tape_on_hand` error for supplier sources |
| One active resolution per supplier SKU | Unique active index + resolution uniqueness validator |
| Barcode → multiple releases | `conflict_flag` + needs_review |
| Unchanged feed | Observation/event dedupe_key |
| Failed/stale feed | `validate_stale_feed_no_mass_unavailable` blocks dual-write batch |

## 3. Assumptions

1. Foundation migration is applied before enabling any dual-write flag.  
2. When Lasgo/Moovies lack `supplier_sku`, identity uses `barcode:{ean}` as `supplier_sku`.  
3. Creating `supplier_only` releases on unmatched offers is desired (`INVENTORY_CREATE_SUPPLIER_ONLY_RELEASES=1` default).  
4. Shopify detailed levels fetch is capped (`INVENTORY_DUAL_WRITE_LEVEL_FETCH_MAX`, default 500) to protect store-sync runtime; remaining rows fall back to `inventory_quantity` as available.  
5. PO dual-write updates `po_incoming_confirmed` only when a `tape_inventory_levels` row already exists (stocked releases). Supplier-only releases keep PO lines without tape rows.  
6. `supplier_sku_resolutions` partial unique index may require select+update fallback via PostgREST (implemented).  
7. Dual-write failures must not fail operational pipelines (warnings only).

## 4. Edge cases discovered

| Edge case | Handling |
|-----------|----------|
| Negative Shopify `available` with CONTINUE/preorder | Stored as-is; warning if committed > on_hand |
| Same barcode, multiple releases | Resolution `needs_review`; identifier `conflict_flag` |
| Offer without SKU or barcode | Skipped (`skipped_identity`) |
| Tape Film staging rows | Excluded from `supplier_offers` dual-write |
| Both PO and Shopify incoming > 0 | Facts kept separate; derived sum uses reconciliation strategy |
| Store sync dry-run | Dual-write skipped |
| Ambiguous barcode match on supplier import | Unresolved offer + review queue; no silent attach |
| Preorder Shopify product with positive on_hand | Warning only (Shopify-owned fact) |

## 5. Files touched (Phase 3b)

**Schema:** `supabase/migrations/20260806120000_inventory_intelligence_foundation.sql` (rewritten for `release_variants`)

**Config / rules:** `app/config/inventory_dual_write.py`, availability + invariant rules, unified contract

**Services:**  
`supplier_resolution_service.py`, `supplier_offer_dual_write_service.py`,  
`shopify_release_dual_write_service.py`, `purchase_order_dual_write_service.py`,  
`inventory_events_service.py`

**Hooks:** `normalize_offers_service.py`, `shopify_store_sync_service.py`, `supplier_orders_report_service.py`

**Tests / verify:** `tests/services/test_inventory_dual_write*.py`, `scripts/observability/inventory_dual_write_verify.py`

## 6. Not started (per instructions)

- Phase 4 backfills  
- Consumer switch to unified availability  
- Enabling dual-write flags in production
