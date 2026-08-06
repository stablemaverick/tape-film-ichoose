# Phase 3b dual-write plan (implemented)

Status: **implemented behind feature flags (default OFF)**.  
Canonical entity: **`release_variants`**.

See also:
- [architecture-and-data-flow.md](./architecture-and-data-flow.md)
- [phase-3b-verification-report.md](./phase-3b-verification-report.md)

## Streams

| Flag | Hook | Writes |
|------|------|--------|
| `INVENTORY_DUAL_WRITE_SHOPIFY` | `shopify_store_sync_service` | `release_variants`, `release_shopify_listings`, identifiers, `tape_inventory_levels`, `tape_stock_synced` events |
| `INVENTORY_DUAL_WRITE_SUPPLIER` | `normalize_offers_service` (Moovies/Lasgo) | resolution, `supplier_offers`, observations, supplier events; may create `supplier_only` releases |
| `INVENTORY_DUAL_WRITE_PO` | `supplier_orders_report_service` | `purchase_orders` / lines; `po_incoming_confirmed` on existing tape levels |

Master switch `INVENTORY_DUAL_WRITE_ENABLED` must be `1`.

## Matching (supplier)

1. Resolve to existing `release_variant` (prior resolution, then barcode).  
2. High confidence → attach.  
3. Uncertain / multi-match → `needs_review`.  
4. None → create `publication_status=supplier_only` (if create flag on).

## Safeguards retained

- Additive; no consumer changes  
- Supplier qty never writes Shopify / tape on_hand  
- Preorder never creates positive TAPE on_hand from supplier paths  
- Idempotent upserts + observation/event dedupe  
- Stale feed mass-unavailable blocked  
- Dual-write errors warn; do not fail operational jobs  

## Enablement (non-prod first)

1. Apply foundation migration.  
2. Run `scripts/observability/inventory_dual_write_verify.py`.  
3. Enable flags in a staging env only.  
4. Run store sync + stock normalize + PO report.  
5. Re-run verify; review `needs_review` resolutions.  
6. Do **not** switch agent consumers or start Phase 4 backfill until reviewed.
