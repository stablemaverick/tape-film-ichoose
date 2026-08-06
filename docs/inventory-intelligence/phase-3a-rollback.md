# Phase 3a/3b rollback notes

Migration: `supabase/migrations/20260806120000_inventory_intelligence_foundation.sql`

## Scope

Additive tables:

- `suppliers`
- `release_variants`
- `release_shopify_listings`
- `variant_identifiers`
- `tape_inventory_levels`
- `supplier_offers`
- `supplier_offer_observations`
- `supplier_sku_resolutions`
- `purchase_orders`
- `purchase_order_lines`
- `inventory_events`

Does **not** alter `catalog_items` writers’ required behaviour. Dual-write hooks are no-ops when flags are off.

## Disable dual-writes first

```bash
# Ensure unset or 0
INVENTORY_DUAL_WRITE_ENABLED=0
INVENTORY_DUAL_WRITE_SHOPIFY=0
INVENTORY_DUAL_WRITE_SUPPLIER=0
INVENTORY_DUAL_WRITE_PO=0
```

## Drop foundation tables (safe when unused / after disabling flags)

```sql
begin;

drop table if exists public.inventory_events cascade;
drop table if exists public.purchase_order_lines cascade;
drop table if exists public.purchase_orders cascade;
drop table if exists public.supplier_sku_resolutions cascade;
drop table if exists public.supplier_offer_observations cascade;
drop table if exists public.supplier_offers cascade;
drop table if exists public.tape_inventory_levels cascade;
drop table if exists public.variant_identifiers cascade;
drop table if exists public.release_shopify_listings cascade;
drop table if exists public.release_variants cascade;
drop table if exists public.suppliers cascade;

commit;
```

Operational pipelines continue using `catalog_items` / `shopify_listings` regardless.
